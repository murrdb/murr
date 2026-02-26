mod state;

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use log::info;
use tokio::sync::RwLock;

use crate::conf::Config;
use crate::core::{MurrError, TableSchema};
use crate::io::directory::{Directory, LocalDirectory};
use crate::io::table::{CachedTable, TableWriter};

use state::TableState;

pub struct MurrService {
    tables: RwLock<HashMap<String, TableState>>,
    data_dir: PathBuf,
    config: Config,
}

impl MurrService {
    pub async fn new(config: Config) -> Result<Self, MurrError> {
        let data_dir = config.storage.cache_dir.clone();
        let mut tables = HashMap::new();

        for dir in LocalDirectory::from_storage(&data_dir).await? {
            let index = match dir.index().await? {
                Some(index) => index,
                None => continue,
            };

            let table_name = dir
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| {
                    MurrError::IoError(format!(
                        "invalid directory name: {}",
                        dir.path().display()
                    ))
                })?
                .to_string();

            let schema = index.schema;
            let cached = if index.segments.is_empty() {
                None
            } else {
                Some(CachedTable::open(dir.path(), &schema, &index.segments)?)
            };

            info!(
                "loaded table '{}' with {} segments",
                table_name,
                index.segments.len()
            );

            tables.insert(
                table_name,
                TableState {
                    dir,
                    schema,
                    cached,
                },
            );
        }

        Ok(Self {
            tables: RwLock::new(tables),
            data_dir,
            config,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn create(&self, table_name: &str, schema: TableSchema) -> Result<(), MurrError> {
        let mut tables = self.tables.write().await;

        if tables.contains_key(table_name) {
            return Err(MurrError::TableAlreadyExists(table_name.to_string()));
        }

        let table_dir = self.data_dir.join(table_name);
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            MurrError::IoError(format!("creating directory {}: {}", table_dir.display(), e))
        })?;

        let mut dir = LocalDirectory::new(&table_dir);
        let writer = TableWriter::create(&schema, &mut dir).await?;
        drop(writer);

        tables.insert(
            table_name.to_string(),
            TableState {
                dir,
                schema,
                cached: None,
            },
        );

        Ok(())
    }

    pub async fn write(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), MurrError> {
        let mut tables = self.tables.write().await;

        let state = tables.get_mut(table_name).ok_or_else(|| {
            MurrError::TableNotFound(table_name.to_string())
        })?;

        let mut writer = TableWriter::open(&mut state.dir).await?;
        writer.add_segment(batch).await?;
        drop(writer);

        let index = state.dir.index().await?.ok_or_else(|| {
            MurrError::TableError(format!(
                "table '{}' index missing after write",
                table_name
            ))
        })?;

        let cached = CachedTable::open(state.dir.path(), &state.schema, &index.segments)?;
        state.cached = Some(cached);

        Ok(())
    }

    pub async fn list_tables(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read().await;
        tables
            .iter()
            .map(|(k, v)| (k.clone(), v.schema.clone()))
            .collect()
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema, MurrError> {
        let tables = self.tables.read().await;
        let state = tables.get(table_name).ok_or_else(|| {
            MurrError::TableNotFound(table_name.to_string())
        })?;
        Ok(state.schema.clone())
    }

    pub async fn read(
        &self,
        table_name: &str,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<RecordBatch, MurrError> {
        let tables = self.tables.read().await;

        let state = tables.get(table_name).ok_or_else(|| {
            MurrError::TableNotFound(table_name.to_string())
        })?;

        let cached = state.cached.as_ref().ok_or_else(|| {
            MurrError::TableError(format!("table '{}' has no data", table_name))
        })?;

        cached.get(keys, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StorageConfig;
    use crate::core::{ColumnSchema, DType};
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> Config {
        Config {
            storage: StorageConfig {
                cache_dir: dir.path().to_path_buf(),
            },
            ..Config::default()
        }
    }

    fn test_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            key: "key".to_string(),
            columns,
        }
    }

    fn test_batch(keys: &[&str], scores: &[f32]) -> RecordBatch {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let key_array: StringArray = keys.iter().map(|k| Some(*k)).collect();
        let score_array: Float32Array = scores.iter().map(|v| Some(*v)).collect();
        RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(key_array), Arc::new(score_array)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_create_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        svc.create("users", test_schema()).await.unwrap();

        let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
        svc.write("users", &batch).await.unwrap();

        let result = svc.read("users", &["c", "a"], &["score"]).await.unwrap();
        assert_eq!(result.num_rows(), 2);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 3.0);
        assert_eq!(vals.value(1), 1.0);
    }

    #[tokio::test]
    async fn test_create_duplicate_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        svc.create("t", test_schema()).await.unwrap();
        let err = svc.create("t", test_schema()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_nonexistent_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        let err = svc.read("nope", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_empty_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        svc.create("empty", test_schema()).await.unwrap();
        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_multiple_writes_accumulate() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        svc.create("t", test_schema()).await.unwrap();

        let batch1 = test_batch(&["a", "b"], &[1.0, 2.0]);
        svc.write("t", &batch1).await.unwrap();

        let batch2 = test_batch(&["c"], &[3.0]);
        svc.write("t", &batch2).await.unwrap();

        let result = svc.read("t", &["a", "b", "c"], &["score"]).await.unwrap();
        assert_eq!(result.num_rows(), 3);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 1.0);
        assert_eq!(vals.value(1), 2.0);
        assert_eq!(vals.value(2), 3.0);
    }

    #[tokio::test]
    async fn test_loads_existing_tables_on_startup() {
        let dir = TempDir::new().unwrap();

        // Create and populate a table, then drop the service
        {
            let svc = MurrService::new(test_config(&dir)).await.unwrap();
            svc.create("users", test_schema()).await.unwrap();
            let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
            svc.write("users", &batch).await.unwrap();
        }

        // New service should discover the existing table
        let svc = MurrService::new(test_config(&dir)).await.unwrap();
        let tables = svc.list_tables().await;
        assert!(tables.contains_key("users"));

        let result = svc.read("users", &["c", "a"], &["score"]).await.unwrap();
        assert_eq!(result.num_rows(), 2);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 3.0);
        assert_eq!(vals.value(1), 1.0);
    }

    #[tokio::test]
    async fn test_loads_empty_table_on_startup() {
        let dir = TempDir::new().unwrap();

        {
            let svc = MurrService::new(test_config(&dir)).await.unwrap();
            svc.create("empty", test_schema()).await.unwrap();
        }

        let svc = MurrService::new(test_config(&dir)).await.unwrap();
        let tables = svc.list_tables().await;
        assert!(tables.contains_key("empty"));

        // Table exists but has no data
        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }
}
