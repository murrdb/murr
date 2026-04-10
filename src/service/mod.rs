mod state;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use log::info;
use tokio::sync::RwLock;

use crate::conf::Config;
use crate::core::{MurrError, TableSchema};
use crate::io::directory::mmap::directory::MMapDirectory;
use crate::io::directory::Directory;
use crate::io::table::Table;
use crate::io::url::LocalUrl;

use state::TableState;

pub struct MurrService {
    tables: RwLock<HashMap<String, TableState>>,
    data_dir: PathBuf,
    url: LocalUrl,
    config: Config,
}

impl MurrService {
    pub async fn new(config: Config) -> Result<Self, MurrError> {
        let data_dir = config.storage.cache_dir.clone();
        let url = LocalUrl {
            path: data_dir.clone(),
        };
        let mut tables = HashMap::new();

        for index in MMapDirectory::list_indexes(&url) {
            let dir = match MMapDirectory::open(&url, &index, 4096, false) {
                Ok(dir) => dir,
                Err(e) => {
                    info!("skipping index '{}': {}", index, e);
                    continue;
                }
            };
            let schema = dir.schema().clone();
            let dir = Arc::new(dir);
            let table = Table::new(dir);

            let has_segments = !schema.columns.is_empty();
            let reader = if has_segments {
                match table.open_reader().await {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        info!("table '{}' has no readable data: {}", index, e);
                        None
                    }
                }
            } else {
                None
            };

            info!("loaded table '{}'", index);
            tables.insert(index, TableState { table, reader });
        }

        Ok(Self {
            tables: RwLock::new(tables),
            data_dir,
            url,
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

        let dir = MMapDirectory::create(&self.url, table_name, schema, 4096, false)?;
        let table = Table::new(Arc::new(dir));

        tables.insert(
            table_name.to_string(),
            TableState {
                table,
                reader: None,
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

        let writer = state.table.open_writer().await?;
        writer.write(batch).await?;

        let reader = match state.reader.take() {
            Some(existing) => existing.reopen().await?,
            None => state.table.open_reader().await?,
        };
        state.reader = Some(reader);

        Ok(())
    }

    pub async fn list_tables(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read().await;
        tables
            .iter()
            .map(|(k, v)| (k.clone(), v.table.schema().clone()))
            .collect()
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema, MurrError> {
        let tables = self.tables.read().await;
        let state = tables.get(table_name).ok_or_else(|| {
            MurrError::TableNotFound(table_name.to_string())
        })?;
        Ok(state.table.schema().clone())
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

        let reader = state.reader.as_ref().ok_or_else(|| {
            MurrError::TableError(format!("table '{}' has no data", table_name))
        })?;

        reader.read(keys, columns).await
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
