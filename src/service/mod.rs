mod state;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use log::info;
use tokio::sync::RwLock;

use crate::conf::Config;
use crate::core::{MurrError, TableSchema};
use crate::io::directory::{Directory, DirectoryReader};
use crate::io::table::reader::TableReader;
use crate::io::table::writer::TableWriter;

use state::TableState;

pub struct MurrService<D: Directory> {
    tables: RwLock<HashMap<String, TableState<D>>>,
    location: D::Location,
    config: Config,
}

impl<D: Directory> MurrService<D> {
    pub async fn new(config: Config, location: D::Location) -> Result<Self, MurrError> {
        let mut tables = HashMap::new();

        for index in D::list_indexes(&location) {
            let dir = match D::open(&location, &index, D::ConfigType::default()) {
                Ok(d) => Arc::new(d),
                Err(e) => {
                    info!("skipping index '{}': {}", index, e);
                    continue;
                }
            };
            let schema = dir.schema().clone();
            let reader = if !schema.columns.is_empty() {
                match open_reader_if_segments(schema.clone(), dir.clone()).await {
                    Ok(r) => r,
                    Err(e) => {
                        info!("table '{}' has no readable data: {}", index, e);
                        None
                    }
                }
            } else {
                None
            };
            info!("loaded table '{}'", index);
            tables.insert(index, TableState { dir, reader });
        }

        Ok(Self {
            tables: RwLock::new(tables),
            location,
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

        let dir = D::create(&self.location, table_name, schema, D::ConfigType::default())?;
        tables.insert(
            table_name.to_string(),
            TableState {
                dir: Arc::new(dir),
                reader: None,
            },
        );

        Ok(())
    }

    pub async fn write(&self, table_name: &str, batch: &RecordBatch) -> Result<(), MurrError> {
        let mut tables = self.tables.write().await;

        let state = tables
            .get_mut(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;

        let writer = TableWriter::open(state.dir.schema().clone(), state.dir.clone()).await?;
        writer.write(batch).await?;

        let reader = match state.reader.take() {
            Some(existing) => existing.reopen().await?,
            None => {
                let r = Arc::new(state.dir.open_reader().await?);
                TableReader::open(state.dir.schema().clone(), r).await?
            }
        };
        state.reader = Some(reader);

        Ok(())
    }

    pub async fn list_tables(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read().await;
        tables
            .iter()
            .map(|(k, v)| (k.clone(), v.dir.schema().clone()))
            .collect()
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema, MurrError> {
        let tables = self.tables.read().await;
        let state = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        Ok(state.dir.schema().clone())
    }

    pub async fn read(
        &self,
        table_name: &str,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<RecordBatch, MurrError> {
        let tables = self.tables.read().await;

        let state = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;

        let reader = state.reader.as_ref().ok_or_else(|| {
            MurrError::TableError(format!("table '{}' has no data", table_name))
        })?;

        reader.read(keys, columns).await
    }
}

async fn open_reader_if_segments<D: Directory>(
    schema: TableSchema,
    dir: Arc<D>,
) -> Result<Option<TableReader<D::ReaderType>>, MurrError> {
    let r = dir.open_reader().await?;
    if r.info().segments.is_empty() {
        return Ok(None);
    }
    let reader = TableReader::open(schema, Arc::new(r)).await?;
    Ok(Some(reader))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StorageConfig;
    use crate::core::{ColumnSchema, DType};
    use crate::io::directory::mmap::directory::MMapDirectory;
    use crate::io::url::LocalUrl;
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

    fn test_location(dir: &TempDir) -> LocalUrl {
        LocalUrl {
            path: dir.path().to_path_buf(),
        }
    }

    fn test_schema() -> TableSchema {
        let mut columns = indexmap::IndexMap::new();
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
        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();

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
        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();

        svc.create("t", test_schema()).await.unwrap();
        let err = svc.create("t", test_schema()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_nonexistent_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();

        let err = svc.read("nope", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_empty_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();

        svc.create("empty", test_schema()).await.unwrap();
        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_multiple_writes_accumulate() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();

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

        {
            let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
                .await
                .unwrap();
            svc.create("users", test_schema()).await.unwrap();
            let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
            svc.write("users", &batch).await.unwrap();
        }

        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();
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
            let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
                .await
                .unwrap();
            svc.create("empty", test_schema()).await.unwrap();
        }

        let svc = MurrService::<MMapDirectory>::new(test_config(&dir), test_location(&dir))
            .await
            .unwrap();
        let tables = svc.list_tables().await;
        assert!(tables.contains_key("empty"));

        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }
}
