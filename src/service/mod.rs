use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};

use arrow::record_batch::RecordBatch;
use log::{info, warn};
use tokio::sync::RwLock;

use crate::conf::{BackendConfig, Config};
use crate::core::{MurrError, TableSchema};
use crate::io4::store::Store;
use crate::io4::store::rocksdb::RocksDBStore;
use crate::io4::table::Table;

pub struct MurrService {
    tables: RwLock<HashMap<String, Table<RocksDBStore>>>,
    store: Arc<StdRwLock<RocksDBStore>>,
    config: Config,
}

impl MurrService {
    pub async fn new(config: Config) -> Result<Self, MurrError> {
        std::fs::create_dir_all(&config.storage.path).map_err(|e| {
            MurrError::IoError(format!(
                "creating storage path {}: {e}",
                config.storage.path.display()
            ))
        })?;

        let store = match &config.storage.backend {
            BackendConfig::Mmap(plain) => {
                RocksDBStore::open_plain(&config.storage.path, plain)?
            }
            BackendConfig::Block(block) => {
                RocksDBStore::open_block(&config.storage.path, block)?
            }
        };
        let store = Arc::new(StdRwLock::new(store));

        let snapshot: Vec<(String, TableSchema)> = {
            let s = store.read().expect("store lock poisoned");
            s.manifest()
                .tables
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        let mut tables: HashMap<String, Table<RocksDBStore>> = HashMap::new();
        for (name, schema) in snapshot {
            match Table::open(store.clone(), name.clone(), schema) {
                Ok(t) => {
                    info!("loaded table '{}'", name);
                    tables.insert(name, t);
                }
                Err(e) => warn!("skipping table '{}': {}", name, e),
            }
        }

        Ok(Self {
            tables: RwLock::new(tables),
            store,
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
        let table = Table::create(self.store.clone(), table_name, schema)?;
        tables.insert(table_name.to_string(), table);
        Ok(())
    }

    pub async fn write(&self, table_name: &str, batch: &RecordBatch) -> Result<(), MurrError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.write(batch)
    }

    pub async fn list_tables(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read().await;
        tables.iter().map(|(k, v)| (k.clone(), v.schema().clone())).collect()
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema, MurrError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        Ok(table.schema().clone())
    }

    pub async fn read(
        &self,
        table_name: &str,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<RecordBatch, MurrError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.read(keys, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StorageConfig;
    use crate::core::{ColumnSchema, DType};
    use crate::io4::store::rocksdb::plain::PlainConfig;
    use arrow::array::{Array, Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> Config {
        Config {
            storage: StorageConfig {
                path: dir.path().to_path_buf(),
                backend: BackendConfig::Mmap(PlainConfig::default()),
            },
            ..Config::default()
        }
    }

    fn test_schema() -> TableSchema {
        let mut columns = indexmap::IndexMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema { dtype: DType::Utf8, nullable: false },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema { dtype: DType::Float32, nullable: true },
        );
        TableSchema { key: "key".to_string(), columns }
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
    async fn test_read_empty_table_returns_nulls() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(test_config(&dir)).await.unwrap();

        svc.create("empty", test_schema()).await.unwrap();
        let result = svc.read("empty", &["a"], &["score"]).await.unwrap();
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(vals.is_null(0));
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

        {
            let svc = MurrService::new(test_config(&dir)).await.unwrap();
            svc.create("users", test_schema()).await.unwrap();
            let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
            svc.write("users", &batch).await.unwrap();
        }

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

        let result = svc.read("empty", &["a"], &["score"]).await.unwrap();
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(vals.is_null(0));
    }
}
