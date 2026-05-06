use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use log::info;
use tokio::sync::RwLock;

use crate::conf::{BackendConfig, Config};
use crate::core::{MurrError, TableSchema};
use crate::io::directory::mmap::directory::MMapDirectory;
use crate::io::directory::mem::directory::MemDirectory;
use crate::io::directory::Directory;
use crate::io::table::{Table, TableOps};

pub struct MurrService {
    tables: RwLock<HashMap<String, Box<dyn TableOps>>>,
    config: Config,
}

impl MurrService {
    pub async fn new(config: Config) -> Result<Self, MurrError> {
        let mut tables: HashMap<String, Box<dyn TableOps>> = HashMap::new();

        match &config.storage.backend {
            BackendConfig::Mmap(cfg) => {
                for name in MMapDirectory::list_indexes(cfg) {
                    match Table::<MMapDirectory>::open(&name, cfg.clone()).await {
                        Ok(t) => {
                            info!("loaded table '{}'", name);
                            tables.insert(name, Box::new(t));
                        }
                        Err(e) => info!("skipping table '{}': {}", name, e),
                    }
                }
            }
            BackendConfig::Mem(_) => {}
            BackendConfig::IoUring(cfg) => {
                core::cfg_select! {
                    target_os = "linux" => {
                        use crate::io::directory::iouring::directory::IoUringDirectory;
                        for name in IoUringDirectory::list_indexes(cfg) {
                            match Table::<IoUringDirectory>::open(&name, cfg.clone()).await {
                                Ok(t) => {
                                    info!("loaded table '{}'", name);
                                    tables.insert(name, Box::new(t));
                                }
                                Err(e) => info!("skipping table '{}': {}", name, e),
                            }
                        }
                    }
                    _ => {
                        let _ = cfg;
                        return Err(crate::io::directory::iouring::unsupported_platform_error());
                    }
                }
            }
        }

        Ok(Self { tables: RwLock::new(tables), config })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn create(&self, table_name: &str, schema: TableSchema) -> Result<(), MurrError> {
        let mut tables = self.tables.write().await;

        if tables.contains_key(table_name) {
            return Err(MurrError::TableAlreadyExists(table_name.to_string()));
        }

        let table: Box<dyn TableOps> = match &self.config.storage.backend {
            BackendConfig::Mmap(cfg) => {
                Box::new(Table::<MMapDirectory>::create(table_name, schema, cfg.clone()).await?)
            }
            BackendConfig::Mem(cfg) => {
                Box::new(Table::<MemDirectory>::create(table_name, schema, cfg.clone()).await?)
            }
            BackendConfig::IoUring(cfg) => {
                core::cfg_select! {
                    target_os = "linux" => {
                        use crate::io::directory::iouring::directory::IoUringDirectory;
                        Box::new(
                            Table::<IoUringDirectory>::create(table_name, schema, cfg.clone()).await?,
                        )
                    }
                    _ => {
                        let _ = (cfg, schema);
                        return Err(crate::io::directory::iouring::unsupported_platform_error());
                    }
                }
            }
        };

        tables.insert(table_name.to_string(), table);
        Ok(())
    }

    pub async fn write(&self, table_name: &str, batch: &RecordBatch) -> Result<(), MurrError> {
        let mut tables = self.tables.write().await;

        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;

        table.write(batch).await
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

        table.read(keys, columns).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StorageConfig;
    use crate::core::{ColumnSchema, DType};
    use crate::io::directory::mmap::directory::MMapConfig;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> Config {
        Config {
            storage: StorageConfig {
                backend: BackendConfig::Mmap(MMapConfig::new(dir.path().to_path_buf())),
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

        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[cfg(target_os = "linux")]
    #[serial_test::file_serial]
    #[tokio::test]
    async fn test_iouring_backend_round_trip() {
        use crate::io::directory::iouring::IoUringConfig;

        let dir = TempDir::new().unwrap();
        // Same memlock-friendly settings as the iouring directory unit
        // tests: skip register_buffers, tiny pools.
        let config = Config {
            storage: StorageConfig {
                backend: BackendConfig::IoUring(IoUringConfig {
                    cache_dir: dir.path().to_path_buf(),
                    workers: 1,
                    ring_size: 8,
                    buffer_slots: 8,
                    register_buffers: false,
                    coalesce_slots: 4,
                    ..IoUringConfig::default()
                }),
            },
            ..Config::default()
        };
        let svc = MurrService::new(config).await.unwrap();

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
}
