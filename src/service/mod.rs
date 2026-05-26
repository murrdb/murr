use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use log::{info, warn};

use crate::conf::Config;
use crate::core::{MurrError, TableSchema};
use crate::io::store::Store;
use crate::io::table::Table;

pub struct MurrService<S: Store> {
    tables: RwLock<HashMap<String, Table<S>>>,
    store: Arc<RwLock<S>>,
    config: Config,
}

impl<S: Store> MurrService<S> {
    pub fn new(store: Arc<RwLock<S>>, config: Config) -> Result<Self, MurrError> {
        let snapshot: Vec<(String, TableSchema)> = {
            let s = store.read().unwrap_or_else(PoisonError::into_inner);
            s.manifest()
                .tables
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let total = snapshot.len();
        info!("Manifest has {} table(s)", total);

        let load_start = Instant::now();
        let mut tables: HashMap<String, Table<S>> = HashMap::new();
        for (name, schema) in snapshot {
            let column_count = schema.columns.len();
            match Table::open(store.clone(), name.clone(), schema) {
                Ok(t) => {
                    info!("loaded table '{}' ({} columns)", name, column_count);
                    tables.insert(name, t);
                }
                Err(e) => warn!("skipping table '{}': {}", name, e),
            }
        }
        info!(
            "Service ready: {}/{} tables loaded in {} ms",
            tables.len(),
            total,
            load_start.elapsed().as_millis()
        );

        Ok(Self {
            tables: RwLock::new(tables),
            store,
            config,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn create(&self, table_name: &str, schema: TableSchema) -> Result<(), MurrError> {
        let mut tables = self.tables.write().unwrap_or_else(PoisonError::into_inner);
        if tables.contains_key(table_name) {
            return Err(MurrError::TableAlreadyExists(table_name.to_string()));
        }
        let table = Table::create(self.store.clone(), table_name, schema)?;
        tables.insert(table_name.to_string(), table);
        Ok(())
    }

    pub fn write(&self, table_name: &str, batch: &RecordBatch) -> Result<(), MurrError> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.write(batch)
    }

    pub fn list_tables(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        tables
            .iter()
            .map(|(k, v)| (k.clone(), v.schema().clone()))
            .collect()
    }

    pub fn get_schema(&self, table_name: &str) -> Result<TableSchema, MurrError> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        Ok(table.schema().clone())
    }

    pub fn read(
        &self,
        table_name: &str,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<RecordBatch, MurrError> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.read(keys, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::{BackendConfig, StorageConfig};
    use crate::core::{ColumnSchema, DTypeName};
    use crate::io::store::rocksdb::RocksDBStore;
    use crate::io::store::rocksdb::plain::PlainConfig;
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

    fn build_service(config: Config) -> MurrService<RocksDBStore> {
        let store = Arc::new(RwLock::new(
            RocksDBStore::open_from_config(&config.storage).unwrap(),
        ));
        MurrService::new(store, config).unwrap()
    }

    fn test_schema() -> TableSchema {
        let mut columns = indexmap::IndexMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DTypeName::Float32,
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

    #[test]
    fn test_create_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("users", test_schema()).unwrap();

        let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
        svc.write("users", &batch).unwrap();

        let result = svc.read("users", &["c", "a"], &["score"]).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 3.0);
        assert_eq!(vals.value(1), 1.0);
    }

    #[test]
    fn test_create_duplicate_errors() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("t", test_schema()).unwrap();
        let err = svc.create("t", test_schema());
        assert!(err.is_err());
    }

    #[test]
    fn test_read_nonexistent_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        let err = svc.read("nope", &["a"], &["score"]);
        assert!(err.is_err());
    }

    #[test]
    fn test_read_empty_table_returns_nulls() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("empty", test_schema()).unwrap();
        let result = svc.read("empty", &["a"], &["score"]).unwrap();
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(vals.is_null(0));
    }

    #[test]
    fn test_multiple_writes_accumulate() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("t", test_schema()).unwrap();

        let batch1 = test_batch(&["a", "b"], &[1.0, 2.0]);
        svc.write("t", &batch1).unwrap();

        let batch2 = test_batch(&["c"], &[3.0]);
        svc.write("t", &batch2).unwrap();

        let result = svc.read("t", &["a", "b", "c"], &["score"]).unwrap();
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

    #[test]
    fn test_loads_existing_tables_on_startup() {
        let dir = TempDir::new().unwrap();

        {
            let svc = build_service(test_config(&dir));
            svc.create("users", test_schema()).unwrap();
            let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
            svc.write("users", &batch).unwrap();
        }

        let svc = build_service(test_config(&dir));
        let tables = svc.list_tables();
        assert!(tables.contains_key("users"));

        let result = svc.read("users", &["c", "a"], &["score"]).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 3.0);
        assert_eq!(vals.value(1), 1.0);
    }

    #[test]
    fn test_loads_empty_table_on_startup() {
        let dir = TempDir::new().unwrap();

        {
            let svc = build_service(test_config(&dir));
            svc.create("empty", test_schema()).unwrap();
        }

        let svc = build_service(test_config(&dir));
        let tables = svc.list_tables();
        assert!(tables.contains_key("empty"));

        let result = svc.read("empty", &["a"], &["score"]).unwrap();
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(vals.is_null(0));
    }
}
