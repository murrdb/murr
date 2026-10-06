use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use log::{info, warn};

use crate::conf::Config;
use crate::core::{FetchRequest, MurrError, TableSchema};
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

    pub fn drop_table(&self, table_name: &str) -> Result<(), MurrError> {
        let mut tables = self.tables.write().unwrap_or_else(PoisonError::into_inner);
        // Unregister first so no reader can grab a Table whose CF is being torn down.
        tables
            .remove(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        self.store
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .drop_table(table_name)
    }

    pub fn write(&self, table_name: &str, batch: &RecordBatch) -> Result<(), MurrError> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.write(batch)
    }

    pub fn compact(&self, table_name: &str) -> Result<(), MurrError> {
        if !self
            .tables
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(table_name)
        {
            return Err(MurrError::TableNotFound(table_name.to_string()));
        }
        // Blocks until done under the store read lock: reads go on, writes wait for the whole compaction.
        self.store
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .compact(table_name)
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

    pub fn read(&self, table_name: &str, request: &FetchRequest) -> Result<RecordBatch, MurrError> {
        let tables = self.tables.read().unwrap_or_else(PoisonError::into_inner);
        let table = tables
            .get(table_name)
            .ok_or_else(|| MurrError::TableNotFound(table_name.to_string()))?;
        table.read(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::{BackendConfig, StorageConfig};
    use crate::core::{ColumnSchema, DTypeName, IDX_COLUMN};
    use crate::io::store::rocksdb::RocksDBStore;
    use crate::io::store::rocksdb::plain::PlainConfig;
    use arrow::array::{Float32Array, StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Scatters the sparse `score` column back to one slot per requested key.
    fn scores_by_position(batch: &RecordBatch, num_keys: usize) -> Vec<Option<f32>> {
        let idx = batch
            .column_by_name(IDX_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut out = vec![None; num_keys];
        for row in 0..batch.num_rows() {
            out[idx.value(row) as usize] = Some(scores.value(row));
        }
        out
    }

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
                key: true,
                strict: true,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
                key: false,
                strict: true,
            },
        );
        TableSchema { columns }
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

    fn fetch(keys: &[&str], columns: &[&str]) -> FetchRequest {
        let key_array: StringArray = keys.iter().map(|k| Some(*k)).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        FetchRequest {
            keys: RecordBatch::try_new(schema, vec![Arc::new(key_array)]).unwrap(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn test_create_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("users", test_schema()).unwrap();

        let batch = test_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]);
        svc.write("users", &batch).unwrap();

        let result = svc.read("users", &fetch(&["c", "a"], &["score"])).unwrap();
        assert_eq!(scores_by_position(&result, 2), [Some(3.0), Some(1.0)]);
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

        let err = svc.read("nope", &fetch(&["a"], &["score"]));
        assert!(err.is_err());
    }

    #[test]
    fn test_read_empty_table_returns_no_rows() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("empty", test_schema()).unwrap();
        let result = svc.read("empty", &fetch(&["a"], &["score"])).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema().field(0).name(), IDX_COLUMN);
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

        let result = svc.read("t", &fetch(&["a", "b", "c"], &["score"])).unwrap();
        assert_eq!(
            scores_by_position(&result, 3),
            [Some(1.0), Some(2.0), Some(3.0)]
        );
    }

    #[test]
    fn test_drop_table_then_recreate_with_new_schema() {
        let dir = TempDir::new().unwrap();
        let svc = build_service(test_config(&dir));

        svc.create("t", test_schema()).unwrap();
        svc.write("t", &test_batch(&["a"], &[1.0])).unwrap();

        svc.drop_table("t").unwrap();
        assert!(svc.list_tables().is_empty());
        assert!(matches!(
            svc.read("t", &fetch(&["a"], &["score"])),
            Err(MurrError::TableNotFound(_))
        ));
        assert!(matches!(
            svc.drop_table("t"),
            Err(MurrError::TableNotFound(_))
        ));

        let mut new_schema = test_schema();
        new_schema.columns.insert(
            "extra".to_string(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: true,
                key: false,
                strict: true,
            },
        );
        svc.create("t", new_schema.clone()).unwrap();
        assert_eq!(svc.get_schema("t").unwrap(), new_schema);

        let result = svc.read("t", &fetch(&["a"], &["score"])).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_dropped_table_does_not_return_on_startup() {
        let dir = TempDir::new().unwrap();

        {
            let svc = build_service(test_config(&dir));
            svc.create("users", test_schema()).unwrap();
            svc.create("gone", test_schema()).unwrap();
            svc.drop_table("gone").unwrap();
        }

        let svc = build_service(test_config(&dir));
        let tables = svc.list_tables();
        assert!(tables.contains_key("users"));
        assert!(!tables.contains_key("gone"));
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

        let result = svc.read("users", &fetch(&["c", "a"], &["score"])).unwrap();
        assert_eq!(scores_by_position(&result, 2), [Some(3.0), Some(1.0)]);
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

        let result = svc.read("empty", &fetch(&["a"], &["score"])).unwrap();
        assert_eq!(result.num_rows(), 0);
    }
}
