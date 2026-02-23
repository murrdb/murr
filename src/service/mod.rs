mod state;

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::{Directory, LocalDirectory};
use crate::io::table::{CachedTable, TableWriter};

use state::TableState;

pub struct MurrService {
    tables: RwLock<HashMap<String, TableState>>,
    data_dir: PathBuf,
}

impl MurrService {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            data_dir,
        }
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
    use crate::core::{ColumnConfig, DType};
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            name: "test".to_string(),
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
        let svc = MurrService::new(dir.path().to_path_buf());

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
        let svc = MurrService::new(dir.path().to_path_buf());

        svc.create("t", test_schema()).await.unwrap();
        let err = svc.create("t", test_schema()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_nonexistent_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(dir.path().to_path_buf());

        let err = svc.read("nope", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_empty_table_errors() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(dir.path().to_path_buf());

        svc.create("empty", test_schema()).await.unwrap();
        let err = svc.read("empty", &["a"], &["score"]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_multiple_writes_accumulate() {
        let dir = TempDir::new().unwrap();
        let svc = MurrService::new(dir.path().to_path_buf());

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
}
