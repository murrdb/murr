use std::sync::Arc;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::Directory;
use crate::io::table::reader::TableReader;
use crate::io::table::writer::TableWriter;

pub struct Table<D: Directory> {
    pub dir: Arc<D>,
}

impl<D: Directory> Table<D> {
    pub fn new(dir: Arc<D>) -> Arc<Self> {
        Arc::new(Table { dir })
    }

    pub fn schema(&self) -> &TableSchema {
        self.dir.schema()
    }

    pub async fn open_reader(self: &Arc<Self>) -> Result<TableReader<D::ReaderType>, MurrError> {
        let reader = Arc::new(self.dir.open_reader().await?);
        TableReader::open(self.dir.schema().clone(), reader).await
    }

    pub async fn open_writer(self: &Arc<Self>) -> Result<TableWriter<D>, MurrError> {
        TableWriter::open(self.dir.schema().clone(), self.dir.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::core::{ColumnSchema, DType};
    use crate::io::directory::mem::directory::MemDirectory;
    use crate::io::directory::Directory;
    use crate::io::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::create(&MemUrl, "default", test_schema(), 4096, false).unwrap())
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
                nullable: false,
            },
        );
        TableSchema {
            key: "key".to_string(),
            columns,
        }
    }

    fn make_batch(keys: &[&str], scores: &[f32]) -> RecordBatch {
        let key_array = Arc::new(StringArray::from(keys.to_vec())) as Arc<dyn Array>;
        let score_array = Arc::new(Float32Array::from(scores.to_vec())) as Arc<dyn Array>;
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
        ]));
        RecordBatch::try_new(schema, vec![key_array, score_array]).unwrap()
    }

    #[tokio::test]
    async fn write_and_read_roundtrip() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["b", "a", "c"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 2.0);
        assert_eq!(arr.value(1), 1.0);
        assert_eq!(arr.value(2), 3.0);
    }

    #[tokio::test]
    async fn missing_keys_produce_nulls() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[10.0, 20.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader
            .read(&["a", "missing", "b"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 10.0);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 20.0);
    }

    #[tokio::test]
    async fn multi_segment_last_write_wins() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[1.0, 2.0]))
            .await
            .unwrap();
        writer
            .write(&make_batch(&["a", "c"], &[10.0, 30.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader
            .read(&["a", "b", "c"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 10.0); // overwritten by segment 1
        assert_eq!(arr.value(1), 2.0);
        assert_eq!(arr.value(2), 30.0);
    }

    #[tokio::test]
    async fn incremental_reopen() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[1.0, 2.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["a"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1.0);

        // Write more data
        writer
            .write(&make_batch(&["c", "a"], &[30.0, 100.0]))
            .await
            .unwrap();

        // Reopen incrementally
        let reader = reader.reopen().await.unwrap();
        let result = reader
            .read(&["a", "b", "c"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100.0); // overwritten
        assert_eq!(arr.value(1), 2.0); // from old segment
        assert_eq!(arr.value(2), 30.0); // new key
    }

    #[tokio::test]
    async fn read_multiple_columns() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["x", "y"], &[42.0, 99.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["y", "x"], &["key", "score"]).await.unwrap();

        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "y");
        assert_eq!(keys.value(1), "x");

        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(scores.value(0), 99.0);
        assert_eq!(scores.value(1), 42.0);
    }

    #[tokio::test]
    async fn read_empty_table() {
        let dir = test_dir();
        let table = Table::new(dir);

        let writer = table.open_writer().await.unwrap();
        writer.write(&make_batch(&[], &[])).await.unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["a"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr.is_null(0));
    }
}
