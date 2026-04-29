pub mod index;
pub mod reader;
pub mod segment;
pub mod writer;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use indexmap::IndexMap;

    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io3::directory::mem::directory::{MemConfig, MemDirectory};
    use crate::io3::directory::Directory;
    use crate::io3::table::reader::TableReader;
    use crate::io3::table::writer::TableWriter;
    use crate::io3::url::MemUrl;

    fn test_schema() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            key: "id".into(),
            columns,
        }
    }

    fn test_dir(schema: TableSchema) -> Arc<MemDirectory> {
        Arc::new(MemDirectory::create(&MemUrl, "default", schema, MemConfig).unwrap())
    }

    fn make_batch(ids: &[&str], scores: &[Option<f32>]) -> RecordBatch {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let id_array: Vec<Option<&str>> = ids.iter().map(|s| Some(*s)).collect();
        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(id_array)),
                Arc::new(Float32Array::from(scores.to_vec())),
            ],
        )
        .unwrap()
    }

    fn project_string(batch: &RecordBatch, name: &str) -> StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    }

    fn project_f32(batch: &RecordBatch, name: &str) -> Float32Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn open_then_read_returns_values() {
        let schema = test_schema();
        let dir = test_dir(schema.clone());
        let writer = TableWriter::open(schema.clone(), dir.clone()).await.unwrap();
        writer
            .write(&make_batch(&["a", "b", "c"], &[Some(1.0), None, Some(3.0)]))
            .await
            .unwrap();

        let dir_reader = Arc::new(dir.open_reader().await.unwrap());
        let table_reader = TableReader::open(schema, dir_reader).await.unwrap();

        let batch = table_reader.read(&["a", "c", "b"], &["id", "score"]).await.unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ids = project_string(&batch, "id");
        let scores = project_f32(&batch, "score");
        assert_eq!(ids.value(0), "a");
        assert_eq!(ids.value(1), "c");
        assert_eq!(ids.value(2), "b");
        assert_eq!(scores.value(0), 1.0);
        assert_eq!(scores.value(1), 3.0);
        assert!(scores.is_null(2));
    }

    #[tokio::test]
    async fn read_missing_keys_returns_nulls() {
        let schema = test_schema();
        let dir = test_dir(schema.clone());
        let writer = TableWriter::open(schema.clone(), dir.clone()).await.unwrap();
        writer.write(&make_batch(&["a"], &[Some(1.0)])).await.unwrap();

        let dir_reader = Arc::new(dir.open_reader().await.unwrap());
        let table_reader = TableReader::open(schema, dir_reader).await.unwrap();

        let batch = table_reader.read(&["missing"], &["id", "score"]).await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(project_string(&batch, "id").is_null(0));
        assert!(project_f32(&batch, "score").is_null(0));
    }

    #[tokio::test]
    async fn read_subset_of_columns() {
        let schema = test_schema();
        let dir = test_dir(schema.clone());
        let writer = TableWriter::open(schema.clone(), dir.clone()).await.unwrap();
        writer.write(&make_batch(&["a"], &[Some(1.5)])).await.unwrap();

        let dir_reader = Arc::new(dir.open_reader().await.unwrap());
        let table_reader = TableReader::open(schema, dir_reader).await.unwrap();

        let batch = table_reader.read(&["a"], &["score"]).await.unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "score");
        assert_eq!(project_f32(&batch, "score").value(0), 1.5);
    }

    #[tokio::test]
    async fn reopen_picks_up_new_segment() {
        let schema = test_schema();
        let dir = test_dir(schema.clone());
        let writer = TableWriter::open(schema.clone(), dir.clone()).await.unwrap();

        let dir_reader = Arc::new(dir.open_reader().await.unwrap());
        let reader_v1 = TableReader::open(schema.clone(), dir_reader).await.unwrap();

        writer.write(&make_batch(&["a"], &[Some(7.0)])).await.unwrap();

        let reader_v2 = reader_v1.reopen().await.unwrap();
        let batch = reader_v2.read(&["a"], &["score"]).await.unwrap();
        assert_eq!(project_f32(&batch, "score").value(0), 7.0);
    }
}
