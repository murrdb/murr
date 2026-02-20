use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use murr::conf::{ColumnConfig, DType};
use murr::io::directory::{Directory, LocalDirectory, TableSchema};
use murr::io::table::{TableReader, TableView, TableWriter};

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
    columns.insert(
        "label".to_string(),
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: true,
        },
    );
    TableSchema { columns }
}

fn test_batch() -> RecordBatch {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("label", DataType::Utf8, true),
    ]));

    let keys: StringArray = vec![Some("alpha"), Some("beta"), Some("gamma"), Some("delta")]
        .into_iter()
        .collect();
    let scores: Float32Array = vec![Some(1.5), Some(2.5), None, Some(4.0)]
        .into_iter()
        .collect();
    let labels: StringArray = vec![Some("hot"), None, Some("cold"), Some("warm")]
        .into_iter()
        .collect();

    RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(keys), Arc::new(scores), Arc::new(labels)],
    )
    .unwrap()
}

fn read_schema() -> HashMap<String, ColumnConfig> {
    let mut schema = HashMap::new();
    schema.insert(
        "score".to_string(),
        ColumnConfig {
            dtype: DType::Float32,
            nullable: true,
        },
    );
    schema.insert(
        "label".to_string(),
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: true,
        },
    );
    schema
}

#[tokio::test]
async fn test_write_reopen_query() {
    let dir = TempDir::new().unwrap();
    let mut local = LocalDirectory::new(dir.path());
    let schema = test_schema();

    // Write a segment via TableWriter.
    let mut writer = TableWriter::create(&schema, &mut local).await.unwrap();
    let batch = test_batch();
    let info = writer.add_segment(&batch).await.unwrap();
    assert_eq!(info.id, 0);
    drop(writer);

    // Reopen from disk.
    let index = local.index().await.unwrap().unwrap();
    assert_eq!(index.segments.len(), 1);

    let view = TableView::open(dir.path(), &index.segments).unwrap();
    let reader = TableReader::from_table(&view, "key", &read_schema()).unwrap();

    // Query in a different order than written.
    let result = reader
        .get(&["delta", "alpha", "gamma", "beta"], &["score", "label"])
        .unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.num_columns(), 2);

    let scores = result
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(scores.value(0), 4.0); // delta
    assert_eq!(scores.value(1), 1.5); // alpha
    assert!(scores.is_null(2)); // gamma (was None)
    assert_eq!(scores.value(3), 2.5); // beta

    let labels = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(labels.value(0), "warm"); // delta
    assert_eq!(labels.value(1), "hot"); // alpha
    assert_eq!(labels.value(2), "cold"); // gamma
    assert!(labels.is_null(3)); // beta (was None)
}

#[tokio::test]
async fn test_missing_keys_return_nulls() {
    let dir = TempDir::new().unwrap();
    let mut local = LocalDirectory::new(dir.path());

    let mut writer = TableWriter::create(&test_schema(), &mut local)
        .await
        .unwrap();
    writer.add_segment(&test_batch()).await.unwrap();
    drop(writer);

    let index = local.index().await.unwrap().unwrap();
    let view = TableView::open(dir.path(), &index.segments).unwrap();
    let reader = TableReader::from_table(&view, "key", &read_schema()).unwrap();

    let result = reader
        .get(&["alpha", "missing", "delta"], &["score"])
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    let scores = result
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.5);
    assert!(scores.is_null(1));
    assert_eq!(scores.value(2), 4.0);
}
