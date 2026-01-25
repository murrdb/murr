use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rstest::rstest;
use tempfile::TempDir;

use murr::conf::{ColumnConfig, Config, DType, LocalSourceConfig, SourceConfig, TableConfig};
use murr::manager::TableLoader;
use murr::parquet::dtype_to_arrow;

/// Generate a deterministic Parquet file based on a TableConfig.
///
/// Creates a Parquet file with the specified number of rows, where:
/// - All string columns use stringified row index ("0", "1", "2", ...)
/// - Numeric columns use row index cast to the appropriate type
/// - Boolean columns use true for odd indices, false for even
fn generate_parquet_file(
    path: &Path,
    config: &TableConfig,
    num_rows: usize,
) -> std::io::Result<()> {
    let schema = build_schema_from_config(config);
    let batch = generate_batch(&schema, num_rows);

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Ok(())
}

fn build_schema_from_config(config: &TableConfig) -> Arc<Schema> {
    let fields: Vec<Field> = config
        .columns
        .iter()
        .map(|(name, col)| Field::new(name, dtype_to_arrow(&col.dtype), col.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

fn generate_batch(schema: &Arc<Schema>, num_rows: usize) -> RecordBatch {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| generate_deterministic_array(field.data_type(), num_rows))
        .collect();
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

fn generate_deterministic_array(dtype: &DataType, num_rows: usize) -> ArrayRef {
    match dtype {
        DataType::Utf8 => {
            // Stringified row index: "0", "1", "2", ...
            let arr: StringArray = (0..num_rows).map(|i| Some(i.to_string())).collect();
            Arc::new(arr)
        }
        DataType::Int16 => {
            let arr: Int16Array = (0..num_rows).map(|i| Some(i as i16)).collect();
            Arc::new(arr)
        }
        DataType::Int32 => {
            let arr: Int32Array = (0..num_rows).map(|i| Some(i as i32)).collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = (0..num_rows).map(|i| Some(i as i64)).collect();
            Arc::new(arr)
        }
        DataType::UInt16 => {
            let arr: UInt16Array = (0..num_rows).map(|i| Some(i as u16)).collect();
            Arc::new(arr)
        }
        DataType::UInt32 => {
            let arr: UInt32Array = (0..num_rows).map(|i| Some(i as u32)).collect();
            Arc::new(arr)
        }
        DataType::UInt64 => {
            let arr: UInt64Array = (0..num_rows).map(|i| Some(i as u64)).collect();
            Arc::new(arr)
        }
        DataType::Float32 => {
            let arr: Float32Array = (0..num_rows).map(|i| Some(i as f32)).collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = (0..num_rows).map(|i| Some(i as f64)).collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            // true for odd indices, false for even
            let arr: BooleanArray = (0..num_rows).map(|i| Some(i % 2 == 1)).collect();
            Arc::new(arr)
        }
        _ => panic!("Unsupported dtype: {:?}", dtype),
    }
}

fn make_table_config(
    source_path: &str,
    key: &str,
    columns: Vec<(&str, DType, bool)>,
) -> TableConfig {
    TableConfig {
        source: SourceConfig::Local(LocalSourceConfig {
            path: source_path.to_string(),
        }),
        poll_interval: Duration::from_secs(60),
        parts: 1,
        key: vec![key.to_string()],
        columns: columns
            .into_iter()
            .map(|(name, dtype, nullable)| (name.to_string(), ColumnConfig { dtype, nullable }))
            .collect(),
    }
}

/// Test the full pipeline with a simple 2-column (key + value) parquet file.
#[tokio::test]
async fn test_pipeline_simple_key_value() {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");
    let partition_dir = source_dir.join("2024-01-14");

    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&partition_dir).unwrap();

    // Create table config
    let table_config = make_table_config(
        source_dir.to_str().unwrap(),
        "id",
        vec![("id", DType::Utf8, false), ("value", DType::Float32, true)],
    );

    // Generate test parquet with 1000 rows
    generate_parquet_file(
        &partition_dir.join("part_0000.parquet"),
        &table_config,
        1000,
    )
    .unwrap();

    // Create _SUCCESS marker
    fs::write(partition_dir.join("_SUCCESS"), "").unwrap();

    // Run the loader
    let loader = TableLoader::new("test_table".to_string(), table_config).unwrap();
    let discovery_result = loader.discover().await.unwrap();

    assert_eq!(discovery_result.partition_date, "2024-01-14");
    assert_eq!(discovery_result.parquet_paths.len(), 1);

    let state = loader.load(discovery_result, &data_dir).await.unwrap();

    assert_eq!(state.partition_date, "2024-01-14");
    assert!(state.ipc_path.exists());

    // Verify table can query data (keys are "0", "500", "999")
    let batch = state.table.get(&["0", "500", "999"], &["value"]).unwrap();
    assert_eq!(batch.num_rows(), 3);

    // Verify values are correct (deterministic: row index as float)
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(values.value(0), 0.0);
    assert_eq!(values.value(1), 500.0);
    assert_eq!(values.value(2), 999.0);
}

/// Test loading config from YAML string.
#[tokio::test]
async fn test_config_from_string() {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");
    let partition_dir = source_dir.join("2024-01-15");

    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&partition_dir).unwrap();

    // Create config from YAML string
    let config_str = format!(
        r#"
server:
  data_dir: "{}"
tables:
  my_table:
    source:
      local:
        path: "{}"
    key: [id]
    columns:
      id:
        dtype: utf8
        nullable: false
      score:
        dtype: float64
        nullable: true
"#,
        data_dir.display(),
        source_dir.display()
    );

    let config = Config::from_str(&config_str).unwrap();
    assert!(config.tables.contains_key("my_table"));

    let table_config = config.tables.get("my_table").unwrap();

    // Generate test parquet
    generate_parquet_file(&partition_dir.join("data.parquet"), table_config, 100).unwrap();
    fs::write(partition_dir.join("_SUCCESS"), "").unwrap();

    // Load via TableLoader
    let loader = TableLoader::new("my_table".to_string(), table_config.clone()).unwrap();
    let discovery = loader.discover().await.unwrap();
    let state = loader.load(discovery, &data_dir).await.unwrap();

    assert_eq!(state.partition_date, "2024-01-15");

    // Query and verify deterministic value (key "50", value 50.0)
    let batch = state.table.get(&["50"], &["score"]).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let scores = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 50.0);
}

/// Verify that array values at indices 0 and 1 match expected deterministic values.
/// Values are generated as row index (0 and 50), so we expect 0 and 50 for numerics,
/// "0" and "50" for strings, and false/false for booleans (both indices are even).
fn verify_array_values(array: &ArrayRef, dtype: &DType) {
    macro_rules! verify_primitive {
        ($array_type:ty, $expected_0:expr, $expected_50:expr) => {{
            let arr = array.as_any().downcast_ref::<$array_type>().unwrap();
            assert_eq!(arr.value(0), $expected_0);
            assert_eq!(arr.value(1), $expected_50);
        }};
    }

    match dtype {
        DType::Utf8 => verify_primitive!(StringArray, "0", "50"),
        DType::Int16 => verify_primitive!(Int16Array, 0, 50),
        DType::Int32 => verify_primitive!(Int32Array, 0, 50),
        DType::Int64 => verify_primitive!(Int64Array, 0, 50),
        DType::Uint16 => verify_primitive!(UInt16Array, 0, 50),
        DType::UInt32 => verify_primitive!(UInt32Array, 0, 50),
        DType::UInt64 => verify_primitive!(UInt64Array, 0, 50),
        DType::Float32 => verify_primitive!(Float32Array, 0.0, 50.0),
        DType::Float64 => verify_primitive!(Float64Array, 0.0, 50.0),
        DType::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!arr.value(0)); // index 0 is even -> false
            assert!(!arr.value(1)); // index 50 is even -> false
        }
    }
}

/// Parameterized test for all supported data types.
/// Tests that each dtype can be written to parquet, converted to Arrow IPC, and loaded.
#[rstest]
#[case::utf8(DType::Utf8)]
#[case::int16(DType::Int16)]
#[case::int32(DType::Int32)]
#[case::int64(DType::Int64)]
#[case::uint16(DType::Uint16)]
#[case::uint32(DType::UInt32)]
#[case::uint64(DType::UInt64)]
#[case::float32(DType::Float32)]
#[case::float64(DType::Float64)]
#[case::bool(DType::Bool)]
#[tokio::test]
async fn test_dtype(#[case] value_dtype: DType) {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");
    let partition_dir = source_dir.join("2024-01-16");

    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&partition_dir).unwrap();

    // Create table config with key (utf8) and value (the dtype being tested)
    let table_config = make_table_config(
        source_dir.to_str().unwrap(),
        "id",
        vec![
            ("id", DType::Utf8, false),
            ("value", value_dtype.clone(), true),
        ],
    );

    // Generate test parquet with 100 rows
    generate_parquet_file(&partition_dir.join("test.parquet"), &table_config, 100).unwrap();
    fs::write(partition_dir.join("_SUCCESS"), "").unwrap();

    // Load via TableLoader
    let loader = TableLoader::new(format!("test_{:?}", value_dtype), table_config).unwrap();
    let discovery = loader.discover().await.unwrap();
    let state = loader.load(discovery, &data_dir).await.unwrap();

    // Query and verify (keys are "0" and "50")
    let batch = state.table.get(&["0", "50"], &["value"]).unwrap();

    assert_eq!(batch.num_rows(), 2);
    verify_array_values(batch.column(0), &value_dtype);
}

/// Test that discovery correctly identifies the latest partition with _SUCCESS marker.
#[tokio::test]
async fn test_discovery_selects_latest_partition() {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");

    fs::create_dir_all(&data_dir).unwrap();

    let table_config = make_table_config(
        source_dir.to_str().unwrap(),
        "id",
        vec![("id", DType::Utf8, false), ("val", DType::Int32, true)],
    );

    // Create multiple partitions
    // 2024-01-10 - complete (but old)
    let p1 = source_dir.join("2024-01-10");
    fs::create_dir_all(&p1).unwrap();
    generate_parquet_file(&p1.join("data.parquet"), &table_config, 10).unwrap();
    fs::write(p1.join("_SUCCESS"), "").unwrap();

    // 2024-01-12 - complete (should be selected)
    let p2 = source_dir.join("2024-01-12");
    fs::create_dir_all(&p2).unwrap();
    generate_parquet_file(&p2.join("data.parquet"), &table_config, 20).unwrap();
    fs::write(p2.join("_SUCCESS"), "").unwrap();

    // 2024-01-13 - incomplete (no _SUCCESS)
    let p3 = source_dir.join("2024-01-13");
    fs::create_dir_all(&p3).unwrap();
    generate_parquet_file(&p3.join("data.parquet"), &table_config, 30).unwrap();
    // No _SUCCESS marker

    let loader = TableLoader::new("test".to_string(), table_config).unwrap();
    let discovery = loader.discover().await.unwrap();

    // Should select 2024-01-12 (latest with _SUCCESS)
    assert_eq!(discovery.partition_date, "2024-01-12");
}

/// Test that loader fails gracefully when no valid partition exists.
#[tokio::test]
async fn test_no_valid_partition_error() {
    let temp_dir = TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");

    fs::create_dir_all(&source_dir).unwrap();

    let table_config = make_table_config(
        source_dir.to_str().unwrap(),
        "id",
        vec![("id", DType::Utf8, false)],
    );

    // Create partition without _SUCCESS marker
    let partition = source_dir.join("2024-01-01");
    fs::create_dir_all(&partition).unwrap();
    generate_parquet_file(&partition.join("data.parquet"), &table_config, 10).unwrap();
    // No _SUCCESS

    let loader = TableLoader::new("test".to_string(), table_config).unwrap();
    let result = loader.discover().await;

    assert!(result.is_err());
}
