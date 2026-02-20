mod common;

use std::fs;

use arrow::array::*;
use rstest::rstest;
use tempfile::TempDir;

use common::{generate_parquet_file, make_table_config, verify_array_values};
use murr::conf::DType;
use murr::old::manager::TableLoader;

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
