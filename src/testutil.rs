//! Test and benchmark utilities.
//!
//! This module is only available when the `testutil` feature is enabled.

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::conf::{ColumnConfig, DType, LocalSourceConfig, SourceConfig, TableConfig};
use crate::old::manager::{TableLoader, TableState};
use crate::old::parquet::dtype_to_arrow;

/// Generate a deterministic Parquet file based on a TableConfig.
///
/// Creates a Parquet file with the specified number of rows, where:
/// - All string columns use stringified row index ("0", "1", "2", ...)
/// - Numeric columns use row index cast to the appropriate type
/// - Boolean columns use true for odd indices, false for even
pub fn generate_parquet_file(
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

pub fn build_schema_from_config(config: &TableConfig) -> Arc<Schema> {
    let fields: Vec<Field> = config
        .columns
        .iter()
        .map(|(name, col)| Field::new(name, dtype_to_arrow(&col.dtype), col.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

pub fn generate_batch(schema: &Arc<Schema>, num_rows: usize) -> RecordBatch {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| generate_deterministic_array(field.data_type(), num_rows))
        .collect();
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

pub fn generate_deterministic_array(dtype: &DataType, num_rows: usize) -> ArrayRef {
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

pub fn make_table_config(
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

/// Verify that array values at indices 0 and 1 match expected deterministic values.
/// Values are generated as row index (0 and 50), so we expect 0 and 50 for numerics,
/// "0" and "50" for strings, and false/false for booleans (both indices are even).
pub fn verify_array_values(array: &ArrayRef, dtype: &DType) {
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

/// Set up a test environment with a loaded table.
/// Returns the table state and temp directory (keep alive to prevent cleanup).
pub async fn setup_test_table(
    table_name: &str,
    num_rows: usize,
) -> (TableState, tempfile::TempDir) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");
    let partition_dir = source_dir.join("2024-01-14");

    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&partition_dir).unwrap();

    let table_config = make_table_config(
        source_dir.to_str().unwrap(),
        "id",
        vec![("id", DType::Utf8, false), ("value", DType::Float32, true)],
    );

    generate_parquet_file(
        &partition_dir.join("part_0000.parquet"),
        &table_config,
        num_rows,
    )
    .unwrap();
    fs::write(partition_dir.join("_SUCCESS"), "").unwrap();

    let loader = TableLoader::new(table_name.to_string(), table_config).unwrap();
    let discovery_result = loader.discover().await.unwrap();
    let state = loader.load(discovery_result, &data_dir).await.unwrap();

    (state, temp_dir)
}

// Benchmark-specific utilities

/// Number of Float32 columns for benchmarks
pub const BENCH_NUM_COLUMNS: usize = 10;

/// RNG seed for deterministic key generation
pub const BENCH_RNG_SEED: u64 = 42;

/// Generate column names for benchmarks: col_0, col_1, ..., col_9
pub fn bench_column_names() -> Vec<String> {
    (0..BENCH_NUM_COLUMNS)
        .map(|i| format!("col_{}", i))
        .collect()
}

/// Generate deterministic random keys for benchmarks.
pub fn bench_generate_keys(num_keys: usize, max_key: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(BENCH_RNG_SEED);
    (0..num_keys)
        .map(|_| rng.gen_range(0..max_key).to_string())
        .collect()
}

/// Create a TableConfig for benchmarks with 10 Float32 columns.
pub fn bench_make_table_config(source_path: &str) -> TableConfig {
    let mut columns = vec![("key", DType::Utf8, false)];
    for i in 0..BENCH_NUM_COLUMNS {
        // Leak the string to get a static reference - acceptable in benchmarks
        let name: &'static str = Box::leak(format!("col_{}", i).into_boxed_str());
        columns.push((name, DType::Float32, true));
    }

    make_table_config(source_path, "key", columns)
}

/// Set up a benchmark table with the specified number of rows.
/// Returns (TableState, TempDir) - keep TempDir alive!
pub async fn setup_benchmark_table(
    table_name: &str,
    num_rows: usize,
) -> (TableState, tempfile::TempDir) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let source_dir = temp_dir.path().join("source");
    let data_dir = temp_dir.path().join("data");
    let partition_dir = source_dir.join("2024-01-14");

    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&partition_dir).unwrap();

    let table_config = bench_make_table_config(source_dir.to_str().unwrap());

    generate_parquet_file(
        &partition_dir.join("part_0000.parquet"),
        &table_config,
        num_rows,
    )
    .unwrap();
    fs::write(partition_dir.join("_SUCCESS"), "").unwrap();

    let loader = TableLoader::new(table_name.to_string(), table_config).unwrap();
    let discovery_result = loader.discover().await.unwrap();
    let state = loader.load(discovery_result, &data_dir).await.unwrap();

    (state, temp_dir)
}
