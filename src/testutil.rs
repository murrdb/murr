//! Test and benchmark utilities.
//!
//! This module is only available when the `testutil` feature is enabled.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

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
            let arr: BooleanArray = (0..num_rows).map(|i| Some(i % 2 == 1)).collect();
            Arc::new(arr)
        }
        _ => panic!("Unsupported dtype: {:?}", dtype),
    }
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
        .map(|_| rng.random_range(0..max_key).to_string())
        .collect()
}
