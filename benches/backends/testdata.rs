//! Shared test data generation utilities for benchmarks.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// RNG seed for deterministic data generation.
pub const BENCH_RNG_SEED: u64 = 42;

/// Number of columns used in benchmarks.
pub const BENCH_NUM_COLUMNS: usize = 10;

/// Generate column names: col_0, col_1, ..., col_{n-1}.
pub fn bench_column_names(num_columns: usize) -> Vec<String> {
    (0..num_columns).map(|i| format!("col_{}", i)).collect()
}

/// Generate deterministic random keys for benchmarks.
pub fn bench_generate_keys(num_keys: usize, max_key: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(BENCH_RNG_SEED);
    (0..num_keys)
        .map(|_| rng.gen_range(0..max_key).to_string())
        .collect()
}

/// Generate benchmark data as (key, values) pairs.
///
/// Returns Vec of (key, values) where:
/// - key is the stringified row index ("0", "1", ...)
/// - values are deterministic f32s: row_idx as base value for all columns
pub fn generate_bench_data(num_rows: usize, num_columns: usize) -> Vec<(String, Vec<f32>)> {
    (0..num_rows)
        .map(|row_idx| {
            let key = row_idx.to_string();
            // Values match what testutil generates: row_idx cast to f32
            let values: Vec<f32> = (0..num_columns).map(|_| row_idx as f32).collect();
            (key, values)
        })
        .collect()
}

/// Pack f32 values into bytes (little-endian).
pub fn pack_floats(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}
