use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use hashbrown::HashMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const NUM_ROWS: usize = 100_000_000;
const NUM_COLS: usize = 10;
const KEY_COUNTS: &[usize] = &[100, 1000];

trait TableReader {
    fn read(&self, keys: &[&str], cols: &[usize]) -> Vec<Vec<f32>>;
}

// --- Columnar layout ---

struct KeyOffset {
    col_offset: usize,
}

struct ColTable {
    index: HashMap<String, KeyOffset>,
    data: Vec<Vec<f32>>, // data[col_index][col_offset]
}

impl ColTable {
    fn new() -> Self {
        eprintln!("ColTable: allocating {} columns x {} rows", NUM_COLS, NUM_ROWS);
        let mut data = Vec::with_capacity(NUM_COLS);
        for col in 0..NUM_COLS {
            let column: Vec<f32> = (0..NUM_ROWS).map(|row| (row * NUM_COLS + col) as f32).collect();
            data.push(column);
        }
        let mut index = HashMap::with_capacity(NUM_ROWS);
        for row in 0..NUM_ROWS {
            index.insert(row.to_string(), KeyOffset { col_offset: row });
        }
        eprintln!("ColTable: ready");
        Self { index, data }
    }
}

impl TableReader for ColTable {
    fn read(&self, keys: &[&str], cols: &[usize]) -> Vec<Vec<f32>> {
        let mut result = vec![Vec::with_capacity(keys.len()); cols.len()];
        for (out_col, &col_idx) in cols.iter().enumerate() {
            let col_data = &self.data[col_idx];
            for key in keys {
                let offset = self.index.get(*key).unwrap();
                result[out_col].push(col_data[offset.col_offset]);
            }
        }
        result
    }
}

// --- Row layout ---

struct RowTable {
    index: HashMap<String, usize>,
    data: Vec<Vec<f32>>, // data[row_index] = all column values
}

impl RowTable {
    fn new() -> Self {
        eprintln!("RowTable: allocating {} rows x {} cols", NUM_ROWS, NUM_COLS);
        let mut data = Vec::with_capacity(NUM_ROWS);
        for row in 0..NUM_ROWS {
            let row_data: Vec<f32> =
                (0..NUM_COLS).map(|col| (row * NUM_COLS + col) as f32).collect();
            data.push(row_data);
        }
        let mut index = HashMap::with_capacity(NUM_ROWS);
        for row in 0..NUM_ROWS {
            index.insert(row.to_string(), row);
        }
        eprintln!("RowTable: ready");
        Self { index, data }
    }
}

impl TableReader for RowTable {
    fn read(&self, keys: &[&str], cols: &[usize]) -> Vec<Vec<f32>> {
        let mut result = vec![Vec::with_capacity(keys.len()); cols.len()];
        for key in keys {
            let row_idx = *self.index.get(*key).unwrap();
            let row = &self.data[row_idx];
            for (out_col, &col_idx) in cols.iter().enumerate() {
                result[out_col].push(row[col_idx]);
            }
        }
        result
    }
}

// --- Benchmark ---

fn generate_random_keys(num_keys: usize, seed: u64) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_keys)
        .map(|_| rng.random_range(0..NUM_ROWS).to_string())
        .collect()
}

fn bench_row_vs_col(c: &mut Criterion) {
    let cols: Vec<usize> = (0..NUM_COLS).collect();

    let col_table = ColTable::new();
    let row_table = RowTable::new();

    let mut group = c.benchmark_group("row_vs_col");
    group.sample_size(100);

    for &num_keys in KEY_COUNTS {
        let mut col_seed: u64 = num_keys as u64 * 1_000_000;
        group.bench_with_input(BenchmarkId::new("col", num_keys), &num_keys, |b, &n| {
            b.iter_batched(
                || {
                    col_seed += 1;
                    generate_random_keys(n, col_seed)
                },
                |keys| {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(col_table.read(black_box(&key_refs), black_box(&cols)))
                },
                BatchSize::PerIteration,
            );
        });

        let mut row_seed: u64 = num_keys as u64 * 1_000_000 + 500_000;
        group.bench_with_input(BenchmarkId::new("row", num_keys), &num_keys, |b, &n| {
            b.iter_batched(
                || {
                    row_seed += 1;
                    generate_random_keys(n, row_seed)
                },
                |keys| {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(row_table.read(black_box(&key_refs), black_box(&cols)))
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_row_vs_col);
criterion_main!(benches);
