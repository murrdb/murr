use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use arrow::array::{Array, Float32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 10_000_000;
const NUM_COLUMNS: usize = 10;
const NUM_KEYS: usize = 1000;
const RNG_SEED: u64 = 42;

struct RowTable {
    index: AHashMap<String, usize>,
    data: Vec<Vec<f32>>,
    col_index: AHashMap<String, usize>,
}

impl RowTable {
    fn new(num_rows: usize, num_columns: usize) -> Self {
        let mut index = AHashMap::with_capacity(num_rows);
        for i in 0..num_rows {
            index.insert(i.to_string(), i);
        }

        let data: Vec<Vec<f32>> = (0..num_rows)
            .map(|i| (0..num_columns).map(|c| (i * num_columns + c) as f32).collect())
            .collect();

        let mut col_index = AHashMap::with_capacity(num_columns);
        for c in 0..num_columns {
            col_index.insert(format!("col_{}", c), c);
        }

        RowTable {
            index,
            data,
            col_index,
        }
    }

    fn get(&self, keys: &[&str], columns: &[&str]) -> RecordBatch {
        let col_offsets: Vec<usize> = columns
            .iter()
            .map(|&name| self.col_index[name])
            .collect();

        let row_indices: Vec<usize> = keys
            .iter()
            .filter_map(|k| self.index.get(*k).copied())
            .collect();

        let fields: Vec<Field> = columns
            .iter()
            .map(|&name| Field::new(name, DataType::Float32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn Array>> = col_offsets
            .iter()
            .map(|&col_off| {
                let values: Vec<f32> = row_indices
                    .iter()
                    .map(|&row_idx| unsafe {
                        *self.data.get_unchecked(row_idx).get_unchecked(col_off)
                    })
                    .collect();
                Arc::new(Float32Array::from(values)) as Arc<dyn Array>
            })
            .collect();

        RecordBatch::try_new(schema, arrays).unwrap()
    }
}

fn generate_keys(num_keys: usize, max_key: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..num_keys)
        .map(|_| rng.gen_range(0..max_key).to_string())
        .collect()
}

fn bench_hashmap_row_get(c: &mut Criterion) {
    let table = RowTable::new(NUM_ROWS, NUM_COLUMNS);

    let col_names: Vec<String> = (0..NUM_COLUMNS).map(|i| format!("col_{}", i)).collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let keys = generate_keys(NUM_KEYS, NUM_ROWS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group(format!("hashmap_row/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.iter(|| table.get(black_box(&key_refs), black_box(&col_refs)))
    });

    group.finish();
}

criterion_group!(benches, bench_hashmap_row_get);
criterion_main!(benches);
