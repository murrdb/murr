use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use arrow::array::{Array, Float32Array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::RngExt as _;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::hint::black_box;

const NUM_ROWS: usize = 10_000_000;
const NUM_COLUMNS: usize = 10;
const NUM_KEYS: usize = 1000;
const RNG_SEED: u64 = 42;

trait Column {
    fn get(&self, offsets: &[usize]) -> Arc<dyn Array>;
}

struct Float32Column {
    data: Vec<f32>,
    validity: Vec<u8>, // bit-packed, LSB first (Arrow convention)
}

impl Column for Float32Column {
    fn get(&self, offsets: &[usize]) -> Arc<dyn Array> {
        let len = offsets.len();
        let values: Vec<f32> = offsets
            .iter()
            .map(|&i| unsafe { *self.data.get_unchecked(i) })
            .collect();
        let mut validity_bytes = vec![0u8; (len + 7) / 8];
        for (out_idx, &src_idx) in offsets.iter().enumerate() {
            unsafe {
                let src_bit = (*self.validity.get_unchecked(src_idx >> 3) >> (src_idx & 7)) & 1;
                *validity_bytes.get_unchecked_mut(out_idx >> 3) |= src_bit << (out_idx & 7);
            }
        }

        let buffer = Buffer::from_vec(validity_bytes);
        let bool_buf = BooleanBuffer::new(buffer, 0, len);
        let nulls = NullBuffer::new(bool_buf);
        Arc::new(Float32Array::new(values.into(), Some(nulls)))
    }
}

struct SimpleTable {
    index: AHashMap<String, usize>,
    columns: AHashMap<String, Box<dyn Column>>,
}

impl SimpleTable {
    fn new(num_rows: usize, num_columns: usize) -> Self {
        let mut index = AHashMap::with_capacity(num_rows);
        for i in 0..num_rows {
            index.insert(i.to_string(), i);
        }

        let mut columns: AHashMap<String, Box<dyn Column>> = AHashMap::with_capacity(num_columns);
        for c in 0..num_columns {
            let data: Vec<f32> = (0..num_rows).map(|i| i as f32).collect();
            let validity = vec![0xFFu8; (num_rows + 7) / 8];
            columns.insert(
                format!("col_{}", c),
                Box::new(Float32Column { data, validity }),
            );
        }

        SimpleTable { index, columns }
    }

    fn get(&self, keys: &[&str], columns: &[&str]) -> RecordBatch {
        let offsets: Vec<usize> = keys
            .iter()
            .filter_map(|k| self.index.get(*k).copied())
            .collect();

        let fields: Vec<Field> = columns
            .iter()
            .map(|&name| Field::new(name, DataType::Float32, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn Array>> = columns
            .iter()
            .map(|col_name| self.columns[*col_name].get(&offsets))
            .collect();

        RecordBatch::try_new(schema, arrays).unwrap()
    }
}

fn generate_keys(num_keys: usize, max_key: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..num_keys)
        .map(|_| rng.random_range(0..max_key).to_string())
        .collect()
}

fn bench_hashmap_get(c: &mut Criterion) {
    let table = SimpleTable::new(NUM_ROWS, NUM_COLUMNS);

    let col_names: Vec<String> = (0..NUM_COLUMNS).map(|i| format!("col_{}", i)).collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let keys = generate_keys(NUM_KEYS, NUM_ROWS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group(format!("hashmap/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.iter(|| table.get(black_box(&key_refs), black_box(&col_refs)))
    });

    group.finish();
}

criterion_group!(benches, bench_hashmap_get);
criterion_main!(benches);
