use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use arrow::array::{Array, Float32Array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
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
            .map(|i| {
                (0..num_columns)
                    .map(|c| (i * num_columns + c) as f32)
                    .collect()
            })
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
        let col_offsets: Vec<usize> = columns.iter().map(|&name| self.col_index[name]).collect();

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

struct ByteBlobTable {
    index: AHashMap<String, usize>,
    data: Vec<Vec<u8>>,
    col_index: AHashMap<String, usize>,
    num_columns: usize,
}

impl ByteBlobTable {
    fn new(num_rows: usize, num_columns: usize) -> Self {
        let mut index = AHashMap::with_capacity(num_rows);
        for i in 0..num_rows {
            index.insert(i.to_string(), i);
        }

        let mut col_index = AHashMap::with_capacity(num_columns);
        for c in 0..num_columns {
            col_index.insert(format!("col_{}", c), c);
        }

        // Each column: 1 byte validity + 4 bytes f32 = 5 bytes per column
        // All values are non-null in test data
        let row_size = num_columns * 5;
        let data: Vec<Vec<u8>> = (0..num_rows)
            .map(|i| {
                let mut row = Vec::with_capacity(row_size);
                for c in 0..num_columns {
                    row.push(1u8); // valid
                    row.extend_from_slice(&((i * num_columns + c) as f32).to_le_bytes());
                }
                row
            })
            .collect();

        ByteBlobTable {
            index,
            data,
            col_index,
            num_columns,
        }
    }

    fn get(&self, keys: &[&str], columns: &[&str]) -> RecordBatch {
        // Resolve which schema positions are requested
        let requested: Vec<usize> = columns.iter().map(|&name| self.col_index[name]).collect();

        // Build a bitset of requested column positions for fast lookup during scan
        let mut requested_set = vec![false; self.num_columns];
        for &pos in &requested {
            requested_set[pos] = true;
        }

        // Map from schema position -> output column index
        let mut schema_pos_to_out: Vec<usize> = vec![0; self.num_columns];
        for (out_idx, &pos) in requested.iter().enumerate() {
            schema_pos_to_out[pos] = out_idx;
        }

        let row_indices: Vec<usize> = keys
            .iter()
            .filter_map(|k| self.index.get(*k).copied())
            .collect();

        let num_out_rows = row_indices.len();
        let num_out_cols = columns.len();

        // Pre-allocate per-column value and validity buffers with exact size
        let mut col_values: Vec<Vec<f32>> = vec![vec![0.0f32; num_out_rows]; num_out_cols];
        let mut col_validity: Vec<Vec<u8>> = vec![vec![0u8; (num_out_rows + 7) / 8]; num_out_cols];

        // Scan each row's byte blob sequentially
        for (out_row, &row_idx) in row_indices.iter().enumerate() {
            let blob = unsafe { self.data.get_unchecked(row_idx) };
            let mut offset = 0usize;

            for col_pos in 0..self.num_columns {
                let valid = unsafe { *blob.get_unchecked(offset) };
                offset += 1;

                if unsafe { *requested_set.get_unchecked(col_pos) } {
                    let out_col = unsafe { *schema_pos_to_out.get_unchecked(col_pos) };
                    if valid != 0 {
                        let value = f32::from_le_bytes(unsafe {
                            [
                                *blob.get_unchecked(offset),
                                *blob.get_unchecked(offset + 1),
                                *blob.get_unchecked(offset + 2),
                                *blob.get_unchecked(offset + 3),
                            ]
                        });
                        unsafe {
                            *col_values.get_unchecked_mut(out_col).get_unchecked_mut(out_row) =
                                value;
                            let validity = col_validity.get_unchecked_mut(out_col);
                            *validity.get_unchecked_mut(out_row >> 3) |= 1 << (out_row & 7);
                        }
                    }
                    // null: value stays 0.0, validity bit stays 0
                }

                if valid != 0 {
                    offset += 4; // skip f32 value bytes
                }
            }
        }

        let fields: Vec<Field> = columns
            .iter()
            .map(|&name| Field::new(name, DataType::Float32, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn Array>> = col_values
            .into_iter()
            .zip(col_validity.into_iter())
            .map(|(values, validity_bytes)| {
                let buffer = Buffer::from_vec(validity_bytes);
                let bool_buf = BooleanBuffer::new(buffer, 0, num_out_rows);
                let nulls = NullBuffer::new(bool_buf);
                Arc::new(Float32Array::new(values.into(), Some(nulls))) as Arc<dyn Array>
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

fn bench_hashmap_byte_blob_get(c: &mut Criterion) {
    let table = ByteBlobTable::new(NUM_ROWS, NUM_COLUMNS);

    let col_names: Vec<String> = (0..NUM_COLUMNS).map(|i| format!("col_{}", i)).collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let keys = generate_keys(NUM_KEYS, NUM_ROWS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group(format!("hashmap_byte_blob/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.iter(|| table.get(black_box(&key_refs), black_box(&col_refs)))
    });

    group.finish();
}

criterion_group!(benches, bench_hashmap_byte_blob_get);
criterion_main!(benches);
