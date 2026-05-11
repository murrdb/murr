use std::hint::black_box;
use std::sync::{Arc, RwLock};

use arrow::datatypes::Schema;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tempfile::TempDir;

use murr::core::{ColumnSchema, DType, TableSchema};
use murr::io;
use murr::testutil::{bench_column_names, generate_batch};

const NUM_ROWS: usize = 100_000_000;
const KEY_COUNTS: &[usize] = &[1000];

fn make_schema() -> (TableSchema, Arc<Schema>) {
    let mut columns = IndexMap::new();
    columns.insert(
        "key".to_string(),
        ColumnSchema {
            dtype: DType::Utf8,
            nullable: false,
        },
    );
    for name in bench_column_names() {
        columns.insert(
            name,
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
    }
    let table_schema = TableSchema {
        key: "key".to_string(),
        columns,
    };
    let arrow_schema = Arc::new(Schema::from(&table_schema));
    (table_schema, arrow_schema)
}

fn generate_random_keys(num_keys: usize, seed: u64) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_keys)
        .map(|_| rng.random_range(0..NUM_ROWS).to_string())
        .collect()
}

fn bench_io(c: &mut Criterion) {
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, NUM_ROWS);

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("io");
    std::fs::create_dir_all(&path).unwrap();
    eprintln!("io: writing {} rows to {}", NUM_ROWS, path.display());

    let table = {
        let store = io::store::memory::MemoryStore::new();
        let store = Arc::new(RwLock::new(store));
        let table = io::table::Table::create(store.clone(), "bench", table_schema.clone()).unwrap();
        table.write(&batch).unwrap();
        table
    };
    eprintln!("setup complete, starting benchmarks");

    // io::Table::read rejects the key column (key is lookup-only),
    // so the bench reads only the data columns.
    let col_names: Vec<String> = bench_column_names();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group("io");
    group.sample_size(100);

    for &num_keys in KEY_COUNTS {
        let mut seed: u64 = num_keys as u64 * 2_000_000;
        let table_ref = &table;
        let col_refs_ref = &col_refs;
        group.bench_with_input(BenchmarkId::new("io", num_keys), &num_keys, |b, &n| {
            b.iter_batched(
                || {
                    seed += 1;
                    generate_random_keys(n, seed)
                },
                |keys| {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(
                        table_ref
                            .read(black_box(&key_refs), black_box(col_refs_ref))
                            .unwrap(),
                    )
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_io);
criterion_main!(benches);
