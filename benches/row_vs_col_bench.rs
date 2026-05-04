use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::Schema;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use murr::core::{ColumnSchema, DType, TableSchema};
use murr::io;
use murr::io::directory::Directory as IoDirectory;
use murr::io3;
use murr::io3::directory::Directory as Io3Directory;
use murr::testutil::{bench_column_names, generate_batch};

const NUM_ROWS: usize = 50_000_000;
const KEY_COUNTS: &[usize] = &[100, 1000];

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

fn bench_io_vs_io3(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, NUM_ROWS);

    let tmp = TempDir::new().unwrap();

    eprintln!(
        "io: writing {} rows to {}",
        NUM_ROWS,
        tmp.path().join("io").display()
    );
    let io_reader = rt.block_on(async {
        let url = io::url::LocalUrl {
            path: tmp.path().join("io"),
        };
        let dir = Arc::new(
            io::directory::mmap::directory::MMapDirectory::create(
                &url,
                "bench",
                table_schema.clone(),
                4096,
                false,
            )
            .unwrap(),
        );
        let table = io::table::Table::new(dir);
        let writer = table.open_writer().await.unwrap();
        writer.write(&batch).await.unwrap();
        table.open_reader().await.unwrap()
    });

    eprintln!(
        "io3: writing {} rows to {}",
        NUM_ROWS,
        tmp.path().join("io3").display()
    );
    let io3_reader = rt.block_on(async {
        let url = io3::url::LocalUrl {
            path: tmp.path().join("io3"),
        };
        let dir = Arc::new(
            io3::directory::mmap::directory::MMapDirectory::create(
                &url,
                "bench",
                table_schema.clone(),
                io3::directory::mmap::directory::MMapConfig,
            )
            .unwrap(),
        );
        let writer = io3::table::writer::TableWriter::open(table_schema.clone(), dir.clone())
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let dir_reader = Arc::new(Io3Directory::open_reader(&dir).await.unwrap());
        io3::table::reader::TableReader::open(table_schema.clone(), dir_reader)
            .await
            .unwrap()
    });
    eprintln!("setup complete, starting benchmarks");

    let col_names: Vec<String> = std::iter::once("key".to_string())
        .chain(bench_column_names())
        .collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group("row_vs_col");
    group.sample_size(100);

    for &num_keys in KEY_COUNTS {
        let mut io_seed: u64 = num_keys as u64 * 1_000_000;
        let io_reader_ref = &io_reader;
        let col_refs_ref = &col_refs;
        group.bench_with_input(BenchmarkId::new("io", num_keys), &num_keys, |b, &n| {
            b.to_async(&rt).iter_batched(
                || {
                    io_seed += 1;
                    generate_random_keys(n, io_seed)
                },
                |keys| async move {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(
                        io_reader_ref
                            .read(black_box(&key_refs), black_box(col_refs_ref))
                            .await
                            .unwrap(),
                    )
                },
                BatchSize::PerIteration,
            );
        });

        let mut io3_seed: u64 = num_keys as u64 * 1_000_000 + 500_000;
        let io3_reader_ref = &io3_reader;
        let col_refs_ref = &col_refs;
        group.bench_with_input(BenchmarkId::new("io3", num_keys), &num_keys, |b, &n| {
            b.to_async(&rt).iter_batched(
                || {
                    io3_seed += 1;
                    generate_random_keys(n, io3_seed)
                },
                |keys| async move {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(
                        io3_reader_ref
                            .read(black_box(&key_refs), black_box(col_refs_ref))
                            .await
                            .unwrap(),
                    )
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_io_vs_io3);
criterion_main!(benches);
