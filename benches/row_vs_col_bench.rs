use std::hint::black_box;
use std::sync::{Arc, RwLock};

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
use murr::io4;
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
    let rt = Runtime::new().unwrap();
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, NUM_ROWS);

    let tmp = TempDir::new().unwrap();

    // eprintln!(
    //     "io: writing {} rows to {}",
    //     NUM_ROWS,
    //     tmp.path().join("io").display()
    // );
    // let io_reader = rt.block_on(async {
    //     let cfg = io::directory::mmap::directory::MMapConfig::new(tmp.path().join("io"));
    //     let dir = Arc::new(
    //         io::directory::mmap::directory::MMapDirectory::create(
    //             "bench",
    //             table_schema.clone(),
    //             cfg,
    //         )
    //         .unwrap(),
    //     );
    //     let writer = io::table::writer::TableWriter::open(table_schema.clone(), dir.clone())
    //         .await
    //         .unwrap();
    //     writer.write(&batch).await.unwrap();
    //     let dir_reader = Arc::new(IoDirectory::open_reader(&dir).await.unwrap());
    //     io::table::reader::TableReader::open(table_schema.clone(), dir_reader)
    //         .await
    //         .unwrap()
    // });

    let io4_path = tmp.path().join("io4");
    std::fs::create_dir_all(&io4_path).unwrap();
    eprintln!("io4: writing {} rows to {}", NUM_ROWS, io4_path.display());
    let io4_table = {
        // let store = io4::store::rocksdb::plain::PlainRocksDBStore::open(
        //     &io4_path,
        //     &io4::store::rocksdb::plain::PlainConfig::default(),
        // )
        // .unwrap();
        let store = io4::store::memory::MemoryStore::new();
        let store = Arc::new(RwLock::new(store));
        let table =
            io4::table::Table::create(store.clone(), "bench", table_schema.clone()).unwrap();
        table.write(&batch).unwrap();
        // eprintln!("io4: compacting...");
        // store.read().unwrap().compact("bench").unwrap();
        table
    };
    eprintln!("setup complete, starting benchmarks");

    let col_names: Vec<String> = std::iter::once("key".to_string())
        .chain(bench_column_names())
        .collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    // io4::Table::read rejects the key column (key is lookup-only in io4),
    // so the io4 path reads only the data columns. Cost asymmetry is one
    // StringArray slot vs the io path.
    let io4_col_names: Vec<String> = bench_column_names();
    let io4_col_refs: Vec<&str> = io4_col_names.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group("io");
    group.sample_size(100);

    for &num_keys in KEY_COUNTS {
        let mut io_seed: u64 = num_keys as u64 * 1_000_000;
        // let io_reader_ref = &io_reader;
        // let col_refs_ref = &col_refs;
        // group.bench_with_input(BenchmarkId::new("io", num_keys), &num_keys, |b, &n| {
        //     b.to_async(&rt).iter_batched(
        //         || {
        //             io_seed += 1;
        //             generate_random_keys(n, io_seed)
        //         },
        //         |keys| async move {
        //             let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        //             black_box(
        //                 io_reader_ref
        //                     .read(black_box(&key_refs), black_box(col_refs_ref))
        //                     .await
        //                     .unwrap(),
        //             )
        //         },
        //         BatchSize::PerIteration,
        //     );
        // });

        let mut io4_seed: u64 = num_keys as u64 * 2_000_000;
        let io4_table_ref = &io4_table;
        let io4_col_refs_ref = &io4_col_refs;
        group.bench_with_input(BenchmarkId::new("io4", num_keys), &num_keys, |b, &n| {
            b.iter_batched(
                || {
                    io4_seed += 1;
                    generate_random_keys(n, io4_seed)
                },
                |keys| {
                    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    black_box(
                        io4_table_ref
                            .read(black_box(&key_refs), black_box(io4_col_refs_ref))
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
