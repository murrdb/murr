use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::Schema;
use criterion::measurement::Measurement;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use murr::core::{ColumnSchema, DType, TableSchema};
use murr::io;
use murr::io::directory::{Directory as IoDirectory, DirectoryReader as IoDirectoryReader};
use murr::io::table::reader::TableReader;
use murr::testutil::{bench_column_names, generate_batch};

const NUM_ROWS: usize = 5_000_000;
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

async fn build_reader<D: IoDirectory>(
    schema: TableSchema,
    config: D::ConfigType,
    batch: &arrow::record_batch::RecordBatch,
) -> TableReader<D::ReaderType> {
    let dir = Arc::new(D::create("bench", schema.clone(), config).unwrap());
    let writer = io::table::writer::TableWriter::open(schema.clone(), dir.clone())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    let dir_reader = Arc::new(IoDirectory::open_reader(&dir).await.unwrap());
    TableReader::open(schema, dir_reader).await.unwrap()
}

fn bench_backend<R, M>(
    group: &mut BenchmarkGroup<M>,
    rt: &Runtime,
    label: &str,
    num_keys: usize,
    reader: &TableReader<R>,
    col_refs: &[&str],
) where
    R: IoDirectoryReader,
    M: Measurement,
{
    let mut seed: u64 = (num_keys as u64) * 1_000_000;
    group.bench_with_input(BenchmarkId::new(label, num_keys), &num_keys, |b, &n| {
        b.to_async(rt).iter_batched(
            || {
                seed += 1;
                generate_random_keys(n, seed)
            },
            |keys| async move {
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                black_box(
                    reader
                        .read(black_box(&key_refs), black_box(col_refs))
                        .await
                        .unwrap(),
                )
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_io(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, NUM_ROWS);
    // Pinned to a mount without inotify/fanotify watchers so the bench is not
    // dominated by `__fsnotify_parent` walks during reads. The directory must
    // exist; create it once on the host (e.g. `mkdir -p /tmp/murrbench_mount`).
    let bench_root = std::path::Path::new("/tmp/murrbench_mount");
    let tmp = TempDir::new_in(bench_root).unwrap_or_else(|e| {
        panic!(
            "failed to create temp dir under {}: {e}. Create it with `mkdir -p {}` first.",
            bench_root.display(),
            bench_root.display()
        )
    });

    eprintln!(
        "setup: {NUM_ROWS} rows, cache root = {}",
        tmp.path().display()
    );

    let mem_reader = rt.block_on(build_reader::<io::directory::mem::directory::MemDirectory>(
        table_schema.clone(),
        io::directory::mem::directory::MemConfig,
        &batch,
    ));

    let mmap_reader = rt.block_on(
        build_reader::<io::directory::mmap::directory::MMapDirectory>(
            table_schema.clone(),
            io::directory::mmap::directory::MMapConfig::new(tmp.path().join("mmap")),
            &batch,
        ),
    );

    #[cfg(target_os = "linux")]
    let uring_reader = rt.block_on(build_reader::<
        io::directory::iouring::directory::IoUringDirectory,
    >(
        table_schema.clone(),
        io::directory::iouring::IoUringConfig {
            cache_dir: tmp.path().join("uring"),
            direct: true,
            workers: 1,
            ring_size: 1024,
            buffer_slots: 1024,
            sqpoll: false,
            register_buffers: false,
            // Coalesce reads landing in the same 128 KiB bucket into one
            // SQE. To re-baseline against the no-coalesce path, set
            // `coalesce_window: 0` and rerun.
            coalesce_window: 16 * 1024,
            coalesce_slots: 32,
            ..io::directory::iouring::IoUringConfig::default()
        },
        &batch,
    ));

    eprintln!("setup complete, starting benchmarks");

    let col_names: Vec<String> = std::iter::once("key".to_string())
        .chain(bench_column_names())
        .collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group("read");
    group.sample_size(100);

    for &num_keys in KEY_COUNTS {
        //bench_backend(&mut group, &rt, "mem", num_keys, &mem_reader, &col_refs);
        //bench_backend(&mut group, &rt, "mmap", num_keys, &mmap_reader, &col_refs);
        #[cfg(target_os = "linux")]
        bench_backend(&mut group, &rt, "uring", num_keys, &uring_reader, &col_refs);
    }

    group.finish();
}

criterion_group!(benches, bench_io);
criterion_main!(benches);
