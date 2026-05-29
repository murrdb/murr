#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use indexmap::IndexMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arrow::datatypes::Schema;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;

use murr::conf::{BackendConfig, Config, StorageConfig};
use murr::core::{ColumnSchema, DTypeName, TableSchema};
use murr::io::store::rocksdb::RocksDBStore;
use murr::io::store::rocksdb::plain::PlainConfig;
use murr::service::MurrService;

mod common;
use common::data::{bench_column_names, generate_batch};

const ROWS_PER_SEGMENT: usize = 50_000;
const SEGMENT_COUNTS: &[usize] = &[1, 32, 128];

fn make_schema() -> (TableSchema, Arc<Schema>) {
    let col_names = bench_column_names();
    let mut columns = IndexMap::new();
    columns.insert(
        "key".to_string(),
        ColumnSchema {
            dtype: DTypeName::Utf8,
            nullable: false,
            cast: false,
        },
    );
    for name in &col_names {
        columns.insert(
            name.clone(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: false,
                cast: false,
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

fn bench_multi_segment_write(c: &mut Criterion) {
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, ROWS_PER_SEGMENT);

    let mut group = c.benchmark_group("multi_segment_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for &num_segments in SEGMENT_COUNTS {
        group.throughput(Throughput::Elements(
            (num_segments * ROWS_PER_SEGMENT) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::new("segments", num_segments),
            &num_segments,
            |b, &n| {
                b.iter(|| {
                    let schema = table_schema.clone();
                    let batch = batch.clone();
                    let dir = TempDir::new().unwrap();
                    let config = Config {
                        storage: StorageConfig {
                            path: dir.path().to_path_buf(),
                            backend: BackendConfig::Mmap(PlainConfig::default()),
                        },
                        ..Config::default()
                    };
                    let store = Arc::new(RwLock::new(
                        RocksDBStore::open_from_config(&config.storage).unwrap(),
                    ));
                    let svc = MurrService::new(store, config).unwrap();
                    svc.create("bench", schema).unwrap();
                    for _ in 0..n {
                        svc.write("bench", &batch).unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_multi_segment_write);
criterion_main!(benches);
