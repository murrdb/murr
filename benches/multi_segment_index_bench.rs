use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::Schema;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use murr::conf::Config;
use murr::conf::StorageConfig;
use murr::core::{ColumnSchema, DType, TableSchema};
use murr::service::MurrService;
use murr::testutil::{bench_column_names, generate_batch};

const ROWS_PER_SEGMENT: usize = 50_000;
const SEGMENT_COUNTS: &[usize] = &[1, 32, 128];

fn make_schema() -> (TableSchema, Arc<Schema>) {
    let col_names = bench_column_names();
    let mut columns = HashMap::new();
    columns.insert(
        "key".to_string(),
        ColumnSchema {
            dtype: DType::Utf8,
            nullable: false,
        },
    );
    for name in &col_names {
        columns.insert(
            name.clone(),
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

fn bench_multi_segment_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (table_schema, arrow_schema) = make_schema();
    let batch = generate_batch(&arrow_schema, ROWS_PER_SEGMENT);

    let mut group = c.benchmark_group("multi_segment_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for &num_segments in SEGMENT_COUNTS {
        group.throughput(Throughput::Elements((num_segments * ROWS_PER_SEGMENT) as u64));

        group.bench_with_input(
            BenchmarkId::new("segments", num_segments),
            &num_segments,
            |b, &n| {
                b.to_async(&rt).iter(|| {
                    let schema = table_schema.clone();
                    let batch = batch.clone();
                    async move {
                        let dir = TempDir::new().unwrap();
                        let config = Config {
                            storage: StorageConfig {
                                cache_dir: dir.path().to_path_buf(),
                            },
                            ..Config::default()
                        };
                        let svc = MurrService::new(config).await.unwrap();
                        svc.create("bench", schema).await.unwrap();
                        for _ in 0..n {
                            svc.write("bench", &batch).await.unwrap();
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_multi_segment_write);
criterion_main!(benches);
