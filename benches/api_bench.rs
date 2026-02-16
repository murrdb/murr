//! Benchmark comparing Murr API against Redis backends.

mod backends;

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use backends::murr::MurrBackend;
use backends::redis_blob::RedisBlobBackend;
use backends::redis_feast::RedisFeastBackend;
use backends::testdata::{BENCH_NUM_COLUMNS, bench_column_names, bench_generate_keys};
use backends::{BenchBackend, BenchConfig};

const ROW_COUNTS: &[usize] = &[10_000_000];
const KEY_COUNTS: &[usize] = &[1000];

/// Benchmark a specific backend implementation.
fn bench_backend<B>(c: &mut Criterion, mut backend: B)
where
    B: BenchBackend,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    let columns = bench_column_names(BENCH_NUM_COLUMNS);

    for &num_rows in ROW_COUNTS {
        let config = BenchConfig {
            table_name: "bench_table".to_string(),
            num_rows,
            num_columns: BENCH_NUM_COLUMNS,
        };

        // Initialize backend outside timing loop
        rt.block_on(backend.init(&config))
            .expect("Backend init failed");

        let mut group = c.benchmark_group(format!("fetch/rows_{}/{}", num_rows, backend.name()));

        if num_rows >= 10_000_000 {
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(30));
            group.warm_up_time(Duration::from_secs(5));
        }

        for &num_keys in KEY_COUNTS {
            let keys = bench_generate_keys(num_keys, num_rows);

            group.throughput(Throughput::Elements(num_keys as u64));
            group.bench_with_input(BenchmarkId::new("keys", num_keys), &num_keys, |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = backend
                            .fetch(black_box(&keys), black_box(&columns))
                            .await
                            .unwrap();
                        black_box(result)
                    })
                })
            });
        }
        group.finish();

        // Cleanup within runtime context
        rt.block_on(backend.cleanup()).ok();
    }

    // Final cleanup - ensure backend is fully dropped within runtime context
    rt.block_on(async {
        // Backend will be dropped here within the async context
        drop(backend);
    });
}

fn bench_murr(c: &mut Criterion) {
    bench_backend(c, MurrBackend::new());
}

fn bench_redis_blob(c: &mut Criterion) {
    bench_backend(c, RedisBlobBackend::new());
}

fn bench_redis_feast(c: &mut Criterion) {
    bench_backend(c, RedisFeastBackend::new());
}

criterion_group!(benches, bench_murr, bench_redis_blob, bench_redis_feast);
criterion_main!(benches);
