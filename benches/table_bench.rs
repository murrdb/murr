use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use tokio::runtime::Runtime;

use murr::testutil::{bench_column_names, bench_generate_keys};

mod benchutil;
use benchutil::{NUM_KEYS, NUM_ROWS};

fn bench_table_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let col_names = bench_column_names();
    let (_dir, svc) = benchutil::setup_service(&rt);
    let svc = Arc::new(svc);

    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group(format!("table/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let svc = svc.clone();
            let key_refs = key_refs.clone();
            let col_refs = col_refs.clone();
            async move {
                svc.read("bench", black_box(&key_refs), black_box(&col_refs))
                    .await
                    .unwrap()
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_table_get);
criterion_main!(benches);
