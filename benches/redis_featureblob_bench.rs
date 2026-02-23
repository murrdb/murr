use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use redis::AsyncCommands;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use tokio::runtime::Runtime;

use murr::testutil::{bench_generate_keys, BENCH_NUM_COLUMNS};

mod benchutil;
use benchutil::{NUM_KEYS, NUM_ROWS};

const PIPELINE_BATCH: usize = 10_000;

fn bench_redis_featureblob(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Start Redis container.
    let (mut con, container) = rt.block_on(async {
        let container = Redis::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let con = client
            .get_multiplexed_tokio_connection()
            .await
            .unwrap();
        (con, container)
    });

    // Load 10M rows as feature blobs: key -> [f32; NUM_COLUMNS] as bytes.
    rt.block_on(async {
        for batch_start in (0..NUM_ROWS).step_by(PIPELINE_BATCH) {
            let batch_end = (batch_start + PIPELINE_BATCH).min(NUM_ROWS);
            let mut pipe = redis::pipe();
            for i in batch_start..batch_end {
                let key = i.to_string();
                let blob: Vec<u8> = (0..BENCH_NUM_COLUMNS)
                    .flat_map(|_| (i as f32).to_le_bytes())
                    .collect();
                pipe.set(key, blob).ignore();
            }
            pipe.query_async::<()>(&mut con).await.unwrap();
        }
    });

    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);

    let mut group = c.benchmark_group(format!("redis_featureblob/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let mut con = con.clone();
            let keys = keys.clone();
            async move {
                let result: Vec<Option<Vec<u8>>> = con.mget(&keys).await.unwrap();
                black_box(result)
            }
        })
    });

    group.finish();
    rt.block_on(async { drop(container) });
}

criterion_group!(benches, bench_redis_featureblob);
criterion_main!(benches);
