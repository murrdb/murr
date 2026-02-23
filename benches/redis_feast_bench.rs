use std::collections::HashMap;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use tokio::runtime::Runtime;

use murr::testutil::{bench_column_names, bench_generate_keys};

mod benchutil;
use benchutil::{NUM_KEYS, NUM_ROWS};

const PIPELINE_BATCH: usize = 10_000;

fn bench_redis_feast(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let col_names = bench_column_names();

    // Start Redis container.
    let (mut con, container) = rt.block_on(async {
        let container = Redis::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let con: redis::aio::MultiplexedConnection = client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        (con, container)
    });

    // Load 10M rows as Feast-style HSETs: key -> {col0: f32_bytes, col1: f32_bytes, ...}
    rt.block_on(async {
        for batch_start in (0..NUM_ROWS).step_by(PIPELINE_BATCH) {
            let batch_end = (batch_start + PIPELINE_BATCH).min(NUM_ROWS);
            let mut pipe = redis::pipe();
            for i in batch_start..batch_end {
                let key = i.to_string();
                let fields: Vec<(&str, Vec<u8>)> = col_names
                    .iter()
                    .map(|name| (name.as_str(), (i as f32).to_le_bytes().to_vec()))
                    .collect();
                pipe.hset_multiple(key, &fields).ignore();
            }
            pipe.query_async::<()>(&mut con).await.unwrap();
        }
    });

    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);

    let mut group = c.benchmark_group(format!("redis_feast/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let mut con = con.clone();
            let keys = keys.clone();
            async move {
                let mut pipe = redis::pipe();
                for key in &keys {
                    pipe.hgetall(key);
                }
                let result: Vec<HashMap<String, Vec<u8>>> =
                    pipe.query_async(&mut con).await.unwrap();
                black_box(result)
            }
        })
    });

    group.finish();
    rt.block_on(async { drop(container) });
}

criterion_group!(benches, bench_redis_feast);
criterion_main!(benches);
