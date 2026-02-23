use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde_json::json;
use tokio::runtime::Runtime;

use murr::api::MurrHttpService;
use murr::testutil::{bench_column_names, bench_generate_keys};

mod benchutil;
use benchutil::{NUM_KEYS, NUM_ROWS};

fn bench_api_fetch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let col_names = bench_column_names();
    let (_dir, svc) = benchutil::setup_service(&rt);

    let api = MurrHttpService::new(std::sync::Arc::new(svc));
    let router = api.router();

    // Bind to an OS-assigned port and spawn the server.
    let port = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        port
    });

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/api/v1/table/bench/fetch");

    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);
    let fetch_body = json!({
        "keys": keys,
        "columns": col_names,
    });

    let mut group = c.benchmark_group(format!("api/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let url = url.clone();
            let fetch_body = fetch_body.clone();
            async move {
                let response = client
                    .post(&url)
                    .header("accept", "application/vnd.apache.arrow.stream")
                    .json(&fetch_body)
                    .send()
                    .await
                    .unwrap();
                let bytes = response.bytes().await.unwrap();
                black_box(bytes)
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_api_fetch);
criterion_main!(benches);
