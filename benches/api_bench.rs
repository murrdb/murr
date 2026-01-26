use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use http_body_util::BodyExt;
use tower::ServiceExt;

use murr::api::{create_router, AppState};
use murr::manager::TableManager;
use murr::testutil::{bench_column_names, bench_generate_keys, setup_benchmark_table};

const ROW_COUNTS: &[usize] = &[100_000, 1_000_000, 10_000_000];
const KEY_COUNTS: &[usize] = &[10, 100, 1000];

fn bench_api_fetch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let columns = bench_column_names();

    for &num_rows in ROW_COUNTS {
        let (state, temp_dir) = rt.block_on(setup_benchmark_table("bench_table", num_rows));

        let manager = Arc::new(TableManager::new(temp_dir.path().join("data")));
        rt.block_on(manager.insert("bench_table".to_string(), state));
        let app_state = AppState { manager };

        let mut group = c.benchmark_group(format!("api/rows_{}", num_rows));

        if num_rows >= 10_000_000 {
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(30));
            group.warm_up_time(Duration::from_secs(5));
        }

        for &num_keys in KEY_COUNTS {
            let keys = bench_generate_keys(num_keys, num_rows);
            let request_body = serde_json::json!({
                "keys": keys,
                "columns": columns,
            })
            .to_string();

            group.throughput(Throughput::Elements(num_keys as u64));
            group.bench_with_input(BenchmarkId::new("keys", num_keys), &num_keys, |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let app = create_router(app_state.clone());
                        let request = Request::builder()
                            .method("POST")
                            .uri("/v1/bench_table/_fetch")
                            .header(header::CONTENT_TYPE, "application/json")
                            .header(header::ACCEPT, "application/vnd.apache.arrow.stream")
                            .body(Body::from(request_body.clone()))
                            .unwrap();

                        let response = app.oneshot(request).await.unwrap();
                        let body = response.into_body().collect().await.unwrap().to_bytes();
                        black_box(body)
                    })
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_api_fetch);
criterion_main!(benches);
