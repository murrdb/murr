#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::{Channel, Server};

use murr::api::MurrFlightService;
use murr::testutil::{bench_column_names, bench_generate_keys};

mod benchutil;
use benchutil::{NUM_KEYS, NUM_ROWS};

fn bench_flight_fetch(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let col_names = bench_column_names();
    let (_dir, svc) = benchutil::setup_service(&rt);

    let flight_svc = MurrFlightService::new(Arc::new(svc));

    let client = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).map(|result| {
                let conn = result?;
                conn.set_nodelay(true)?;
                Ok::<_, std::io::Error>(conn)
            });
            Server::builder()
                .add_service(FlightServiceServer::new(flight_svc))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        FlightServiceClient::new(channel)
    });

    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);
    let ticket_bytes = serde_json::to_vec(&serde_json::json!({
        "table": "bench",
        "keys": keys,
        "columns": col_names,
    }))
    .unwrap();

    let mut group = c.benchmark_group(format!("flight/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let mut client = client.clone();
            let ticket_bytes = ticket_bytes.clone();
            async move {
                let response = client.do_get(Ticket::new(ticket_bytes)).await.unwrap();
                let stream = FlightRecordBatchStream::new_from_flight_data(
                    response
                        .into_inner()
                        .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
                );
                let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                black_box(batches)
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_flight_fetch);
criterion_main!(benches);
