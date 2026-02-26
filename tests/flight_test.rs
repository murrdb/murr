use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightDescriptor, FlightData, Ticket};
use futures::TryStreamExt;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};

use murr::conf::{Config, StorageConfig};
use murr::core::{ColumnSchema, DType, TableSchema};
use murr::service::MurrService;

/// Guard that shuts down the Flight server when dropped.
struct ServerGuard {
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

struct TestHarness {
    _dir: TempDir,
    _guard: ServerGuard,
    client: FlightServiceClient<Channel>,
}

async fn setup() -> TestHarness {
    let dir = TempDir::new().unwrap();
    let config = Config {
        storage: StorageConfig {
            cache_dir: dir.path().to_path_buf(),
        },
        ..Config::default()
    };
    let service = Arc::new(MurrService::new(config).await.unwrap());

    // Create and populate a table
    let schema = TableSchema {
        key: "id".to_string(),
        columns: HashMap::from([
            (
                "id".to_string(),
                ColumnSchema {
                    dtype: DType::Utf8,
                    nullable: false,
                },
            ),
            (
                "score".to_string(),
                ColumnSchema {
                    dtype: DType::Float32,
                    nullable: true,
                },
            ),
        ]),
    };
    service.create("features", schema).await.unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
    ]));
    let ids: StringArray = vec![Some("a"), Some("b"), Some("c")].into_iter().collect();
    let scores: Float32Array = vec![Some(1.0), Some(2.0), None].into_iter().collect();
    let batch =
        RecordBatch::try_new(arrow_schema, vec![Arc::new(ids), Arc::new(scores)]).unwrap();
    service.write("features", &batch).await.unwrap();

    // Start Flight server on OS-assigned port with shutdown signal
    let flight_svc = murr::api::MurrFlightService::new(service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async { let _ = shutdown_rx.await; },
            )
            .await
            .unwrap();
    });

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = FlightServiceClient::new(channel);

    TestHarness {
        _dir: dir,
        _guard: ServerGuard { _shutdown: shutdown_tx },
        client,
    }
}

#[tokio::test]
async fn test_do_get_round_trip() {
    let mut harness = setup().await;

    let ticket = serde_json::to_vec(&serde_json::json!({
        "table": "features",
        "keys": ["a", "b", "c"],
        "columns": ["score"]
    }))
    .unwrap();

    let response = harness.client.do_get(Ticket::new(ticket)).await.unwrap();
    let stream = FlightRecordBatchStream::new_from_flight_data(
        response.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    );
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);

    let scores = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.0);
    assert_eq!(scores.value(1), 2.0);
    assert!(scores.is_null(2));
}

#[tokio::test]
async fn test_do_get_not_found() {
    let mut harness = setup().await;

    let ticket = serde_json::to_vec(&serde_json::json!({
        "table": "nonexistent",
        "keys": ["a"],
        "columns": ["score"]
    }))
    .unwrap();

    let result = harness.client.do_get(Ticket::new(ticket)).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_do_get_invalid_ticket() {
    let mut harness = setup().await;

    let result = harness.client.do_get(Ticket::new(b"not json".to_vec())).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_list_flights() {
    let mut harness = setup().await;

    let response = harness
        .client
        .list_flights(arrow_flight::Criteria::default())
        .await
        .unwrap();
    let infos: Vec<arrow_flight::FlightInfo> =
        response.into_inner().try_collect().await.unwrap();

    assert_eq!(infos.len(), 1);
    let info = &infos[0];
    assert_eq!(
        info.flight_descriptor.as_ref().unwrap().path,
        vec!["features"]
    );

    let schema = Schema::try_from(info.clone()).unwrap();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"score"));
}

#[tokio::test]
async fn test_get_flight_info() {
    let mut harness = setup().await;

    let descriptor = FlightDescriptor::new_path(vec!["features".to_string()]);
    let info = harness.client.get_flight_info(descriptor).await.unwrap().into_inner();

    let schema = Schema::try_from(info).unwrap();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"score"));
}

#[tokio::test]
async fn test_get_flight_info_has_key_metadata() {
    let mut harness = setup().await;

    let descriptor = FlightDescriptor::new_path(vec!["features".to_string()]);
    let info = harness.client.get_flight_info(descriptor).await.unwrap().into_inner();

    let schema = Schema::try_from(info).unwrap();
    assert_eq!(schema.metadata().get("key").map(|s| s.as_str()), Some("id"));
}

#[tokio::test]
async fn test_get_schema() {
    let mut harness = setup().await;

    let descriptor = FlightDescriptor::new_path(vec!["features".to_string()]);
    let result = harness.client.get_schema(descriptor).await.unwrap().into_inner();

    let schema = Schema::try_from(&result).unwrap();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"score"));
}

#[tokio::test]
async fn test_do_put_unimplemented() {
    let mut harness = setup().await;

    let result = harness
        .client
        .do_put(futures::stream::empty::<FlightData>())
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
}
