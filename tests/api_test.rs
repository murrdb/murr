use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

use murr::api::MurrApi;
use murr::service::MurrService;

fn setup() -> (TempDir, Router) {
    let dir = TempDir::new().unwrap();
    let service = MurrService::new(dir.path().to_path_buf());
    let api = MurrApi::new(service);
    let router = api.router();
    (dir, router)
}

async fn body_bytes(router: Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, bytes)
}

async fn body_json(router: Router, req: Request<Body>) -> (StatusCode, Value) {
    let (status, bytes) = body_bytes(router, req).await;
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

fn table_schema_json() -> Value {
    json!({
        "name": "features",
        "key": "id",
        "columns": {
            "id": {"dtype": "utf8", "nullable": false},
            "score": {"dtype": "float32", "nullable": true}
        }
    })
}

fn arrow_ipc_batch(keys: &[&str], scores: &[f32]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
    ]));
    let key_array: StringArray = keys.iter().map(|k| Some(*k)).collect();
    let score_array: Float32Array = scores.iter().map(|v| Some(*v)).collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(key_array), Arc::new(score_array)])
            .unwrap();

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    buf
}

#[tokio::test]
async fn test_openapi() {
    let (_dir, router) = setup();
    let req = Request::get("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (status, json) = body_json(router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["openapi"], "3.1.0");
    assert!(json["paths"].is_object());
    assert!(json["paths"]["/health"].is_object());
    assert!(json["paths"]["/api/v1/table/{name}/fetch"].is_object());
}

#[tokio::test]
async fn test_health() {
    let (_dir, router) = setup();
    let req = Request::get("/health").body(Body::empty()).unwrap();
    let (status, bytes) = body_bytes(router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"OK");
}

#[tokio::test]
async fn test_get_nonexistent_table() {
    let (_dir, router) = setup();
    let req = Request::get("/api/v1/table/nope")
        .body(Body::empty())
        .unwrap();
    let (status, _) = body_json(router, req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_create_duplicate_table() {
    let (_dir, router) = setup();
    let schema = serde_json::to_vec(&table_schema_json()).unwrap();

    let req = Request::put("/api/v1/table/features")
        .header("content-type", "application/json")
        .body(Body::from(schema.clone()))
        .unwrap();
    let (status, _) = body_bytes(router.clone(), req).await;
    assert_eq!(status, StatusCode::CREATED);

    let req = Request::put("/api/v1/table/features")
        .header("content-type", "application/json")
        .body(Body::from(schema))
        .unwrap();
    let (status, _) = body_bytes(router, req).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_list_and_get_table() {
    let (_dir, router) = setup();
    let schema = serde_json::to_vec(&table_schema_json()).unwrap();

    // Create table
    let req = Request::put("/api/v1/table/features")
        .header("content-type", "application/json")
        .body(Body::from(schema))
        .unwrap();
    let (status, _) = body_bytes(router.clone(), req).await;
    assert_eq!(status, StatusCode::CREATED);

    // List tables
    let req = Request::get("/api/v1/table")
        .body(Body::empty())
        .unwrap();
    let (status, json) = body_json(router.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.get("features").is_some());
    assert_eq!(json["features"]["key"], "id");

    // Get single table
    let req = Request::get("/api/v1/table/features")
        .body(Body::empty())
        .unwrap();
    let (status, json) = body_json(router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["key"], "id");
    assert!(json["columns"]["id"].is_object());
    assert!(json["columns"]["score"].is_object());
}

#[tokio::test]
async fn test_full_round_trip() {
    let (_dir, router) = setup();

    // 1. Create table
    let schema = serde_json::to_vec(&table_schema_json()).unwrap();
    let req = Request::put("/api/v1/table/features")
        .header("content-type", "application/json")
        .body(Body::from(schema))
        .unwrap();
    let (status, _) = body_bytes(router.clone(), req).await;
    assert_eq!(status, StatusCode::CREATED);

    // 2. Write segment 1 as JSON
    let write_json = json!({
        "columns": {
            "id": ["a", "b"],
            "score": [1.0, 2.0]
        }
    });
    let req = Request::put("/api/v1/table/features/write")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&write_json).unwrap()))
        .unwrap();
    let (status, _) = body_bytes(router.clone(), req).await;
    assert_eq!(status, StatusCode::OK);

    // 3. Write segment 2 as Arrow IPC
    let ipc_bytes = arrow_ipc_batch(&["c"], &[3.0]);
    let req = Request::put("/api/v1/table/features/write")
        .header("content-type", "application/vnd.apache.arrow.stream")
        .body(Body::from(ipc_bytes))
        .unwrap();
    let (status, _) = body_bytes(router.clone(), req).await;
    assert_eq!(status, StatusCode::OK);

    // 4. Fetch as JSON
    let fetch_body = json!({"keys": ["a", "b", "c"], "columns": ["score"]});
    let req = Request::post("/api/v1/table/features/fetch")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(Body::from(serde_json::to_vec(&fetch_body).unwrap()))
        .unwrap();
    let (status, json) = body_json(router.clone(), req).await;
    assert_eq!(status, StatusCode::OK);

    let scores = json["columns"]["score"].as_array().unwrap();
    assert_eq!(scores.len(), 3);
    assert_eq!(scores[0].as_f64().unwrap() as f32, 1.0);
    assert_eq!(scores[1].as_f64().unwrap() as f32, 2.0);
    assert_eq!(scores[2].as_f64().unwrap() as f32, 3.0);

    // 5. Fetch as Arrow IPC
    let req = Request::post("/api/v1/table/features/fetch")
        .header("content-type", "application/json")
        .header("accept", "application/vnd.apache.arrow.stream")
        .body(Body::from(serde_json::to_vec(&fetch_body).unwrap()))
        .unwrap();
    let (status, bytes) = body_bytes(router, req).await;
    assert_eq!(status, StatusCode::OK);

    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);

    let scores = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.0);
    assert_eq!(scores.value(1), 2.0);
    assert_eq!(scores.value(2), 3.0);
}
