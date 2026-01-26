mod common;

use std::sync::Arc;

use arrow::array::Float32Array;
use arrow::ipc::reader::StreamReader;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use tower::ServiceExt;

use common::setup_test_table;
use murr::api::{AppState, create_router};
use murr::manager::TableManager;

/// Create an AppState with a pre-loaded table for testing.
async fn create_test_app_state(table_name: &str, num_rows: usize) -> (AppState, tempfile::TempDir) {
    let (state, temp_dir) = setup_test_table(table_name, num_rows).await;

    let manager = Arc::new(TableManager::new(temp_dir.path().join("data")));
    manager.insert(table_name.to_string(), state).await;

    (AppState { manager }, temp_dir)
}

/// Test fetch endpoint with JSON response (default).
#[tokio::test]
async fn test_fetch_json_response() {
    let (state, _temp_dir) = create_test_app_state("test_table", 1000).await;
    let app = create_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/v1/test_table/_fetch")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"keys": ["0", "500", "999"], "columns": ["value"]}"#,
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["num_rows"], 3);
    assert_eq!(json["columns"].as_array().unwrap().len(), 1);
    assert_eq!(json["columns"][0]["name"], "value");

    let values = json["columns"][0]["values"].as_array().unwrap();
    assert_eq!(values[0], 0.0);
    assert_eq!(values[1], 500.0);
    assert_eq!(values[2], 999.0);
}

/// Test fetch endpoint with Arrow IPC response.
#[tokio::test]
async fn test_fetch_arrow_ipc_response() {
    let (state, _temp_dir) = create_test_app_state("test_table", 1000).await;
    let app = create_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/v1/test_table/_fetch")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/vnd.apache.arrow.stream")
        .body(Body::from(
            r#"{"keys": ["0", "500", "999"], "columns": ["value"]}"#,
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/vnd.apache.arrow.stream"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();

    // Parse the Arrow IPC stream
    let cursor = std::io::Cursor::new(body.to_vec());
    let reader = StreamReader::try_new(cursor, None).unwrap();

    let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 1);

    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(values.value(0), 0.0);
    assert_eq!(values.value(1), 500.0);
    assert_eq!(values.value(2), 999.0);
}

/// Test fetch endpoint returns 404 for non-existent table.
#[tokio::test]
async fn test_fetch_table_not_found() {
    let (state, _temp_dir) = create_test_app_state("test_table", 100).await;
    let app = create_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/v1/nonexistent/_fetch")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"keys": ["0"], "columns": ["value"]}"#))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["code"], "TABLE_NOT_FOUND");
}

/// Test health endpoint.
#[tokio::test]
async fn test_health_endpoint() {
    let (state, _temp_dir) = create_test_app_state("test_table", 100).await;
    let app = create_router(state);

    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");
    assert_eq!(json["tables_loaded"], 1);
}
