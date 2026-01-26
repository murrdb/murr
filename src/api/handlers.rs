use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::http::header::ACCEPT;
use axum::response::{IntoResponse, Response};

use super::error::ApiError;
use super::response::{ARROW_IPC_CONTENT_TYPE, arrow_ipc_response, json_response};
use super::state::AppState;
use super::types::{FetchRequest, HealthResponse};

/// POST /v1/:table/_fetch
///
/// Fetch rows by keys from a table. Returns either JSON or Arrow IPC
/// based on the Accept header.
pub async fn fetch_handler(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Result<Response, ApiError> {
    // Get table from manager
    let table = state
        .manager
        .get(&table_name)
        .await
        .ok_or_else(|| ApiError::TableNotFound(table_name.clone()))?;

    // Convert keys to &str for Table::get
    let keys: Vec<&str> = request.keys.iter().map(|s| s.as_str()).collect();
    let columns: Vec<&str> = request.columns.iter().map(|s| s.as_str()).collect();

    // Execute query
    let batch = table.get(&keys, &columns)?;

    // Content negotiation based on Accept header
    let wants_arrow = headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| {
            s.contains(ARROW_IPC_CONTENT_TYPE) || s.contains("application/octet-stream")
        });

    if wants_arrow {
        arrow_ipc_response(batch)
    } else {
        json_response(batch)
    }
}

/// GET /health
///
/// Health check endpoint returning server status.
pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let tables_loaded = state.manager.table_count().await;

    Json(HealthResponse {
        status: "healthy".to_string(),
        tables_loaded,
    })
}
