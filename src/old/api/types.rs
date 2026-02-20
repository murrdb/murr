use serde::{Deserialize, Serialize};

/// Request body for the fetch endpoint.
#[derive(Debug, Deserialize)]
pub struct FetchRequest {
    pub keys: Vec<String>,
    pub columns: Vec<String>,
}

/// JSON response format for the fetch endpoint (used for debugging).
#[derive(Debug, Serialize)]
pub struct FetchResponseJson {
    pub num_rows: usize,
    pub columns: Vec<ColumnData>,
}

/// Column data in the JSON response.
#[derive(Debug, Serialize)]
pub struct ColumnData {
    pub name: String,
    pub values: Vec<serde_json::Value>,
}

/// Error response format.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub tables_loaded: usize,
}
