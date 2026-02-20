use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::core::MurrError;

use super::types::ErrorResponse;

/// API-specific errors with HTTP status code mapping.
#[derive(Debug)]
pub enum ApiError {
    TableNotFound(String),
    InvalidRequest(String),
    Internal(String),
}

impl From<MurrError> for ApiError {
    fn from(err: MurrError) -> Self {
        match &err {
            MurrError::TableError(msg) => ApiError::InvalidRequest(msg.clone()),
            _ => ApiError::Internal(err.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::TableNotFound(name) => (
                StatusCode::NOT_FOUND,
                "TABLE_NOT_FOUND",
                format!("Table '{}' not found or not yet loaded", name),
            ),
            ApiError::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, "INVALID_REQUEST", msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg),
        };

        let body = ErrorResponse {
            error: message,
            code: code.to_string(),
        };

        (status, Json(body)).into_response()
    }
}
