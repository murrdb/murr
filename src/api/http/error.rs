use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::core::MurrError;

pub struct ApiError(pub MurrError);

impl From<MurrError> for ApiError {
    fn from(err: MurrError) -> Self {
        ApiError(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            MurrError::TableNotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            MurrError::TableAlreadyExists(msg) => (StatusCode::CONFLICT, msg.clone()),
            MurrError::TableError(msg) | MurrError::SegmentError(msg) => {
                (StatusCode::BAD_REQUEST, msg.clone())
            }
            MurrError::IoError(msg)
            | MurrError::ArrowError(msg)
            | MurrError::ConfigParsingError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg.clone())
            }
        };

        (status, Json(json!({"error": message}))).into_response()
    }
}
