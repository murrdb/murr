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
        let status = match &self.0 {
            MurrError::TableNotFound(_) => StatusCode::NOT_FOUND,
            MurrError::TableAlreadyExists(_) => StatusCode::CONFLICT,
            MurrError::TableError(_) | MurrError::SegmentError(_) => StatusCode::BAD_REQUEST,
            MurrError::IoError(_)
            | MurrError::ArrowError(_)
            | MurrError::ConfigParsingError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.0.to_string();

        (status, Json(json!({"error": message}))).into_response()
    }
}
