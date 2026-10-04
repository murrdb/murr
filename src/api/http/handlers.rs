use std::sync::{Arc, LazyLock};

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use mime::Mime;

use crate::api::batch::{IpcStream, ParquetFile};
use crate::api::fetch::{IpcFetchRequest, JsonFetchRequest};
use crate::core::{FetchRequest, MurrError, TableSchema};
use crate::io::store::Store;
use crate::service::MurrService;

use super::convert::{FetchResponse, WriteRequest};
use super::error::ApiError;

const ARROW_IPC_MIME: &str = "application/vnd.apache.arrow.stream";
const PARQUET_MIME: &str = "application/vnd.apache.parquet";

static OPENAPI_JSON: LazyLock<serde_json::Value> = LazyLock::new(|| {
    let yaml = include_str!("../../../openapi.yaml");
    serde_yaml_ng::from_str(yaml).expect("openapi.yaml must be valid YAML")
});

pub async fn openapi() -> Json<serde_json::Value> {
    Json(OPENAPI_JSON.clone())
}

pub async fn health() -> &'static str {
    "OK"
}

pub async fn list_tables<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
) -> Result<Json<std::collections::HashMap<String, TableSchema>>, ApiError> {
    let svc = service.clone();
    let tables = tokio::task::spawn_blocking(move || svc.list_tables())
        .await
        .map_err(join_to_api_error)?;
    Ok(Json(tables))
}

pub async fn get_schema<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
    Path(name): Path<String>,
) -> Result<Json<TableSchema>, ApiError> {
    let svc = service.clone();
    let schema = tokio::task::spawn_blocking(move || svc.get_schema(&name))
        .await
        .map_err(join_to_api_error)??;
    Ok(Json(schema))
}

pub async fn create_table<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
    Path(name): Path<String>,
    Json(schema): Json<TableSchema>,
) -> Result<StatusCode, ApiError> {
    let svc = service.clone();
    tokio::task::spawn_blocking(move || svc.create(&name, schema))
        .await
        .map_err(join_to_api_error)??;
    Ok(StatusCode::CREATED)
}

pub async fn drop_table<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let svc = service.clone();
    tokio::task::spawn_blocking(move || svc.drop_table(&name))
        .await
        .map_err(join_to_api_error)??;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn fetch<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let wants_arrow = headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains(ARROW_IPC_MIME));
    let content_type = ContentType::try_from(&headers)?;

    let svc = service.clone();
    tokio::task::spawn_blocking(move || -> Result<Response, ApiError> {
        let request = match content_type {
            ContentType::ArrowIpc => FetchRequest::try_from(IpcFetchRequest(&body))?,
            ContentType::Json => {
                let json: JsonFetchRequest = serde_json::from_slice(&body)
                    .map_err(|e| ApiError(MurrError::TableError(format!("invalid JSON: {e}"))))?;
                let schema = svc.get_schema(&name)?;
                FetchRequest::try_from((json, &schema))?
            }
            ContentType::Parquet => {
                return Err(ApiError(MurrError::TableError(
                    "fetch requests cannot be sent as Parquet".into(),
                )));
            }
        };
        let batch = svc.read(&name, &request)?;

        if wants_arrow {
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                    .map_err(|e| ApiError(e.into()))?;
                writer.write(&batch).map_err(|e| ApiError(e.into()))?;
                writer.finish().map_err(|e| ApiError(e.into()))?;
            }
            Ok(([(CONTENT_TYPE, ARROW_IPC_MIME)], buf).into_response())
        } else {
            let FetchResponse(json) = FetchResponse::try_from(&batch).map_err(ApiError)?;
            Ok(Json(json).into_response())
        }
    })
    .await
    .map_err(join_to_api_error)?
}

pub async fn write_table<S: Store>(
    State(service): State<Arc<MurrService<S>>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let content_type = ContentType::try_from(&headers)?;

    let svc = service.clone();
    tokio::task::spawn_blocking(move || -> Result<StatusCode, ApiError> {
        let batch = match content_type {
            ContentType::ArrowIpc => RecordBatch::try_from(IpcStream(&body))?,
            ContentType::Parquet => RecordBatch::try_from(ParquetFile(body))?,
            ContentType::Json => {
                let write: WriteRequest = serde_json::from_slice(&body)
                    .map_err(|e| ApiError(MurrError::TableError(format!("invalid JSON: {e}"))))?;
                let schema = svc.get_schema(&name)?;
                write.into_record_batch(&schema)?
            }
        };

        svc.write(&name, &batch)?;
        Ok(StatusCode::OK)
    })
    .await
    .map_err(join_to_api_error)?
}

enum ContentType {
    Json,
    ArrowIpc,
    Parquet,
}

impl TryFrom<&HeaderMap> for ContentType {
    type Error = ApiError;

    fn try_from(headers: &HeaderMap) -> Result<Self, ApiError> {
        let unsupported = |value: &str| {
            ApiError(MurrError::TableError(format!(
                "unsupported content type '{value}'"
            )))
        };
        let value = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let mime: Mime = value.parse().map_err(|_| unsupported(value))?;
        match mime.essence_str() {
            ARROW_IPC_MIME => Ok(ContentType::ArrowIpc),
            PARQUET_MIME => Ok(ContentType::Parquet),
            json if json == mime::APPLICATION_JSON.essence_str() => Ok(ContentType::Json),
            _ => Err(unsupported(value)),
        }
    }
}

fn join_to_api_error(e: tokio::task::JoinError) -> ApiError {
    ApiError(MurrError::IoError(format!("blocking task failed: {e}")))
}
