use std::io::Cursor;
use std::sync::{Arc, LazyLock};


use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::core::{MurrError, TableSchema};
use crate::service::MurrService;

use super::convert::{FetchResponse, WriteRequest, apply_schema_casts};
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

pub async fn list_tables(
    State(service): State<Arc<MurrService>>,
) -> Result<Json<std::collections::HashMap<String, TableSchema>>, ApiError> {
    let svc = service.clone();
    let tables = tokio::task::spawn_blocking(move || svc.list_tables())
        .await
        .map_err(join_to_api_error)?;
    Ok(Json(tables))
}

pub async fn get_schema(
    State(service): State<Arc<MurrService>>,
    Path(name): Path<String>,
) -> Result<Json<TableSchema>, ApiError> {
    let svc = service.clone();
    let schema = tokio::task::spawn_blocking(move || svc.get_schema(&name))
        .await
        .map_err(join_to_api_error)??;
    Ok(Json(schema))
}

pub async fn create_table(
    State(service): State<Arc<MurrService>>,
    Path(name): Path<String>,
    Json(schema): Json<TableSchema>,
) -> Result<StatusCode, ApiError> {
    let svc = service.clone();
    tokio::task::spawn_blocking(move || svc.create(&name, schema))
        .await
        .map_err(join_to_api_error)??;
    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
pub struct FetchRequest {
    pub keys: Vec<String>,
    pub columns: Vec<String>,
}

pub async fn fetch(
    State(service): State<Arc<MurrService>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(req): Json<FetchRequest>,
) -> Result<Response, ApiError> {
    let wants_arrow = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains(ARROW_IPC_MIME));

    let svc = service.clone();
    tokio::task::spawn_blocking(move || -> Result<Response, ApiError> {
        let keys: Vec<&str> = req.keys.iter().map(String::as_str).collect();
        let columns: Vec<&str> = req.columns.iter().map(String::as_str).collect();
        let batch = svc.read(&name, &keys, &columns)?;

        if wants_arrow {
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                    .map_err(|e| ApiError(e.into()))?;
                writer.write(&batch).map_err(|e| ApiError(e.into()))?;
                writer.finish().map_err(|e| ApiError(e.into()))?;
            }
            Ok(([(axum::http::header::CONTENT_TYPE, ARROW_IPC_MIME)], buf).into_response())
        } else {
            let FetchResponse(json) = FetchResponse::try_from(&batch).map_err(ApiError)?;
            Ok(Json(json).into_response())
        }
    })
    .await
    .map_err(join_to_api_error)?
}

pub async fn write_table(
    State(service): State<Arc<MurrService>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let svc = service.clone();
    tokio::task::spawn_blocking(move || -> Result<StatusCode, ApiError> {
        let table_schema = svc.get_schema(&name)?;

        let batch = if content_type.contains(ARROW_IPC_MIME) {
            let cursor = Cursor::new(&body);
            let mut reader = StreamReader::try_new(cursor, None)
                .map_err(|e| ApiError(e.into()))?;
            reader
                .next()
                .ok_or_else(|| ApiError(MurrError::TableError("empty Arrow IPC stream".into())))?
                .map_err(|e| ApiError(e.into()))?
        } else if content_type.contains(PARQUET_MIME) {
            let reader = ParquetRecordBatchReaderBuilder::try_new(body)
                .map_err(|e| ApiError(MurrError::TableError(format!("invalid Parquet: {e}"))))?
                .build()
                .map_err(|e| ApiError(MurrError::TableError(format!("invalid Parquet: {e}"))))?;
            let batches: Vec<_> = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| ApiError(e.into()))?;
            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| ApiError(e.into()))?
        } else {
            let write: WriteRequest = serde_json::from_slice(&body)
                .map_err(|e| ApiError(MurrError::TableError(format!("invalid JSON: {e}"))))?;
            write.into_record_batch(&table_schema).map_err(ApiError)?
        };

        let batch = apply_schema_casts(batch, &table_schema).map_err(ApiError)?;
        svc.write(&name, &batch)?;
        Ok(StatusCode::OK)
    })
    .await
    .map_err(join_to_api_error)?
}

fn join_to_api_error(e: tokio::task::JoinError) -> ApiError {
    ApiError(MurrError::IoError(format!("blocking task failed: {e}")))
}
