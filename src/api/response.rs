use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;
use serde_json::Value;

use super::error::ApiError;
use super::types::{ColumnData, FetchResponseJson};

/// Arrow IPC content type.
pub const ARROW_IPC_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Serialize a RecordBatch to Arrow IPC streaming format.
pub fn arrow_ipc_response(batch: RecordBatch) -> Result<Response, ApiError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| ApiError::Internal(e.to_string()))?;
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ARROW_IPC_CONTENT_TYPE)
        .body(Body::from(buffer))
        .map_err(|e| ApiError::Internal(e.to_string()))
}

/// Serialize a RecordBatch to JSON format (human-readable, for debugging).
pub fn json_response(batch: RecordBatch) -> Result<Response, ApiError> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    let columns: Vec<ColumnData> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let array = batch.column(idx);
            let values = (0..num_rows)
                .map(|row| array_value_to_json(array.as_ref(), row))
                .collect();

            ColumnData {
                name: field.name().clone(),
                values,
            }
        })
        .collect();

    let response = FetchResponseJson { num_rows, columns };
    let json = serde_json::to_string(&response).map_err(|e| ApiError::Internal(e.to_string()))?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .map_err(|e| ApiError::Internal(e.to_string()))
}

/// Convert an Arrow array value at the given index to a JSON Value.
fn array_value_to_json(array: &dyn Array, idx: usize) -> Value {
    if array.is_null(idx) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(idx))
        }
        _ => Value::Null,
    }
}
