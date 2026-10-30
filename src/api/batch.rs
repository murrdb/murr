//! Wire payload to `RecordBatch` conversions shared by the write and fetch paths.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use axum::body::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

use crate::core::{MurrError, TableSchema};

/// Body of an Arrow IPC stream. Converts into a single batch which keeps the
/// stream schema and its metadata.
pub struct IpcStream<'a>(pub &'a [u8]);

impl TryFrom<IpcStream<'_>> for RecordBatch {
    type Error = MurrError;

    fn try_from(stream: IpcStream<'_>) -> Result<Self, MurrError> {
        let invalid = |e| MurrError::TableError(format!("invalid Arrow IPC stream: {e}"));
        let reader = StreamReader::try_new(Cursor::new(stream.0), None).map_err(invalid)?;
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().map_err(invalid)?;
        concat_batches(&schema, &batches).map_err(invalid)
    }
}

/// Body of a Parquet file. Converts into a single batch of all its row groups.
pub struct ParquetFile(pub Bytes);

impl TryFrom<ParquetFile> for RecordBatch {
    type Error = MurrError;

    fn try_from(file: ParquetFile) -> Result<Self, MurrError> {
        let invalid = |e| MurrError::TableError(format!("invalid Parquet: {e}"));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.0).map_err(invalid)?;
        let schema = builder.schema().clone();
        let reader = builder.build().map_err(invalid)?;
        let batches: Vec<RecordBatch> = reader
            .collect::<Result<_, _>>()
            .map_err(|e| MurrError::TableError(format!("invalid Parquet: {e}")))?;
        concat_batches(&schema, &batches)
            .map_err(|e| MurrError::TableError(format!("invalid Parquet: {e}")))
    }
}

/// A JSON column map and the names of the table columns to build out of it.
pub struct JsonColumns<'a> {
    pub values: &'a HashMap<String, Vec<Value>>,
    pub schema: &'a TableSchema,
    pub columns: Vec<&'a str>,
}

impl TryFrom<JsonColumns<'_>> for RecordBatch {
    type Error = MurrError;

    fn try_from(json: JsonColumns<'_>) -> Result<Self, MurrError> {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for name in json.columns {
            let config = json
                .schema
                .columns
                .get(name)
                .ok_or_else(|| MurrError::TableError(format!("unknown column '{name}'")))?;
            let values = json
                .values
                .get(name)
                .ok_or_else(|| MurrError::TableError(format!("missing column '{name}'")))?;
            let codec = config.dtype.codec();
            fields.push(Field::new(name, codec.arrow_dtype(), config.nullable));
            let array = codec
                .from_json(values)
                .map_err(|e| MurrError::TableError(format!("column '{name}': {e}")))?;
            arrays.push(array);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| MurrError::TableError(e.to_string()))
    }
}
