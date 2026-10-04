//! Wire formats of a fetch request. Each one is an adaptor converting into the
//! Arrow-native `FetchRequest`, which is the only shape the service layer sees.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::batch::{IpcStream, JsonColumns};
use crate::core::{FetchRequest, MurrError, TableSchema};

/// Key of the IPC stream schema metadata entry holding the JSON list of columns to return.
pub const COLUMNS_METADATA: &str = "columns";

/// Arrow IPC stream body: batches are the key columns, the columns to return
/// are listed in the schema metadata.
pub struct IpcFetchRequest<'a>(pub &'a [u8]);

impl TryFrom<IpcFetchRequest<'_>> for FetchRequest {
    type Error = MurrError;

    fn try_from(request: IpcFetchRequest<'_>) -> Result<Self, MurrError> {
        let keys = RecordBatch::try_from(IpcStream(request.0))?;
        let schema = keys.schema();
        let columns = schema.metadata().get(COLUMNS_METADATA).ok_or_else(|| {
            MurrError::TableError(format!(
                "missing '{COLUMNS_METADATA}' entry in Arrow schema metadata"
            ))
        })?;
        let columns: Vec<String> = serde_json::from_str(columns).map_err(|e| {
            MurrError::TableError(format!("invalid '{COLUMNS_METADATA}' metadata: {e}"))
        })?;
        Ok(FetchRequest { keys, columns })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonFetchRequest {
    pub keys: HashMap<String, Vec<Value>>,
    pub columns: Vec<String>,
}

/// JSON carries no types, so the key arrays are built by the dtypes of the table schema.
impl TryFrom<(JsonFetchRequest, &TableSchema)> for FetchRequest {
    type Error = MurrError;

    fn try_from((request, schema): (JsonFetchRequest, &TableSchema)) -> Result<Self, MurrError> {
        let keys = RecordBatch::try_from(JsonColumns {
            values: &request.keys,
            schema,
            columns: schema
                .key_columns()
                .map(|(name, _)| name.as_str())
                .collect(),
        })?;
        if keys.num_columns() != request.keys.len() {
            return Err(MurrError::TableError(
                "keys must contain only the key columns of the table".into(),
            ));
        }
        Ok(FetchRequest {
            keys,
            columns: request.columns,
        })
    }
}
