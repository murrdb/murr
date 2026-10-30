use arrow::record_batch::RecordBatch;

/// A batch lookup: one row of `keys` per key to look up, holding exactly the key columns
/// of the table, and the value columns to return for them. The response holds only the
/// keys that were found, each tagged with its position in `keys` (see `IDX_COLUMN`).
pub struct FetchRequest {
    pub keys: RecordBatch,
    pub columns: Vec<String>,
}
