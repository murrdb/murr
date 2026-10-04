use arrow::record_batch::RecordBatch;

/// A batch lookup: one row of `keys` per requested row, holding exactly the key columns
/// of the table, and the value columns to return for them.
pub struct FetchRequest {
    pub keys: RecordBatch,
    pub columns: Vec<String>,
}
