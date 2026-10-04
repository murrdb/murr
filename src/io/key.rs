use arrow::array::{Array, BinaryArray, BinaryBuilder, RecordBatch};

use crate::{
    core::{MurrError, TableSchema},
    io::codec::KeyEncoder,
};

/// Encoded key columns of a batch, one `BinaryArray` per key component.
pub struct KeyBatch {
    rows: usize,
    columns: Vec<BinaryArray>,
}

impl KeyBatch {
    pub fn new(rows: usize) -> Self {
        Self {
            rows,
            columns: Vec::new(),
        }
    }

    pub fn append(&mut self, column: BinaryArray) -> Result<(), MurrError> {
        if column.len() != self.rows {
            return Err(MurrError::TableError(format!(
                "key column has {} rows, expected {}",
                column.len(),
                self.rows
            )));
        }
        self.columns.push(column);
        Ok(())
    }

    /// Concatenates the components of each row into a single key.
    pub fn finish(mut self) -> BinaryArray {
        // a single-column key is already its own encoded form, skip the copy
        if self.columns.len() == 1 {
            return self.columns.remove(0);
        }
        let bytes = self.columns.iter().map(|c| c.value_data().len()).sum();
        let mut keys = BinaryBuilder::with_capacity(self.rows, bytes);
        let mut key = Vec::new();
        for row in 0..self.rows {
            key.clear();
            for column in &self.columns {
                key.extend_from_slice(column.value(row));
            }
            keys.append_value(&key);
        }
        keys.finish()
    }
}

struct KeyColumn {
    name: String,
    encoder: Box<dyn KeyEncoder>,
}

/// Key-side counterpart of `SegmentSchema`: the key columns of a table in schema order.
pub struct KeySchema {
    columns: Vec<KeyColumn>,
}

impl KeySchema {
    pub fn new(table: &TableSchema) -> Result<Self, MurrError> {
        let columns: Vec<KeyColumn> = table
            .key_columns()
            .map(|(name, col)| {
                if col.nullable {
                    return Err(MurrError::TableError(format!(
                        "key column '{name}' cannot be nullable"
                    )));
                }
                let encoder = col
                    .dtype
                    .key_encoder()
                    .map_err(|e| MurrError::TableError(format!("key column '{name}': {e}")))?;
                Ok(KeyColumn {
                    name: name.clone(),
                    encoder,
                })
            })
            .collect::<Result<_, _>>()?;
        if columns.is_empty() {
            return Err(MurrError::TableError(
                "table must have at least one key column".into(),
            ));
        }
        Ok(Self { columns })
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn encode(&self, batch: &RecordBatch) -> Result<BinaryArray, MurrError> {
        let mut keys = KeyBatch::new(batch.num_rows());
        for column in &self.columns {
            let name = &column.name;
            let array = batch
                .column_by_name(name)
                .ok_or_else(|| MurrError::TableError(format!("missing key column '{name}'")))?;
            if array.null_count() > 0 {
                return Err(MurrError::TableError(format!(
                    "null in key column '{name}'"
                )));
            }
            let encoded = column
                .encoder
                .encode_keys(array.as_ref())
                .map_err(|e| MurrError::TableError(format!("key column '{name}': {e}")))?;
            keys.append(encoded)?;
        }
        Ok(keys.finish())
    }
}
