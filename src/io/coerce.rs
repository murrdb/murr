use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, RecordBatch, RecordBatchOptions},
    compute::{CastOptions, cast_with_options},
    datatypes::{Field, Schema},
};

use crate::core::{ColumnSchema, MurrError, TableSchema};

/// Casts the columns of an incoming batch to the dtypes of the table schema.
/// Which Arrow types a column accepts is declared by its dtype, see
/// `DType::widens_from` and `DType::rounds_from`.
pub struct Coercion {
    table: TableSchema,
}

impl Coercion {
    pub fn new(table: &TableSchema) -> Self {
        Self {
            table: table.clone(),
        }
    }

    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, MurrError> {
        let schema = batch.schema();
        let mut fields = Vec::with_capacity(batch.num_columns());
        let mut arrays = Vec::with_capacity(batch.num_columns());
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            let array = match self.table.columns.get(field.name()) {
                Some(column) => Self::cast(field.name(), column, array)?,
                None => array.clone(),
            };
            let dtype = array.data_type().clone();
            fields.push(Field::clone(field).with_data_type(dtype));
            arrays.push(array);
        }
        let schema = Schema::new_with_metadata(fields, schema.metadata().clone());
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::new(schema), arrays, &options)
            .map_err(|e| MurrError::TableError(e.to_string()))
    }

    fn cast(name: &str, column: &ColumnSchema, array: &ArrayRef) -> Result<ArrayRef, MurrError> {
        let codec = column.dtype.codec();
        let from = array.data_type();
        let to = codec.arrow_dtype();
        if *from == to {
            return Ok(array.clone());
        }
        let rounds = codec.rounds_from(from);
        if rounds && column.strict {
            return Err(MurrError::TableError(format!(
                "column '{name}': cast of {from} to {to} rounds values and needs 'strict: false'"
            )));
        }
        if !rounds && !codec.widens_from(from) {
            return Err(MurrError::TableError(format!(
                "column '{name}': expected {to}, got {from}"
            )));
        }
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        cast_with_options(array, &to, &options)
            .map_err(|e| MurrError::TableError(format!("column '{name}': {e}")))
    }
}
