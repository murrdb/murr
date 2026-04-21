use arrow::array::{Array, StringArray};

use crate::{
    core::MurrError,
    io3::{
        batch::RowBatch,
        column::{ArrayDecoder, ArrayEncoder},
        model::SegmentColumnSchema,
    },
};

impl ArrayDecoder for String {
    type A = StringArray;
    fn decode_to(column: &SegmentColumnSchema, rows: &RowBatch) -> Result<Self::A, MurrError> {
        let bitset_size = rows.schema.bitset_size as usize;
        let offset = column.offset as usize;
        let col_index = column.index as usize;
        let mut values: Vec<Option<&str>> = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            match row.is_null(col_index) {
                true => values.push(None),
                false => values.push(Some(row.get_dynamic_value(bitset_size, offset)?)),
            }
        }
        Ok(StringArray::from(values))
    }
}

impl ArrayEncoder for String {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError> {
        let data = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!("expected Utf8, got {:?}", array.data_type()))
            })?;

        let bitset_size = rows.schema.bitset_size;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows.rows[index];
            match value {
                None => row.set_null(column.index as usize),
                Some(s) => row.set_dynamic_value(
                    bitset_size as usize,
                    column.offset as usize,
                    s.as_bytes(),
                ),
            }
        }
        Ok(())
    }
}
