use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};

use crate::{
    core::MurrError,
    io3::{
        column::{ColumnCodec, downcast},
        model::SegmentColumnSchema,
        row::Row,
    },
};

pub struct Utf8Codec;

impl ColumnCodec for Utf8Codec {
    fn encode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        array: &dyn Array,
        rows: &mut [Row],
    ) -> Result<(), MurrError> {
        let data = downcast::<StringArray>(array, "Utf8")?;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows[index];
            match value {
                None => row.set_null(col.index as usize),
                Some(s) => row.set_dynamic_value(bitset_size, col.offset as usize, s.as_bytes()),
            }
        }
        Ok(())
    }

    fn decode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        rows: &[Row],
    ) -> Result<ArrayRef, MurrError> {
        let offset = col.offset as usize;
        let col_index = col.index as usize;
        let mut values: Vec<Option<&str>> = Vec::with_capacity(rows.len());
        for row in rows {
            if row.is_null(col_index) {
                values.push(None);
            } else {
                let bytes = row.get_dynamic_bytes(bitset_size, offset)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| MurrError::SegmentError(format!("invalid utf8: {e}")))?;
                values.push(Some(s));
            }
        }
        Ok(Arc::new(StringArray::from(values)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Float32Array;

    use super::*;
    use crate::{core::DType, io3::model::SegmentSchema};

    fn schema() -> SegmentSchema {
        SegmentSchema::new(&vec![SegmentColumnSchema {
            index: 0,
            dtype: DType::Utf8,
            name: "s".into(),
            offset: 0,
        }])
    }

    fn fresh_rows(schema: &SegmentSchema, n: usize) -> Vec<Row> {
        (0..n)
            .map(|_| Row::new(schema, schema.bitset_size, schema.capacity))
            .collect()
    }

    #[test]
    fn roundtrip_with_nulls_empty_and_unicode() {
        let codec = Utf8Codec;
        let schema = schema();
        let array = StringArray::from(vec![
            Some("hello"),
            None,
            Some(""),
            Some("δ-unicode"),
            Some("world"),
        ]);
        let mut rows = fresh_rows(&schema, array.len());
        codec
            .encode(&schema.columns[0], schema.bitset_size, &array, &mut rows)
            .unwrap();
        let decoded = codec
            .decode(&schema.columns[0], schema.bitset_size, &rows)
            .unwrap();
        let back = decoded.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(back, &array);
    }

    #[test]
    fn decode_rejects_invalid_utf8() {
        let codec = Utf8Codec;
        let schema = schema();
        let mut rows = fresh_rows(&schema, 1);
        rows[0].set_dynamic_value(schema.bitset_size, 0, &[0xFF, 0xFE, 0xFD]);
        let err = codec.decode(&schema.columns[0], schema.bitset_size, &rows);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn encode_rejects_wrong_array_type() {
        let codec = Utf8Codec;
        let schema = schema();
        let wrong = Float32Array::from(vec![Some(1.0_f32)]);
        let mut rows = fresh_rows(&schema, 1);
        let err = codec.encode(&schema.columns[0], schema.bitset_size, &wrong, &mut rows);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
