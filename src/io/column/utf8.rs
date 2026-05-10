use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};

use crate::{
    core::MurrError,
    io::{
        column::{ColumnDecoder, ColumnEncoder, downcast},
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub struct Utf8Encoder {
    column: SegmentColumnSchema,
    builder: StringBuilder,
}

impl Utf8Encoder {
    pub fn new(column: SegmentColumnSchema, rows: usize) -> Self {
        Self {
            column,
            builder: StringBuilder::with_capacity(rows, rows * 16),
        }
    }
}

impl ColumnEncoder for Utf8Encoder {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError> {
        if row.is_null(&self.column) {
            self.builder.append_null();
        } else {
            let bytes = row.read_dynamic(&self.column);
            let s = std::str::from_utf8(bytes)
                .map_err(|e| MurrError::SegmentError(format!("invalid utf8: {e}")))?;
            self.builder.append_value(s);
        }
        Ok(())
    }
    fn add_empty(&mut self) -> Result<(), MurrError> {
        self.builder.append_null();
        Ok(())
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub struct Utf8Decoder {
    column: SegmentColumnSchema,
    array: StringArray,
}

impl Utf8Decoder {
    pub fn new(column: SegmentColumnSchema, array: &dyn Array) -> Result<Self, MurrError> {
        let typed = downcast::<StringArray>(array, "Utf8")?;
        Ok(Self {
            column,
            array: typed.clone(),
        })
    }
}

impl ColumnDecoder for Utf8Decoder {
    fn write_to_row(&self, index: usize, row: &mut WriteRow) {
        if !self.array.is_null(index) {
            row.write_dynamic(&self.column, self.array.value(index).as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Float32Array;

    use super::*;
    use crate::{
        core::DType,
        io::{row::read::ReadRow, schema::SegmentSchema},
    };

    fn single() -> (SegmentSchema, SegmentColumnSchema) {
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DType::Utf8,
            name: "s".into(),
            offset: 0,
        };
        (SegmentSchema::new(std::slice::from_ref(&c)), c)
    }

    #[test]
    fn roundtrip_with_nulls_empty_unicode() {
        let (schema, c) = single();
        let input = StringArray::from(vec![
            Some("hello"),
            None,
            Some(""),
            Some("δ-unicode"),
            Some("world"),
        ]);

        let dec = Utf8Decoder::new(c.clone(), &input).unwrap();
        let bufs: Vec<Vec<u8>> = (0..input.len())
            .map(|i| {
                let mut w = WriteRow::new(&schema, "");
                dec.write_to_row(i, &mut w);
                w.bytes
            })
            .collect();

        let mut enc = Utf8Encoder::new(c, input.len());
        for b in &bufs {
            enc.add_row(&ReadRow::new(&schema, b)).unwrap();
        }
        let out_arr = enc.build();
        assert_eq!(
            out_arr.as_any().downcast_ref::<StringArray>().unwrap(),
            &input
        );
    }

    #[test]
    fn encoder_rejects_invalid_utf8() {
        let (schema, c) = single();
        let mut w = WriteRow::new(&schema, "");
        w.write_dynamic(&c, &[0xFF, 0xFE, 0xFD]);
        let row = ReadRow::new(&schema, &w.bytes);

        let mut enc = Utf8Encoder::new(c, 1);
        let err = enc.add_row(&row);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn decoder_rejects_wrong_array_type() {
        let (_schema, c) = single();
        let wrong = Float32Array::from(vec![Some(1.0_f32)]);
        let err = Utf8Decoder::new(c, &wrong);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
