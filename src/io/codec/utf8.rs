use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, StringArray, StringBuilder},
    datatypes::DataType,
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, downcast},
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub struct Utf8Codec;

impl Codec for Utf8Codec {
    fn dtype(&self) -> DType {
        DType::Utf8
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Utf8
    }

    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        let typed = downcast::<StringArray>(arr, "Utf8")?;
        Ok((0..typed.len())
            .map(|i| {
                if typed.is_null(i) {
                    Value::Null
                } else {
                    Value::String(typed.value(i).to_string())
                }
            })
            .collect())
    }

    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        let arr: StringArray = vals
            .iter()
            .map(|v| match v {
                Value::Null => Ok(None),
                Value::String(s) => Ok(Some(s.as_str())),
                _ => Err(MurrError::TableError(format!("expected string, got {v}"))),
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(arr))
    }

    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(Utf8Encoder {
            column: col,
            builder: StringBuilder::with_capacity(rows, rows * 16),
        })
    }

    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        let typed = downcast::<StringArray>(arr, "Utf8")?;
        Ok(Box::new(Utf8Decoder {
            column: col,
            array: typed.clone(),
        }))
    }
}

struct Utf8Encoder {
    column: SegmentColumnSchema,
    builder: StringBuilder,
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

struct Utf8Decoder {
    column: SegmentColumnSchema,
    array: StringArray,
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
    use super::*;
    use crate::io::{codec::{codec_for, test_util::{assert_json_roundtrip, assert_row_roundtrip}}, schema::SegmentSchema};
    use arrow::array::Float32Array;
    use rstest::rstest;

    fn single_col() -> (SegmentSchema, SegmentColumnSchema) {
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DType::Utf8,
            name: "s".into(),
            offset: 0,
        };
        (SegmentSchema::new(std::slice::from_ref(&c)), c)
    }

    #[rstest]
    #[case::ascii(Some("hello"))]
    #[case::null(None)]
    #[case::empty(Some(""))]
    #[case::unicode(Some("δ-unicode"))]
    fn row_roundtrip(#[case] v: Option<&str>) {
        assert_row_roundtrip(DType::Utf8, &StringArray::from(vec![v]));
    }

    #[rstest]
    #[case::ascii(Some("hello"))]
    #[case::null(None)]
    #[case::empty(Some(""))]
    #[case::unicode(Some("δ-unicode"))]
    fn json_roundtrip(#[case] v: Option<&str>) {
        assert_json_roundtrip(DType::Utf8, &StringArray::from(vec![v]));
    }

    #[test]
    fn encoder_rejects_invalid_utf8() {
        let (schema, c) = single_col();
        let mut w = WriteRow::new(&schema, "");
        w.write_dynamic(&c, &[0xFF, 0xFE, 0xFD]);
        let row = ReadRow::new(&schema, &w.bytes);

        let mut enc = codec_for(c.dtype).make_encoder(c, 1);
        let err = enc.add_row(&row);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn decoder_rejects_wrong_array_type() {
        let (_schema, c) = single_col();
        let wrong = Float32Array::from(vec![Some(1.0_f32)]);
        let err = Utf8Codec.make_decoder(c, &wrong);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn json_from_invalid_type() {
        let values = vec![Value::from(42)];
        assert!(Utf8Codec.from_json(&values).is_err());
    }
}
