use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, BooleanBuilder},
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

pub struct BoolCodec;

impl Codec for BoolCodec {
    fn dtype(&self) -> DType {
        DType::Bool
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Boolean
    }

    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        let typed = downcast::<BooleanArray>(arr, "Boolean")?;
        Ok((0..typed.len())
            .map(|i| {
                if typed.is_null(i) {
                    Value::Null
                } else {
                    Value::Bool(typed.value(i))
                }
            })
            .collect())
    }

    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        let mut builder = BooleanBuilder::with_capacity(vals.len());
        for v in vals {
            match v {
                Value::Null => builder.append_null(),
                Value::Bool(b) => builder.append_value(*b),
                _ => return Err(MurrError::TableError(format!("expected bool, got {v}"))),
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(BoolEncoder {
            column: col,
            builder: BooleanBuilder::with_capacity(rows),
        })
    }

    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        let typed = downcast::<BooleanArray>(arr, "Boolean")?;
        Ok(Box::new(BoolDecoder {
            column: col,
            array: typed.clone(),
        }))
    }
}

struct BoolEncoder {
    column: SegmentColumnSchema,
    builder: BooleanBuilder,
}

impl ColumnEncoder for BoolEncoder {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError> {
        if row.is_null(&self.column) {
            self.builder.append_null();
        } else {
            self.builder
                .append_value(row.read_static::<u8>(&self.column) != 0);
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

struct BoolDecoder {
    column: SegmentColumnSchema,
    array: BooleanArray,
}

impl ColumnDecoder for BoolDecoder {
    fn write_to_row(&self, index: usize, row: &mut WriteRow) {
        if !self.array.is_null(index) {
            row.write_static::<u8>(&self.column, self.array.value(index) as u8);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Float32Array;
    use rstest::rstest;

    #[rstest]
    #[case::t(Some(true))]
    #[case::null(None)]
    #[case::f(Some(false))]
    fn row_roundtrip(#[case] v: Option<bool>) {
        assert_row_roundtrip(DType::Bool, &BooleanArray::from(vec![v]));
    }

    #[rstest]
    #[case::t(Some(true))]
    #[case::null(None)]
    #[case::f(Some(false))]
    fn json_roundtrip(#[case] v: Option<bool>) {
        assert_json_roundtrip(DType::Bool, &BooleanArray::from(vec![v]));
    }

    #[test]
    fn decoder_rejects_wrong_array_type() {
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DType::Bool,
            name: "b".into(),
            offset: 0,
        };
        let wrong = Float32Array::from(vec![Some(1.0_f32)]);
        let err = BoolCodec.make_decoder(c, &wrong);
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }

    #[test]
    fn json_from_invalid_type() {
        let values = vec![Value::from(42)];
        assert!(BoolCodec.from_json(&values).is_err());
    }
}
