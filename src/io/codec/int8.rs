use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int8Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int8;

impl DType for Int8 {
    fn name(&self) -> DTypeName {
        DTypeName::Int8
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int8
    }
    fn size(&self) -> usize {
        1
    }
}

impl ArrowCodec for Int8 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Int8Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Int8Type>::new(col, arr)?))
    }
}

impl JsonCodec for Int8 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int8Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int8Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Int8Array;
    use rstest::rstest;

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i8::MIN))]
    #[case::max(Some(i8::MAX))]
    #[case::zero(Some(0))]
    fn row_roundtrip(#[case] v: Option<i8>) {
        assert_row_roundtrip(DTypeName::Int8, &Int8Array::from(vec![v]));
    }

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i8::MIN))]
    #[case::max(Some(i8::MAX))]
    #[case::zero(Some(0))]
    fn json_roundtrip(#[case] v: Option<i8>) {
        assert_json_roundtrip(DTypeName::Int8, &Int8Array::from(vec![v]));
    }

    #[test]
    fn json_overflow_rejected() {
        // serde_json deserializer enforces range; 200 doesn't fit in i8.
        let values = vec![Value::from(200i64)];
        assert!(Int8.from_json(&values).is_err());
    }
}
