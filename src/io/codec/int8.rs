use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int8Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int8Codec;

impl Codec for Int8Codec {
    fn dtype(&self) -> DType {
        DType::Int8
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int8
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int8Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int8Type>(vals)
    }
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
        assert_row_roundtrip(DType::Int8, &Int8Array::from(vec![v]));
    }

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i8::MIN))]
    #[case::max(Some(i8::MAX))]
    #[case::zero(Some(0))]
    fn json_roundtrip(#[case] v: Option<i8>) {
        assert_json_roundtrip(DType::Int8, &Int8Array::from(vec![v]));
    }

    #[test]
    fn json_overflow_rejected() {
        // serde_json deserializer enforces range; 200 doesn't fit in i8.
        let values = vec![Value::from(200i64)];
        assert!(Int8Codec.from_json(&values).is_err());
    }
}
