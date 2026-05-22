use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int64Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int64Codec;

impl Codec for Int64Codec {
    fn dtype(&self) -> DType {
        DType::Int64
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int64
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int64Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int64Type>(vals)
    }
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Int64Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Int64Type>::new(col, arr)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Int64Array;
    use rstest::rstest;

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i64::MIN))]
    #[case::max(Some(i64::MAX))]
    #[case::zero(Some(0))]
    fn row_roundtrip(#[case] v: Option<i64>) {
        assert_row_roundtrip(DType::Int64, &Int64Array::from(vec![v]));
    }

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i64::MIN))]
    #[case::max(Some(i64::MAX))]
    #[case::zero(Some(0))]
    fn json_roundtrip(#[case] v: Option<i64>) {
        assert_json_roundtrip(DType::Int64, &Int64Array::from(vec![v]));
    }
}
