use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int32Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int32;

impl DType for Int32 {
    fn name(&self) -> DTypeName {
        DTypeName::Int32
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int32
    }
    fn size(&self) -> usize {
        4
    }
}

impl ArrowCodec for Int32 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Int32Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Int32Type>::new(col, arr)?))
    }
}

impl JsonCodec for Int32 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int32Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int32Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DTypeName;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Int32Array;
    use rstest::rstest;

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i32::MIN))]
    #[case::max(Some(i32::MAX))]
    #[case::zero(Some(0))]
    fn row_roundtrip(#[case] v: Option<i32>) {
        assert_row_roundtrip(DTypeName::Int32, &Int32Array::from(vec![v]));
    }

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i32::MIN))]
    #[case::max(Some(i32::MAX))]
    #[case::zero(Some(0))]
    fn json_roundtrip(#[case] v: Option<i32>) {
        assert_json_roundtrip(DTypeName::Int32, &Int32Array::from(vec![v]));
    }
}
