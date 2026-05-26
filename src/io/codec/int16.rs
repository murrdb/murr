use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int16Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int16;

impl DType for Int16 {
    fn name(&self) -> DTypeName {
        DTypeName::Int16
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int16
    }
    fn size(&self) -> usize {
        2
    }
}

impl ArrowCodec for Int16 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Int16Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Int16Type>::new(col, arr)?))
    }
}

impl JsonCodec for Int16 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int16Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int16Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DTypeName;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Int16Array;
    use rstest::rstest;

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i16::MIN))]
    #[case::max(Some(i16::MAX))]
    #[case::zero(Some(0))]
    fn row_roundtrip(#[case] v: Option<i16>) {
        assert_row_roundtrip(DTypeName::Int16, &Int16Array::from(vec![v]));
    }

    #[rstest]
    #[case::neg(Some(-7))]
    #[case::null(None)]
    #[case::min(Some(i16::MIN))]
    #[case::max(Some(i16::MAX))]
    #[case::zero(Some(0))]
    fn json_roundtrip(#[case] v: Option<i16>) {
        assert_json_roundtrip(DTypeName::Int16, &Int16Array::from(vec![v]));
    }
}
