use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Float64Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Float64;

impl DType for Float64 {
    fn name(&self) -> DTypeName {
        DTypeName::Float64
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Float64
    }
    fn size(&self) -> usize {
        8
    }
}

impl ArrowCodec for Float64 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Float64Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Float64Type>::new(col, arr)?))
    }
}

impl JsonCodec for Float64 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Float64Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Float64Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DTypeName;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Float64Array;
    use rstest::rstest;

    #[rstest]
    #[case::pos(Some(3.15))]
    #[case::null(None)]
    #[case::neg(Some(-1e10))]
    #[case::zero(Some(0.0))]
    fn row_roundtrip(#[case] v: Option<f64>) {
        assert_row_roundtrip(DTypeName::Float64, &Float64Array::from(vec![v]));
    }

    #[rstest]
    #[case::pos(Some(3.15))]
    #[case::null(None)]
    #[case::neg(Some(-1e10))]
    #[case::zero(Some(0.0))]
    fn json_roundtrip(#[case] v: Option<f64>) {
        assert_json_roundtrip(DTypeName::Float64, &Float64Array::from(vec![v]));
    }
}
