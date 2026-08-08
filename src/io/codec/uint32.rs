use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, UInt32Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct UInt32;

impl DType for UInt32 {
    fn name(&self) -> DTypeName {
        DTypeName::UInt32
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::UInt32
    }
    fn size(&self) -> usize {
        4
    }
}

impl ArrowCodec for UInt32 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<UInt32Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<UInt32Type>::new(col, arr)?))
    }
}

impl JsonCodec for UInt32 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<UInt32Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<UInt32Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DTypeName;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::UInt32Array;
    use rstest::rstest;

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u32::MAX))]
    #[case::mid(Some(7))]
    fn row_roundtrip(#[case] v: Option<u32>) {
        assert_row_roundtrip(DTypeName::UInt32, &UInt32Array::from(vec![v]));
    }

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u32::MAX))]
    #[case::mid(Some(7))]
    fn json_roundtrip(#[case] v: Option<u32>) {
        assert_json_roundtrip(DTypeName::UInt32, &UInt32Array::from(vec![v]));
    }
}
