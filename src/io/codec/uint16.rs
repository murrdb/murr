use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, UInt16Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct UInt16;

impl DType for UInt16 {
    fn name(&self) -> DTypeName {
        DTypeName::UInt16
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::UInt16
    }
    fn size(&self) -> usize {
        2
    }
}

impl ArrowCodec for UInt16 {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<UInt16Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<UInt16Type>::new(col, arr)?))
    }
}

impl JsonCodec for UInt16 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<UInt16Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<UInt16Type>(vals)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DTypeName;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::UInt16Array;
    use rstest::rstest;

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u16::MAX))]
    #[case::mid(Some(7))]
    fn row_roundtrip(#[case] v: Option<u16>) {
        assert_row_roundtrip(DTypeName::UInt16, &UInt16Array::from(vec![v]));
    }

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u16::MAX))]
    #[case::mid(Some(7))]
    fn json_roundtrip(#[case] v: Option<u16>) {
        assert_json_roundtrip(DTypeName::UInt16, &UInt16Array::from(vec![v]));
    }
}
