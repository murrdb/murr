use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, UInt64Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct UInt64Codec;

impl Codec for UInt64Codec {
    fn dtype(&self) -> DType {
        DType::UInt64
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::UInt64
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<UInt64Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<UInt64Type>(vals)
    }
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<UInt64Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<UInt64Type>::new(col, arr)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::UInt64Array;
    use rstest::rstest;

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u64::MAX))]
    #[case::mid(Some(7))]
    #[case::above_2_53(Some((1u64 << 53) + 1))]
    fn row_roundtrip(#[case] v: Option<u64>) {
        assert_row_roundtrip(DType::UInt64, &UInt64Array::from(vec![v]));
    }

    #[rstest]
    #[case::null(None)]
    #[case::zero(Some(0))]
    #[case::max(Some(u64::MAX))]
    #[case::mid(Some(7))]
    #[case::above_2_53(Some((1u64 << 53) + 1))]
    fn json_roundtrip(#[case] v: Option<u64>) {
        assert_json_roundtrip(DType::UInt64, &UInt64Array::from(vec![v]));
    }

    #[test]
    fn json_preserves_precision_above_2_53() {
        // 2^53 + 1 cannot survive an f64 detour; serde_json's Number::as_u64 keeps it exact.
        let big: u64 = (1u64 << 53) + 1;
        let arr = UInt64Array::from(vec![Some(big)]);
        let json = UInt64Codec.to_json(&arr).unwrap();
        assert_eq!(json[0], Value::from(big));
    }
}
