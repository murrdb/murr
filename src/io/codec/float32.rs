use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Float32Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Float32Codec;

impl Codec for Float32Codec {
    fn dtype(&self) -> DType {
        DType::Float32
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Float32
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Float32Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Float32Type>(vals)
    }
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder> {
        Box::new(primitive::Encoder::<Float32Type>::new(col, rows))
    }
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError> {
        Ok(Box::new(primitive::Decoder::<Float32Type>::new(col, arr)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::codec::test_util::{assert_json_roundtrip, assert_row_roundtrip};
    use arrow::array::Float32Array;
    use rstest::rstest;

    #[rstest]
    #[case::pos(Some(1.5))]
    #[case::null(None)]
    #[case::neg(Some(-2.5))]
    #[case::zero(Some(0.0))]
    fn row_roundtrip(#[case] v: Option<f32>) {
        assert_row_roundtrip(DType::Float32, &Float32Array::from(vec![v]));
    }

    #[rstest]
    #[case::pos(Some(1.5))]
    #[case::null(None)]
    #[case::neg(Some(-2.5))]
    #[case::zero(Some(0.0))]
    fn json_roundtrip(#[case] v: Option<f32>) {
        assert_json_roundtrip(DType::Float32, &Float32Array::from(vec![v]));
    }

    #[test]
    fn json_from_invalid_type() {
        let values = vec![Value::String("not a number".into())];
        assert!(Float32Codec.from_json(&values).is_err());
    }

    #[test]
    fn row_roundtrip_nan() {
        // NaN bit-pattern doesn't compare equal under to_data(); needs custom check.
        use crate::io::{
            codec::codec_for,
            row::{read::ReadRow, write::WriteRow},
            schema::SegmentSchema,
        };
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DType::Float32,
            name: "v".into(),
            offset: 0,
        };
        let schema = SegmentSchema::new(std::slice::from_ref(&c));
        let input = Float32Array::from(vec![Some(f32::NAN)]);
        let dec = codec_for(c.dtype).make_decoder(c.clone(), &input).unwrap();
        let mut w = WriteRow::new(&schema, "");
        dec.write_to_row(0, &mut w);
        let mut enc = codec_for(c.dtype).make_encoder(c, 1);
        enc.add_row(&ReadRow::new(&schema, &w.bytes)).unwrap();
        let out = enc.build();
        let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!(out.value(0).is_nan());
    }
}
