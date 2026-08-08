use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Float32Type},
};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        codec::{ArrowCodec, ColumnDecoder, ColumnEncoder, JsonCodec, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Float32;

impl DType for Float32 {
    fn name(&self) -> DTypeName {
        DTypeName::Float32
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Float32
    }
    fn size(&self) -> usize {
        4
    }
}

impl ArrowCodec for Float32 {
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

impl JsonCodec for Float32 {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Float32Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Float32Type>(vals)
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
        assert_row_roundtrip(DTypeName::Float32, &Float32Array::from(vec![v]));
    }

    #[rstest]
    #[case::pos(Some(1.5))]
    #[case::null(None)]
    #[case::neg(Some(-2.5))]
    #[case::zero(Some(0.0))]
    fn json_roundtrip(#[case] v: Option<f32>) {
        assert_json_roundtrip(DTypeName::Float32, &Float32Array::from(vec![v]));
    }

    #[test]
    fn json_from_invalid_type() {
        let values = vec![Value::String("not a number".into())];
        assert!(Float32.from_json(&values).is_err());
    }

    #[test]
    fn row_roundtrip_nan() {
        // NaN bit-pattern doesn't compare equal under to_data(); needs custom check.
        use crate::io::{
            row::{read::ReadRow, write::WriteRow},
            schema::SegmentSchema,
        };
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DTypeName::Float32,
            name: "v".into(),
            offset: 0,
        };
        let schema = SegmentSchema::new(std::slice::from_ref(&c));
        let input = Float32Array::from(vec![Some(f32::NAN)]);
        let dec = c.dtype.codec().make_decoder(c.clone(), &input).unwrap();
        let mut w = WriteRow::new(&schema, "");
        dec.write_to_row(0, &mut w);
        let mut enc = c.dtype.codec().make_encoder(c, 1);
        enc.add_row(&ReadRow::new(&schema, &w.bytes)).unwrap();
        let out = enc.build();
        let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!(out.value(0).is_nan());
    }
}
