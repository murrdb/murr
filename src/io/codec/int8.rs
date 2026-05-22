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
    use arrow::array::Int8Array;

    #[test]
    fn json_overflow_rejected() {
        // serde_json deserializer enforces range; 200 doesn't fit in i8.
        let values = vec![Value::from(200i64)];
        assert!(Int8Codec.from_json(&values).is_err());
    }

    #[test]
    fn json_roundtrip() {
        let arr: ArrayRef = std::sync::Arc::new(Int8Array::from(vec![
            Some(-7),
            None,
            Some(i8::MAX),
            Some(0),
        ]));
        let json = Int8Codec.to_json(arr.as_ref()).unwrap();
        let back = Int8Codec.from_json(&json).unwrap();
        assert_eq!(arr.to_data(), back.to_data());
    }
}
