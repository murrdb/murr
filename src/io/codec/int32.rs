use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Int32Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Int32Codec;

impl Codec for Int32Codec {
    fn dtype(&self) -> DType {
        DType::Int32
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Int32
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Int32Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Int32Type>(vals)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn json_roundtrip() {
        let arr: ArrayRef = std::sync::Arc::new(Int32Array::from(vec![
            Some(-7),
            None,
            Some(i32::MAX),
            Some(0),
        ]));
        let json = Int32Codec.to_json(arr.as_ref()).unwrap();
        let back = Int32Codec.from_json(&json).unwrap();
        assert_eq!(arr.to_data(), back.to_data());
    }
}
