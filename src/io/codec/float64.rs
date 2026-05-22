use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Float64Type},
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        codec::{Codec, ColumnDecoder, ColumnEncoder, primitive},
        schema::SegmentColumnSchema,
    },
};

pub struct Float64Codec;

impl Codec for Float64Codec {
    fn dtype(&self) -> DType {
        DType::Float64
    }
    fn arrow_dtype(&self) -> DataType {
        DataType::Float64
    }
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError> {
        primitive::to_json::<Float64Type>(arr)
    }
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError> {
        primitive::from_json::<Float64Type>(vals)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn json_roundtrip() {
        let arr: ArrayRef =
            std::sync::Arc::new(Float64Array::from(vec![Some(3.15), None, Some(2.72)]));
        let json = Float64Codec.to_json(arr.as_ref()).unwrap();
        let back = Float64Codec.from_json(&json).unwrap();
        assert_eq!(arr.to_data(), back.to_data());
    }
}
