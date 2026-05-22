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
    use arrow::array::UInt64Array;

    #[test]
    fn json_preserves_precision_above_2_53() {
        // 2^53 + 1 cannot survive an f64 detour; serde_json's Number::as_u64 keeps it exact.
        let big: u64 = (1u64 << 53) + 1;
        let arr: ArrayRef = std::sync::Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(big),
            None,
            Some(u64::MAX),
        ]));
        let json = UInt64Codec.to_json(arr.as_ref()).unwrap();
        assert_eq!(json[1], Value::from(big));
        let back = UInt64Codec.from_json(&json).unwrap();
        assert_eq!(arr.to_data(), back.to_data());
    }
}
