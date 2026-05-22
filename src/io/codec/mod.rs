pub mod bool_;
pub mod float32;
pub mod float64;
pub mod int16;
pub mod int32;
pub mod int64;
pub mod int8;
pub mod primitive;
pub mod uint16;
pub mod uint32;
pub mod uint64;
pub mod uint8;
pub mod utf8;

#[cfg(test)]
pub(crate) mod test_util;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};
use serde_json::Value;

use crate::{
    core::{DType, MurrError},
    io::{
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub trait Codec: Send + Sync {
    fn dtype(&self) -> DType;
    fn arrow_dtype(&self) -> DataType;

    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError>;
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError>;

    fn make_encoder(
        &self,
        col: SegmentColumnSchema,
        rows: usize,
    ) -> Box<dyn ColumnEncoder>;
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError>;
}

pub trait ColumnEncoder: Send {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError>;
    fn add_empty(&mut self) -> Result<(), MurrError>;
    fn build(&mut self) -> ArrayRef;
}

pub trait ColumnDecoder: Send + Sync {
    fn write_to_row(&self, index: usize, row: &mut WriteRow);
}

pub fn codec_for(dtype: DType) -> &'static dyn Codec {
    match dtype {
        DType::Utf8 => &utf8::Utf8Codec,
        DType::Bool => &bool_::BoolCodec,
        DType::Int8 => &int8::Int8Codec,
        DType::Int16 => &int16::Int16Codec,
        DType::Int32 => &int32::Int32Codec,
        DType::Int64 => &int64::Int64Codec,
        DType::UInt8 => &uint8::UInt8Codec,
        DType::UInt16 => &uint16::UInt16Codec,
        DType::UInt32 => &uint32::UInt32Codec,
        DType::UInt64 => &uint64::UInt64Codec,
        DType::Float32 => &float32::Float32Codec,
        DType::Float64 => &float64::Float64Codec,
    }
}

pub(crate) fn downcast<'a, A: Array + 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a A, MurrError> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        MurrError::SegmentError(format!("expected {expected}, got {:?}", array.data_type()))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;

    use super::*;

    #[test]
    fn make_decoder_rejects_dtype_mismatch() {
        let c = SegmentColumnSchema {
            index: 0,
            dtype: DType::Float32,
            name: "x".into(),
            offset: 0,
        };
        let wrong: ArrayRef = Arc::new(StringArray::from(vec!["nope"]));
        let err = codec_for(c.dtype).make_decoder(c.clone(), wrong.as_ref());
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
