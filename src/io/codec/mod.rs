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

use arrow::array::{Array, ArrayRef};
use serde_json::Value;

use crate::{
    core::{DType, DTypeName, MurrError},
    io::{
        row::{read::ReadRow, write::WriteRow},
        schema::SegmentColumnSchema,
    },
};

pub trait ArrowCodec: Send + Sync {
    fn make_encoder(&self, col: SegmentColumnSchema, rows: usize) -> Box<dyn ColumnEncoder>;
    fn make_decoder(
        &self,
        col: SegmentColumnSchema,
        arr: &dyn Array,
    ) -> Result<Box<dyn ColumnDecoder>, MurrError>;
}

pub trait JsonCodec: Send + Sync {
    fn to_json(&self, arr: &dyn Array) -> Result<Vec<Value>, MurrError>;
    fn from_json(&self, vals: &[Value]) -> Result<ArrayRef, MurrError>;
}

pub trait ColumnEncoder: Send {
    fn add_row(&mut self, row: &ReadRow) -> Result<(), MurrError>;
    fn add_empty(&mut self) -> Result<(), MurrError>;
    fn build(&mut self) -> ArrayRef;
}

pub trait ColumnDecoder: Send + Sync {
    fn write_to_row(&self, index: usize, row: &mut WriteRow);
}

/// Aggregate trait exposing the three per-dtype roles through one trait object.
/// A blanket impl wires every concrete per-type struct that already implements
/// `DType + ArrowCodec + JsonCodec`.
pub trait Codec: DType + ArrowCodec + JsonCodec {}
impl<T: DType + ArrowCodec + JsonCodec> Codec for T {}

impl DTypeName {
    pub fn codec(self) -> Box<dyn Codec> {
        match self {
            DTypeName::Utf8 => Box::new(utf8::Utf8),
            DTypeName::Bool => Box::new(bool_::Bool),
            DTypeName::Int8 => Box::new(int8::Int8),
            DTypeName::Int16 => Box::new(int16::Int16),
            DTypeName::Int32 => Box::new(int32::Int32),
            DTypeName::Int64 => Box::new(int64::Int64),
            DTypeName::UInt8 => Box::new(uint8::UInt8),
            DTypeName::UInt16 => Box::new(uint16::UInt16),
            DTypeName::UInt32 => Box::new(uint32::UInt32),
            DTypeName::UInt64 => Box::new(uint64::UInt64),
            DTypeName::Float32 => Box::new(float32::Float32),
            DTypeName::Float64 => Box::new(float64::Float64),
        }
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
            dtype: DTypeName::Float32,
            name: "x".into(),
            offset: 0,
        };
        let wrong: ArrayRef = Arc::new(StringArray::from(vec!["nope"]));
        let err = c.dtype.codec().make_decoder(c.clone(), wrong.as_ref());
        assert!(matches!(err, Err(MurrError::SegmentError(_))));
    }
}
