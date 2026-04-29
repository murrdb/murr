pub mod primitive;
pub mod utf8;

use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Float32Type, Float64Type},
};

use crate::{
    core::{DType, MurrError},
    io3::{
        column::{primitive::PrimitiveCodec, utf8::Utf8Codec},
        model::SegmentColumnSchema,
        row::Row,
    },
};

pub trait ColumnCodec: Send + Sync {
    fn encode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        array: &dyn Array,
        rows: &mut [Row],
    ) -> Result<(), MurrError>;

    fn decode(
        &self,
        col: &SegmentColumnSchema,
        bitset_size: usize,
        rows: &[Row],
    ) -> Result<ArrayRef, MurrError>;
}

static F32_CODEC: PrimitiveCodec<Float32Type> = PrimitiveCodec(PhantomData);
static F64_CODEC: PrimitiveCodec<Float64Type> = PrimitiveCodec(PhantomData);
static UTF8_CODEC: Utf8Codec = Utf8Codec;

pub fn codec_for(dtype: DType) -> &'static dyn ColumnCodec {
    match dtype {
        DType::Float32 => &F32_CODEC,
        DType::Float64 => &F64_CODEC,
        DType::Utf8 => &UTF8_CODEC,
    }
}

pub(crate) fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a T, MurrError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        MurrError::SegmentError(format!(
            "expected {expected}, got {:?}",
            array.data_type()
        ))
    })
}
