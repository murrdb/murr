pub mod float32;
pub mod float64;
pub mod primitive;
pub mod utf8;

use arrow::array::Array;

use crate::{
    core::MurrError,
    io3::{batch::RowBatch, model::SegmentColumnSchema},
};

pub trait ArrayDecoder {
    type A: Array;
    fn decode_to(column: &SegmentColumnSchema, rows: &RowBatch) -> Result<Self::A, MurrError>;
}

pub trait ArrayEncoder {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError>;
}
