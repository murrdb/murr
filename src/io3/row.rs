// a wrapper for reading

// row layout is similar to cap'n'proto:
// [null_bitset_size: u8] [null bitset: &[u8]] [columns] [dynamic column payloads]
//
// static columns have known length and just packed together at known offsets from the SegmentSchema
// example for 4 f32 cols: [col0:f32 col1:f32 col2:f32 col3:f32] []

// dynamic cols are stored as offsets to the payloads section:
// - u32 offset has static size, so can be part of the static section
// - as it has a variable length, first 4 bytes of the payload is the length of the payload

// example for f32+utf8 column:
// [col0: f32, col1: u32] [col1_length: u32, ... col1_bytes ...]

use crate::{core::MurrError, proto::ext, proto::model::SegmentColumnSchema};

pub struct Row {
    pub bytes: Vec<u8>,
}

pub struct 

impl Row {
    pub fn new(capacity: usize) -> Self {
        Row {
            bytes: vec![0u8; capacity],
        }
    }

    fn write_static(
        &mut self,
        schema: &SegmentColumnSchema,
        bytes: &[u8],
    ) -> Result<(), MurrError> {
        schema.dtype().size();
        todo!()
    }
}
