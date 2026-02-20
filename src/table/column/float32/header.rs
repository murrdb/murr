use bytemuck::{Pod, Zeroable};

use crate::core::MurrError;

/// Fixed-size header at the start of a dense float32 column segment.
///
/// All byte offsets are relative to the start of the column data.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Float32Header {
    pub(super) num_values: u32,
    pub(super) payload_offset: u32,
    pub(super) null_bitmap_offset: u32,
    pub(super) null_bitmap_size: u32,
}

pub(super) const HEADER_SIZE: usize = std::mem::size_of::<Float32Header>();

impl Float32Header {
    /// Parse the header from the beginning of a column data slice.
    pub(super) fn parse(data: &[u8]) -> Result<&Float32Header, MurrError> {
        if data.len() < HEADER_SIZE {
            return Err(MurrError::TableError(
                "dense float32 segment too small for header".into(),
            ));
        }
        Ok(bytemuck::from_bytes(&data[..HEADER_SIZE]))
    }
}
