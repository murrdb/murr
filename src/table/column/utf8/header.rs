use bytemuck::{Pod, Zeroable};

use crate::core::MurrError;

/// Fixed-size header at the start of a dense string column segment.
///
/// All byte offsets are relative to the start of the column data.
/// Value offsets (`[i32; num_values]`) immediately follow the header.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Utf8Header {
    pub(super) num_values: u32,
    pub(super) payload_offset: u32,
    pub(super) payload_size: u32,
    pub(super) null_bitmap_offset: u32,
    pub(super) null_bitmap_size: u32,
}

pub(super) const HEADER_SIZE: usize = std::mem::size_of::<Utf8Header>();

impl Utf8Header {
    /// Parse the header from the beginning of a column data slice.
    pub(super) fn parse(data: &[u8]) -> Result<&Utf8Header, MurrError> {
        if data.len() < HEADER_SIZE {
            return Err(MurrError::TableError(
                "dense string segment too small for header".into(),
            ));
        }
        Ok(bytemuck::from_bytes(&data[..HEADER_SIZE]))
    }
}
