use std::sync::Arc;

use arrow::array::Array;

use crate::core::MurrError;

#[derive(Clone, Copy, Debug)]
pub struct SegmentIndex {
    pub segment_id: u32,
    pub segment_offset: u32,
}

pub trait Column {
    /// Pull N elements by index from segments, return Arrow Array.
    fn get_indexes(&self, indexes: &[SegmentIndex]) -> Result<Arc<dyn Array>, MurrError>;

    /// Return all values as a single Arrow Array (for building key index).
    fn get_all(&self) -> Result<Arc<dyn Array>, MurrError>;

    /// Total number of values across all segments.
    fn size(&self) -> u32;
}
