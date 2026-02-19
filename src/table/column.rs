use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::Field;

use crate::core::MurrError;

#[derive(Clone, Copy, Debug)]
pub enum KeyOffset {
    SegmentOffset {
        segment_id: u32,
        segment_offset: u32,
    },
    MissingKey,
}

pub trait Column {
    /// Arrow field definition for this column.
    fn field(&self) -> &Field;

    /// Pull N elements by index from segments, return Arrow Array.
    /// MissingKey entries produce null values in the output.
    fn get_indexes(&self, indexes: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError>;

    /// Return all values as a single Arrow Array (for building key index).
    fn get_all(&self) -> Result<Arc<dyn Array>, MurrError>;

    /// Total number of values across all segments.
    fn size(&self) -> u32;
}
