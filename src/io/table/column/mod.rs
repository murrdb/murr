mod bitmap;
pub mod float32;
pub mod utf8;

pub(crate) use bitmap::read_u32;

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::Field;

use crate::core::ColumnConfig;
use crate::core::MurrError;

pub use float32::Float32Column;
pub use utf8::Utf8Column;

#[derive(Clone, Copy, Debug)]
pub enum KeyOffset {
    SegmentOffset {
        segment_id: u32,
        segment_offset: u32,
    },
    MissingKey,
}

pub trait ColumnSegment<'a>: Sized {
    type ArrayType: Array;

    fn parse(name: &str, config: &ColumnConfig, data: &'a [u8]) -> Result<Self, MurrError>;

    fn write(config: &ColumnConfig, array: &Self::ArrayType) -> Result<Vec<u8>, MurrError>;
}

pub trait Column: Send + Sync {
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
