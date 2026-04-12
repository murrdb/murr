pub mod footer;
pub mod reader;
pub mod writer;

use arrow::array::{ArrowPrimitiveType, PrimitiveArray};
use arrow::datatypes::ArrowNativeType;
use bytemuck::Pod;
pub use reader::ScalarColumnReader;
pub use writer::write_scalar;

use crate::core::MurrError;
use crate::io::bytes::FromBytes;
use crate::io::column::{ColumnSegmentBytes, ColumnWriter};
use crate::io::info::ColumnInfo;

pub trait ScalarCodec: Send + Sync + 'static {
    type ArrowType: ArrowPrimitiveType<Native = Self::Native> + ScalarArrow<Codec = Self>;
    type Native: Pod + Default + ArrowNativeType + FromBytes<Self::Native> + Send + Sync;
    const ELEMENT_SIZE: u32;
    const ZERO: Self::Native;
}

/// Reverse mapping from `ArrowPrimitiveType` back to its `ScalarCodec`.
pub trait ScalarArrow: ArrowPrimitiveType {
    type Codec: ScalarCodec<ArrowType = Self>;
}

impl<T: ScalarArrow> ColumnWriter for PrimitiveArray<T> {
    fn write_column(&self, column: &ColumnInfo) -> Result<ColumnSegmentBytes, MurrError> {
        write_scalar::<T::Codec>(column, self)
    }
}
