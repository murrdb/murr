pub mod footer;
pub mod reader;
pub mod writer;

use arrow::{array::ArrowPrimitiveType, datatypes::ArrowNativeType};
use bytemuck::Pod;
pub use reader::ScalarColumnReader;
pub use writer::ScalarColumnWriter;

use crate::io::bytes::FromBytes;

pub trait ScalarCodec: Send + Sync + 'static {
    type ArrowType: ArrowPrimitiveType<Native = Self::Native>;
    type Native: Pod + Default + ArrowNativeType + FromBytes<Self::Native> + Send + Sync;
    const ELEMENT_SIZE: u32;
    const ZERO: Self::Native;
}
