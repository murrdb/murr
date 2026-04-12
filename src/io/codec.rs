use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::ArrowNativeType;
use bytemuck::Pod;

use crate::io::bytes::FromBytes;

pub trait ScalarCodec: Send + Sync + 'static {
    type ArrowType: ArrowPrimitiveType<Native = Self::Native>;
    type Native: Pod + Default + ArrowNativeType + FromBytes<Self::Native> + Send + Sync;
    const ELEMENT_SIZE: u32;
    const ZERO: Self::Native;
    const NAME: &'static str;
}

pub struct Float32Codec;

impl ScalarCodec for Float32Codec {
    type ArrowType = arrow::datatypes::Float32Type;
    type Native = f32;
    const ELEMENT_SIZE: u32 = 4;
    const ZERO: f32 = 0.0;
    const NAME: &'static str = "Float32";
}
