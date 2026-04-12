use crate::io::column::scalar::ScalarCodec;

pub mod footer;
pub mod reader;
pub mod writer;

pub struct Float32Codec;

impl ScalarCodec for Float32Codec {
    type ArrowType = arrow::datatypes::Float32Type;
    type Native = f32;
    const ELEMENT_SIZE: u32 = 4;
    const ZERO: f32 = 0.0;
    const NAME: &'static str = "Float32";
}
