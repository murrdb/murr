use arrow::datatypes::Float32Type;

use crate::io::column::scalar::{ScalarArrow, ScalarCodec, ScalarColumnReader};

pub type Float32ColumnReader<R> = ScalarColumnReader<R, Float32Codec>;

pub struct Float32Codec;

impl ScalarCodec for Float32Codec {
    type ArrowType = Float32Type;
    type Native = f32;
    const ELEMENT_SIZE: u32 = 4;
    const ZERO: f32 = 0.0;
}

impl ScalarArrow for Float32Type {
    type Codec = Float32Codec;
}
