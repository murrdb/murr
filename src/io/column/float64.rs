use arrow::datatypes::Float64Type;

use crate::io::column::scalar::{ScalarArrow, ScalarCodec, ScalarColumnReader};

pub type Float64ColumnReader<R> = ScalarColumnReader<R, Float64Codec>;

pub struct Float64Codec;

impl ScalarCodec for Float64Codec {
    type ArrowType = Float64Type;
    type Native = f64;
    const ELEMENT_SIZE: u32 = 8;
    const ZERO: f64 = 0.0;
}

impl ScalarArrow for Float64Type {
    type Codec = Float64Codec;
}
