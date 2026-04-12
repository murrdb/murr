use crate::io::column::scalar::{ScalarCodec, ScalarColumnReader, ScalarColumnWriter};

pub type Float64ColumnReader<R> = ScalarColumnReader<R, Float64Codec>;
pub type Float64ColumnWriter = ScalarColumnWriter<Float64Codec>;

pub struct Float64Codec;

impl ScalarCodec for Float64Codec {
    type ArrowType = arrow::datatypes::Float64Type;
    type Native = f64;
    const ELEMENT_SIZE: u32 = 8;
    const ZERO: f64 = 0.0;
    const NAME: &'static str = "Float64";
}
