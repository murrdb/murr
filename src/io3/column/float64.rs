use arrow::datatypes::Float64Type;

use crate::io3::{column::primitive::{PrimitiveArrayDecoder, PrimitiveArrayEncoder}, row::Row};

impl PrimitiveArrayEncoder for f64 {
    type ArrowType = Float64Type;
    fn set_primitive(row: &mut Row, bitset_size: usize, offset: usize, value: &f64) {
        row.set_static_value(bitset_size, offset, &value.to_le_bytes());
    }
}

impl PrimitiveArrayDecoder for f64 {
    type ArrowType = Float64Type;
    fn get_primitive(row: &Row, bitset_size: usize, offset: usize) -> f64 {
        let start = bitset_size + offset;
        let bytes: [u8; 8] = row.bytes[start..start + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }
}
