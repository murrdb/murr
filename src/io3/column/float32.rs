use arrow::datatypes::Float32Type;

use crate::io3::{column::primitive::{PrimitiveArrayDecoder, PrimitiveArrayEncoder}, row::Row};

impl PrimitiveArrayEncoder for f32 {
    type ArrowType = Float32Type;
    fn set_primitive(row: &mut Row, bitset_size: usize, offset: usize, value: &f32) {
        row.set_static_value(bitset_size, offset, &value.to_le_bytes());
    }
}

impl PrimitiveArrayDecoder for f32 {
    type ArrowType = Float32Type;
    fn get_primitive(row: &Row, bitset_size: usize, offset: usize) -> f32 {
        let start = bitset_size + offset;
        let bytes: [u8; 4] = row.bytes[start..start + 4].try_into().unwrap();
        f32::from_le_bytes(bytes)
    }
}