pub trait Encoder<T> {
    type A: arrow::array::Array;
    fn to_bytes(values: &Self::A) -> Vec<u8>;
}

pub trait Decoder<T> {
    fn from_bytes(page: &[u8], page_offset: u32, size: u32) -> T;
}
