use arrow::{array::Array, buffer::BooleanBuffer};

use crate::io2::{directory::Directory, table::key_offset::KeyOffset};

pub struct NullBitmap {
    pub offset: u32,
    pub size: u32,
}

impl NullBitmap {
    fn new(offset: u32, size: u32) -> Self {
        NullBitmap { offset, size }
    }

    async fn get_nulls<D: Directory>(
        &self,
        reader: &D::ReaderType<'_>,
        keys: &[KeyOffset],
    ) -> Vec<usize> {
        todo!()
    }

    fn write(&self, values: Box<dyn Array>) -> Vec<u8> {
        todo!()
    }
}
