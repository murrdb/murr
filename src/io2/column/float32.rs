use std::sync::Arc;

use ahash::HashMap;
use arrow::array::Array;

use async_trait::async_trait;

use crate::{
    core::MurrError,
    io2::{
        column::{Column, ColumnReader, ColumnSegmentBytes, ColumnWriter, OffsetSize},
        directory::Directory,
        info::ColumnInfo,
        table::key_offset::KeyOffset,
    },
};

pub struct Float32Column {
    column: ColumnInfo,
}

impl<D: Directory> Column<D> for Float32Column {
    type R = Float32ColumnReader<D>;
    type W = Float32ColumnWriter<D>;
    fn reader(&self) -> Self::R {
        todo!()
    }
    fn writer(&self) -> Self::W {
        todo!()
    }
}

pub struct Float32ColumnHeader {
    pub payload: OffsetSize,
    pub null_bitmap: OffsetSize,
}

pub struct Float32ColumnReader<D: Directory> {
    pub reader: Arc<D::ReaderType>,
    pub segments: HashMap<u32, Float32ColumnHeader>,
}

#[async_trait]
impl<D: Directory> ColumnReader<D> for Float32ColumnReader<D> {
    async fn read(
        &self,
        _reader: &D::ReaderType,
        _keys: &[KeyOffset],
    ) -> Result<Arc<dyn Array>, MurrError> {
        todo!()
    }
}

pub struct Float32ColumnWriter<D: Directory> {
    pub writer: Arc<D::WriterType>,
}

#[async_trait]
impl<D: Directory> ColumnWriter<D> for Float32ColumnWriter<D> {
    async fn write(&self, _values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError> {
        todo!()
    }
}
