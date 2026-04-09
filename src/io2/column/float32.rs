use std::sync::Arc;

use arrow::array::Array;

use async_trait::async_trait;

use crate::{
    core::MurrError,
    io2::{
        column::{Column, ColumnReader, ColumnSegmentBytes, ColumnWriter},
        directory::Directory,
        info::ColumnInfo,
        table::key_offset::KeyOffset,
    },
};

pub struct Float32Column {
    column: ColumnInfo,
}

impl<D: Directory> Column<D> for Float32Column {
    type R = Float32ColumnReader;
    type W = Float32ColumnWriter;
    fn reader(&self) -> Self::R {
        todo!()
    }
    fn writer(&self) -> Self::W {
        todo!()
    }
}

pub struct Float32ColumnHeader {
    //
}

pub struct Float32ColumnReader {}

#[async_trait]
impl<D: Directory> ColumnReader<D> for Float32ColumnReader {
    async fn read(
        &self,
        reader: &D::ReaderType<'_>,
        keys: &[KeyOffset],
    ) -> Result<Arc<dyn Array>, MurrError> {
        todo!()
    }
}

pub struct Float32ColumnWriter {}

#[async_trait]
impl ColumnWriter for Float32ColumnWriter {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError> {
        todo!()
    }
}
