use std::sync::Arc;

use arrow::array::Array;
use async_trait::async_trait;

use crate::{
    core::MurrError,
    io2::{directory::Directory, info::ColumnInfo, table::key_offset::KeyOffset},
};

pub mod float32;

pub struct SegmentBytes {
    pub bytes: Vec<u8>,
}

pub struct ColumnSegmentBytes {
    pub column: ColumnInfo,
    pub bytes: SegmentBytes,
    pub num_values: u32,
}

impl ColumnSegmentBytes {
    pub fn new(column: ColumnInfo, bytes: Vec<u8>, num_values: u32) -> Self {
        ColumnSegmentBytes {
            column,
            bytes: SegmentBytes { bytes },
            num_values,
        }
    }
}

pub trait Column<D: Directory> {
    type R: ColumnReader<D>;
    type W: ColumnWriter;
    fn reader(&self) -> Self::R;
    fn writer(&self) -> Self::W;
}

#[async_trait]
pub trait ColumnReader<D: Directory>: Send + Sync {
    async fn read(
        &self,
        reader: &D::ReaderType<'_>,
        keys: &[KeyOffset],
    ) -> Result<Arc<dyn Array>, MurrError>;
}

#[async_trait]
pub trait ColumnWriter: Send + Sync {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError>;
}
