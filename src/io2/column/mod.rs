use std::sync::Arc;

use arrow::array::Array;
use async_trait::async_trait;

use crate::{
    core::MurrError,
    io2::{
        directory::Directory,
        info::{ColumnInfo, ColumnSegments},
        table::key_offset::KeyOffset,
    },
};

pub mod float32;
pub mod reopen;
pub mod utf8;

pub const MAX_COLUMN_HEADER_SIZE: u32 = 4096;

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

#[derive(Debug, Clone)]
pub struct OffsetSize {
    pub offset: u32,
    pub size: u32,
}

pub trait ColumnFooter: Clone + Send + Sync {
    fn base_offset(&self) -> u32;
    fn bitmap(&self) -> &OffsetSize;
    fn parse(data: &[u8], base_offset: u32) -> Result<Self, MurrError>;
}

pub trait Column<D: Directory>: Send + Sync {
    type R: ColumnReader<D>;
    type W: ColumnWriter<D>;
    fn reader(&self) -> Self::R;
    fn writer(&self) -> Self::W;
}

#[async_trait]
pub trait ColumnReader<D: Directory>: Send + Sync {
    async fn open(
        reader: Arc<D::ReaderType>,
        column: &ColumnSegments,
        previous: &Option<Self>,
    ) -> Result<Self, MurrError>
    where
        Self: Sized;
    async fn reopen(
        &self,
        reader: Arc<D::ReaderType>,
        column: &ColumnSegments,
    ) -> Result<Box<dyn ColumnReader<D>>, MurrError>;
    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError>;
}

#[async_trait]
pub trait ColumnWriter<D: Directory>: Send + Sync {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError>;
}
