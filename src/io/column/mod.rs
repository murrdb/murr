use std::sync::Arc;

use arrow::array::Array;
use async_trait::async_trait;

use crate::{
    core::MurrError,
    io::{
        directory::Reader,
        info::{ColumnInfo, ColumnSegments},
        table::key_offset::KeyOffset,
    },
};

pub mod float32;
pub mod reopen;
pub mod utf8;

pub const MAX_COLUMN_HEADER_SIZE: u32 = 4096;

pub struct ColumnSegmentBytes {
    pub column: ColumnInfo,
    pub bytes: Vec<u8>,
    pub num_values: u32,
}

impl ColumnSegmentBytes {
    pub fn new(column: ColumnInfo, bytes: Vec<u8>, num_values: u32) -> Self {
        ColumnSegmentBytes {
            column,
            bytes,
            num_values,
        }
    }
}

pub fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

pub fn align8_padding(len: u32) -> u32 {
    (8 - (len % 8)) % 8
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

pub trait Column: Send + Sync {
    type R: ColumnReader;
    type W: ColumnWriter;
    fn reader(&self) -> Self::R;
    fn writer(&self) -> Self::W;
}

#[async_trait]
pub trait ColumnReader: Send + Sync {
    async fn open(
        reader: Arc<dyn Reader>,
        column: &ColumnSegments,
        previous: &Option<Self>,
    ) -> Result<Self, MurrError>
    where
        Self: Sized;
    async fn reopen(
        &self,
        reader: Arc<dyn Reader>,
        column: &ColumnSegments,
    ) -> Result<Arc<dyn ColumnReader>, MurrError>;
    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError>;
}

#[async_trait]
pub trait ColumnWriter: Send + Sync {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError>;
}
