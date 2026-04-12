use std::sync::Arc;

use arrow::array::Array;
use async_trait::async_trait;

use crate::{
    core::MurrError,
    io::{
        directory::DirectoryReader,
        info::{ColumnInfo, ColumnSegments},
        table::key_offset::KeyOffset,
    },
};

pub mod float32;
pub mod float64;
pub mod reopen;
pub mod scalar;
pub mod utf8;

pub const MAX_COLUMN_HEADER_SIZE: u32 = 4096;

pub struct PayloadBytes {
    pub bytes: Vec<u8>,
    pub padding: u32,
}

impl PayloadBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        let padding = align8_padding(bytes.len() as u32);
        PayloadBytes { bytes, padding }
    }

    pub fn padded_len(&self) -> u32 {
        self.bytes.len() as u32 + self.padding
    }
}

pub struct ColumnSegmentBytes {
    pub column: ColumnInfo,
    pub buffers: Vec<PayloadBytes>,
    pub footer: Vec<u8>,
    pub num_values: u32,
}

impl ColumnSegmentBytes {
    pub fn new(
        column: ColumnInfo,
        buffers: Vec<PayloadBytes>,
        footer: Vec<u8>,
        num_values: u32,
    ) -> Self {
        ColumnSegmentBytes {
            column,
            buffers,
            footer,
            num_values,
        }
    }

    pub fn byte_len(&self) -> usize {
        self.buffers
            .iter()
            .map(|b| b.padded_len() as usize)
            .sum::<usize>()
            + self.footer.len()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.byte_len());
        for payload in &self.buffers {
            buf.extend_from_slice(&payload.bytes);
            buf.resize(buf.len() + payload.padding as usize, 0);
        }
        buf.extend_from_slice(&self.footer);
        buf
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
    fn bitmap(&self) -> &OffsetSize;
    fn parse(data: &[u8], base_offset: u32) -> Result<Self, MurrError>;
    fn encode(&self) -> Vec<u8>;
}

#[async_trait]
pub trait ColumnReader<R: DirectoryReader>: Send + Sync {
    async fn open(
        reader: Arc<R>,
        column: &ColumnSegments,
        previous: &Option<Self>,
    ) -> Result<Self, MurrError>
    where
        Self: Sized;
    async fn reopen(
        &self,
        reader: Arc<R>,
        column: &ColumnSegments,
    ) -> Result<Arc<dyn ColumnReader<R>>, MurrError>;
    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError>;
}

pub trait ColumnWriter: Array {
    fn write_column(&self, column: &ColumnInfo) -> Result<ColumnSegmentBytes, MurrError>;
}
