use std::sync::Arc;

use arrow::array::Array;
use async_trait::async_trait;

use crate::{
    core::MurrError,
    io::table::TableWriter,
    io2::{
        directory::Reader,
        info::{ColumnInfo, SegmentInfo},
        table::key_offset::KeyOffset,
    },
};

pub struct SegmentBytes {
    pub bytes: Vec<u8>,
}

pub struct ColumnSegmentBytes {
    column: ColumnInfo,
    bytes: SegmentBytes,
}

pub trait Column {
    fn reader(&self) -> Arc<dyn ColumnReader>;
    fn writer(&self, table: &mut TableWriter) -> Arc<dyn ColumnWriter>;
}

#[async_trait]
pub trait ColumnReader {
    async fn read(&self, reader: &Reader, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError>;
}

#[async_trait]
pub trait ColumnWriter {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError>;
}
