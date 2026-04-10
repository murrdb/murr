pub mod mem;
pub mod mmap;

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    core::{MurrError, TableSchema},
    io::{
        bytes::{FromBytes, StringOffsetPair},
        column::ColumnSegmentBytes,
        info::TableInfo,
        url::Url,
    },
};

pub const METADATA_JSON: &str = "_metadata.json";

#[async_trait]
pub trait Directory: Sized + Send + Sync + 'static {
    type Location: Url;
    type ReaderType: DirectoryReader + Send + Sync;
    type WriterType: DirectoryWriter + Send + Sync;

    fn create(url: &Self::Location, index: &str, schema: TableSchema, page_size: u32, direct: bool) -> Result<Self, MurrError>;
    fn open(url: &Self::Location, index: &str, page_size: u32, direct: bool) -> Result<Self, MurrError>;
    fn list_indexes(url: &Self::Location) -> Vec<String>;
    fn schema(&self) -> &TableSchema;
    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError>;
    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError>;
}

pub struct ReadRequest {
    pub offset: u32,
    pub size: u32,
}

pub struct SegmentReadRequest {
    pub segment: u32,
    pub read: ReadRequest,
}

/// Object-safe reader interface used by columns, bitmap, and table reader.
#[async_trait]
pub trait Reader: Send + Sync {
    fn info(&self) -> &TableInfo;
    async fn reopen(&self) -> Result<Arc<dyn Reader>, MurrError>;
    async fn read_f32(&self, requests: &[SegmentReadRequest]) -> Result<Vec<f32>, MurrError>;
    async fn read_u64(&self, requests: &[SegmentReadRequest]) -> Result<Vec<u64>, MurrError>;
    async fn read_bytes(&self, requests: &[SegmentReadRequest]) -> Result<Vec<Vec<u8>>, MurrError>;
    async fn read_string(&self, requests: &[SegmentReadRequest]) -> Result<Vec<String>, MurrError>;
    async fn read_string_offset_pair(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<StringOffsetPair>, MurrError>;
}

/// Concrete directory-aware reader with construction and reopen support.
#[async_trait]
pub trait DirectoryReader: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn reopen_reader(&self) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read<T: Send, C: FromBytes<T> + Send>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError>;
}

/// Blanket impl: every DirectoryReader is automatically a Reader.
#[async_trait]
impl<R: DirectoryReader + 'static> Reader for R {
    fn info(&self) -> &TableInfo {
        DirectoryReader::info(self)
    }

    async fn reopen(&self) -> Result<Arc<dyn Reader>, MurrError> {
        let new = DirectoryReader::reopen_reader(self).await?;
        Ok(Arc::new(new))
    }

    async fn read_f32(&self, requests: &[SegmentReadRequest]) -> Result<Vec<f32>, MurrError> {
        self.read::<f32, f32>(requests).await
    }

    async fn read_u64(&self, requests: &[SegmentReadRequest]) -> Result<Vec<u64>, MurrError> {
        self.read::<u64, u64>(requests).await
    }

    async fn read_bytes(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<Vec<u8>>, MurrError> {
        self.read::<Vec<u8>, Vec<u8>>(requests).await
    }

    async fn read_string(&self, requests: &[SegmentReadRequest]) -> Result<Vec<String>, MurrError> {
        self.read::<String, String>(requests).await
    }

    async fn read_string_offset_pair(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<StringOffsetPair>, MurrError> {
        self.read::<StringOffsetPair, StringOffsetPair>(requests).await
    }
}

/// Directory-aware writer with construction support.
#[async_trait]
pub trait DirectoryWriter: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &[ColumnSegmentBytes]) -> Result<(), MurrError>;
}
