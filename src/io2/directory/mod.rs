pub mod mem;
pub mod mmap;

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    core::MurrError,
    io2::{bytes::FromBytes, column::ColumnSegmentBytes, info::TableInfo, url::Url},
};

pub const METADATA_JSON: &str = "_metadata.json";

#[async_trait]
pub trait Directory: Sized + Send + Sync + 'static {
    type Location: Url;
    type ReaderType: Reader + Send + Sync;
    type WriterType: Writer + Send + Sync;

    fn open(url: &Self::Location, page_size: u32, direct: bool) -> Self;
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

#[async_trait]
pub trait Reader: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn reopen(&self) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read<T: Send, C: FromBytes<T> + Send>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError>;
}

#[async_trait]
pub trait Writer: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &[ColumnSegmentBytes]) -> Result<(), MurrError>;
}
