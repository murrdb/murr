pub mod mmap;
pub mod mem;

use std::sync::Arc;

use crate::{
    core::{MurrError, TableSchema},
    io::{info::TableInfo, table::segment::SegmentBytes},
};

use async_trait::async_trait;

#[async_trait]
pub trait Directory: Sized + Send + Sync + 'static {
    const METADATA_JSON: &str = "_metadata.json";
    type ReaderType: DirectoryReader;
    type WriterType: DirectoryWriter;
    type ConfigType: DirectoryConfig;

    fn create(index: &str, schema: TableSchema, config: Self::ConfigType) -> Result<Self, MurrError>;
    fn open(index: &str, config: Self::ConfigType) -> Result<Self, MurrError>;
    fn list_indexes(config: &Self::ConfigType) -> Vec<String>;
    fn schema(&self) -> &TableSchema;
    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError>;
    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRequest {
    pub offset: u32,
    pub size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadRequest {
    pub segment: u32,
    pub read: ReadRequest,
}

#[derive(PartialEq, Eq, Debug)]
pub struct SegmentReadResponse {
    pub request: SegmentReadRequest,
    pub bytes: Vec<u8>,
}

pub trait DirectoryConfig: Sized + Send + Sync + Default {}

#[async_trait]
pub trait DirectoryReader: Sized + Send + Sync + 'static {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn reopen_reader(&self) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<SegmentReadResponse>, MurrError>;
}

#[async_trait]
pub trait DirectoryWriter: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError>;
}
