pub mod mem;

use std::sync::Arc;

use crate::{
    core::{MurrError, TableSchema},
    io3::{
        info::TableInfo, model::SegmentSchema, row::Row, table::segment::SegmentBytes, url::Url,
    },
};

use async_trait::async_trait;

#[async_trait]
pub trait Directory: Sized + Send + Sync + 'static {
    const METADATA_JSON: &str = "_metadata.json";
    type Location: Url;
    type ReaderType: DirectoryReader;
    type WriterType: DirectoryWriter;
    type ConfigType: DirectoryConfig;

    fn create(
        url: &Self::Location,
        index: &str,
        schema: TableSchema,
        config: Self::ConfigType,
    ) -> Result<Self, MurrError>;
    fn open(url: &Self::Location, index: &str, config: Self::ConfigType)
    -> Result<Self, MurrError>;
    fn list_indexes(url: &Self::Location) -> Vec<String>;
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

pub trait DirectoryConfig: Sized + Send + Sync {}

// Directory-aware reader with construction and reopen support.
#[async_trait]
pub trait DirectoryReader: Sized + Send + Sync + 'static {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn reopen_reader(&self) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read(&self, requests: &[SegmentReadRequest]) -> Result<Vec<Vec<u8>>, MurrError>;
}

// Directory-aware writer with construction support.
#[async_trait]
pub trait DirectoryWriter: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError>;
}
