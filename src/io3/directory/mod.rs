use std::sync::Arc;

use crate::{
    core::{MurrError, TableSchema},
    io3::{info::TableInfo, model::SegmentSchema, row::Row, url::Url},
};

use async_trait::async_trait;

#[async_trait]
pub trait Directory: Sized + Send + Sync + 'static {
    type Location: Url;
    type ReaderType: DirectoryReader + Send + Sync;
    type WriterType: DirectoryWriter + Send + Sync;

    fn create(
        url: &Self::Location,
        index: &str,
        schema: TableSchema,
        page_size: u32,
        direct: bool,
    ) -> Result<Self, MurrError>;
    fn open(
        url: &Self::Location,
        index: &str,
        page_size: u32,
        direct: bool,
    ) -> Result<Self, MurrError>;
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

// /// Directory-aware reader with construction and reopen support.
#[async_trait]
pub trait DirectoryReader: Sized + Send + Sync + 'static {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn reopen_reader(&self) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read(&self, requests: &[SegmentReadRequest]) -> Result<Vec<Row>, MurrError>;
}

pub struct SegmentBytes {
    schema: SegmentSchema,
    keys: Vec<String>,
    bytes: Vec<Row>,
}

// /// Directory-aware writer with construction support.
#[async_trait]
pub trait DirectoryWriter: Sized + Send + Sync {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError>;
}
