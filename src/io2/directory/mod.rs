pub mod mmap;

use std::sync::Arc;

use crate::{
    core::MurrError,
    io2::{
        bytes::FromBytes,
        info::{ColumnInfo, TableInfo},
        url::Url,
    },
};

pub const METADATA_JSON: &str = "_metadata.json";

#[allow(async_fn_in_trait)]
pub trait Directory: Sized + Send + Sync + 'static {
    type Location: Url;
    type ReaderType: TableReader;
    type WriterType: TableWriter;

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

#[allow(async_fn_in_trait)]
pub trait TableReader: Sized {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read<T, C: FromBytes<T>>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError>;
}

#[allow(async_fn_in_trait)]
pub trait TableWriter: Sized {
    type D: Directory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError>;
    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError>;
}

pub struct SegmentBytes {
    pub id: u32,
    pub payload: Vec<u8>,
    pub columns: Vec<ColumnInfo>,
}
