pub mod mem;
pub mod mmap;

use crate::{
    core::MurrError,
    io2::{bytes::FromBytes, column::ColumnSegmentBytes, info::TableInfo, url::Url},
};

pub const METADATA_JSON: &str = "_metadata.json";

#[allow(async_fn_in_trait)]
pub trait Directory: Sized + Send + Sync + 'static {
    type Location: Url;
    type ReaderType<'a>: Reader<'a>;
    type WriterType<'a>: Writer<'a>;

    fn open(url: &Self::Location, page_size: u32, direct: bool) -> Self;
    async fn open_reader(&self) -> Result<Self::ReaderType<'_>, MurrError>;
    async fn open_writer(&self) -> Result<Self::WriterType<'_>, MurrError>;
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
pub trait Reader<'a>: Sized {
    type D: Directory;

    async fn new(dir: &'a Self::D) -> Result<Self, MurrError>;
    fn info(&self) -> &TableInfo;
    async fn read<T, C: FromBytes<T>>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError>;
}

#[allow(async_fn_in_trait)]
pub trait Writer<'a>: Sized {
    type D: Directory;

    async fn new(dir: &'a Self::D) -> Result<Self, MurrError>;
    async fn write(&self, segment: &[ColumnSegmentBytes]) -> Result<(), MurrError>;
}
