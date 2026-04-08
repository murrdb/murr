pub mod mmap;
use std::sync::Arc;

use crate::{
    core::MurrError,
    io2::{
        bytes::FromBytes,
        info::{ColumnInfo, TableInfo},
    },
};

const METADATA_JSON: &str = "_metadata.json";

trait Directory {
    type Location: Url;
    type ReaderType: TableReader;
    type WriterType: TableWriter;
    fn open(uri: &Location, page_size: u32, direct: bool) -> Self;

    async fn open_reader() -> Self::ReaderType;
    async fn open_writer() -> Self::WriterType;
}

struct ReadRequest {
    offset: u32,
    size: u32,
}

struct SegmentReadRequest {
    segment: u32,
    read: ReadRequest,
}

trait TableReader {
    type D: Directory;
    async fn new(dir: Arc<Self::D>) -> Self;
    async fn info() -> Result<TableInfo, MurrError>;
    async fn read<T, C: FromBytes<T>>(requests: &[SegmentReadRequest])
    -> Result<Vec<T>, MurrError>;
}

trait TableWriter {
    type D: Directory;
    async fn new(dir: Arc<Self::D>) -> Self;
    async fn write_info(info: &TableInfo) -> Result<(), MurrError>;
    async fn write_segment(segment: SegmentBytes) -> Result<(), MurrError>;
}

pub struct SegmentBytes {
    payload: Vec<u8>,
    columns: Vec<ColumnInfo>,
}
