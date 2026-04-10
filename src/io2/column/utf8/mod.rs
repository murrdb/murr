pub mod footer;
pub mod reader;
pub mod writer;

use std::sync::Arc;

use crate::io2::column::Column;
use crate::io2::info::ColumnInfo;

pub struct Utf8Column {
    column: Arc<ColumnInfo>,
}

impl Utf8Column {
    pub fn new(column: Arc<ColumnInfo>) -> Self {
        Utf8Column { column }
    }
}

impl Column for Utf8Column {
    type R = reader::Utf8ColumnReader;
    type W = writer::Utf8ColumnWriter;

    fn reader(&self) -> Self::R {
        todo!("use Utf8ColumnReader::open() directly instead")
    }

    fn writer(&self) -> Self::W {
        writer::Utf8ColumnWriter::new(self.column.clone())
    }
}
