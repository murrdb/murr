pub mod footer;
pub mod reader;
pub mod writer;

use std::sync::Arc;

use crate::io2::column::Column;
use crate::io2::info::ColumnInfo;

pub struct Float32Column {
    column: Arc<ColumnInfo>,
}

impl Float32Column {
    pub fn new(column: Arc<ColumnInfo>) -> Self {
        Float32Column { column }
    }
}

impl Column for Float32Column {
    type R = reader::Float32ColumnReader;
    type W = writer::Float32ColumnWriter;

    fn reader(&self) -> Self::R {
        todo!("use Float32ColumnReader::open() directly instead")
    }

    fn writer(&self) -> Self::W {
        writer::Float32ColumnWriter::new(self.column.clone())
    }
}
