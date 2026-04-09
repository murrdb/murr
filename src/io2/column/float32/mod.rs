pub mod footer;
pub mod reader;
pub mod writer;

use std::sync::Arc;

use crate::io2::column::Column;
use crate::io2::directory::Directory;
use crate::io2::info::ColumnInfo;

pub struct Float32Column<D: Directory> {
    dir: Arc<D>,
    column: Arc<ColumnInfo>,
}

impl<D: Directory> Float32Column<D> {
    pub fn new(dir: Arc<D>, column: Arc<ColumnInfo>) -> Self {
        Float32Column { dir, column }
    }
}

impl<D: Directory> Column<D> for Float32Column<D> {
    type R = reader::Float32ColumnReader<D>;
    type W = writer::Float32ColumnWriter<D>;

    fn reader(&self) -> Self::R {
        todo!("use Float32ColumnReader::open() directly instead")
    }

    fn writer(&self) -> Self::W {
        writer::Float32ColumnWriter::new(self.dir.clone(), self.column.clone())
    }
}
