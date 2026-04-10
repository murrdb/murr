pub mod footer;
pub mod reader;
pub mod writer;

use std::sync::Arc;

use crate::io2::column::Column;
use crate::io2::directory::Directory;
use crate::io2::info::ColumnInfo;

pub struct Utf8Column<D: Directory> {
    dir: Arc<D>,
    column: Arc<ColumnInfo>,
}

impl<D: Directory> Utf8Column<D> {
    pub fn new(dir: Arc<D>, column: Arc<ColumnInfo>) -> Self {
        Utf8Column { dir, column }
    }
}

impl<D: Directory> Column<D> for Utf8Column<D> {
    type R = reader::Utf8ColumnReader<D>;
    type W = writer::Utf8ColumnWriter<D>;

    fn reader(&self) -> Self::R {
        todo!("use Utf8ColumnReader::open() directly instead")
    }

    fn writer(&self) -> Self::W {
        writer::Utf8ColumnWriter::new(self.dir.clone(), self.column.clone())
    }
}
