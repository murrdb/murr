use arrow::array::{Array, StringBuilder};

use crate::{core::MurrError, io3::row::Row, proto::model::SegmentColumnSchema};

pub trait ColumnReader {
    type A: Array;
    fn new(schema: SegmentColumnSchema) -> Result<Self, MurrError>
    where
        Self: Sized;
    fn read(&mut self, row: &Row);
    fn finish(&self) -> dyn Array;
}
