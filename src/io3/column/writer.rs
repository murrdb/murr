use arrow::array::Array;

use crate::{
    core::MurrError,
    io3::row::Row,
    proto::model::{SegmentColumnSchema, SegmentSchema},
};

pub struct RowBuffer {
    rows: Vec<Row>,
}

impl RowBuffer {
    fn new(rows: usize, schema: &SegmentSchema) -> Self {
        let capacity = schema.capacity();
        RowBuffer {
            rows: (0..rows).map(|_| Row::new(capacity)).collect(),
        }
    }
}

pub trait ColumnWriter {
    type A: Array;
    fn new(schema: &SegmentColumnSchema) -> Result<Self, MurrError>
    where
        Self: Sized;
    fn write(&self, values: Self::A) -> Result<(), MurrError>;
}
