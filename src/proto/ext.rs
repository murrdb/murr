use arrow::datatypes::SchemaRef;

use crate::{
    core::DType,
    proto::model::{SegmentColumnSchema, SegmentSchema},
};

impl SegmentSchema {
    fn from_arrow_schema(schema: &SchemaRef) -> Self {
        todo!()
    }
}

impl DType {
    fn size(&self) -> usize {
        match self {
            DType::Float32 => 4,
            DType::Float64 => 8,
            DType::Utf8 => 4,
        }
    }
}

impl SegmentSchema {
    pub fn capacity(&self) -> usize {
        self.columns.iter().map(|c| c.dtype().size()).sum()
    }
}
