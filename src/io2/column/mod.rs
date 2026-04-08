use std::sync::Arc;

use arrow::{array::Array, datatypes::Field};

use crate::{
    core::{ColumnSchema, MurrError},
    io2::directory::SegmentBytes,
};

const IS_MISSING: u32 = u32::MAX;

pub struct SegmentOffset {
    segment: u32,
    offset: u32,
}

impl SegmentOffset {
    pub fn missing() -> Self {
        SegmentOffset {
            segment: IS_MISSING,
            offset: 0,
        }
    }
    pub fn is_missing(self) -> bool {
        self.segment == IS_MISSING
    }
}

trait ColumnReader {
    fn field() -> Field;
    async fn read(keys: &[SegmentOffset]) -> Result<Arc<dyn Array>, MurrError>;
}

trait ColumnWriter {
    type A: Array;
    fn write(
        buffer: SegmentBytes,
        config: ColumnSchema,
        values: Self::A,
    ) -> Result<Vec<u8>, MurrError>;
}
