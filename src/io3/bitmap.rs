use arrow::array::RecordBatch;

use crate::io3::model::SegmentColumnSchema;

pub struct Bitset<'a> {
    pub size: u8, // so 256*8=2048 max columns in bitmap
    pub payload: &'a [u8],
}

impl Bitset<'_> {
    fn new(payload: &[u8]) -> Bitset {
        let size = payload[0];
        Bitset { size, payload }
    }

    fn is_null(&self, column: &SegmentColumnSchema) -> bool {
        // compute offset and do a check
        todo!()
    }

    fn write(batch: RecordBatch) -> Vec<Vec<u8>> {
        todo!()
    }
}
