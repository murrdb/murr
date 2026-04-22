use serde::{Deserialize, Serialize};

use crate::{
    core::MurrError,
    io::directory::{ReadRequest, SegmentReadRequest},
    io3::model::{OffsetSize, SegmentSchema},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentFooter {
    pub version: u32,
    pub name: u32,
    pub schema: SegmentSchema,
    pub keys: OffsetSize,
    pub rows: OffsetSize,
}

impl SegmentFooter {
    fn from_last_block(bytes: &[u8]) -> Result<SegmentFooter, MurrError> {
        todo!()
    }
}

pub struct SegmentReader {
    footer: SegmentFooter,
}

impl SegmentReader {
    fn new(footer: SegmentFooter) -> Self {
        SegmentReader { footer }
    }

    fn key_request(&self) -> SegmentReadRequest {
        SegmentReadRequest {
            segment: self.footer.name,
            read: self.footer.keys.into(),
        }
    }
}
