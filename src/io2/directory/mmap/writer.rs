use std::sync::Arc;

use crate::io2::directory::{TableWriter, mmap::directory::MMapDirectory};

pub struct MMapWriter {
    dir: Arc<MMapDirectory>,
}

impl TableWriter for MMapWriter {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Self {
        MMapWriter { dir: dir }
    }

    async fn write_info(info: &crate::io2::info::TableInfo) -> Result<(), crate::core::MurrError> {
        todo!()
    }

    async fn write_segment(
        segment: crate::io2::directory::SegmentBytes,
    ) -> Result<(), crate::core::MurrError> {
        todo!()
    }
}
