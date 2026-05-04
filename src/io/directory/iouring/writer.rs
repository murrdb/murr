use std::sync::Arc;

use async_trait::async_trait;
use log::debug;

use crate::core::MurrError;
use crate::io::directory::DirectoryWriter;
use crate::io::directory::file_writer;
use crate::io::directory::iouring::directory::IoUringDirectory;
use crate::io::info::SegmentInfo;
use crate::io::table::segment::SegmentBytes;

pub struct IoUringWriter {
    dir: Arc<IoUringDirectory>,
}

#[async_trait]
impl DirectoryWriter for IoUringWriter {
    type D = IoUringDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        Ok(IoUringWriter { dir })
    }

    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        let bytes = segment.to_bytes()?;
        let size_bytes = bytes.len() as u32;
        let num_values = segment.footer.row_count;

        let metadata_path = self.dir.metadata_path();
        let segment_id = file_writer::next_segment_id(&metadata_path);
        let seg_path = self.dir.segment_path(segment_id);

        debug!(
            "iouring write: segment={segment_id} path={} bytes={size_bytes} rows={num_values}",
            seg_path.display()
        );

        file_writer::atomic_write(&seg_path, &bytes)?;
        file_writer::append_segment_info(
            &metadata_path,
            &self.dir.schema,
            SegmentInfo {
                id: segment_id,
                size_bytes,
                num_values,
            },
        )?;
        Ok(())
    }
}
