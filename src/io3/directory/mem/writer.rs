use std::sync::Arc;

use async_trait::async_trait;
use log::debug;

use crate::core::MurrError;
use crate::io3::directory::DirectoryWriter;
use crate::io3::directory::mem::directory::MemDirectory;
use crate::io3::info::SegmentInfo;
use crate::io3::table::segment::SegmentBytes;

pub struct MemWriter {
    dir: Arc<MemDirectory>,
}

#[async_trait]
impl DirectoryWriter for MemWriter {
    type D = MemDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        Ok(MemWriter { dir })
    }

    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        let bytes = segment.to_bytes()?;
        let size_bytes = bytes.len() as u32;
        let num_values = segment.footer.row_count;

        let mut segments = self
            .dir
            .segments
            .write()
            .map_err(|e| MurrError::IoError(format!("segments lock poisoned: {e}")))?;
        let mut metadata = self
            .dir
            .metadata
            .write()
            .map_err(|e| MurrError::IoError(format!("metadata lock poisoned: {e}")))?;

        let id = segments.len() as u32;
        segments.push(Some(bytes));
        metadata.segments.push(SegmentInfo {
            id,
            size_bytes,
            num_values,
        });

        debug!("mem write: segment={id} size={size_bytes} rows={num_values}");
        Ok(())
    }
}
