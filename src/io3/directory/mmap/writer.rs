use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;

use crate::core::MurrError;
use crate::io3::directory::mmap::directory::MMapDirectory;
use crate::io3::directory::DirectoryWriter;
use crate::io3::info::{SegmentInfo, TableInfo};
use crate::io3::table::segment::SegmentBytes;

pub struct MMapWriter {
    dir: Arc<MMapDirectory>,
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

impl MMapWriter {
    fn load_existing_info(&self) -> Option<TableInfo> {
        let path = self.dir.metadata_path();
        std::fs::read(&path)
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())
    }

    fn next_segment_id(&self) -> u32 {
        self.load_existing_info()
            .map(|info| info.segments.len() as u32)
            .unwrap_or(0)
    }

    fn flush_segment(&self, segment_id: u32, data: &[u8]) -> Result<(), MurrError> {
        let seg_path = self.dir.segment_path(segment_id);
        let tmp = tmp_path(&seg_path);

        let mut file = std::fs::File::create(&tmp)
            .map_err(|e| MurrError::IoError(format!("creating {}: {e}", tmp.display())))?;
        file.write_all(data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", tmp.display())))?;
        file.sync_all()
            .map_err(|e| MurrError::IoError(format!("syncing {}: {e}", tmp.display())))?;
        drop(file);

        std::fs::rename(&tmp, &seg_path).map_err(|e| {
            MurrError::IoError(format!(
                "renaming {} to {}: {e}",
                tmp.display(),
                seg_path.display()
            ))
        })?;
        Ok(())
    }

    fn flush_info(&self, info: &TableInfo) -> Result<(), MurrError> {
        let path = self.dir.metadata_path();
        let data = serde_json::to_vec_pretty(info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let tmp = tmp_path(&path);
        std::fs::write(&tmp, &data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, &path).map_err(|e| {
            MurrError::IoError(format!(
                "renaming {} to {}: {e}",
                tmp.display(),
                path.display()
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl DirectoryWriter for MMapWriter {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        Ok(MMapWriter { dir })
    }

    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        let bytes = segment.to_bytes()?;
        let size_bytes = bytes.len() as u32;
        let num_values = segment.footer.row_count;
        let segment_id = self.next_segment_id();

        debug!(
            "mmap write: segment={segment_id} path={} bytes={size_bytes} rows={num_values}",
            self.dir.segment_path(segment_id).display()
        );

        // Write segment file before updating metadata: an orphaned .seg is harmless,
        // but a metadata entry pointing to a missing file would error on reader open.
        self.flush_segment(segment_id, &bytes)?;

        let mut info = self.load_existing_info().unwrap_or_else(|| TableInfo {
            schema: self.dir.schema.clone(),
            segments: Vec::new(),
        });
        info.segments.push(SegmentInfo {
            id: segment_id,
            size_bytes,
            num_values,
        });

        self.flush_info(&info)?;
        Ok(())
    }
}
