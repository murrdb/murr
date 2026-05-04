use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use memmap2::Mmap;

use crate::core::MurrError;
use crate::io::directory::mmap::directory::MMapDirectory;
use crate::io::directory::{DirectoryReader, SegmentReadRequest, SegmentReadResponse};
use crate::io::info::TableInfo;

pub struct MMapReader {
    dir: Arc<MMapDirectory>,
    info: TableInfo,
    mmaps: Vec<Option<Arc<Mmap>>>,
}

impl MMapReader {
    fn load_info(dir: &MMapDirectory) -> Result<TableInfo, MurrError> {
        let path = dir.metadata_path();
        let data = std::fs::read(&path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", path.display())))
    }

    fn load_mmaps(
        dir: &MMapDirectory,
        info: &TableInfo,
        existing: &[Option<Arc<Mmap>>],
    ) -> Result<Vec<Option<Arc<Mmap>>>, MurrError> {
        if info.segments.is_empty() {
            return Ok(Vec::new());
        }
        let max_id = info.segments.iter().map(|s| s.id).max().unwrap() as usize;
        let mut mmaps: Vec<Option<Arc<Mmap>>> = (0..=max_id).map(|_| None).collect();

        for seg in &info.segments {
            let idx = seg.id as usize;
            if let Some(existing_mmap) = existing.get(idx).and_then(|m| m.as_ref()) {
                mmaps[idx] = Some(Arc::clone(existing_mmap));
                continue;
            }
            let path = dir.segment_path(seg.id);
            let file = std::fs::File::open(&path)
                .map_err(|e| MurrError::IoError(format!("opening {}: {e}", path.display())))?;
            // SAFETY: segment files are written once, synced, and renamed into place;
            // they are never truncated or modified after creation.
            let mmap = unsafe { Mmap::map(&file) }
                .map_err(|e| MurrError::IoError(format!("mmapping {}: {e}", path.display())))?;
            mmaps[idx] = Some(Arc::new(mmap));
        }

        Ok(mmaps)
    }
}

#[async_trait]
impl DirectoryReader for MMapReader {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = Self::load_info(&dir)?;
        let mmaps = Self::load_mmaps(&dir, &info, &[])?;
        Ok(MMapReader { dir, info, mmaps })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        let info = Self::load_info(&self.dir)?;
        let mmaps = Self::load_mmaps(&self.dir, &info, &self.mmaps)?;
        Ok(MMapReader {
            dir: self.dir.clone(),
            info,
            mmaps,
        })
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<SegmentReadResponse>, MurrError> {
        debug!("mmap read: {} requests", requests.len());
        let mut results = Vec::with_capacity(requests.len());
        for req in requests {
            let mmap = self
                .mmaps
                .get(req.segment as usize)
                .and_then(|m| m.as_ref())
                .ok_or_else(|| {
                    MurrError::SegmentError(format!("segment {} not loaded", req.segment))
                })?;
            let start = req.read.offset as usize;
            let end = start + req.read.size as usize;
            let slice = mmap.get(start..end).ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "segment {} read out of bounds: offset={} size={} mmap_len={}",
                    req.segment,
                    req.read.offset,
                    req.read.size,
                    mmap.len()
                ))
            })?;
            results.push(SegmentReadResponse {
                request: *req,
                bytes: slice.to_vec(),
            });
        }
        Ok(results)
    }
}
