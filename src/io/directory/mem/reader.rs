use std::sync::Arc;

use async_trait::async_trait;
use log::debug;

use crate::core::MurrError;
use crate::io::directory::mem::directory::MemDirectory;
use crate::io::directory::{DirectoryReader, SegmentReadRequest, SegmentReadResponse};
use crate::io::info::TableInfo;

pub struct MemReader {
    dir: Arc<MemDirectory>,
    info: TableInfo,
}

#[async_trait]
impl DirectoryReader for MemReader {
    type D = MemDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = dir
            .metadata
            .read()
            .map_err(|e| MurrError::IoError(format!("metadata lock poisoned: {e}")))?
            .clone();
        Ok(MemReader { dir, info })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        Self::new(self.dir.clone()).await
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<SegmentReadResponse>, MurrError> {
        debug!("mem read: {} requests", requests.len());
        let segments = self
            .dir
            .segments
            .read()
            .map_err(|e| MurrError::IoError(format!("segments lock poisoned: {e}")))?;

        let mut results: Vec<SegmentReadResponse> = Vec::with_capacity(requests.len());
        for req in requests {
            let idx = req.segment as usize;
            let data = segments.get(idx).and_then(|s| s.as_ref()).ok_or_else(|| {
                MurrError::SegmentError(format!("segment {} not found", req.segment))
            })?;
            let start = req.read.offset as usize;
            let end = start + req.read.size as usize;
            let slice = data.get(start..end).ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "segment {} read out of bounds: offset={} size={} len={}",
                    req.segment,
                    req.read.offset,
                    req.read.size,
                    data.len()
                ))
            })?;
            let result = SegmentReadResponse {
                request: req.clone(),
                bytes: slice.to_vec(),
            };
            results.push(result);
        }
        Ok(results)
    }
}
