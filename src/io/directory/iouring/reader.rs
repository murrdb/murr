use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, warn};
use tokio::sync::oneshot;

use crate::core::MurrError;
use crate::io::directory::iouring::directory::IoUringDirectory;
use crate::io::directory::iouring::pool::{BatchJob, IoUringPool};
use crate::io::directory::{DirectoryReader, SegmentReadRequest, SegmentReadResponse};
use crate::io::info::TableInfo;

pub struct IoUringReader {
    dir: Arc<IoUringDirectory>,
    info: TableInfo,
    files: Arc<Vec<Option<Arc<File>>>>,
    pool: Arc<IoUringPool>,
}

impl IoUringReader {
    fn load_info(dir: &IoUringDirectory) -> Result<TableInfo, MurrError> {
        let path = dir.metadata_path();
        let data = std::fs::read(&path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", path.display())))
    }

    fn load_files(
        dir: &IoUringDirectory,
        info: &TableInfo,
        existing: &[Option<Arc<File>>],
    ) -> Result<Vec<Option<Arc<File>>>, MurrError> {
        if info.segments.is_empty() {
            return Ok(Vec::new());
        }
        let max_id = info.segments.iter().map(|s| s.id).max().unwrap() as usize;
        let mut files: Vec<Option<Arc<File>>> = (0..=max_id).map(|_| None).collect();

        for seg in &info.segments {
            let idx = seg.id as usize;
            if let Some(existing_file) = existing.get(idx).and_then(|f| f.as_ref()) {
                files[idx] = Some(Arc::clone(existing_file));
                continue;
            }
            let path = dir.segment_path(seg.id);
            let mut opts = OpenOptions::new();
            opts.read(true);
            let mut flags = libc::O_NOATIME;
            if dir.cfg.direct {
                flags |= libc::O_DIRECT;
            }
            opts.custom_flags(flags);
            let file = opts
                .open(&path)
                .map_err(|e| MurrError::IoError(format!("opening {}: {e}", path.display())))?;
            // Tell the kernel: random access, no readahead. With small scattered
            // row reads on a working set larger than RAM, default readahead pulls
            // in 16-128 KB of neighbour pages we won't touch — wasted bandwidth
            // and page-cache pressure that evicts data we *would* reuse. No-op
            // under O_DIRECT (page cache is bypassed) but harmless.
            if !dir.cfg.direct {
                let rc = unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM)
                };
                if rc != 0 {
                    warn!(
                        "posix_fadvise(POSIX_FADV_RANDOM) on {} failed: errno {rc}",
                        path.display()
                    );
                }
            }
            files[idx] = Some(Arc::new(file));
        }

        Ok(files)
    }
}

#[async_trait]
impl DirectoryReader for IoUringReader {
    type D = IoUringDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = Self::load_info(&dir)?;
        let files = Arc::new(Self::load_files(&dir, &info, &[])?);
        let pool = dir.pool()?;
        Ok(IoUringReader {
            dir,
            info,
            files,
            pool,
        })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        let info = Self::load_info(&self.dir)?;
        let files = Arc::new(Self::load_files(&self.dir, &info, &self.files)?);
        Ok(IoUringReader {
            dir: self.dir.clone(),
            info,
            files,
            pool: Arc::clone(&self.pool),
        })
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<SegmentReadResponse>, MurrError> {
        debug!("iouring read: {} requests", requests.len());
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let (tx, rx) = oneshot::channel();
        let job = BatchJob {
            files: Arc::clone(&self.files),
            requests: requests.to_vec(),
            respond: tx,
        };
        self.pool.submit(job)?;
        rx.await
            .map_err(|_| MurrError::IoError("io_uring worker dropped response".to_string()))?
    }
}
