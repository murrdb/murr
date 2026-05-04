use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use io_uring::{opcode, types};
use log::debug;

use crate::core::MurrError;
use crate::io::directory::iouring::IoUringConfig;
use crate::io::directory::iouring::directory::IoUringDirectory;
use crate::io::directory::iouring::ring::{AlignedBuf, with_ring};
use crate::io::directory::{DirectoryReader, SegmentReadRequest, SegmentReadResponse};
use crate::io::info::TableInfo;

pub struct IoUringReader {
    dir: Arc<IoUringDirectory>,
    info: TableInfo,
    files: Vec<Option<Arc<File>>>,
    cfg: IoUringConfig,
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
            if dir.cfg.direct {
                opts.custom_flags(libc::O_DIRECT);
            }
            let file = opts
                .open(&path)
                .map_err(|e| MurrError::IoError(format!("opening {}: {e}", path.display())))?;
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
        let files = Self::load_files(&dir, &info, &[])?;
        let cfg = dir.cfg.clone();
        Ok(IoUringReader {
            dir,
            info,
            files,
            cfg,
        })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        let info = Self::load_info(&self.dir)?;
        let files = Self::load_files(&self.dir, &info, &self.files)?;
        Ok(IoUringReader {
            dir: self.dir.clone(),
            info,
            files,
            cfg: self.cfg.clone(),
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

        let cfg = self.cfg.clone();
        let files = self.files.clone();
        let requests: Vec<SegmentReadRequest> = requests.to_vec();

        tokio::task::spawn_blocking(move || run_batch(cfg, files, requests))
            .await
            .map_err(|e| MurrError::IoError(format!("iouring blocking task panicked: {e}")))?
    }
}

fn run_batch(
    cfg: IoUringConfig,
    files: Vec<Option<Arc<File>>>,
    requests: Vec<SegmentReadRequest>,
) -> Result<Vec<SegmentReadResponse>, MurrError> {
    let page_size = cfg.page_size as usize;
    if !page_size.is_power_of_two() || page_size == 0 {
        return Err(MurrError::ConfigParsingError(format!(
            "page_size must be a non-zero power of two, got {page_size}"
        )));
    }

    // Resolve fds + plan aligned reads.
    struct Plan {
        fd: i32,
        request: SegmentReadRequest,
        aligned_offset: u64,
        delta: usize,
        size: usize,
        buf: AlignedBuf,
    }
    let mut plans: Vec<Plan> = Vec::with_capacity(requests.len());
    for req in requests {
        let file = files
            .get(req.segment as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| {
                MurrError::SegmentError(format!("segment {} not loaded", req.segment))
            })?;
        let offset = req.read.offset as u64;
        let size = req.read.size as usize;
        let aligned_offset = offset & !(page_size as u64 - 1);
        let delta = (offset - aligned_offset) as usize;
        let aligned_len = (delta + size).div_ceil(page_size) * page_size;
        plans.push(Plan {
            fd: file.as_raw_fd(),
            request: req,
            aligned_offset,
            delta,
            size,
            buf: AlignedBuf::new(aligned_len, page_size),
        });
    }

    let mut results: Vec<Option<SegmentReadResponse>> = (0..plans.len()).map(|_| None).collect();
    let chunk_size = cfg.ring_size as usize;

    with_ring(&cfg, |ring| -> Result<(), MurrError> {
        let mut idx = 0usize;
        while idx < plans.len() {
            let end = (idx + chunk_size).min(plans.len());
            let chunk_len = end - idx;

            // Submit chunk.
            {
                let mut sq = ring.submission();
                for i in idx..end {
                    let p = &mut plans[i];
                    let entry = opcode::Read::new(
                        types::Fd(p.fd),
                        p.buf.as_mut_ptr(),
                        p.buf.len() as u32,
                    )
                    .offset(p.aligned_offset)
                    .build()
                    .user_data(i as u64);
                    // SAFETY: AlignedBuf is owned by `plans[i]` until we extract results below;
                    // its pointer outlives the SQE up to and through submit_and_wait.
                    unsafe {
                        sq.push(&entry).map_err(|e| {
                            MurrError::IoError(format!("io_uring sq push failed: {e}"))
                        })?;
                    }
                }
            }

            ring.submit_and_wait(chunk_len)
                .map_err(|e| MurrError::IoError(format!("io_uring submit_and_wait: {e}")))?;

            let mut completed = 0;
            for cqe in ring.completion() {
                let i = cqe.user_data() as usize;
                let res = cqe.result();
                let p = &plans[i];
                if res < 0 {
                    return Err(MurrError::SegmentError(format!(
                        "segment {} read failed: errno {}",
                        p.request.segment, -res
                    )));
                }
                let bytes_read = res as usize;
                if bytes_read < p.delta + p.size {
                    return Err(MurrError::SegmentError(format!(
                        "segment {} short read: got {} bytes, need {} (offset={} size={})",
                        p.request.segment,
                        bytes_read,
                        p.delta + p.size,
                        p.request.read.offset,
                        p.request.read.size
                    )));
                }
                results[i] = Some(SegmentReadResponse {
                    request: p.request,
                    bytes: p.buf.copy_window(p.delta, p.size),
                });
                completed += 1;
            }
            if completed != chunk_len {
                return Err(MurrError::IoError(format!(
                    "io_uring chunk mismatch: submitted {chunk_len}, completed {completed}"
                )));
            }

            idx = end;
        }
        Ok(())
    })?;

    Ok(results.into_iter().map(|r| r.unwrap()).collect())
}
