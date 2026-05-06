use std::alloc::{Layout, alloc, dealloc};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, unbounded};
use io_uring::{IoUring, opcode, types};
use tokio::sync::oneshot;

use crate::core::MurrError;
use crate::io::directory::iouring::IoUringConfig;
use crate::io::directory::{SegmentReadRequest, SegmentReadResponse};

/// Per-class slot accounting. The arena is one aligned allocation of
/// `slot_size * slot_count` bytes; slots are carved at fixed offsets.
struct BufferClass {
    arena: NonNull<u8>,
    slot_size: usize,
    slot_count: u32,
    align: usize,
    free: Vec<u32>,
}

unsafe impl Send for BufferClass {}

impl BufferClass {
    fn new(slot_size: usize, slot_count: u32, page_size: usize) -> Self {
        let total = slot_size
            .checked_mul(slot_count as usize)
            .expect("buffer class arena size overflow");
        let layout = Layout::from_size_align(total, page_size)
            .expect("invalid layout for buffer class");
        let raw = unsafe { alloc(layout) };
        let arena = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));

        // Pre-fault every page so the first real read doesn't take a
        // first-touch page fault. Volatile so the compiler can't elide it.
        unsafe {
            let mut off = 0usize;
            while off < total {
                std::ptr::write_volatile(arena.as_ptr().add(off), 0u8);
                off += page_size;
            }
        }

        let free: Vec<u32> = (0..slot_count).rev().collect();
        Self {
            arena,
            slot_size,
            slot_count,
            align: page_size,
            free,
        }
    }
}

impl Drop for BufferClass {
    fn drop(&mut self) {
        let total = self.slot_size * self.slot_count as usize;
        let layout = Layout::from_size_align(total, self.align)
            .expect("layout was valid at construction");
        unsafe { dealloc(self.arena.as_ptr(), layout) };
    }
}

/// Handle returned by `BufferPool::acquire`. Either references a slot in a
/// pre-allocated class, or carries the layout for a one-shot heap allocation
/// when no class fits or every fitting class is exhausted.
pub(crate) enum BufHandle {
    Pooled { class_idx: u8, slot_idx: u32 },
    Heap { ptr: NonNull<u8>, layout: Layout },
}

pub(crate) struct BufferPool {
    page_size: usize,
    classes: Vec<BufferClass>,
}

impl BufferPool {
    pub fn new(cfg: &IoUringConfig) -> Self {
        // Class 0: page-sized slots for sub-page reads (or single-member
        // groups when coalescing is disabled). Class 1 (optional):
        // `coalesce_window`-sized slots for coalesced groups so they can
        // also use READ_FIXED. Reads larger than the largest class fall
        // through `acquire` to the heap path — kept rare to avoid the
        // page-fault churn documented in iouring_perf_findings.md.
        let page_size = cfg.page_size as usize;
        let mut classes = vec![BufferClass::new(page_size, cfg.buffer_slots, page_size)];
        if cfg.coalesce_window > 0 && cfg.coalesce_slots > 0 {
            classes.push(BufferClass::new(
                cfg.coalesce_window as usize,
                cfg.coalesce_slots,
                page_size,
            ));
        }
        Self { page_size, classes }
    }

    pub fn acquire(&mut self, len: usize) -> (NonNull<u8>, BufHandle) {
        for (i, c) in self.classes.iter_mut().enumerate() {
            if c.slot_size >= len {
                if let Some(slot_idx) = c.free.pop() {
                    let offset = slot_idx as usize * c.slot_size;
                    let ptr = unsafe { NonNull::new_unchecked(c.arena.as_ptr().add(offset)) };
                    return (
                        ptr,
                        BufHandle::Pooled {
                            class_idx: i as u8,
                            slot_idx,
                        },
                    );
                }
                // class fits but is exhausted — try next bigger class
            }
        }

        // No fitting class available. Fall back to a one-shot aligned alloc.
        let aligned_len = len.max(self.page_size);
        let layout = Layout::from_size_align(aligned_len, self.page_size)
            .expect("invalid overflow layout");
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        (ptr, BufHandle::Heap { ptr, layout })
    }

    pub fn release(&mut self, handle: BufHandle) {
        match handle {
            BufHandle::Pooled { class_idx, slot_idx } => {
                self.classes[class_idx as usize].free.push(slot_idx);
            }
            BufHandle::Heap { ptr, layout } => unsafe {
                dealloc(ptr.as_ptr(), layout);
            },
        }
    }

    /// Iovecs covering each class arena, in class-index order. Pass to
    /// `Submitter::register_buffers` so pool-served reads can use
    /// `IORING_OP_READ_FIXED` (`buf_index` == class index).
    pub fn iovecs(&self) -> Vec<libc::iovec> {
        self.classes
            .iter()
            .map(|c| libc::iovec {
                iov_base: c.arena.as_ptr() as *mut std::ffi::c_void,
                iov_len: c.slot_size * c.slot_count as usize,
            })
            .collect()
    }
}

pub(crate) struct BatchJob {
    pub files: Arc<Vec<Option<Arc<File>>>>,
    pub requests: Vec<SegmentReadRequest>,
    pub respond: oneshot::Sender<Result<Vec<SegmentReadResponse>, MurrError>>,
}

pub(crate) struct IoUringPool {
    tx: Option<Sender<BatchJob>>,
    workers: Vec<JoinHandle<()>>,
}

impl IoUringPool {
    pub fn new(cfg: IoUringConfig) -> Result<Self, MurrError> {
        let workers_n = cfg.workers.max(1);
        let (tx, rx) = unbounded::<BatchJob>();
        let mut workers = Vec::with_capacity(workers_n);
        for i in 0..workers_n {
            let rx = rx.clone();
            let cfg = cfg.clone();
            let handle = std::thread::Builder::new()
                .name(format!("murr-iouring-{i}"))
                .spawn(move || worker_loop(rx, cfg))
                .map_err(|e| MurrError::IoError(format!("spawning io_uring worker: {e}")))?;
            workers.push(handle);
        }
        Ok(Self {
            tx: Some(tx),
            workers,
        })
    }

    pub fn submit(&self, job: BatchJob) -> Result<(), MurrError> {
        self.tx
            .as_ref()
            .ok_or_else(|| MurrError::IoError("io_uring pool already shut down".to_string()))?
            .send(job)
            .map_err(|_| MurrError::IoError("io_uring pool: workers gone".to_string()))
    }
}

impl Drop for IoUringPool {
    fn drop(&mut self) {
        // Drop the sender so all worker `recv()` calls return Err and exit.
        self.tx.take();
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

fn build_ring(cfg: &IoUringConfig) -> io::Result<IoUring> {
    let mut builder = IoUring::builder();
    if cfg.sqpoll {
        builder.setup_sqpoll(1000);
    }
    builder.build(cfg.ring_size)
}

fn worker_loop(rx: Receiver<BatchJob>, cfg: IoUringConfig) {
    // Declare bufs before ring so ring drops first: while the ring is alive
    // it holds the pool's arenas as registered (pinned) buffers.
    let mut bufs = BufferPool::new(&cfg);
    let mut ring = match build_ring(&cfg) {
        Ok(r) => r,
        Err(e) => {
            // Worker can't run; drain queue with errors so submitters fail
            // fast instead of hanging on the oneshot receiver.
            let msg = format!("io_uring worker init failed: {e}");
            while let Ok(job) = rx.recv() {
                let _ = job.respond.send(Err(MurrError::IoError(msg.clone())));
            }
            return;
        }
    };

    if cfg.register_buffers {
        let iovecs = bufs.iovecs();
        // SAFETY: `bufs` outlives `ring` (declaration order above), so the
        // arena pages stay valid for as long as the ring may reference them.
        if let Err(e) = unsafe { ring.submitter().register_buffers(&iovecs) } {
            let msg = format!("io_uring register_buffers failed: {e}");
            while let Ok(job) = rx.recv() {
                let _ = job.respond.send(Err(MurrError::IoError(msg.clone())));
            }
            return;
        }
    }

    while let Ok(job) = rx.recv() {
        let result = execute_batch(&mut ring, &mut bufs, &cfg, &job.files, job.requests);
        let _ = job.respond.send(result);
    }
}

/// One member of a coalesced group: an original SegmentReadRequest plus its
/// byte offset into the merged buffer.
struct MemberSlice {
    original_idx: usize,
    rel_offset: u32,
    size: u32,
    request: SegmentReadRequest,
}

/// A merged read covering `[offset, offset + size)` of `segment`. One or
/// more `members` will be sliced out of the resulting buffer.
struct CoalescedGroup {
    segment: u32,
    offset: u64,
    size: u32,
    members: Vec<MemberSlice>,
}

struct Plan {
    fd: RawFd,
    segment: u32,
    aligned_offset: u64,
    delta: usize,
    size: usize,
    buf_ptr: NonNull<u8>,
    buf_len: usize,
    handle: BufHandle,
    members: Vec<MemberSlice>,
}

/// Bucket requests by `(segment, offset / window)`. All requests in the
/// same bucket merge into one read covering `[min_offset, max_end)`.
/// `window == 0` produces one degenerate single-member group per request.
fn coalesce(reqs: Vec<SegmentReadRequest>, window: u32) -> Vec<CoalescedGroup> {
    if window == 0 {
        return reqs
            .into_iter()
            .enumerate()
            .map(|(i, r)| CoalescedGroup {
                segment: r.segment,
                offset: r.read.offset as u64,
                size: r.read.size,
                members: vec![MemberSlice {
                    original_idx: i,
                    rel_offset: 0,
                    size: r.read.size,
                    request: r,
                }],
            })
            .collect();
    }

    // IndexMap preserves first-insertion order so coalesced groups submit
    // in roughly the same order as the input batch — predictable for
    // debugging and avoids surprising the SQE-fill loop.
    let mut groups: indexmap::IndexMap<(u32, u32), Vec<(usize, SegmentReadRequest)>> =
        indexmap::IndexMap::new();
    for (i, req) in reqs.into_iter().enumerate() {
        let bucket = req.read.offset / window;
        groups.entry((req.segment, bucket)).or_default().push((i, req));
    }

    groups
        .into_values()
        .map(|members| {
            let segment = members[0].1.segment;
            let start = members.iter().map(|(_, r)| r.read.offset).min().unwrap();
            let end = members
                .iter()
                .map(|(_, r)| r.read.offset + r.read.size)
                .max()
                .unwrap();
            let size = end - start;
            let members = members
                .into_iter()
                .map(|(idx, r)| MemberSlice {
                    original_idx: idx,
                    rel_offset: r.read.offset - start,
                    size: r.read.size,
                    request: r,
                })
                .collect();
            CoalescedGroup {
                segment,
                offset: start as u64,
                size,
                members,
            }
        })
        .collect()
}

/// Releases all plan buffers back to the pool when dropped, regardless of
/// whether the batch returned `Ok` or bailed via `?`.
struct PlanGuard<'a> {
    plans: Vec<Plan>,
    bufs: &'a mut BufferPool,
}

impl Drop for PlanGuard<'_> {
    fn drop(&mut self) {
        for plan in self.plans.drain(..) {
            self.bufs.release(plan.handle);
        }
    }
}

fn execute_batch(
    ring: &mut IoUring,
    bufs: &mut BufferPool,
    cfg: &IoUringConfig,
    files: &[Option<Arc<File>>],
    requests: Vec<SegmentReadRequest>,
) -> Result<Vec<SegmentReadResponse>, MurrError> {
    let page_size = cfg.page_size as usize;
    if !page_size.is_power_of_two() || page_size == 0 {
        return Err(MurrError::ConfigParsingError(format!(
            "page_size must be a non-zero power of two, got {page_size}"
        )));
    }

    let total_requests = requests.len();
    let groups = coalesce(requests, cfg.coalesce_window);

    let mut plans: Vec<Plan> = Vec::with_capacity(groups.len());
    for group in groups {
        let file = files
            .get(group.segment as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| {
                MurrError::SegmentError(format!("segment {} not loaded", group.segment))
            })?;
        let offset = group.offset;
        let size = group.size as usize;
        let (aligned_offset, delta, buf_len) = if cfg.direct {
            let aligned_offset = offset & !(page_size as u64 - 1);
            let delta = (offset - aligned_offset) as usize;
            let buf_len = (delta + size).div_ceil(page_size) * page_size;
            (aligned_offset, delta, buf_len)
        } else {
            (offset, 0, size)
        };
        let (buf_ptr, handle) = bufs.acquire(buf_len);
        plans.push(Plan {
            fd: file.as_raw_fd(),
            segment: group.segment,
            aligned_offset,
            delta,
            size,
            buf_ptr,
            buf_len,
            handle,
            members: group.members,
        });
    }

    let guard = PlanGuard { plans, bufs };
    let mut results: Vec<Option<SegmentReadResponse>> =
        (0..total_requests).map(|_| None).collect();
    let chunk_size = cfg.ring_size as usize;

    let mut idx = 0usize;
    while idx < guard.plans.len() {
        let end = (idx + chunk_size).min(guard.plans.len());
        let chunk_len = end - idx;

        {
            let mut sq = ring.submission();
            for i in idx..end {
                let p = &guard.plans[i];
                let entry = match p.handle {
                    BufHandle::Pooled { class_idx, .. } if cfg.register_buffers => {
                        opcode::ReadFixed::new(
                            types::Fd(p.fd),
                            p.buf_ptr.as_ptr(),
                            p.buf_len as u32,
                            class_idx as u16,
                        )
                        .offset(p.aligned_offset)
                        .build()
                        .user_data(i as u64)
                    }
                    _ => opcode::Read::new(types::Fd(p.fd), p.buf_ptr.as_ptr(), p.buf_len as u32)
                        .offset(p.aligned_offset)
                        .build()
                        .user_data(i as u64),
                };
                // SAFETY: each plan's buffer is owned by `guard.plans[i]` and
                // outlives the SQE through `submit_and_wait`. Pooled buffers
                // sit inside the registered arena selected by `class_idx`.
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
            let p = &guard.plans[i];
            if res < 0 {
                return Err(MurrError::SegmentError(format!(
                    "segment {} read failed: errno {}",
                    p.segment, -res
                )));
            }
            let bytes_read = res as usize;
            if bytes_read < p.delta + p.size {
                return Err(MurrError::SegmentError(format!(
                    "segment {} short read: got {} bytes, need {} (merged offset={} size={})",
                    p.segment,
                    bytes_read,
                    p.delta + p.size,
                    p.aligned_offset + p.delta as u64,
                    p.size
                )));
            }
            // SAFETY: the kernel wrote `bytes_read` bytes starting at buf_ptr;
            // delta+size <= buf_len by construction. Each member's
            // (rel_offset + size) is bounded by p.size so the slice stays
            // inside the merged read.
            for member in &p.members {
                let off = p.delta + member.rel_offset as usize;
                let bytes = unsafe {
                    std::slice::from_raw_parts(p.buf_ptr.as_ptr().add(off), member.size as usize)
                        .to_vec()
                };
                results[member.original_idx] = Some(SegmentReadResponse {
                    request: member.request,
                    bytes,
                });
            }
            completed += 1;
        }
        if completed != chunk_len {
            return Err(MurrError::IoError(format!(
                "io_uring chunk mismatch: submitted {chunk_len}, completed {completed}"
            )));
        }

        idx = end;
    }

    drop(guard);
    Ok(results.into_iter().map(|r| r.unwrap()).collect())
}
