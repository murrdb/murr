use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::cell::RefCell;
use std::io;
use std::ptr::NonNull;

use io_uring::IoUring;

use crate::core::MurrError;
use crate::io::directory::iouring::IoUringConfig;

pub(crate) struct AlignedBuf {
    ptr: NonNull<u8>,
    layout: Layout,
}

unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    pub(crate) fn new(len: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(len, align)
            .expect("invalid layout: len/align must be valid (align power-of-two)");
        // SAFETY: layout has non-zero size; len > 0 enforced by caller (page_size >= align >= 1).
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        AlignedBuf { ptr, layout }
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn len(&self) -> usize {
        self.layout.size()
    }

    pub(crate) fn copy_window(&self, start: usize, len: usize) -> Vec<u8> {
        // SAFETY: ptr is valid for self.layout.size() bytes; caller ensures start+len <= len().
        let slice = unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(start), len) };
        slice.to_vec()
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr came from alloc_zeroed with the same layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

fn build_ring(cfg: &IoUringConfig) -> io::Result<IoUring> {
    let mut builder = IoUring::builder();
    if cfg.sqpoll {
        // 1s idle: kernel polls the SQ for 1s after activity, then sleeps until kicked.
        builder.setup_sqpoll(1000);
    }
    builder.build(cfg.ring_size)
}

thread_local! {
    static RING: RefCell<Option<(IoUringConfig, IoUring)>> = const { RefCell::new(None) };
}

/// Run `f` with a thread-local cached `IoUring`. The first call on a thread builds the
/// ring; subsequent calls reuse it. If `cfg` differs from the cached config, the cached
/// ring is dropped and rebuilt — keeps the cache coherent across configs.
///
/// `f` must leave the ring in a clean state (all CQEs drained); the caller's batch loop
/// already does this after each `submit_and_wait`, so callers don't see leftover state.
pub(crate) fn with_ring<F, R>(cfg: &IoUringConfig, f: F) -> Result<R, MurrError>
where
    F: FnOnce(&mut IoUring) -> Result<R, MurrError>,
{
    RING.with(|cell| {
        let mut slot = cell.borrow_mut();
        let needs_rebuild = match slot.as_ref() {
            Some((cached_cfg, _)) => cached_cfg != cfg,
            None => true,
        };
        if needs_rebuild {
            let ring = build_ring(cfg)
                .map_err(|e| MurrError::IoError(format!("io_uring setup: {e}")))?;
            *slot = Some((cfg.clone(), ring));
        }
        let (_, ring) = slot.as_mut().unwrap();
        f(ring)
    })
}
