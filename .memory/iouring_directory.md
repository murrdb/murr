# io_uring Directory backend

A linux-only third Directory backend (alongside `MMapDirectory`, `MemDirectory`) that uses `io_uring` for batched segment reads. Lives in `src/io/directory/iouring/`.

## On-disk format

Identical to `MMapDirectory`: `{cache_dir}/{index}/{:08}.seg` files + `_metadata.json`. An mmap-written table can be reopened with iouring and vice versa — the choice is purely a runtime configuration.

## Concurrency model: thread-local ring, not a shared reactor

`IoUringReader::read` clones its inputs into `tokio::task::spawn_blocking` and runs the batch synchronously. The `io_uring::IoUring` itself is cached per blocking-pool thread via `thread_local!` and reused across calls (see `ring::with_ring`). The cache is keyed by `IoUringConfig`; if the config changes, the cached ring is dropped and rebuilt.

Concurrency comes for free: each concurrent `read()` lands on a different blocking-pool thread with its own cached ring. No mutex, no shared state, no reactor thread, no channels. Considered and rejected:
- **`Mutex<IoUring>`** — would serialize every concurrent read, since `submit_and_wait` blocks while I/O is in flight. Defeats io_uring's purpose.
- **Per-call ring setup** — the simpler first cut, but `io_uring_setup` + `mmap` runs ~10–30µs per call and tight benchmark loops hit `RLIMIT_MEMLOCK` / ENOMEM after a few hundred ring creations. The thread_local cache amortizes setup across all calls on a thread without locking.
- **Dedicated reactor thread + mpsc/oneshot per request** — the canonical io_uring pattern, but ~80 lines of multiplexer code (channel drain, in-flight HashMap, SQ-full back-pressure). Not needed once the per-thread cache makes setup free.

The reader's batch loop drains every CQE before returning, so the cached ring is always in a clean state for the next caller.

## Always-aligned reads

Reads are rounded out to `page_size` boundaries unconditionally — even when `direct: false`. Aligned page-boundary reads play better with the kernel page cache and prefetcher; the `direct` config knob only toggles `O_DIRECT` on the file open, not the alignment math. One code path for both modes.

The trait's `ReadRequest { offset, size }` is byte-granular, so the reader rounds offset down + length up to `page_size`, allocates a page-aligned `AlignedBuf`, submits the read, then copies `buf[delta..delta+size]` into the owned `Vec<u8>` the trait demands. The copy is unavoidable given the trait contract.

## Writer is sync, not io_uring

Writes go through shared helpers in `src/io/directory/file_writer.rs` (`atomic_write` / `next_segment_id` / `append_segment_info`) — the same code MMapWriter uses post-refactor. `IoUringWriter::write` is a thin wrapper around those. Considered using io_uring for writes too, but:
- Segment files are not naturally page-sized; padding to `page_size` would force readers to mask trailing bytes (and re-derive segment size from metadata, breaking the simple "read N bytes from offset 0" pattern).
- Writes are infrequent compared to reads; the optimization isn't worth the format change.
- Reader with `O_DIRECT` still sees buffered+`fsync`'d data correctly once `rename` returns — `O_DIRECT` requires aligned I/O, not aligned files.

## Platform gating

`IoUringConfig` (the data struct) is always compiled so `BackendConfig::IoUring(IoUringConfig)` resolves on every target — non-linux YAML configs still parse. The implementation submodules (`directory`, `reader`, `writer`, `ring`) are gated via `core::cfg_select! { target_os = "linux" => ... }` (Rust 1.95). The two `MurrService` match arms (`new`, `create`) use the same `cfg_select!` and on non-linux return `MurrError::ConfigParsingError("io_uring backend is only supported on Linux")` — explicit error, not panic.

Linux-only deps in `Cargo.toml` use `[target.'cfg(target_os = "linux")'.dependencies]`: `io-uring = "0.7"`, `libc = "0.2"`.

## Config surface

```yaml
storage:
  backend:
    type: iouring
    cache_dir: /var/lib/murr   # default: shared resolve_cache_dir() with mmap
    ring_size: 256             # SQ depth
    direct: false              # O_DIRECT flag on file open
    page_size: 4096            # alignment for buffers + offsets (always applied)
    sqpoll: false              # IORING_SETUP_SQPOLL kernel polling thread
```

## Reuse and refactor

`MMapWriter` was refactored to delegate to `src/io/directory/file_writer.rs` so `IoUringWriter` could share the same atomic-write + metadata-append code. `resolve_cache_dir` in `mmap/directory.rs` was promoted to `pub(crate)` so `IoUringConfig::default_cache_dir` reuses the same fallback chain (`cwd → /var/lib/murr → /data → tmp`).
