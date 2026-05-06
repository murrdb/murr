# io_uring Directory backend

A linux-only third Directory backend (alongside `MMapDirectory`, `MemDirectory`)
that uses `io_uring` for batched segment reads. Lives in
`src/io/directory/iouring/`.

## On-disk format

Identical to `MMapDirectory`: `{cache_dir}/{index}/{:08}.seg` files +
`_metadata.json`. An mmap-written table can be reopened with iouring and vice
versa — the choice is purely a runtime configuration.

## Concurrency model: worker pool with registered buffers

`IoUringDirectory` lazily constructs a single shared `IoUringPool` (in `pool.rs`)
on first reader open. The pool spawns `cfg.workers` OS threads, each owning:

- one `io_uring::IoUring` instance built with `IoUring::builder().build(ring_size)`
- one `BufferPool` (single page-sized class — see below)
- registered buffer iovec covering the pool arena (`Submitter::register_buffers`)

Submission: `IoUringReader::read` builds a `BatchJob` (Arc-cloned files, request
list, oneshot tx) and pushes it to a `crossbeam_channel::unbounded`. One worker
takes the whole batch and processes it synchronously via `execute_batch`:

1. Acquire one pool slot (or heap fallback) per request.
2. Push SQEs in chunks of `ring_size` and `submit_and_wait(chunk_len)`.
3. Drain CQEs, copy bytes from each pool slot into a fresh `Vec<u8>`, release
   slots back to the pool via `PlanGuard::drop` (RAII so buffers always return
   even on early-exit `?`).

One BatchJob = one query. Concurrent queries fan out across workers via the
channel. `workers` therefore acts as **max in-flight queries**, not as I/O
parallelism — a single NVMe absorbs queue depth from one ring fine.

### Why this shape (and what was rejected)

- **Old design (now gone): `thread_local!` ring per blocking-pool thread.**
  Worked but couldn't register buffers across the unbounded thread set, and
  the lifecycle of the pool buffers had to match the ring's. Replaced by the
  pool model so we could pin buffers once at worker startup.
- **`Mutex<IoUring>`**: serializes every concurrent submit. Defeats the point.
- **One ring per query**: `io_uring_setup` + `register_buffers` is ~10–30 µs
  + memlock pressure. Building/tearing down rings per query is unworkable.
- **Single ring per device with async multiplex**: structurally cleaner for
  high concurrency (one completion-drainer routes CQEs to per-request
  oneshots), but ~200 lines of careful code. Deferred. Current N-worker model
  is the simpler equivalent that caps concurrency at N instead of allowing
  unbounded overlap on one ring.

## Buffer pool: single page-sized class + heap fallback

`BufferPool::new` allocates **one** `BufferClass` of `slot_size = page_size`
and `cfg.buffer_slots` slots. `acquire(len)`:

- if `len <= page_size` and a slot is free: pooled (used with `READ_FIXED`)
- else: one-shot `posix_memalign` heap allocation (used with regular `Read`)

Why one class:

- Registered buffers count against `RLIMIT_MEMLOCK` per *user* (not per
  process). Total pinned per worker = `page_size * buffer_slots`. With sane
  defaults (4 KB × 256 = 1 MB per worker) we stay under typical 8 MB limits
  even with multiple workers and parallel processes.
- Multi-class arenas (1, 2, 4, 8 pages × N) blow the memlock budget on dev
  machines and add complexity for marginal gain. Reads larger than a page
  are rare (footers, multi-row scans); heap fallback is fine for them.

Sizing **matters**: if `buffer_slots < per-batch read count`, the overflow
spills to heap. Heap allocs are fresh anonymous pages from mimalloc — kernel
faults each one (alloc + zero + memcg charge), then mimalloc `MADV_FREE`s
between iterations, so the next iteration faults again. We saw this destroy
the bench (1.8 ms vs 370 µs) when `buffer_slots=256` < batch=1000. **Set
`buffer_slots >= max expected batch size**.

## SQE dispatch: READ_FIXED for pool, Read for heap

In `execute_batch`:

```rust
match p.handle {
    BufHandle::Pooled { class_idx, .. } =>
        opcode::ReadFixed::new(fd, ptr, len, class_idx as u16).offset(off)
    BufHandle::Heap { .. } =>
        opcode::Read::new(fd, ptr, len).offset(off)
}
```

`READ_FIXED` skips per-IO `get_user_pages_fast` / `import_ubuf` because the
buffer pages were pinned at `register_buffers` time. The heap path uses
regular `Read` because heap buffers aren't in the registered set.

## Page alignment is conditional on `cfg.direct`

In `execute_batch`, the per-request alignment math is gated:

- `direct = true` (O_DIRECT): page-align `offset` down, pad `len` up to a
  page multiple. Required by O_DIRECT semantics.
- `direct = false` (buffered): no alignment. Submit exact `(offset, size)`.
  The kernel internally fetches at page granularity into the page cache,
  but `copy_page_to_iter` honors the requested length and stops there.

Earlier, alignment ran unconditionally — buffered reads of 16 bytes became
4096-byte kernel copies (256× wasted work). Conditional alignment alone took
the bench from 710 µs → 436 µs.

## File flags

`IoUringReader::load_files` opens segment files with:

- `O_NOATIME` always — skips per-read `touch_atime` (~6% of warm reads).
  Caveat: requires owner or `CAP_FOWNER`. Fine for files we wrote.
- `O_DIRECT` only if `cfg.direct`.
- `posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM)` after open (skipped under
  `O_DIRECT`, no-op there). Disables kernel readahead — for scattered small
  reads on a working set larger than RAM, default readahead pulls in 16–128
  KB of neighbour pages we won't touch, wasting bandwidth and evicting
  hotter pages.

## Registration failure & memlock budget

`register_buffers` can fail with `ENOMEM` if `RLIMIT_MEMLOCK` is exhausted
(pinning is per-user). Currently the worker drains the queue with errors and
exits — same severity as ring-build failure. Production deployments should
bump memlock (`prlimit --memlock=...:...` or systemd `LimitMEMLOCK=`).

For the test suite: io_uring buffer accounting is per-user, so the
`lib.rs`/`main.rs` test binaries running concurrently share the budget.
Tests are gated with `#[file_serial]` (serial_test crate, `file_locks`
feature) so they don't race across processes, AND use a tiny `test_dir`
config (`workers: 1`, `ring_size: 8`, `buffer_slots: 8`) so each test pins
~32 KB. The tiny config matters even with serialization because the kernel
releases io_uring memory asynchronously over an RCU grace period — back-to-
back tests can stack pinned-budget if each one was 4 MB.

## Writer is sync, not io_uring

Writes go through shared helpers in `src/io/directory/file_writer.rs` —
the same code MMapWriter uses. `IoUringWriter::write` is a thin wrapper.
Writes are infrequent vs reads; the optimization isn't worth the format
change (segment files would need page padding to round-trip cleanly through
O_DIRECT readers).

## Platform gating

`IoUringConfig` (the data struct) is always compiled so
`BackendConfig::IoUring(IoUringConfig)` resolves on every target — non-linux
YAML configs still parse. The implementation submodules (`directory`,
`reader`, `writer`, `pool`) are gated via
`core::cfg_select! { target_os = "linux" => ... }` (Rust 1.95). Non-linux
`MurrService` returns `MurrError::ConfigParsingError("io_uring backend is
only supported on Linux")`.

Linux-only deps: `io-uring = "0.7"`, `libc = "0.2"`, `crossbeam-channel`.

## Config surface

```yaml
storage:
  backend:
    type: iouring
    cache_dir: /var/lib/murr   # default: shared resolve_cache_dir() with mmap
    ring_size: 256             # SQ depth per worker; chunk size for submission
    direct: false              # O_DIRECT + per-request page alignment
    page_size: 4096            # buffer slot size + alignment unit
    sqpoll: false              # IORING_SETUP_SQPOLL kernel polling thread
    workers: 4                 # number of worker threads / rings; max in-flight queries
    buffer_slots: 256          # pool slots per worker; MUST be >= max batch size
    register_buffers: true     # pin pool arenas via register_buffers + use READ_FIXED
    coalesce_window: 131072    # bucket size for read coalescing (bytes); 0 disables
    coalesce_slots: 32         # number of coalesce-class buffer slots per worker
```

**Note**: `buffer_slots` is the most operationally important knob — undersizing
it triggers heap-fallback page-fault churn that dwarfs any io_uring win.
Pin to your expected per-query batch size.

## Read coalescing

Within a batch, requests landing in the same `(segment, offset / coalesce_window)`
bucket are merged into a single SQE. The merged read covers
`[min_offset, max_end)` across all members; per-member responses are sliced
out of the resulting buffer at `(req.offset - merged_offset, req.size)`.
Cross-segment requests cannot coalesce (different fds), and cross-bucket
requests stay separate even if they are physically adjacent.

Why aligned buckets and not greedy/sliding-window:
- Bucket model is deterministic and trivial to reason about — max merged
  span ≈ `coalesce_window`, so buffer sizing is predictable.
- Greedy is more flexible but adds parameters (max gap, max merged span)
  with no operational gain on the production access pattern (clustered
  hot pages serve many keys per page).

### Why a second buffer class

Coalesced reads routinely exceed `page_size` (4 KiB), so they cannot use
the existing single-class pool. A second `BufferClass(coalesce_window,
coalesce_slots, page_size)` is allocated and registered alongside the page
class. `BufferPool::acquire` already walks classes in order, so small reads
keep using class 0 (page) and coalesced reads land on class 1 (coalesce).
Reads larger than `coalesce_window` (rare: oversized single requests, large
footers) fall through to the heap path.

Memlock budget per worker: `page_size × buffer_slots + coalesce_window ×
coalesce_slots`. Defaults: `4 KiB × 256 + 128 KiB × 32 ≈ 5 MiB/worker`.

Disable coalescing with `coalesce_window: 0` (or `coalesce_slots: 0`) —
in that case the second class is not allocated and the worker degenerates
to one-plan-per-request behavior.

## Buffer registration toggle

`register_buffers: true` (default) pins the pool arenas via
`Submitter::register_buffers` and uses `IORING_OP_READ_FIXED` to skip
per-IO `import_ubuf` / `get_user_pages_fast`. It is the right setting for
production but consumes per-user `RLIMIT_MEMLOCK`.

`register_buffers: false` skips the registration call and falls back to
`IORING_OP_READ` for every SQE. Used by the unit tests because the
per-user memlock budget is shared across concurrent test binaries and
the kernel's RCU-grace-period release of prior tests' io_uring memory
would otherwise stack pinned pages over the 8 MiB default budget.
Production should leave it `true`.

## Reuse and refactor

`MMapWriter` was refactored to delegate to `src/io/directory/file_writer.rs`
so `IoUringWriter` shares the same atomic-write + metadata-append code.
`resolve_cache_dir` in `mmap/directory.rs` is `pub(crate)` so
`IoUringConfig::default_cache_dir` reuses the same fallback chain
(`cwd → /var/lib/murr → /data → tmp`).
