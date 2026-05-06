# io_uring perf findings + workload guidance

Distilled from a perf-investigation session benchmarking `read_bench` against
mmap and io_uring backends. The bench is **fully warm-cache** (5M rows on
ext4, working set fits) — its numbers don't translate directly to a 10× RAM
production workload, but the structural learnings do.

## Bench journey: 710 µs → 358 µs (~2× of mmap's 195 µs)

| Change | Time | Why it helped |
|---|---|---|
| Initial state | 710 µs | Multi-class buffer pool; page-aligned reads even when `direct=false`; regular `Read` opcode |
| Drop alignment when `direct=false` | 436 µs | 16-byte reads no longer became 4096-byte kernel copies; `_copy_to_iter` share dropped 53% → 36% |
| Single 4K class + READ_FIXED + `buffer_slots=1024` | 370 µs | Pool fits the whole 1000-read batch (no heap fallback); registered buffers skip `import_ubuf`/`get_user_pages_fast` per IO |
| `O_NOATIME` + watcher-free mount (`/tmp/murrbench_mount`) | 361 µs | Eliminated `touch_atime` (~6%) and `__fsnotify_parent` (~23%) |

mmap baseline: **195 µs**. The remaining gap is structural for buffered-read
syscall semantics (see "structural ceiling" below).

## Things that hurt or didn't help

- **SQPOLL: regression (361 → 382 µs).** The kernel poller saves the
  *submission* syscall, but for synchronous batched workloads you still need
  a syscall to wait for completions, so net savings ≈ 0. Meanwhile SQPOLL
  adds wakeup latency (poller sleeps after `idle_ms`), SQ head/tail cacheline
  ping-pong, and `io_sq_tw`/`tctx_task_work_run` overhead. SQPOLL needs
  sustained high QPS to stay hot; bursty/synchronous patterns lose.
- **Pool too small for batch: 4× regression (370 → 1800 µs).** With
  `buffer_slots=256` and a 1000-read batch, 744 reads spilled to heap.
  Profile showed 21% `_copy_to_iter` faulting on user buffer + 20%
  `clear_page_erms` + ~10% memcg charging — fresh anonymous-page churn,
  because mimalloc `MADV_FREE`s buffers between iterations. **Lesson:
  `buffer_slots` MUST be ≥ max expected batch size or perf collapses.**
- **DIO (`O_DIRECT`) on small rows: dramatic regression on the warm bench.**
  4K alignment forces 40× read amplification on 100-byte rows, AND the page
  cache is bypassed so warm-cache hits become real disk reads. Profile shows
  the entire iomap/blk_mq/nvme path lighting up. DIO is the wrong tool for
  small-row warm-cache workloads.
- **Ring size = chunk size with SQPOLL: panic.** With SQPOLL, the io-uring
  crate's local SQ head/tail can lag the kernel's, so `sq.push` returns
  `PushError("queue is full")` even though the kernel has consumed entries.
  Mitigation: keep `ring_size` strictly larger than per-chunk submission
  count, OR don't use SQPOLL (current default).

## The structural ceiling for buffered io_uring

After exhausting tunable wins, ~165 µs of the iouring-vs-mmap gap remains.
Breakdown of what's irreducible for this workload:

- **`copy_page_to_iter` / `_copy_to_iter`** (~14% of time): kernel memcpy
  page cache → user buffer. mmap pays zero here because the user code reads
  directly from PTE-mapped page cache pages. There is no buffered-read
  opcode that hands you a reference into the page cache. `splice`, `tee`,
  `sendfile` all terminate in a copy when the destination is a user buffer.
- **`io_import_reg_buf`** (~9%): per-IO registered-buffer lookup +
  bounds-check. Replaces the more expensive `import_ubuf`/
  `get_user_pages_fast` from the unregistered path; it's the residual
  cost of `READ_FIXED`.
- **Per-SQE init** (`io_init_req`, `io_prep_rw`, ~8%): kernel-side SQE
  bookkeeping. Scales linearly with read count. Only attackable by
  submitting fewer (larger) reads — see read coalescing below.

User-space costs that ARE reducible but didn't get done:

- **`to_vec()` per response** (~5–7%): currently `pool.rs` does
  `from_raw_parts(...).to_vec()` to produce `SegmentReadResponse.bytes:
  Vec<u8>`. Replacing with `Bytes`/`Arc<[u8]>` that owns a refcount on the
  pool slot eliminates this copy AND the `_int_malloc`/`_int_free_*` churn.
  Modest impact in warm-cache, free win regardless. **Not implemented.**

## Workload guidance: production != bench

Production target pattern: **200–1000 scattered ~100-byte row reads per query,
working set 10× RAM, ~10% hot data**. This shifts the cost model entirely:

- **You will be I/O-bound, not CPU-bound.** Per-query NVMe service time
  (~1–3 ms) dwarfs the entire io_uring path (<1 ms). Optimizing
  `_copy_to_iter` is a rounding error.
- **Read amplification is the dominant cost.** A 100-byte row pulled from
  4K-block storage = 40× amplification regardless of buffered or DIO.
- **Page cache hit rate = the throughput multiplier.** With 10% hot, the
  hot subset hopefully fits in RAM. Page cache will naturally retain it
  unless evicted by readahead waste (mitigated by `POSIX_FADV_RANDOM`,
  already wired up) or other workloads.

### What to build for this pattern (priority order)

1. **Read coalescing within a batch.** Sort `SegmentReadRequest`s by
   `(segment_id, offset)` and merge runs within ~32 KB. Hot pages serve many
   keys per page; coalescing collapses 100 hits on the hot set into a few
   larger reads. Highest pure-I/O lever. Lives in `TableReader::read` or
   `execute_batch`. Not implemented.
2. **Userspace decoded-row LRU above `TableReader::read`.** With 90% of
   accesses on 10% of data, a process-resident LRU sized to the hot subset
   skips I/O entirely on hits. Cache-hit cost ~50 ns vs cold-read ~10 µs.
   Not implemented.
3. **`POSIX_FADV_RANDOM`** — done. Disables the kernel's 16–128 KB readahead
   that would waste bandwidth and evict hot pages.
4. **`IORING_REGISTER_FILES` + `IOSQE_FIXED_FILE`** — saves ~2% per IO on
   `fget`. Not implemented.
5. **Bytes-based response** — see above.

### What NOT to build

- **DIO for this workload**: 40× amplification kills it; you also lose the
  hot-set caching benefit.
- **SQPOLL**: bursty/synchronous workloads are exactly its anti-pattern.
- **More aggressive pool classes**: complexity without benefit. Single 4K
  class + heap fallback is simpler and matches the typical row size.

## Architectural question parked: single ring per device vs N workers

Current model has N workers, each with its own ring + buffer pool. For a
single device this is over-engineered — multiple rings don't make a single
NVMe faster, the device's hardware queue handles that. The N is effectively
a "max concurrent in-flight queries" cap.

Cleaner long-term shape (deferred): one ring per device, multiple async
submitters multiplex via per-request oneshots routed by `user_data`, single
completion-drainer. Allows queries to overlap on the device naturally
without a thread-per-query budget. ~200 lines of careful code; defer until
concurrent-query perf becomes a measured problem.

## Bench harness gotchas worth remembering

- The bench `TempDir` is forced to `/tmp/murrbench_mount` — a mount with
  no inotify/fanotify watchers, so `__fsnotify_parent` doesn't dominate.
  Operator must `mkdir -p /tmp/murrbench_mount` once.
- mem/mmap reader bench calls are commented out by default — uncomment to
  re-baseline against them.
- For profiling SQPOLL: `perf record` against the bench binary alone misses
  the kernel poller thread's CPU. Use `perf record -a` (system-wide) to see
  where SQPOLL spent its time.
