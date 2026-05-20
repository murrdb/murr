# Global allocator: jemalloc

## Decision

Murr uses jemalloc as the single global allocator for both Rust and the embedded RocksDB C++. The `tikv-jemallocator` crate is set as Rust's `#[global_allocator]` in `src/main.rs`, and the `rocksdb` crate is pulled in with the `jemalloc` feature so RocksDB's C++ is built with `-DROCKSDB_JEMALLOC` and linked against the same jemalloc.

Each top-level bench binary (`benches/multi_segment_index_bench.rs`, `benches/read_block.rs`, `benches/read_memory.rs`, `benches/read_plain.rs`) declares its own `#[global_allocator]` because they're separate criterion binaries with no shared entry point; `benches/common/` is a library module and can't host it.

## Why

RocksDB is the dominant memory consumer in the process (block cache, write buffers, compaction). Facebook tunes RocksDB around jemalloc — fragmentation behavior on LSM compactions and arena layout are aligned with what jemalloc provides. Picking jemalloc end-to-end means one heap, one arena layout, one RSS number to reason about.

## Why not the alternatives

- **mimalloc in Rust + libc malloc in RocksDB (the previous state)**: RocksDB runs on glibc ptmalloc, which fragments badly under the kind of allocation pressure compactions produce. RSS grows over time even when working-set size is stable.
- **mimalloc in Rust + jemalloc in RocksDB (mixed)**: works because `rocksdb-rust` copies buffers across FFI boundaries (no cross-allocator free), but produces two arenas in one process. RSS attribution becomes confusing and the small mimalloc-on-small-Rust-allocs win isn't worth the bookkeeping cost.
- **Keep mimalloc, skip rocksdb's jemalloc feature**: see above — RocksDB stays on libc malloc and we miss out on `JemallocNodumpAllocator` and the tuning RocksDB upstream assumes.

## Implementation notes

- `librocksdb-sys`'s `jemalloc` feature pulls `tikv-jemalloc-sys` v0.6 with `unprefixed_malloc_on_supported_platforms`, which interposes libc malloc on Linux. Combined with `tikv-jemallocator` (also using `tikv-jemalloc-sys` v0.6), Cargo unifies into a single jemalloc build linked by both Rust and the C++ side.
- The librocksdb-sys jemalloc feature is a no-op on `android`, `dragonfly`, `musl`, `darwin`. On those targets RocksDB falls back to libc malloc but the Rust side still uses jemalloc via `tikv-jemallocator`. Linux is the primary target.
