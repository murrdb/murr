# io4 RocksDBStore — Block backend

Per-backend deep-dive on the block-based-table arm of the unified `RocksDBStore`. Read alongside `io4_rocksdb_store.md` for the cross-cutting design (one struct, GAT, manifest sidecar, etc.).

## When to pick block over plain

PlainTable requires the entire SST to be mmap-resident — the hash index assumes random access into the whole file at constant cost. That works while the dataset fits in RAM. Once it doesn't, the kernel evicts pages under memory pressure and PlainTable's "constant-time" lookups become page faults.

BlockBasedTable was designed for that case: an on-disk block index, optional bloom filter to short-circuit definite misses, and an LRU `block_cache` to keep the hot working set in RAM. It also benefits from RocksDB's leveled compaction in a way PlainTable doesn't (PlainTable SSTs can't be merged, only stacked).

Rule of thumb in this codebase:
- Dataset fits comfortably in RAM, read-mostly, hash point lookups → plain.
- Dataset spills past RAM, point lookups against a working set → block.
- Mixed read/range or persistence-first workloads → block (range scans are not on PlainTable's path at all).

## Why `sort_keys = true` and the scatter-restore in `read`

For block-based SSTs `batched_multi_get_cf_opt(.., sorted_input = true, ..)` lets RocksDB skip its own internal sort on the way through the block index, and (more importantly) lets it walk consecutive keys in the same data block in one pass instead of re-seeking per key. So the block backend always pre-sorts on the way in.

That breaks caller order, so the read path:

1. Builds `order: Vec<usize>` indexing into the caller's `keys`, sorted by key bytes.
2. Materializes a sorted `Vec<&[u8]>` via that permutation.
3. Calls `batched_multi_get_cf_opt(.., sorted, true, ..)`.
4. Scatters each result back into a `Vec<Option<…>>` slot array indexed by the caller's original position, then unwraps to the dense `Vec<…>` that `MultiGetResult` expects.

The scatter array uses `Option<…>` because `Result<Option<DBPinnableSlice<'a>>, Error>` has no `Default`. That's the only extra alloc on the sorted path — the sort itself is in-place over the index `Vec`.

`#[case::block]` in the shared `rstest` suite exercises this through `read_preserves_caller_key_order`, which deliberately mixes orderings + a missing key in the middle to catch any off-by-one in the permutation mapping.

## Why `mmap_reads` and `use_direct_reads` are mutually exclusive

`set_allow_mmap_reads(true)` and `set_use_direct_reads(true)` are exclusive in RocksDB — the latter opens SSTs with `O_DIRECT`, bypassing the page cache entirely, which is incompatible with mmap'd reads. `BlockConfig::default` picks `mmap_reads = true, use_direct_reads = false`. If a user sets both via config the underlying `DB::open_cf_with_opts` errors at startup; we don't pre-validate, since the surface error is already specific.

## Why `bloom_filter_bits_per_key` defaults to `None`

A bloom filter only pays off when the workload has misses (a sizeable fraction of `multi_get` queries that aren't in the SST). For murr's batch-feature-fetch shape — keys come from a recently-loaded partition and are usually present — the filter is dead memory most of the time. Default off; turn it on per-table if profiling shows actual miss volume.

`whole_key_filtering = true` (the default when bloom is enabled) means the filter is keyed on the whole key, not a prefix. This codebase has no prefix queries, so prefix filtering would just bloat the filter for nothing.

## Why `data_block_hash_index = true` by default

`DataBlockIndexType::BinaryAndHash` adds a small per-block hash index on top of the default binary search inside each data block. Costs ~`data_block_hash_ratio` (0.75) extra space per block; saves a `log2(entries-per-block)` comparisons on point lookups. For the murr access pattern (point lookups into bulk-loaded data, no range scans inside a block) the win is consistent.

## Why shared write-side defaults are reused from `plain.rs`

`write_buffer_size`, `target_file_size_base`, `disable_auto_compactions` apply identically to both backends — they're `Options`-level (memtable + leveled compaction) settings, not table-format settings. Their `default_*` fns live in `plain.rs` and are re-exported `pub(super)` for `BlockConfig` to reuse. Rationale: a third "shared defaults" module is more friction than re-exporting four functions. If a third backend ever shows up that needs them, then it's worth promoting.
