# io RocksDBStore

## Shape

`io::store::rocksdb::RocksDBStore` is a single concrete `Store` impl that supports two SST formats — PlainTable (in-memory hash point lookups) and BlockBasedTable (general-purpose block index + bloom). The two backends are selected at construction time:

```rust
RocksDBStore::open(path, &PlainConfig::default())?           // plain backend (default constructor)
RocksDBStore::open_block(path, &BlockConfig::default())?     // block backend
```

Both factories delegate to a private `open_inner(path, cf_opts, read_opts, write_buffer_size, sort_keys)` that owns the bootstrap (`DB::list_cf` → `DB::open_cf_with_opts` → `Manifest::from_file`). The variance between the two backends collapses to:

1. The `Options` value built from the config (`From<&PlainConfig> for Options` / `From<&BlockConfig> for Options`).
2. Whether `read_opts` carries the block-side `async_io` / `verify_checksums` knobs.
3. The `sort_keys: bool` runtime field that drives the read path.

`open*(path, …)` does `DB::list_cf` → `DB::open_cf_with_opts`, so existing column families are picked up automatically. New tables go through `Store::create_table`, which calls `DB::create_cf` — writes fail-fast with `MurrError::TableNotFound` on typo'd table names instead of auto-creating CFs.

`open` (plain) is named asymmetrically with `open_block` because plain is the default in this codebase — most call sites want the in-memory hash backend; block is opt-in for larger-than-RAM datasets.

## Why one struct, not two — and why `sort_keys` is a runtime field

Two parallel `PlainRocksDBStore` / `BlockRocksDBStore` types were considered and dropped: the variance is small (the two `Options` builders + one bool), and the duplication of `create_table`/`write`/`compact`/manifest plumbing wasn't earning its keep. A generic `RocksDBStore<P: RocksDBProfile>` was also considered — it's strictly more expressive (`SORTED_INPUT` could be a const, monomorphized away) but adds turbofish noise at every callsite for no measurable win on a one-bit branch.

`sort_keys: bool` lives on the struct (not on the config) because the value is fixed at construction time and never re-derived. After `open*` returns, callers and the `Store` trait don't see configs at all — only the concrete store with all the runtime state baked in.

## Why `Store::read` takes a `ReadBatchBuilder` instead of returning borrowed bytes

`Store::read` no longer returns `Result<Self::R<'a>, _>` with a GAT-bound `ReadResult`. The trait now reads:

```rust
fn read(&self, table: &str, keys: &[&[u8]], builder: ReadBatchBuilder<'_>)
    -> Result<RecordBatch, MurrError>;
```

The caller constructs the `ReadBatchBuilder` (which owns Arrow column encoders and the target segment schema) and hands it down by value. The store iterates RocksDB's `batched_multi_get_cf_opt` results internally, calling `builder.add_row(pinned.as_ref())` or `builder.add_empty()` per key while the `DBPinnableSlice<'_>` slots are still live, then finalises the builder into a `RecordBatch` and returns it.

**Why**: a future LMDB/heed-backed `Store` needs its `RoTxn` alive *across* the per-key iteration. Returning borrowed slices out of `read` would force the impl into self-referential ownership (txn + slices in one struct) or eager cloning of every row (defeating zero-copy). Pushing the builder down lets each impl bound the slice lifetime to its own fn frame.

**Why not return `Vec<Vec<u8>>` instead of a builder**: defeats the RocksDB pinned-slice zero-copy path — every read would allocate per-row even when the encoder only needs to memcpy a fixed-width field. The builder is the minimum interface that lets the store keep zero-copy on the hot path while still erasing the slice lifetime at the trait boundary.

**Positional alignment** (every input key must produce exactly one output slot) is still preserved — the store calls either `add_row` or `add_empty` for each key, and the inverse-permutation table on the `sort_keys = true` path restores caller order before the loop.

## Why `ReadMethod::ParGet` exists alongside `Get`

`Get` walks `keys.iter()` and calls `db.get_pinned_cf_opt` once per key on a single core. For a 1k-key fetch every lookup is independent CPU-bound work (PlainTable hash probe + memcpy into the pinned slice), so the loop is embarrassingly parallel. `ParGet` is the same loop with `keys.par_iter()` — rayon's global pool (sized to `num_cpus`) fans the work out, and `par_iter().collect()` preserves input order so positional alignment with the caller's `keys` slice holds **without** a scatter step (unlike `MultiGetSorted`, which has to undo a key-bytes sort before handing slices to the builder).

Only the rocksdb lookup phase is parallelized — the downstream `for r in &raw { builder.add_row(...) }` loop stays serial because the Arrow encoders mutate column buffers in order.

**Why a new variant rather than replacing `Get`**: keeps the sequential path so benchmarks can A/B the two, and avoids a silent regression on small key batches where rayon scheduling overhead dominates the per-key work. The choice is one config flip in `PlainConfig::read_method` / `BlockConfig::read_method`.

**Why rayon's global pool, not a per-store custom pool**: no thread-count knob to misconfigure, and the global pool is shared across whatever else might use rayon (today: nothing) — if that becomes a real contention source later it's easy to switch to a `ThreadPoolBuilder` owned by `RocksDBStore`. Pre-alpha defaults beat premature configurability.

`DBPinnableSlice<'_>`, `&DB`, `&ColumnFamily`, `&ReadOptions` are all `Send + Sync` in rocksdb-0.24 — no wrapping or `unsafe` is needed to share them across rayon workers.

## Why `ReadMethod::ParMultiGet` exists alongside `ParGet`

`ParGet` fans out one `get_pinned_cf_opt` per key across rayon workers — full parallelism, but every key pays the full point-lookup setup cost. `ParMultiGet` splits the input into `rayon::current_num_threads()` contiguous chunks and calls `batched_multi_get_cf_opt(..., sorted_input = false, ...)` once per chunk in parallel. Each chunk thus amortizes the rocksdb MultiGet path's per-call setup across ~`n / num_threads` keys.

The fan-out uses `par_chunks(chunk_size).flat_map_iter(|chunk| batched_multi_get_cf_opt(...))`. `par_chunks` yields contiguous slices in input order; the indexed parallel iterator + `flat_map_iter` concatenates per-chunk result vectors in that same order, so global positional alignment with the caller's `keys` slice is preserved without a scatter step.

**No sort.** This is deliberately the unsorted counterpart of `MultiGetSorted` — chunks are sliced off the caller's key array as-is. Sorting per chunk would help block-based table block-walk locality, but it also forces a scatter back to caller order; that variant (`ParMultiGetSorted`) is left for later if measurements show the unsorted-parallel ceiling isn't enough.

Chunk size is `keys.len().div_ceil(num_threads).max(1)` so the empty-keys and `keys.len() < num_threads` cases stay legal (`slice::chunks(0)` panics).

## Why PlainTable + mmap + NoopTransform + Vector memtable (plain backend)

PlainTable is RocksDB's hash-indexed SST format — built for in-memory point-lookup workloads. It has hard prerequisites:

- **`set_allow_mmap_reads(true)`** — PlainTable refuses to load otherwise.
- **`set_prefix_extractor(SliceTransform::create_noop())`** — required even for variable-length keys with no prefix queries; the noop transform tells PlainTable "treat the whole key as the prefix" so its hash index works.
- **`set_memtable_factory(MemtableFactory::Vector)`** — pairs with PlainTable for the bulk-load shape (one big write per partition reload, then read-heavy). Default skiplist also works but Vector is cheaper for the murr access pattern.
- **`user_key_length: 0`** — keys are variable-length byte sequences in this codebase.

## Why `sort_keys = false` for the plain backend

`sorted_input` on `batched_multi_get_cf_opt` is a hint for **block-based** SSTs — when keys are pre-sorted, RocksDB can skip its internal sort step on the path through block-based table metadata. PlainTable does point lookups via its hash index and doesn't read keys in sorted order at all, so the hint is a no-op here. Hardcoding `sort_keys = false` for plain avoids paying for an O(n log n) sort + O(n) scatter-restore that wouldn't help PlainTable read.

For the block backend the hint is real — see `io_block_rocksdb_store.md`.

## Why `WriteBatch` instead of per-key `put_cf`

One `WriteBatch` → one `DB::write_opt` call → one WAL append + one memtable insert per N rows. Per-key `put_cf` would do N WAL fsyncs (or N WAL appends with default WriteOptions) and N memtable insertions. For batch ingest from Parquet partitions this is the difference between an O(N) syscall path and an O(1) one.

## Why a sidecar `manifest.json`, not a `_meta` column family

Per-table `TableSchema` is persisted in `<db_path>/manifest.json` (JSON, atomic tmp+rename in `Manifest::to_file`). The store loads it on `open*` and rewrites on `create_table`. The `Store` trait exposes `manifest()` so the service layer can rehydrate its registry on restart without the caller re-supplying schemas.

Considered and rejected: a reserved `_meta` column family inside RocksDB. It would have given atomic checkpoint inclusion for free, but required a second `Options` profile (block-based, not PlainTable+mmap) and pushed metadata schema design into the trait. At pre-alpha, the sidecar's debuggability (`cat manifest.json`) and zero-friction `MemoryStore` parity (just an in-memory field) outweigh the atomicity argument.

Known limitation: there is a small crash window between `db.create_cf` and `manifest.to_file`. If the process dies between the two, the next `open*` sees an orphan CF that is invisible through the manifest. Acceptable until a `drop_table` API exists to clean orphans up.

## Why `Store::compact` is on the trait, not inherent

`compact_range_cf(.., None, None)` collapses L0 SSTs into the leveled tree after a bulk write. The service layer wants to call this after a partition reload regardless of which backend is active, so it lives on `Store` and dispatches identically on both `RocksDBStore` factories. `MemoryStore::compact` is a no-op — there is nothing to coalesce in a `HashMap`.
