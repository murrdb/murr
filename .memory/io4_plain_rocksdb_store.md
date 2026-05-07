# io4 PlainRocksDBStore

## Shape

`io4::store::rocksdb::plain::PlainRocksDBStore` is the first concrete `Store` impl. State:

```rust
pub struct PlainRocksDBStore {
    db: DB,                    // SingleThreaded; cf_handle returns &ColumnFamily (no Arc clone per read)
    cf_opts: Options,          // reused for create_cf
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}
```

`open(path, PlainConfig)` does `DB::list_cf` → `DB::open_cf`, so existing column families are picked up automatically. New tables go through `Store::create_table`, which calls `DB::create_cf` — writes fail-fast with `MurrError::TableNotFound` on typo'd table names instead of auto-creating CFs.

## Why GAT on `Store::R<'a>`

`MGetResult<'a>` borrows from `&self.db` because `DBPinnableSlice<'a>` is a pointer into a RocksDB-pinned buffer. The trait therefore can't use a non-generic associated type — it needs `type R<'a>: ReadResult where Self: 'a` and `fn read<'a>(&'a self, …) -> Result<Self::R<'a>, _>`.

## Why `bytes()` returns `Iterator<Item = Result<Option<&[u8]>, MurrError>>`

Two requirements collide:

1. **Pipe RocksDB's response as-is.** `batched_multi_get_cf_opt` returns `Vec<Result<Option<DBPinnableSlice<'a>>, Error>>` — one slot per input key. We move that Vec straight into `MGetResult` without rebuilding it. No validation walk in `read`, no per-row reallocation.
2. **Preserve positional alignment.** The column encoder needs to emit a null row for any key the KV didn't have, so missing slots cannot be silently filtered out.

The trait yields `Result<Option<&[u8]>, MurrError>` so each per-row error and each per-row absence surfaces at iteration time rather than forcing an eager scan in `read`. **Why not** an early validation walk: even though it's just pointer iteration, it's an unnecessary pass on the hot path that the consumer would do anyway.

## Why PlainTable + mmap + NoopTransform + Vector memtable

PlainTable is RocksDB's hash-indexed SST format — built for in-memory point-lookup workloads, which is exactly what murr does. It has hard prerequisites:

- **`set_allow_mmap_reads(true)`** — PlainTable refuses to load otherwise.
- **`set_prefix_extractor(SliceTransform::create_noop())`** — required even for variable-length keys with no prefix queries; the noop transform tells PlainTable "treat the whole key as the prefix" so its hash index works.
- **`set_memtable_factory(MemtableFactory::Vector)`** — pairs with PlainTable for the bulk-load shape (one big write per partition reload, then read-heavy). Default skiplist also works but Vector is cheaper for the murr access pattern.
- **`user_key_length: 0`** — keys are variable-length byte sequences in this codebase.

## Why `sorted_input: false` for `batched_multi_get_cf_opt`

`sorted_input` is a hint for **block-based** SSTs — when keys are pre-sorted, RocksDB can skip its internal sort step on the path through block-based table metadata. PlainTable does point lookups via its hash index and doesn't read keys in sorted order at all, so the hint is a no-op here. Hardcoding `false` avoids making a contract with callers about key order that PlainTable wouldn't honor anyway.

## Why `WriteBatch` instead of per-key `put_cf`

One `WriteBatch` → one `DB::write_opt` call → one WAL append + one memtable insert per N rows. Per-key `put_cf` would do N WAL fsyncs (or N WAL appends with default WriteOptions) and N memtable insertions. For batch ingest from Parquet partitions this is the difference between an O(N) syscall path and an O(1) one.
