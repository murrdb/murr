# io3 segment wire format

## Layout

```
[row space] [key space] [footer] [SegmentTrailer (8 bytes)]
```

- **row space**: per row `[row_size: u32 LE] [row_bytes...]`. `row_bytes` is `Row.bytes` from `io3::row::Row`.
- **key space**: per row `[key_len: u32 LE] [key_bytes] [row_offset: u32 LE] [row_size: u32 LE]`, where `row_offset` is the byte offset (within the row space) of that row's `row_size` prefix.
- **footer**: `bincode::serde::encode_to_vec(&SegmentFooterV1, bincode::config::standard())` — varint, compact.
- **SegmentTrailer** (always last 8 bytes): `[footer_size: u32 LE][version: u32 LE]`. Hand-encoded with `to_le_bytes` / `from_le_bytes` — bincode for two u32s would force a fixed-int config dance just to keep the tail length stable; naive read/write is clearer. `SegmentTrailer` is the single source of truth for trailing-bytes layout.

## Why the key space duplicates row_size

The key space holds `(row_offset, row_size)` per key so the index can locate a row from a key without touching the footer or scanning the row space. Trade: 4 extra bytes per row for O(1) random access from any key.

## Why a single pass over rows

Each `Row` pre-allocates its byte buffer in `Row::new` (row.rs:23-28), so `row.bytes.len()` is final before we ever append to `rows_buf`. That means `current_row_offset` can be tracked inline as we emit both buffers — no second pass needed for offset accounting. We also skip `rows.iter().map(|r| r.bytes.len() + 4).sum()` for capacity hinting; a flat 1MB initial capacity is cheaper than the extra full scan.

## Why bincode 2.x with `serde` feature

`SegmentFooterV1` and `SegmentSchema` already carry `serde::{Serialize, Deserialize}` for the existing JSON `_metadata.json` use. Using bincode's `serde` feature lets us reuse those derives — single source of truth for what the type's wire shape looks like.

## io3 MemDirectory (`src/io3/directory/mem/`)

`MemDirectory { metadata: RwLock<TableInfo>, segments: RwLock<Vec<Option<Vec<u8>>>> }` — segments indexed by id (vec position), not a HashMap keyed by formatted name. Avoids format!() on every read.

Writer holds both write locks simultaneously (segments first, then metadata) so id assignment and vec push are atomic. Id = `segments.len()` at write time.

Reader snapshots `TableInfo` at open/reopen time; `info()` returns the cached snapshot without locking. `read()` takes a read lock on segments and slice-copies requested bytes. Bounds-checked (returns `SegmentError` on out-of-range).

`create()` initializes with empty `TableInfo { schema, segments: [] }`. `open()` returns an error (no persistent storage). `list_indexes()` returns `[]`.

No `_metadata.json` file in mem — the `METADATA_JSON` trait const exists but mem uses in-memory structs directly, matching the semantics without unnecessary JSON serialization.

## TableReader (io3) read path

Two batched directory reads on `open` (one footer batch, one keys batch) and a third batch on `read()` for row payloads. Reason: the directory's `read()` is request-batched, so we pay one round trip per logical phase rather than per segment.

Footer reads use a fixed-size tail (last `min(size_bytes, 64 KiB)` per segment) instead of a two-phase trailer-then-footer dance. Bincode-encoded `SegmentFooterV1` for realistic schemas is well under that bound; if an outlier ever exceeds it, the reader returns `SegmentError`.

Schema is **immutable per table**. The reader derives a canonical `SegmentSchema` from `TableSchema` at construction time (IndexMap insertion order, no sort), and validates each loaded segment's footer schema against it via `PartialEq`. Schema migration is deliberately not supported — recreate the table to change schema. `TableWriter::write` canonicalizes the input `RecordBatch` column order by projecting it to match `Schema::from(&self.schema)` before calling `Segment::write`, ensuring footer schemas always match what the reader expects.

`TableSchema::columns` uses `IndexMap<String, ColumnSchema>` (insertion-order-preserving) instead of `HashMap`. This makes column ordering deterministic: writer and reader derive the same canonical `SegmentSchema` from the same schema without any sort step.

`reopen` deletion path:
1. Walk `self.segments` against the new info's id set; clear slots whose id no longer exists.
2. `KeyIndex::prune_segments(&removed_ids)` drops index entries pointing to dropped segments.
3. Run `open`'s footer + keys batches across only the new segments and `KeyIndex::add_segment` them.

Tombstone-as-deletion (a row whose every column is null) was considered and rejected — deletion is a segment-level concept. Open and reopen never read the rows section.

Missing keys in `read()` materialize as an `all_null` row (built by `Row::all_null` which bulk-fills the bitset bytes with `0xFF` instead of looping per-column), which then decodes to nulls in the output `RecordBatch`. The `RecordBatch` is built via the new `TryFrom<ColumnBatch> for RecordBatch` and projected to the requested columns via `RecordBatch::project`.

## Column codec layer (`src/io3/column/`)

`ColumnCodec` is the single dtype-dispatch boundary for `RowBatch ↔ ColumnBatch` serde. One trait, two methods (`encode` / `decode`), one registry (`codec_for(DType) -> &'static dyn ColumnCodec`). `batch.rs` no longer matches on dtype — it just iterates schema columns and calls into the codec. Adding a primitive dtype = one line in `codec_for`; adding a non-primitive dtype = one new file + one line.

`PrimitiveCodec<T: ArrowPrimitiveType>` covers any arrow primitive whose `Native: bytemuck::Pod`. Encode does `bytemuck::bytes_of(&native)`; decode does `bytemuck::pod_read_unaligned::<T::Native>(slice)`. **Host-endian on disk** — cross-endian segment portability is explicitly not a goal (x86_64 and aarch64 are both LE). The previous explicit-LE design was dropped because it required either per-type adapters or `num_traits` + `generic_const_exprs` for marginal benefit.

`PhantomData<fn() -> T>` (not `PhantomData<T>`) so the codec is `Send + Sync` regardless of `T`'s auto-traits.

`Row` static-cell API is typed: `write_static::<T: NoUninit>(.., value: T)` and `read_static::<T: Pod>(..) -> T` encapsulate `bytes_of` / `pod_read_unaligned` so codecs don't construct intermediate `&[u8]`. Dynamic payloads use byte-level `set_dynamic_value` / `get_dynamic_bytes` since the payload size isn't known to the type system. Utf8 validation lives in `Utf8Codec`, not in `Row` — keeps the dynamic helpers reusable for future `Binary`/`List` codecs without parallel methods.

`Utf8Codec` validates utf8 once per row at decode and bubbles up `MurrError::SegmentError` on invalid bytes.

## Open items

- `SegmentFooterV1.id` is hardcoded to `0` in `Segment::write` with a TODO. Real IDs will be assigned externally by the segment registry / `KeyIndex` when it learns to add segments.

## io3 TableWriter (`src/io3/table/writer.rs`)

`TableWriter<D: Directory>` wraps a `D::WriterType` (obtained via `dir.open_writer()`) and a `TableSchema`. `write(&RecordBatch)` projects the batch to canonical column order via `Schema::from(&self.schema)` (IndexMap iteration order = insertion order), then calls `Segment::write()` and `DirectoryWriter::write()`. No segment ID assignment here — IDs come from the directory (position in the segments vec).
