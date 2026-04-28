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

## Open items

- `SegmentFooterV1.id` is hardcoded to `0` in `Segment::write` with a TODO. Real IDs will be assigned externally by the segment registry / `KeyIndex` when it learns to add segments.
- `Segment::footer` is currently private; the in-module test reads it directly. Promote to `pub(crate)` when `KeyIndex::add_segments` (currently `todo!()`) needs it.
