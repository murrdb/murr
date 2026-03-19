# Incremental Index Rebuild with File-Based Segment IDs

## Decision
Changed `KeyOffset::segment_id` from positional index into `Vec<Segment>` to the actual file-based segment ID (parsed from filename, e.g., `00000042.seg` → 42). This makes the key index stable across table reopens.

## Why
The old approach rebuilt the entire key index (`AHashMap<String, KeyOffset>`) from scratch on every `MurrService::write()`. With 10M rows and 334 batches, each write re-indexed progressively more data, causing ingestion throughput to degrade significantly.

## Why not just pass segment count?
We considered keeping positional segment IDs and tracking how many segments were previously indexed. However, file-based IDs are more robust:
- Index entries remain valid even if segments are reordered or gaps appear (future compaction)
- Simpler to reason about: segment_id directly maps to the `.seg` file
- Prepares for future segment deletion/compaction

## Implementation
- `TableView` stores `Vec<Option<Segment>>` indexed by file-based segment ID (None slots for compaction gaps)
- `KeyIndex` struct in `io::table::index` encapsulates all key→offset logic
- `KeyIndex::build_incremental()` accepts `Option<Arc<KeyIndex>>` and only indexes segments with IDs > max_segment_id in previous index
- Arc must be sole owner (try_unwrap), returns error otherwise (indicates bug)
- Columns (`Float32Column`, `Utf8Column`) also use `Vec<Option<ParsedSegment>>` indexed by file-based ID

## Key files
- `src/io/table/index.rs` — KeyIndex struct (new)
- `src/io/table/view.rs` — Vec<Option<Segment>> change
- `src/io/table/column/float32/mod.rs` — Vec<Option<Float32Segment>> change
- `src/io/table/column/utf8/mod.rs` — Vec<Option<Utf8Segment>> change
- `src/io/table/reader.rs` — Uses KeyIndex, passes previous_index
- `src/io/table/cached.rs` — Extracts Arc<KeyIndex> from old table before consuming
