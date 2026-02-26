# Storage & Query Execution Design

The storage subsystem of murr is a custom columnar format optimized for the access pattern "give me columns X, Y, Z for keys 1–200". It replaces an earlier Arrow IPC-based approach with a zero-copy binary segment format, designed for memory-mapped reads and eventual S3-backed storage.

The overall storage architecture — immutable segments, directory abstraction, segment-level metadata — is heavily inspired by Apache Lucene's index format. See also `directory_design.md` for the Lucene-inspired directory trait design.

## Design Evolution

The old stack (`src/old/`) reads Parquet files, converts them to Arrow IPC, and serves them via Axum. It works but ties the storage format to Arrow's serialization, which is heavyweight for an in-memory cache that only needs scatter-gather reads by key.

The new stack (`src/io/` + `src/service/`) introduces a custom `.seg` binary format that stores columns as raw byte blobs — no Arrow encoding on disk. Arrow is used only at the query boundary: columns are gathered into Arrow arrays for the response. This separation means the on-disk format is minimal and zero-copy-friendly, while the API remains Arrow-compatible.

The migration was incremental: segments first, then columns, then table reader/writer, then directory, then service — each layer building on the previous.

## Segment Binary Format

**Files:** `src/io/segment/{format,read,write}.rs`

A segment is the atomic unit of write — one RecordBatch becomes one `.seg` file. Segments are immutable once written, never modified in place. This mirrors Lucene's immutable segment model. The wire format (v2):

```
[MURR magic (4B)][version u32 NE (4B)]
[column payloads, 8-byte aligned]
[bincode-encoded SegmentFooter]
[footer_size u32 NE (4B)]
```

The segment footer is a `SegmentFooter { columns: Vec<FooterEntry> }` encoded with `bincode::config::standard()`. Each `FooterEntry` contains `{ name: String, offset: u32, size: u32 }`.

The footer-at-the-end layout (with footer size as the last 4 bytes) follows the same pattern as Lucene's compound file format — the reader seeks to the end first to find metadata, then uses it to locate column data.

**Write path** (`WriteSegment`): accumulates `(name, Vec<u8>)` column payloads, then serializes header + 8-byte-padded payloads + bincode footer in one pass.

**Read path** (`Segment`): memory-maps the file via `memmap2`, validates magic/version, decodes bincode footer from the end, builds `HashMap<name, Range<u32>>` for O(1) column lookup. `column(name)` returns `Option<&[u8]>` — zero-copy slice into the mmap.

Segment identity is derived from filename: `00000000.seg` → id 0. Padded naming ensures lexicographic sort matches insertion order.

## Column Layer

**Files:** `src/io/table/column/`

Two traits define the column abstraction:

### ColumnSegment (per-dtype encoding)

`ColumnSegment<'a>` handles encoding/decoding of a single column within a single segment:
- `parse(name, config, data: &[u8])` → deserialize from raw bytes
- `write(config, array: &ArrayType)` → serialize to `Vec<u8>`

Implementations: `Float32Segment`, `Utf8Segment`. Each uses a footer-based layout: data blocks first (8-byte aligned), then a bincode-encoded footer with offsets/sizes, then a u32 footer_len suffix. This makes writing single-pass (dump data, then encode footer with known offsets). `bytemuck::cast_slice` is still used for zero-copy payload data access (f32, i32, u64 arrays).

**Float32 wire format** (footer-based):
```
[f32 values][pad to 8]
[null bitmap: u64 words][pad to 8]
[bincode Float32Footer: num_values, payload_offset, null_bitmap_offset, null_bitmap_size]
[footer_len: u32 NE]
```

**Utf8 wire format** (footer-based):
```
[i32 value offsets][pad to 8]
[concatenated string bytes][pad to 8]
[null bitmap: u64 words][pad to 8]
[bincode Utf8Footer: num_values, offsets_offset, payload_offset, payload_size, null_bitmap_offset, null_bitmap_size]
[footer_len: u32 NE]
```

### Column (multi-segment abstraction)

`Column` aggregates segments and provides the query interface:
- `get_indexes(&[KeyOffset])` → `Arc<dyn Array>` — scatter-gather by key offsets
- `get_all()` → `Arc<dyn Array>` — concatenate all segments
- `field()` → Arrow `Field` (cached, not rebuilt per query)

`Float32Column` and `Utf8Column` hold `Vec<Segment>` and dispatch to the correct segment by `segment_id` in `KeyOffset`.

### Null Bitmap

**File:** `src/io/table/column/bitmap.rs`

Uses u64-word bit array (bit set = valid). Design choices from benchmarking (see `hash_benchmarks.md`):

- **u64 words** instead of u8 or bitvec: bitvec has 6–7 abstraction layers per bit access. Manual u64 bit ops are much faster.
- **Lazy write allocation**: `NullBitmap::write()` only allocates u64 words when the first null is encountered. Returns empty vec if all values are valid — common case for non-nullable columns.
- **Branch-once null checking**: The column checks nullability at the segment level, not per element:
  1. If `ColumnConfig.nullable == false` → skip all null checks
  2. If nullable but segment has no bitmap → skip null checks for this segment
  3. If nullable AND bitmap exists → dereference bitmap once, check per element

This avoids the double-check (Option + index) on every value access. Benchmarks showed that skipping the bitmap entirely for non-nullable columns returns performance to the no-nulls baseline.

## Table Layer

**Files:** `src/io/table/{view,reader,writer,cached}.rs`

### TableView
Owns `Vec<Segment>` — the physical data. Created by opening all `.seg` files from a directory via mmap.

### TableReader
The query engine. Two data structures:
- `columns: AHashMap<String, Box<dyn Column>>` — column registry
- `index: AHashMap<String, KeyOffset>` — key → (segment_id, offset) mapping

Built from a `TableView` + schema:
1. For each column in schema, collect byte slices from all segments, construct the appropriate Column impl
2. Load the key column (always Utf8, non-nullable), call `get_all()` to get all keys
3. Build key index — **last-write-wins**: later segments overwrite earlier entries for the same key

`get(keys, columns)` resolves each key to a `KeyOffset` (or `MissingKey`), then calls `Column::get_indexes()` per column. Missing keys produce null values. Output order matches input key order.

### TableWriter
Short-lived, borrows a mutable directory reference. Two constructors:
- `create(schema, dir)` — new table, writes `table.json`, fails if table exists
- `open(dir)` — existing table, loads schema from `table.json`

`add_segment(batch)` encodes each column via `ColumnSegment::write()`, assembles into `WriteSegment`, writes the `.seg` file with the next sequential id.

### CachedTable
Solves the self-referential ownership problem: `TableReader` borrows from `TableView` (segment mmaps), but both must live together. Uses `ouroboros` crate:

```rust
#[self_referencing]
pub struct CachedTable {
    view: TableView,
    #[borrows(view)]
    #[covariant]
    reader: TableReader<'this>,
}
```

This is the struct held in the service registry — owns the data and provides query access.

## Directory Abstraction

**Files:** `src/io/directory/{mod,local}.rs`

The directory abstraction follows Lucene's `Directory` concept — a namespace that owns segments and provides I/O primitives, decoupling logical data organization from physical storage. See `directory_design.md` for detailed rationale.

```rust
pub trait Directory {
    async fn index(&self) -> Result<Option<IndexInfo>>;
    async fn write(&mut self, name: &str, data: &[u8]) -> Result<()>;
}
```

`IndexInfo` bundles `TableSchema` + `Vec<SegmentInfo>`. `None` means empty directory (no `table.json`).

`LocalDirectory` holds a path, scans for `.seg` files (sorted by name), reads `table.json` for schema. Async interface is future-proof for S3 implementations.

`TableSchema` is serialized as `table.json`:
```json
{ "key": "user_id", "columns": { "user_id": { "dtype": "utf8", "nullable": false }, ... } }
```

## Service Layer

**Files:** `src/service/{mod,state}.rs`

`MurrService` is the application-level registry:
```rust
struct MurrService {
    tables: RwLock<HashMap<String, TableState>>,
    data_dir: PathBuf,
}
```

`TableState` owns `LocalDirectory`, `TableSchema`, and `Option<CachedTable>`.

### Request flow

1. **Create**: `create(table_name, schema)` — writes `table.json` via `TableWriter::create`, inserts empty `TableState`
2. **Write**: `write(table_name, batch)` — opens `TableWriter`, calls `add_segment(batch)`, then **rebuilds** `CachedTable` (re-mmaps all segments, rebuilds key index). This is the reload path — any external writer adding a `.seg` file triggers the same rebuild.
3. **Read**: `read(table_name, keys, columns)` — acquires read lock, delegates to `CachedTable::get()`

The write-then-rebuild pattern means the table is always consistent: readers see a snapshot, writers produce a new snapshot atomically.

## Key Design Decisions

### Immutable segments (Lucene-inspired)
Segments are write-once, following Lucene's immutable segment model. No in-place updates, no WAL. This simplifies concurrency (readers never contend with writers on the same segment) and maps naturally to object storage. Future segment compaction (merging small segments into larger ones) follows the same pattern as Lucene's merge policy.

### Last-write-wins key resolution
When building the key index, later segments overwrite earlier ones. This enables incremental updates: write a new segment with updated rows, and they shadow the old values. Future: segment compaction can merge and deduplicate.

### AHashMap for key index
Benchmark-driven: AHash (AES-NI based) reduced hashing from 26% to ~2% of query time. See `hash_benchmarks.md` Experiment 8.

### bincode for headers/footers, bytemuck for payloads
Segment and column metadata (footers) use `bincode` 2.x with `config::standard()` for serialization. This removes the `bytemuck::Pod`/`Zeroable` constraint (fixed-size `#[repr(C)]` structs only), allowing future headers to include variable-length fields (metadata, statistics, compression flags). Payload data (f32 arrays, i32 offset arrays, u64 null bitmaps) still uses `bytemuck::cast_slice` for zero-copy access from mmap'd memory — this is pure data casting, not structured serialization.

Column metadata was also moved from header-at-start to footer-at-end, matching the segment-level pattern. This makes writing single-pass: dump all data blocks first, then encode the footer with final offsets/sizes.

All alignment was standardized to 8-byte (from mixed 4/8-byte) since we target 64-bit systems only.

### ouroboros for self-referential ownership
`CachedTable` must own segment data (mmaps) while `TableReader` borrows from it. Rust's borrow checker can't express this directly. `ouroboros` generates safe self-referential code.

### Two-loop gather pattern
Benchmarks showed that separating value gather (`.collect()` with `extend_trusted`) from bitmap gather preserves compiler optimizations. Fusing into a single loop forces `push()` which loses `extend_trusted`. See `hash_benchmarks.md` Experiments 3–4.

### Column-level Arrow Field caching
Each `Column` stores its Arrow `Field` definition to avoid rebuilding it per query during `RecordBatch` construction.

## Performance Characteristics

**Note:** All benchmark numbers referenced here and in `hash_benchmarks.md` are in **microseconds (µs)**, not milliseconds.

At 10M rows, 10 Float32 columns, 1000 random key lookups:
- Key lookup: ~2–3µs (dominated by string comparison in AHashMap)
- Value gather per column: ~3–4µs (random memory access into mmapped segments)
- Null bitmap overhead: near-zero for non-nullable columns (branch-once optimization)
- Total: ~30–40µs for the full scatter-gather

See `hash_benchmarks.md` for detailed profiling across 13 experiments.
