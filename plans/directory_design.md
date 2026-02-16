# Directory Design

Lucene-inspired storage abstraction for murr. Separates logical data organization from physical storage so the same code works over heap memory today and S3 tomorrow.

## Logical Layout

```
/<segment_id>/<file_name>
```

One directory = one table. No table-level abstraction in the directory itself — the caller manages the mapping from table names to directories.

- **Segment** — an immutable unit of data written in one batch. Segments are append-only: once committed, never modified. Contains a fixed set of files.
- **File** — a flat byte buffer within a segment. Holds one type of data (f32 column, u32 index, string dictionary, etc.).

This hierarchy maps naturally to prefix-based listing on S3 (`s3://bucket/segment/file`), making future S3 implementations straightforward.

## Why Segments Are Immutable

Segments model a write-once batch of data. This simplifies concurrency (readers never contend with writers on the same segment), eliminates the need for WAL or journaling, and maps well to object storage where overwrites are expensive or impossible. Merging segments is a future concern.

## Traits

### Directory

Owns the namespace. Three methods:

- `list()` — returns a `DirectoryListing` with all segments and their file names in one call. Designed for bulk metadata loading — you pull the full directory state from storage once (a recursive file listing or S3 `ListBucket` call) rather than navigating level by level.
- `reader(segment, file)` — opens a reader for a specific file. Async because future backends need I/O.
- `write(segment)` — atomically inserts a finalized `Segment` and returns the assigned segment ID.

`Send + Sync` so it can live in an `Arc` shared across tokio tasks.

### Reader — random read optimized

Bound to a single file within a segment. Designed for the access pattern "give me values at these known byte offsets":

- `read_f32_batch(offsets) -> Vec<f32>` — scatter-gather read of f32 values at arbitrary byte positions.
- `read_u32_batch(offsets) -> Vec<u32>` — same for u32.
- `read_vec_str(offset) -> Vec<String>` — read a length-prefixed string list starting at one offset.

The batch read API exists because the primary use case is key-based lookups: a hash index maps keys to byte offsets, then the reader gathers values at those offsets in one call. This is the hot path measured in `hash_benchmarks.md`.

`Send + Sync` because multiple query handlers may read from the same file concurrently.

### Writer — segment builder

Builds an entire segment independently of the directory. Write methods take a `file` parameter and buffer data locally into a `Segment`. The writer has no reference to the directory — it's a pure data builder.

- `write_f32(file, values)`, `write_u32(file, values)`, `write_str(file, values)` — append to a named file, return byte offset.
- `finish()` — consume the writer and return a `Segment` struct.

The caller then passes the `Segment` to `Directory::write()` to atomically commit it. This separation means writers don't need lifetime ties to the directory, and the directory only needs to handle the atomic insert.

Synchronous (not async) because it only buffers into local memory. `Send` only (not `Sync`) — a single task owns the writer during construction.

### Segment

Opaque struct holding finalized file data (`Vec<(String, Vec<u8>)>`). Created by `Writer::finish()`, consumed by `Directory::write()`. The `pub(crate)` visibility on its fields prevents external construction while allowing the directory implementation to read the data.

## Data Encoding

Native byte order throughout. No portability — this is an in-process cache, not a wire format.

### Fixed-width (f32, u32)

Written via `to_ne_bytes()` per value. No length prefix — the caller knows how many values it wrote and stores the offset. Each value occupies exactly 4 bytes, so `offset + i * 4` addresses element `i`.

### Strings

```
<total_byte_size: u32> [<str_len: u32><str_bytes>] [<str_len: u32><str_bytes>] ...
```

`total_byte_size` covers everything after itself (all the len+bytes pairs). This allows a reader to know when the string list ends without a separate count field. Written with a placeholder header that gets patched after all strings are appended.

## HeapDirectory — In-Memory Implementation

Backs everything with `Vec<u8>`. Structure:

```
segments: RwLock<HashMap<usize, HashMap<String, Arc<HeapFile>>>>
next_segment: AtomicUsize
```

- `Arc<HeapFile>` without interior mutability — files are immutable after commit, readers only need shared access.
- `AtomicUsize` for monotonic segment ID assignment on `write()`.
- `RwLock` on the segments map — readers take a read lock to look up files, `write()` takes a write lock to insert.

### Read path

- **`from_ne_bytes` with bounds-checked slicing** — safe Rust, returns `MurrError` on out-of-bounds reads.
- **`.iter().map().collect()`** instead of `push()` loops — triggers `extend_trusted` optimization in the standard library, which pre-allocates and fills without per-element capacity checks. Measured at ~2x faster than `push()` in benchmarks (see `hash_benchmarks.md`).

### Write path (HeapWriter)

Buffers into a local `HashMap<String, Vec<u8>>`. Each `write_*` call appends `to_ne_bytes()` to the appropriate file buffer. `finish()` moves the buffers into a `Segment`. No I/O until the caller calls `Directory::write()`.

For strings, writes a zero placeholder for `total_byte_size`, appends all `<len><bytes>` pairs, then patches the header.

## Files

```
src/directory/
├── mod.rs          — re-exports Directory, DirectoryListing, Reader, Writer, Segment, HeapDirectory, HeapWriter
├── directory.rs    — Directory trait + DirectoryListing struct
├── reader.rs       — Reader trait
├── writer.rs       — Writer trait + Segment struct
└── heap.rs         — HeapDirectory, HeapReader, HeapWriter, HeapFile
```
