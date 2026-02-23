# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Note to agents like Claude Code

The project uses .memory directory as an append-only log of architectural decisions made while developing:
* before doing planning, read the .memorydirectory for relevant topics discussed/implemented in the past
* when a plan has an architecural decision which can be important context in the future, always include a point to append the summary and reasoning (why are we making it and why not something else) for the change.
* update .memory only for important bits of information.


## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval - fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Custom binary segment format with memory-mapped reads
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. The codebase is migrating from an Arrow IPC-based storage layer (in `src/old/`) to a custom binary segment format (in `src/io/`). The new `MurrService` in `src/service/` wraps the new storage, but `main.rs` still runs the old stack.

## Common Commands

```bash
cargo build                  # Build the project
cargo test                   # Run all tests
cargo test <name>            # Run specific test by name
cargo check                  # Fast syntax/type check without codegen
cargo clippy                 # Linting
cargo fmt                    # Format code
cargo bench --bench <name>   # Run a specific benchmark (table_bench, api_bench, hashmap_bench, hashmap_row_bench)
```

## Architecture

### Two Stacks: Old (Arrow IPC) vs New (Custom Segments)

The codebase has two parallel storage implementations:

**Old stack (`src/old/`)** — Currently wired into `main.rs`:
- `discovery/` — Finds latest date-partitioned directory with `_SUCCESS` marker in S3/local
- `parquet/` — Converts Parquet files to Arrow IPC via streaming
- `manager/` — `TableLoader` (discover → convert → load), `TableManager` (RwLock registry)
- `api/` — Axum REST API: `POST /v1/{table}/_fetch` (JSON + Arrow IPC responses), `GET /health`

**New stack (`src/io/` + `src/service/`)** — The replacement, not yet in `main.rs`:
- Custom `.seg` binary format replaces Arrow IPC
- Only `Float32` and `Utf8` column types implemented so far (old stack supports all 10 dtypes)
- `MurrService` provides `create()`, `write()`, `read()` API

### Module Structure

**`io/segment/`** — Custom binary `.seg` format
- `format.rs` — Wire format: `[MURR magic][version u32 LE][column payloads (4-byte aligned)][footer entries][footer_size u32 LE]`
- `write.rs` — `WriteSegment` builder: `add_column(name, bytes)` then `write(w)`
- `read.rs` — `Segment::open(path)` memory-maps file, validates magic+version, parses footer, provides `column(name) -> Option<&[u8]>` zero-copy access

**`io/directory/`** — Storage directory abstraction
- `Directory` trait with `index()` and `write()` methods
- `LocalDirectory` reads `table.json` (schema) + scans `*.seg` files
- `TableSchema` and `SegmentInfo` types describe directory contents

**`io/table/`** — Table layer built on segments
- `writer.rs` — `TableWriter` creates `table.json` and writes `{id:08}.seg` files from `RecordBatch`
- `reader.rs` — `TableReader` builds key index (`AHashMap<String, KeyOffset>`) across segments; last segment wins for duplicate keys
- `view.rs` — `TableView` opens all segment files, holds `Vec<Segment>`
- `cached.rs` — `CachedTable` uses `ouroboros` self-referential struct to own `TableView` + borrow `TableReader`
- `table.rs` — **Old-style** Arrow IPC `Table` (retained for old stack)

**`io/table/column/`** — Per-dtype column implementations
- `Column` trait: `get_indexes(&[KeyOffset]) -> Arc<dyn Array>`, `get_all()`, `size()`
- `ColumnSegment` trait: `parse(name, config, data)`, `write(config, array) -> Vec<u8>`
- `float32/` — `Float32Column` with 16-byte segment header, 8-byte aligned payload, optional null bitmap
- `utf8/` — `Utf8Column` with 20-byte segment header, i32 value offsets, concatenated strings, optional null bitmap
- `bitmap.rs` — `NullBitmap` using u64-word bit array (bit set = valid)

**`service/`** — High-level service wrapping the new storage
- `MurrService` — `RwLock<HashMap<String, TableState>>` table registry
- `create(table_name, schema)` → `write(table_name, batch)` → `read(table_name, keys, columns)` flow
- Each `TableState` holds `LocalDirectory`, `TableSchema`, `Option<CachedTable>`

**`core/`** — Error types (`MurrError` with `thiserror`), CLI args (`clap`), logging (`env_logger`)

**`conf/`** — YAML configuration: `Config`, `ServerConfig`, `TableConfig`, `SourceConfig`, `DType` enum. Uses `#[serde(deny_unknown_fields)]` for strict validation.

**`testutil.rs`** — Feature-gated (`testutil`) test helpers: `generate_parquet_file()`, `setup_test_table()`, `setup_benchmark_table()`, `bench_generate_keys()`

### Key Design Patterns

- **Self-referential structs**: `CachedTable` uses `ouroboros` to own a `TableView` while borrowing from it in `TableReader`
- **`AHashMap`** used in `TableReader` for faster hashing than std `HashMap`
- **`bytemuck`** for zero-copy casting of segment headers
- **`memmap2`** for memory-mapped segment reads — both old (Arrow IPC) and new (.seg) paths
- **Feature-gated test utilities**: `testutil` feature enables `tempfile` + `rand` deps for test/bench helpers

### Configuration Format

```yaml
server:
  host: localhost
  port: 8080
  data_dir: /var/lib/murr

tables:
  user_features:
    source:
      s3:
        bucket: my-bucket
        prefix: features/
        region: us-east-1
        endpoint: http://localhost:9000  # optional, for MinIO/LocalStack
    poll_interval: 5m  # default: 1m
    parts: 8           # default: 8
    key: [user_id]
    columns:
      user_id:
        dtype: utf8
        nullable: false
      click_rate:
        dtype: float32
        nullable: true  # default: true
```

Supported dtypes: `utf8`, `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `bool`
(New stack currently only implements `utf8` and `float32`)

### Testing

- Unit tests in most modules via `#[cfg(test)]`
- Integration tests in `tests/loading.rs` (old stack pipeline) and `tests/api.rs` (HTTP endpoints)
- Parameterized dtype tests using `rstest`
- Test fixtures in `tests/fixtures/`
- Benchmarks: `table_bench` (10M rows), `api_bench` (Murr vs Redis comparison via `testcontainers`), `hashmap_bench`, `hashmap_row_bench`
