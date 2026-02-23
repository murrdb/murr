# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Note to agents like Claude Code

The project uses .memory directory as an append-only log of architectural decisions made while developing:
* before doing planning, read the .memory directory for relevant topics discussed/implemented in the past
* when a plan has an architectural decision which can be important context in the future, always include a point to append the summary and reasoning (why are we making it and why not something else) for the change.
* update .memory only for important bits of information.

## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval — fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Custom binary segment format with memory-mapped reads
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. The codebase uses a custom binary `.seg` format (`src/io/`) with `MurrService` (`src/service/`) wrapping the storage layer. An Axum HTTP API (`src/api/`) is wired up in `main.rs`, serving on `0.0.0.0:8080`. Only `Float32` and `Utf8` column types are implemented so far.

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

### Module Structure

**`io/segment/`** — Custom binary `.seg` format
- `format.rs` — Wire format: `[MURR magic][version u32 LE][column payloads (4-byte aligned)][footer entries][footer_size u32 LE]`
- `write.rs` — `WriteSegment` builder: `add_column(name, bytes)` then `write(w)`
- `read.rs` — `Segment::open(path)` memory-maps file, validates magic+version, parses footer, provides `column(name) -> Option<&[u8]>` zero-copy access

**`io/directory/`** — Storage directory abstraction
- `Directory` trait with `index()` (returns `IndexInfo`: schema + segment list) and `write()` methods
- `LocalDirectory` reads `table.json` (schema) + scans `*.seg` files

**`io/table/`** — Table layer built on segments
- `writer.rs` — `TableWriter` creates `table.json` and writes `{id:08}.seg` files from `RecordBatch`
- `reader.rs` — `TableReader` builds key index (`AHashMap<String, KeyOffset>`) across segments; last segment wins for duplicate keys
- `view.rs` — `TableView` opens all segment files, holds `Vec<Segment>`
- `cached.rs` — `CachedTable` uses `ouroboros` self-referential struct to own `TableView` + borrow `TableReader`
- `table.rs` — Legacy Arrow IPC `Table` type (still used by benchmarks, not part of new storage path)

**`io/table/column/`** — Per-dtype column implementations
- `Column` trait: `get_indexes(&[KeyOffset]) -> Arc<dyn Array>`, `get_all()`, `size()`
- `ColumnSegment` trait: `parse(name, config, data)`, `write(config, array) -> Vec<u8>`
- `float32/` — `Float32Column` with 16-byte segment header, 8-byte aligned payload, optional null bitmap
- `utf8/` — `Utf8Column` with 20-byte segment header, i32 value offsets, concatenated strings, optional null bitmap
- `bitmap.rs` — `NullBitmap` using u64-word bit array (bit set = valid)

**`service/`** — High-level service wrapping the storage layer
- `MurrService` — `RwLock<HashMap<String, TableState>>` table registry
- `create(table_name, schema)` → `write(table_name, batch)` → `read(table_name, keys, columns)` flow
- `state.rs` — `TableState` holds `LocalDirectory`, `TableSchema`, `Option<CachedTable>`

**`api/`** — Axum HTTP API layer
- `mod.rs` — `MurrApi` struct: `new()`, `router()`, `serve()`
- `handlers.rs` — Route handlers with `State<Arc<MurrService>>` extractors
- `convert.rs` — `FetchResponse` (batch→JSON) and `WriteRequest` (JSON→batch) conversions
- `error.rs` — `ApiError` newtype mapping `MurrError` → HTTP status codes
- Content negotiation: fetch supports JSON or Arrow IPC response (`Accept` header); write supports JSON or Arrow IPC request (`Content-Type` header)

**`core/`** — Error types (`MurrError` with `thiserror`, variants: `ConfigParsingError`, `IoError`, `ArrowError`, `TableError`, `SegmentError`), CLI args (`clap`), logging (`env_logger`), schema types (`DType`, `ColumnConfig`, `TableSchema`)

**`conf/`** — YAML configuration: `Config`, `ServerConfig`. Uses `#[serde(deny_unknown_fields)]` for strict validation.

**`testutil.rs`** — Feature-gated (`testutil`) test helpers: `generate_parquet_file()`, `setup_test_table()`, `setup_benchmark_table()`, `bench_generate_keys()`

### Key Design Patterns

- **Self-referential structs**: `CachedTable` uses `ouroboros` to own a `TableView` while borrowing from it in `TableReader`
- **`AHashMap`** used in `TableReader` for faster hashing than std `HashMap`
- **`bytemuck`** for zero-copy casting of segment headers
- **`memmap2`** for memory-mapped segment reads
- **Feature-gated test utilities**: `testutil` feature enables `tempfile` + `rand` deps for test/bench helpers

### Configuration Format

```yaml
server:
  host: localhost    # default: localhost
  port: 8080        # default: 8080
  data_dir: /var/lib/murr  # default: /var/lib/murr
```

Tables are created at runtime via the API (`PUT /api/v1/table/{name}`) with a `TableSchema` JSON body specifying `key`, and `columns` (each with `dtype` and optional `nullable`).

Supported dtypes: `utf8`, `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `bool`
(Currently only `utf8` and `float32` are implemented in the segment column layer)

### Testing

- Unit tests in most modules via `#[cfg(test)]` (including inline tests in `service/mod.rs`, `convert.rs`)
- E2E API tests in `tests/api_test.rs` using `tower::ServiceExt::oneshot()` against the router (no TCP server needed)
- Parameterized dtype tests using `rstest`
- Test fixtures in `tests/fixtures/`
- Benchmarks: `table_bench` (10M rows), `api_bench` (Murr vs Redis comparison via `testcontainers`), `hashmap_bench`, `hashmap_row_bench`
