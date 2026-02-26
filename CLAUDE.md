# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Note to agents like Claude Code

The project uses .memory directory as an append-only log of architectural decisions made while developing:
* before doing planning, read the .memory directory for relevant topics discussed/implemented in the past
* when a plan has an architectural decision which can be important context in the future, always include a point to append the summary and reasoning (why are we making it and why not something else) for the change.
* update .memory only for important bits of information.

### Build notes

* when you change dependencies, do a `cargo clean` to purge old cache to save on disk space.
* we have also benches which are excluded from `cargo check`, so always do `cargo check --all-targets` to validate that the codebase is clean

## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval — fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Custom binary segment format with memory-mapped reads
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. The codebase uses a custom binary `.seg` format (`src/io/`) with `MurrService` (`src/service/`) wrapping the storage layer. Two API layers serve concurrently: Axum HTTP and Arrow Flight gRPC (via `tokio::try_join!` in `main.rs`), with listen addresses driven by config. Only `Float32` and `Utf8` column types are implemented so far.

## Common Commands

```bash
cargo build                  # Build the project
cargo test                   # Run all tests
cargo test <name>            # Run specific test by name
cargo check                  # Fast syntax/type check without codegen
cargo clippy                 # Linting
cargo fmt                    # Format code
cargo bench --bench <name>   # Run a specific benchmark (table_bench, http_bench, flight_bench, hashmap_bench, hashmap_row_bench, redis_feast_bench, redis_featureblob_bench)
```

### Python bindings

```bash
cd python
uv venv .venv --python 3.14  # One-time venv setup
source .venv/bin/activate
uv pip install maturin pytest pyarrow pydantic
maturin develop              # Build and install in dev mode
pytest tests/ -v             # Run Python tests
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
- `MurrService` — Owns `Config`, holds `RwLock<HashMap<String, TableState>>` table registry; constructor takes `Config` (not a path)
- `create(table_name, schema)` → `write(table_name, batch)` → `read(table_name, keys, columns)` flow
- `config()` accessor exposes config to API layers (serve methods read listen addresses from it)
- `state.rs` — `TableState` holds `LocalDirectory`, `TableSchema`, `Option<CachedTable>`

**`api/http/`** — Axum HTTP API layer
- `mod.rs` — `MurrHttpService` struct: `new()`, `router()`, `serve()` (reads listen addr from config)
- `handlers.rs` — Route handlers with `State<Arc<MurrService>>` extractors
- `convert.rs` — `FetchResponse` (batch→JSON) and `WriteRequest` (JSON→batch) conversions
- `error.rs` — `ApiError` newtype mapping `MurrError` → HTTP status codes
- Content negotiation: fetch supports JSON or Arrow IPC response (`Accept` header); write supports JSON or Arrow IPC request (`Content-Type` header)

**`api/flight/`** — Arrow Flight gRPC layer (read-only)
- `mod.rs` — `MurrFlightService` implementing `FlightService` trait via tonic
- `ticket.rs` — `FetchTicket { table, keys, columns }` JSON-encoded ticket format
- `error.rs` — `MurrError` → `tonic::Status` conversion
- Implemented RPCs: `do_get` (fetch by keys+columns), `get_flight_info`, `get_schema`, `list_flights`
- All write RPCs (`do_put`, `do_exchange`, `do_action`) return `Unimplemented`

**`core/`** — Error types (`MurrError` with `thiserror`, variants: `ConfigParsingError`, `IoError`, `ArrowError`, `TableError`, `SegmentError`), CLI args (`clap`), logging (`env_logger`), schema types (`DType`, `ColumnSchema`, `TableSchema`)

**`conf/`** — Hierarchical configuration loaded via `Config::from_args(&CliArgs)`:
- `config.rs` — `Config` struct with `server` + `storage` fields; loads from optional YAML file (`--config`) then env vars (`MURR_` prefix, `_` separator)
- `server.rs` — `ServerConfig` containing `HttpConfig` (default `0.0.0.0:8080`) and `GrpcConfig` (default `0.0.0.0:8081`), each with `addr()` method
- `storage.rs` — `StorageConfig` with `cache_dir` auto-resolution: tries `<cwd>/murr` → `/var/lib/murr/murr` → `/data/murr` → `<tmpdir>/murr`, picking first writable location
- All config structs use `#[serde(deny_unknown_fields)]` for strict validation

**`testutil.rs`** — Feature-gated (`testutil`) test helpers: `generate_parquet_file()`, `setup_test_table()`, `setup_benchmark_table()`, `bench_generate_keys()`

**`python/`** — PyO3/maturin Python bindings (workspace member `murr-python`, PyPI package `murr`)
- `src/lib.rs` — `PyLocalMurr` pyclass wrapping `MurrService` with owned tokio `Runtime` for sync API
- `src/error.rs` — `MurrError` → Python exception mapping (`into_py_err`)
- `python/murr/schema.py` — Pydantic v2 models: `DType`, `ColumnSchema`, `TableSchema`
- `python/murr/client.py` — `LocalMurr` wrapper: Pydantic validation + JSON bridge to Rust
- Arrow RecordBatch passed zero-copy via Arrow C Data Interface (`arrow::pyarrow`)
- Schema passed as JSON strings between Python (Pydantic `model_dump_json`) and Rust (`serde_json`)

### Key Design Patterns

- **Self-referential structs**: `CachedTable` uses `ouroboros` to own a `TableView` while borrowing from it in `TableReader`
- **`AHashMap`** used in `TableReader` for faster hashing than std `HashMap`
- **`bytemuck`** for zero-copy casting of segment headers
- **`memmap2`** for memory-mapped segment reads
- **Feature-gated test utilities**: `testutil` feature enables `tempfile` + `rand` deps for test/bench helpers

### Configuration Format

Config is loaded from an optional YAML file (`--config path.yaml`) overlaid with environment variables (`MURR_` prefix, `_` separator, e.g. `MURR_SERVER_HTTP_PORT=9090`).

```yaml
server:
  http:
    host: "0.0.0.0"    # default: 0.0.0.0
    port: 8080          # default: 8080
  grpc:
    host: "0.0.0.0"    # default: 0.0.0.0
    port: 8081          # default: 8081
storage:
  cache_dir: /custom/path  # default: auto-resolved (see conf/storage.rs)
```

Tables are created at runtime via the API (`PUT /api/v1/table/{name}`) with a `TableSchema` JSON body specifying `key`, and `columns` (each with `dtype` and optional `nullable`).

Supported dtypes: `utf8`, `float32`

### Testing

- Unit tests in most modules via `#[cfg(test)]` (including inline tests in `service/mod.rs`, `convert.rs`)
- E2E API tests in `tests/api_test.rs` using `tower::ServiceExt::oneshot()` against the router (no TCP server needed)
- Parameterized dtype tests using `rstest`
- Test fixtures in `tests/fixtures/`
- Benchmarks: `table_bench` (10M rows), `http_bench` and `flight_bench` (Murr HTTP/Flight vs Redis comparison via `testcontainers`), `hashmap_bench`, `hashmap_row_bench`, `redis_feast_bench`, `redis_featureblob_bench`
