# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Note to agents like Claude Code

The project uses .memory directory as an append-only log of architectural decisions made while developing:
* before doing planning, read the .memory directory for relevant topics discussed/implemented in the past
* when a plan has an architectural decision which can be important context in the future, always include a point to append the summary and reasoning (why are we making it and why not something else) for the change.
* update .memory only for important bits of information. 
* change-remove obsolete sections if the impl drifts too far from the original design.

### Tests

* prefer functional tests using the public module API, avoid testing internal implementation
* always test happy path
* for potential failure paths consider the possibility of such failure to happen. If the failure is highly unlikely, it's ok not to make test for it.
* do not test for obvious things.

### Benchmarks

* when changing benchmark code, do not run them as is - they might take some time to finish. instead ask user to run them with a specific command.

### Code comments

* do not comment obvious things, prefer make the code to look self-explanatory.
* if there's a gotcha or some non-obvious algorithm, add a one-line comment explaining the reasoning.

### GH PR descriptions

* after you finish implementing a big feature (e.g. using a plan mode), write a 2-3 paragraph comment explaining the reasoning.
* the comment should be in raw markdown so I can copy-paste it as is. do not over-use formatting.
* do not go too deep on implementation details, focus on why things were changed and user-visible changes.
* prefer human language, no AI slop please. Also do not use em-dashes.

### Build notes

* when you change dependencies, do a `cargo clean` to purge old cache to save on disk space.
* we have also benches which are excluded from `cargo check`, so always do `cargo check --all-targets` to validate that the codebase is clean
* if you see a potentially pre-existing issue, never mess with git history, do not do `git stash` unless explicitly asked to.

## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval — fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Custom binary segment format with memory-mapped reads
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. The codebase is a RocksDB-backed columnar KV store: `src/io/` wraps RocksDB (PlainTable or BlockBasedTable) with an Arrow-aware `Table` layer; `src/service/MurrService` holds one `RocksDBStore` and many `Table<RocksDBStore>` entries (one CF per table). Two API layers serve concurrently: Axum HTTP and Arrow Flight gRPC (via `tokio::try_join!` in `main.rs`), with listen addresses driven by config. Only `Float32`, `Float64`, and `Utf8` column types are implemented so far.

## Common Commands

```bash
cargo build                  # Build the project
cargo test                   # Run all tests
cargo test <name>            # Run specific test by name
cargo check                  # Fast syntax/type check without codegen
cargo clippy                 # Linting
cargo fmt                    # Format code
cargo bench --bench <name>   # Run a specific benchmark (multi_segment_index_bench)
```

Python bindings live in a separate repo: [shuttie/murr-python](https://github.com/shuttie/murr-python).

## Architecture

### Module Structure

**`io/`** — RocksDB-backed storage layer
- `store/mod.rs` — `Store` trait (multi-table KV) + `Manifest` sidecar for per-table `TableSchema`
- `store/rocksdb/` — `RocksDBStore` with two SST profiles: `open_plain` (PlainTable + mmap, in-memory hash point lookups) and `open_block` (BlockBasedTable, on-disk index + optional bloom). One DB, one CF per table.
- `store/memory.rs` — `MemoryStore` for tests
- `schema.rs` — `SegmentSchema` (non-key columns + offsets/bitset indices), derived from `TableSchema`
- `row/{read,write}.rs` — `ReadRow` / `WriteRow` byte-level row codec: `[null_bitset][static columns][dynamic payloads]`
- `column/` — `ColumnEncoder` / `ColumnDecoder` traits with `encoder_for(col, n)` / `decoder_for(col, arr)` factories; `PrimitiveEncoder<T>` (Float32/Float64) and `Utf8Encoder`
- `table/mod.rs` — `Table<S: Store>` glue between Arrow `RecordBatch` and the byte-level row format; `read(keys, columns)` and `write(batch)` both take `&self`
- `fs/` — experimental S3/local Filesystem trait stub (unused today)

**`service/`** — High-level service wrapping the storage layer
- `MurrService` — Owns `Config`, holds `tokio::sync::RwLock<HashMap<String, Table<RocksDBStore>>>` and a shared `Arc<std::sync::RwLock<RocksDBStore>>`; constructor takes `Config` (not a path)
- `create(table_name, schema)` → `write(table_name, batch)` → `read(table_name, keys, columns)` flow
- `config()` accessor exposes config to API layers (serve methods read listen addresses from it)
- Startup rehydration: walks `store.manifest().tables` and opens a `Table` per entry; missing manifest entries → CF is invisible to the service

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

**`core/`** — Error types (`MurrError` with `thiserror`, variants: `ConfigParsingError`, `IoError`, `ArrowError`, `TableNotFound`, `TableAlreadyExists`, `TableError`, `SegmentError`), CLI args (`clap`), logging (`env_logger`), schema types (`DType`, `ColumnSchema`, `TableSchema`)

**`conf/`** — Hierarchical configuration loaded via `Config::from_args(&CliArgs)`:
- `config.rs` — `Config` struct with `server` + `storage` fields; loads from optional YAML file (`--config`) then env vars (`MURR_` prefix, `_` separator)
- `server.rs` — `ServerConfig` containing `HttpConfig` (default `0.0.0.0:8080`) and `GrpcConfig` (default `0.0.0.0:8081`), each with `addr()` method
- `storage.rs` — `StorageConfig { path, backend }` where `backend` is a flattened `BackendConfig::Mmap(PlainConfig) | Block(BlockConfig)` — the inner configs are the `io::store::rocksdb::*` tunables themselves
- `path.rs` — `resolve_cache_dir()` auto-resolution: tries `<cwd>/murr` → `/var/lib/murr/murr` → `/data/murr` → `<tmpdir>/murr`, picking first writable location

**`util/`** — Miscellaneous utilities (`logo.rs` — ASCII art banner)

**`testutil.rs`** — Feature-gated (`testutil`) test helpers: `generate_parquet_file()`, `setup_test_table()`, `setup_benchmark_table()`, `bench_generate_keys()`

### Key Design Patterns

- **Keys are lookup-only**: `Table::read(keys, columns)` rejects requests for the key column — the row blob excludes the key, callers already have it in `keys`
- **`Arc<RwLock<RocksDBStore>>` shared by all tables**: outer `tokio::RwLock` over the table registry, inner `std::RwLock` over the store. Concurrent reads/writes on different tables run in parallel; same-table serialisation happens at the store lock
- **`bytemuck`** for zero-copy casting of fixed-width column values inside row blobs
- **Manifest sidecar (`manifest.json`)** is the source of truth for which CFs are known to the service — CFs without a manifest entry stay invisible
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
  path: /var/lib/murr   # default: auto-resolved (see conf/path.rs)
  mmap: {}              # or `block: {}` — pick exactly one; inner keys are RocksDB tunables
```

Tables are created at runtime via the API (`PUT /api/v1/table/{name}`) with a `TableSchema` JSON body specifying `key`, and `columns` (each with `dtype` and optional `nullable`).

Supported dtypes: `utf8`, `float32`, `float64`

### Testing

- Unit tests in most modules via `#[cfg(test)]` (including inline tests in `service/mod.rs`, `convert.rs`)
- E2E HTTP tests in `tests/api_test.rs` using `tower::ServiceExt::oneshot()` against the router (no TCP server needed)
- E2E Flight gRPC tests in `tests/flight_test.rs`
- Parameterized dtype tests using `rstest`
- Test fixtures in `tests/fixtures/`
- Benchmarks: `multi_segment_index_bench` (segment-accumulating writes), `row_vs_col_bench` (MemoryStore read throughput)
