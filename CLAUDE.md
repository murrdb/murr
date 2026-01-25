# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval - fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Arrow IPC RecordBatch maps directly to `np.ndarray` and `torch.Tensor`
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. Core data pipeline (discovery, parquet conversion, in-memory tables) is implemented. REST API layer not yet implemented.

## Common Commands

### Building
```bash
cargo build           # Build the project
cargo build --release # Build with optimizations
```

### Running
```bash
cargo run             # Build and run the binary
```

### Testing
```bash
cargo test            # Run all tests
cargo test <name>     # Run specific test
```

### Code Quality
```bash
cargo check           # Fast syntax/type check without codegen
cargo clippy          # Linting
cargo fmt             # Format code
```

## Architecture

The project uses Rust edition 2024, which requires a recent nightly or stable Rust toolchain that supports this edition.

### Module Structure

**`core/`** - Core types and error handling
- `error.rs` - `MurrError` enum with variants for config, IO, Arrow, Parquet, table, discovery errors
- `args.rs` - CLI argument parsing using `clap` with optional `--config` flag
- `logger.rs` - Logging setup using `env_logger`

**`conf/`** - Configuration management (YAML format)
- `config.rs` - Main `Config` struct with `from_str()`, `from_file()`, `from_args()` methods
- `server.rs` - `ServerConfig` (host: "localhost", port: 8080, data_dir: "/var/lib/murr")
- `table.rs` - `TableConfig`, `SourceConfig` (S3/local), `ColumnConfig`, `DType` enum
- `S3SourceConfig` supports optional `endpoint` for S3-compatible stores (MinIO, LocalStack)
- Uses `#[serde(deny_unknown_fields)]` for strict YAML validation

**`discovery/`** - Partition detection and Parquet file enumeration
- `partition.rs` - Date parsing (YYYY-MM-DD), partition finding, path filtering
- `store.rs` - Factory functions for LocalFileSystem and S3 ObjectStore creation
- `discovery.rs` - `Discovery` trait, `ObjectStoreDiscovery` implementation
- Finds latest partition with `_SUCCESS` marker, returns list of `.parquet` files

**`parquet/`** - Parquet-to-Arrow IPC conversion
- `convert.rs` - `convert_parquet_to_ipc()` streams N Parquet files to single Arrow IPC file
- `schema.rs` - `validate_schema()` checks Parquet schema against `TableConfig`
- Validates column existence, type matching, nullability constraints

**`table/`** - In-memory columnar table implementation
- `table.rs` - `Table` struct with memory-mapped Arrow IPC via `memmap2`
- `KeyIndex: HashMap<String, (u32, u32)>` for O(1) key lookups (batch_index, row_offset)
- `Table::open()` memory-maps IPC file, reads footer, loads batches, builds key index
- `Table::get(keys, columns)` uses Arrow's `take()` for zero-copy column retrieval

**`manager/`** - Table orchestration and state management
- `manager.rs` - `TableManager` with `RwLock<HashMap<String, TableState>>` for thread-safe table registry
- `loader.rs` - `TableLoader` orchestrates discovery → convert → load pipeline
- `state.rs` - `TableState` holds `Arc<Table>`, partition_date, ipc_path

**`api/`** - REST API (not yet implemented)

### Error Handling

`MurrError` enum in `src/core/error.rs` with `thiserror`. Variants:
- `ConfigParsingError`, `IoError`, `ArrowError`, `TableError`
- `ParquetError`, `ObjectStoreError`, `DiscoveryError`, `NoValidPartition`

Implements `From` for seamless error propagation from Arrow, Parquet, object_store, std::io.

### Dependencies
- `arrow` (v57) + `parquet` (v57) - Columnar data processing with IPC and async features
- `object_store` (v0.12) - Unified S3/local storage abstraction
- `config` + `serde` - YAML configuration parsing
- `tokio` + `tokio-stream` + `async-trait` - Async runtime with streaming support
- `thiserror` - Error type derivation
- `memmap2` - Memory mapping for zero-copy file access
- `clap` - CLI argument parsing
- `humantime-serde` - Human-readable duration parsing (e.g., "5m")
- `chrono` - Date parsing for partition detection
- `bytes` - Efficient byte buffer handling

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

  # Local filesystem source example
  local_features:
    source:
      local:
        path: /data/features
    key: [doc_id]
    columns: {}  # empty columns = skip schema validation
```

Supported data types: `utf8`, `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `bool`

### Data Layout Convention

Murr expects date-partitioned directories with `_SUCCESS` markers:
```
s3://bucket/prefix/
  2024-01-13/
    part_0000.parquet
    _SUCCESS
  2024-01-14/
    part_0000.parquet
    part_0001.parquet
    _SUCCESS  <- Loads from latest partition with _SUCCESS
```

### Data Flow

**Loading Pipeline:**
```
main.rs spawns per-table discovery loops
  → TableLoader::discover() finds latest partition with _SUCCESS
  → TableLoader::load() converts Parquet → Arrow IPC, opens Table
  → TableManager::insert() stores TableState
  → Loop sleeps for poll_interval, repeats
```

**Query Flow:**
```
Table::get(keys, columns)
  → KeyIndex lookup: O(1) per key → (batch_index, row_offset)
  → Group keys by batch
  → Arrow take() for zero-copy column selection per batch
  → concat_batches() → single RecordBatch in query order
```

### Testing

- Unit tests in each module via `#[cfg(test)]`
- Integration tests in `tests/loading.rs` covering full pipeline
- Parameterized dtype tests using `rstest` for all 10 supported types
- Test fixtures in `tests/fixtures/` for discovery scenarios
