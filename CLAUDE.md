# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Murr is a columnar in-memory cache for AI/ML inference workloads, written in Rust (edition 2024). It serves as a Redis replacement optimized for batch feature retrieval - fetching specific columns for batches of document keys in a single request.

**Key design goals:**
- Pull-based data sync: Workers poll S3/Iceberg for new Parquet partitions and reload automatically
- Zero-copy responses: Arrow IPC RecordBatch maps directly to `np.ndarray` and `torch.Tensor`
- Stateless: No primary/replica coordination, horizontal scaling by pointing workers at S3
- Columnar storage: Optimized for "give me columns X, Y, Z for keys 1-200" access patterns

**Status:** Pre-alpha. Configuration parsing works, core functionality (data loading, query processing, API) not yet implemented.

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

The codebase is organized into three main modules:

**`core/`** - Core types and error handling
- `error.rs` - Defines `MurrError` enum using `thiserror` for structured error handling (ConfigParsingError, IoError)
- `args.rs` - CLI argument parsing using `clap` with optional `--config` flag
- `logger.rs` - Logging setup using `env_logger`

**`conf/`** - Configuration management (YAML format)
- `config.rs` - Main `Config` struct that deserializes from YAML using the `config` crate
- `server.rs` - `ServerConfig` struct with host (default: "localhost"), port (default: 8080), data_dir (default: "/var/lib/murr")
- `table.rs` - Table configuration: `TableConfig`, `SourceConfig` (S3/local), `ColumnConfig`, `DType` enum
- Configuration uses `#[serde(deny_unknown_fields)]` to strictly validate YAML structure
- `Config::from_str()`, `Config::from_file()`, `Config::from_args()` methods for loading configuration

**Entry points**
- `main.rs` - Binary entry point, currently minimal with logging initialization
- `lib.rs` - Library root (currently minimal)

### Error Handling

All custom errors use the `MurrError` enum defined in `src/core/error.rs`. Errors are constructed using `thiserror` for clean error messages. Configuration parsing errors are wrapped as `ConfigParsingError(String)`.

### Dependencies
- `config` - Configuration file parsing (YAML format)
- `serde` - Serialization/deserialization with derive features
- `thiserror` - Error type derivation
- `anyhow` - Error handling utilities
- `log` + `env_logger` - Logging
- `clap` - CLI argument parsing
- `humantime-serde` - Human-readable duration parsing (e.g., "5m", "1h")

### Configuration Format

Configuration uses YAML. Example:
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
    poll_interval: 5m
    parts: 8
    key: [user_id]
    columns:
      user_id:
        dtype: utf8
        nullable: false
      click_rate:
        dtype: float32
        nullable: true
```

Supported data types: `utf8`, `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `bool`
