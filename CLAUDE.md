# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Murr is a Rust project using edition 2024. This is a new project in early development with minimal code structure.

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
- `error.rs` - Defines `MurrError` enum using `thiserror` for structured error handling
- Currently includes `ConfigParsingError` variant for configuration-related errors

**`conf/`** - Configuration management
- `config.rs` - Main `Config` struct that deserializes from TOML using the `config` crate
- `server.rs` - `Server` configuration struct with host (default: "localhost") and port (default: 8080)
- Configuration uses `#[serde(deny_unknown_fields)]` to strictly validate TOML structure
- `Config::from_str()` method parses TOML strings and returns `Result<Config, MurrError>`

**Entry points**
- `main.rs` - Binary entry point, currently minimal with logging initialization
- `lib.rs` - Library root (currently minimal)

### Error Handling

All custom errors use the `MurrError` enum defined in `src/core/error.rs`. Errors are constructed using `thiserror` for clean error messages. Configuration parsing errors are wrapped as `ConfigParsingError(String)`.

### Dependencies
- `config` (0.15.19) - Configuration file parsing and management
- `serde` (1.0.228) - Serialization/deserialization with derive features
- `thiserror` (2.0.17) - Error type derivation
- `anyhow` (1.0.100) - Error handling utilities
- `log` (0.4.29) - Logging facade with key-value support
- `simplelog` (0.12.2) - Simple logging implementation
