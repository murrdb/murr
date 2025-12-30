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

The project currently has a simple structure:
- `src/main.rs` - Binary entry point
- `src/lib.rs` - Library code (currently minimal)
- `src/core/mod.rs` - Core error types (MurrError enum)
- `src/conf/mod.rs` - Configuration module with TOML-based config parsing (work in progress)

The project uses Rust edition 2024, which requires a recent nightly or stable Rust toolchain that supports this edition.

### Dependencies
- `config` (0.15.19) - Configuration management
- `serde` (1.0.228) - Serialization/deserialization with derive features
