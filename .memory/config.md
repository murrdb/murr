# Configuration System

## Structure

Config is defined in `src/conf/` with nested structs:

```yaml
server:
  http:
    host: "0.0.0.0"  # default
    port: 8080        # default
  grpc:
    host: "0.0.0.0"  # default
    port: 8081        # default
storage:
  path: /var/lib/murr   # auto-resolved if omitted
  mmap: {}              # or `block: {}` — pick exactly one
```

Structs: `Config` → `ServerConfig` (has `HttpConfig`, `GrpcConfig`) + `StorageConfig`. `StorageConfig` carries `path: PathBuf` plus a flattened `BackendConfig` enum whose variants are the `io` layer's RocksDB tunable structs: `BackendConfig::Mmap(PlainConfig)` and `BackendConfig::Block(BlockConfig)` (both defined under `src/io/store/rocksdb/`).

## Config Loading

`Config::from_args(CliArgs)` uses the `config` crate builder:
1. Optional YAML file via `--config` CLI arg
2. Environment variable overrides with `MURR_` prefix, `_` separator (e.g. `MURR_SERVER_HTTP_PORT=9090`, `MURR_STORAGE_PATH=/path`)
3. Missing values filled by serde `#[serde(default)]` annotations

No clap-to-config bridging — env vars cover the same use case without duplication.

## Backend Config

`BackendConfig` is an externally-tagged enum flattened into `StorageConfig`, so YAML callers write `mmap: {…}` / `block: {…}` at the same level as `path`. The `io` layer's `PlainConfig` / `BlockConfig` are `Serialize + Deserialize + Default + Clone` and used as-is — no separate "user-facing" vs "io-layer" config struct. This means `conf/` depends on `io/`, which is a one-way dependency (`io/` does not import `conf/`).

Why one struct instead of two: avoids duplicating fields between the user config layer and the IO layer. Adding a tunable to a backend (e.g. a future S3 backend config) is a single-place change.

**`deny_unknown_fields` is intentionally absent on `StorageConfig`** because `#[serde(flatten)]` over an externally-tagged enum forwards every unknown field to the enum, which conflicts with strict-mode parsing. Inner `PlainConfig` / `BlockConfig` keep field-level strictness independently.

## cache_dir Auto-Resolution

`StorageConfig.path` defaults via `conf::path::resolve_cache_dir()`. Fallback chain:
1. `<cwd>/murr` — if cwd is writable
2. `/var/lib/murr/murr` — for service/deb deployments
3. `/data/murr` — for Docker
4. `<tmpdir>/murr` — last resort

Writability checked via temp file probe. Returns `Result<PathBuf, MurrError::ConfigParsingError>` with details on failure. The helper lives in `src/conf/path.rs`.

## Wiring

- `MurrService::new(Config)` stores the full config, opens one `RocksDBStore` (plain or block based on `config.storage.backend`), and rehydrates `Table<RocksDBStore>` entries from `store.manifest().tables`
- `MurrService::config()` accessor exposes config to API layers
- `MurrHttpService::serve()` / `MurrFlightService::serve()` take no args — read listen address from `service.config().server.http/grpc.addr()`
- `main.rs`: `parse args → load config → log config → create service → serve`

## Test Pattern

Tests use a helper that creates a `Config` with `StorageConfig { path: tempdir.path().to_path_buf(), backend: BackendConfig::Mmap(PlainConfig::default()) }` and default server config. The `TempDir` must be kept alive for the test duration (dropped = deleted).
