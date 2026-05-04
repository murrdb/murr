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
  backend:
    type: mmap        # or: mem
    cache_dir: /custom/path   # mmap-only; auto-resolved if omitted
```

Structs: `Config` → `ServerConfig` (has `HttpConfig`, `GrpcConfig`) + `StorageConfig`. `StorageConfig` wraps a `BackendConfig` enum (tagged on `type`) whose variants are the IO-layer config structs themselves: `BackendConfig::Mmap(MMapConfig)` and `BackendConfig::Mem(MemConfig)`.

## Config Loading

`Config::from_args(CliArgs)` uses the `config` crate builder:
1. Optional YAML file via `--config` CLI arg
2. Environment variable overrides with `MURR_` prefix, `_` separator (e.g. `MURR_SERVER_HTTP_PORT=9090`, `MURR_STORAGE_BACKEND__CACHE__DIR=/path`)
3. Missing values filled by serde `#[serde(default)]` annotations

No clap-to-config bridging — env vars cover the same use case without duplication.

## Backend Config

The `BackendConfig` enum lives in `src/conf/storage.rs` and wraps the IO-layer configs directly — there is intentionally no separate "user-facing" vs "IO-layer" config struct. `MMapConfig` (defined in `src/io/directory/mmap/directory.rs`) is `Serialize + Deserialize + Default + Clone` and used as-is in YAML. This means `conf/` depends on `io/`, which is a one-way dependency (io/ does not import conf/).

Why one struct instead of two: avoids duplicating fields between the user config layer and the IO layer. The IO `Directory` trait takes `Self::ConfigType` directly — same struct the user wrote in YAML. Adding fields to a backend (e.g. a future `S3Config { bucket, prefix, endpoint }`) is a single-place change.

## cache_dir Auto-Resolution

`MMapConfig::default_cache_dir()` resolves at config construction time. Fallback chain:
1. `<cwd>/murr` — if cwd is writable
2. `/var/lib/murr/murr` — for service/deb deployments
3. `/data/murr` — for Docker
4. `<tmpdir>/murr` — last resort

Writability checked via temp file probe. Returns `Result<PathBuf, MurrError::ConfigParsingError>` with details on failure. The function lives in `io/directory/mmap/directory.rs` since it's MMap-specific filesystem logic — moved there from `conf/storage.rs` when `MMapConfig` absorbed `cache_dir`.

## Wiring

- `MurrService::new(Config)` stores the full config and matches on `config.storage.backend` to construct concrete `Table<D>` instances
- `MurrService::config()` accessor exposes config to API layers
- `MurrHttpService::serve()` / `MurrFlightService::serve()` take no args — read listen address from `service.config().server.http/grpc.addr()`
- `main.rs`: `parse args → load config → log config → create service → serve`

## Test Pattern

Tests use a helper that creates a `Config` with `StorageConfig { backend: BackendConfig::Mmap(MMapConfig::new(tempdir.path())) }` and default server config. The `TempDir` must be kept alive for the test duration (dropped = deleted).
