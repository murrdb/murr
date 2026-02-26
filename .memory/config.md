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
  cache_dir: /custom/path  # auto-resolved if omitted
```

Structs: `Config` → `ServerConfig` (has `HttpConfig`, `GrpcConfig`) + `StorageConfig`.

## Config Loading

`Config::from_args(CliArgs)` uses the `config` crate builder:
1. Optional YAML file via `--config` CLI arg
2. Environment variable overrides with `MURR_` prefix, `_` separator (e.g. `MURR_SERVER_HTTP_PORT=9090`)
3. Missing values filled by serde `#[serde(default)]` annotations

No clap-to-config bridging — env vars cover the same use case without duplication.

## cache_dir Auto-Resolution

`StorageConfig::default_cache_dir()` resolves at config construction time. Fallback chain:
1. `<cwd>/murr` — if cwd is writable
2. `/var/lib/murr/murr` — for service/deb deployments
3. `/data/murr` — for Docker
4. `<tmpdir>/murr` — last resort

Writability checked via temp file probe. Returns `Result<PathBuf, MurrError::ConfigParsingError>` with details on failure.

## Wiring

- `MurrService::new(Config)` stores the full config, uses `config.storage.cache_dir` as `data_dir`
- `MurrService::config()` accessor exposes config to API layers
- `MurrHttpService::serve()` / `MurrFlightService::serve()` take no args — read listen address from `service.config().server.http/grpc.addr()`
- `main.rs`: `parse args → load config → log config → create service → serve`

## Test Pattern

Tests use a helper that creates a `Config` with `StorageConfig { cache_dir: tempdir.path() }` and default server config. The `TempDir` must be kept alive for the test duration (dropped = deleted).
