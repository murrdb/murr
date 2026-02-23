# API Layer Design

## Architecture

`MurrApi` (`src/api/mod.rs`) owns an `Arc<MurrService>` and builds an Axum `Router`. It does not leak Axum types into its public interface beyond the `Router` (used for testing via `tower::ServiceExt::oneshot()`).

**Module structure:**
- `mod.rs` — `MurrApi` struct: `new()`, `router()`, `serve()`
- `handlers.rs` — one free function per route, all receive `State<Arc<MurrService>>`
- `convert.rs` — `FetchResponse` (batch→JSON) and `WriteRequest` (JSON→batch) conversions
- `error.rs` — `ApiError` newtype mapping `MurrError` → HTTP status codes

## Routes

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| GET | `/openapi.json` | `openapi` | OpenAPI spec as JSON |
| GET | `/health` | `health` | Returns `"OK"` |
| GET | `/api/v1/table` | `list_tables` | JSON map of table name → schema |
| GET | `/api/v1/table/{name}` | `get_table` | JSON schema for one table |
| PUT | `/api/v1/table/{name}` | `create_table` | Accepts `TableSchema` JSON, returns 201 |
| POST | `/api/v1/table/{name}/fetch` | `fetch` | Read data (content negotiation on response) |
| PUT | `/api/v1/table/{name}/write` | `write_table` | Write data (content negotiation on request) |

## Content Negotiation

**Fetch (read):** Request is always JSON `{"keys": [...], "columns": [...]}`. Response format is determined by `Accept` header:
- `application/json` (default) → `{"columns": {"col1": [v1, v2], ...}}`
- `application/vnd.apache.arrow.stream` → Arrow IPC stream bytes

**Write:** Response is always `200 OK`. Request format is determined by `Content-Type` header:
- `application/json` (default) → `{"columns": {"col1": [v1, v2], ...}}` (needs table schema to know column types)
- `application/vnd.apache.arrow.stream` → Arrow IPC stream bytes (self-describing, no schema lookup needed)

## RecordBatch ↔ JSON Conversion (`convert.rs`)

**Batch → JSON:** `FetchResponse` newtype wrapping `serde_json::Value`. Uses `TryFrom<&RecordBatch>` with `downcast_ref` on concrete Arrow array types (`Float32Array`, `StringArray`). Errors on unsupported types.

**JSON → Batch:** `WriteRequest` struct with `into_record_batch(self, schema: &TableSchema)`. Requires the `TableSchema` because JSON values alone don't encode the Arrow type. Iterates over `schema.columns` to determine field types and builds the corresponding Arrow arrays.

Why not `From`/`Into`: Both conversions are fallible (unsupported types), so `TryFrom` and an explicit method returning `Result` are used instead.

Why a newtype: Rust orphan rule prevents implementing `TryFrom<&RecordBatch> for serde_json::Value` directly since neither type is local.

## Error Mapping (`error.rs`)

`ApiError(MurrError)` implements `IntoResponse`:

| MurrError pattern | HTTP Status |
|---|---|
| `TableError` containing "not found" | 404 Not Found |
| `TableError` containing "already exists" | 409 Conflict |
| `TableError` / `SegmentError` (other) | 400 Bad Request |
| `IoError` / `ArrowError` / `ConfigParsingError` | 500 Internal Server Error |

Response body: `{"error": "<message>"}`.

## Design Decisions

**Why Axum state instead of handler closures:** `Arc<MurrService>` as router state is the idiomatic Axum pattern. Handlers are plain `async fn` with `State` extractors — no closures, no trait objects. This makes handlers independently testable and the router composable.

**Why `Bytes` for write_table instead of `Json`:** The `write_table` handler accepts raw `Bytes` because it needs to dispatch on `Content-Type` before parsing. Using `Json<WriteRequest>` would force JSON parsing before we check the header. With `Bytes`, we first check `Content-Type`, then either deserialize JSON or decode Arrow IPC.

**Why `Column: Send + Sync` bound:** Added to the `Column` trait in `io/table/column/mod.rs` to make `MurrService` (which contains `Box<dyn Column>` via `CachedTable`) shareable across Axum's async task pool. The concrete implementations (`Float32Column`, `Utf8Column`) only hold `&[u8]` references to mmap'd data, which are inherently `Send + Sync`.

## OpenAPI Spec

The API schema lives in `openapi.yaml` (OpenAPI 3.1, YAML for human editability). It is embedded at compile time via `include_str!` and parsed once into JSON via `serde_yaml_ng` using `std::sync::LazyLock`. Served at `GET /openapi.json`.

## Testing

- Unit tests in `convert.rs`: direct conversion + round-trip tests for both directions
- E2E tests in `tests/api_test.rs`: uses `tower::ServiceExt::oneshot()` against the `Router` — no real TCP server needed. Covers full create→write(JSON)→write(Arrow)→fetch(JSON)→fetch(Arrow) flow, plus OpenAPI endpoint validation.
