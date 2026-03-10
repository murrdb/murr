# Python HTTP Client

## Decision
Pure-Python HTTP clients (`MurrClientSync`, `MurrClientAsync`) in `python/murr/http.py` using `httpx` for both sync and async.

## Why httpx
- Single dependency for both sync (`httpx.Client`) and async (`httpx.AsyncClient`)
- Built-in connection pooling and keep-alive
- Considered aiohttp+urllib3 but rejected: two deps, different APIs, more complexity

## Wire format
- **Arrow IPC** for `read()`/`write()` — binary, fast, self-describing
- **JSON** for schema operations (`create_table`, `list_tables`, `get_schema`) — small payloads

## Error mapping
HTTP status codes mapped to Python exceptions matching the local client convention:
- 404 → `FileNotFoundError`
- 409 → `ValueError`
- other 4xx/5xx → `RuntimeError`

Note: server error body `{"error": msg}` contains just the inner message (e.g. table name), not the full "table not found: X" message that the Rust PyO3 client produces.

## Factory integration
`Murr.connect(endpoint)` in both `sync.py` and `aio.py` creates the HTTP client. Lazy import from `murr.http` to avoid requiring httpx when using only local clients.

## Files
- `python/murr/http.py` — `MurrClientSync`, `MurrClientAsync`, `_raise_for_status`
- `python/murr/_base.py` — `batch_to_ipc()`, `ipc_to_batch()` Arrow IPC helpers
- `python/tests/test_sync_http.py`, `python/tests/test_aio_http.py` — integration tests
