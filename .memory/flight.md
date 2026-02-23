# Arrow Flight Protocol Support

## Decision
Added read-only Arrow Flight protocol alongside existing HTTP/Axum API.

## Architecture
- `api/http/` — HTTP layer (renamed from `api/`, `MurrApi` → `MurrHttpService`)
- `api/flight/` — Flight gRPC layer (`MurrFlightService` wrapping tonic server)
- Both share `Arc<MurrService>` and run concurrently via `tokio::select!`
- Flight on port 8081 (configurable via `ServerConfig.flight_port`), HTTP on 8080

## Flight Methods Implemented
- `do_get` — fetch by keys+columns (core use case)
- `get_flight_info` / `get_schema` — schema discovery by table name
- `list_flights` — list all tables
- All write methods return `Unimplemented`

## Ticket Format
JSON-encoded `FetchTicket { table, keys, columns }` in `Ticket.ticket` bytes.
Chose JSON over custom protobuf for simplicity and language-agnostic construction.

## Schema Conversion
`From<&DType> for DataType` and `From<&TableSchema> for Schema` in `core/schema.rs` — reusable across layers.

## Key Dependencies
- `arrow-flight = "58"` (matches `arrow = "58"`)
- `tonic = "0.14"` (pulled by arrow-flight 58)
- `prost = "0.13"`, `futures = "0.3"`
