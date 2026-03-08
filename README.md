# Murr

[![CI Status](https://github.com/shuttie/murr/workflows/CI/badge.svg)](https://github.com/shuttie/murr/actions)
[![License: Apache 2](https://img.shields.io/badge/License-Apache2-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Last commit](https://img.shields.io/github/last-commit/shuttie/murr)
![Last release](https://img.shields.io/github/release/shuttie/murr)

Columnar in-memory cache for AI inference workloads. A faster Redis/RocksDB replacement optimized for batch low-latency zero-copy reads and writes.

> This `README.md` is 100% human written.

## What is Murr?

![system diagram](doc/img/overview.png)

Murr is a caching layer for ML/AI data serving that sits between your batch data pipelines and inference apps:

- **Tiered storage**: hot data in memory, cold data on disk with S3-based replication. It's 2026, RAM is expensive, keep only hot data there.
- **Batch-in and batch-out**: native batch reads and writes from columnar storage, no per-row overhead. Dumping 1GB Parquet/Arrow files to an ingestion API is a valid usage scenario.
- **Zero-copy wire protocol**: zero conversion when building `np.ndarray`, `pd.DataFrame` and `pt.Tensor` from API replies. Yes, Redis is fast, but parsing its responses is not (especially in Python!).
- **Stateless**: Murr is not a database, all state is persisted on S3. When Redis node gets down, you're cooked - but Murr always bootstraps from S3.

Murr shines when:
* **your data is heavy and tabular**: giant parquet dump on S3 your AI inference/ML prep offline job produces is a perfect fit.
* **reads are batched**: pull 100 columns per 1000 documents your agent wants to analyze? Great!
* **you care about costs**: yes Redis with 1TB RAM will work well, but disk/D3 offload makes things operationally easier and cheaper.

Three-line quickstart:
```shell
uv pip install murrdb
```

```python
TODO
```

## Why Murr?

TLDR: You have latency, simplicity, costs -- choose only two. Murrdb tries to do all three: it's the fastest, cheapest and easiest to operate at once. A bold claim, I know.

![comparison with competitors](doc/img/compare.png)

For a use case of `read N datapoints over M documents` (agent reading document attributes, ML ranker fetching feature values), apart from being the fastest, Murrdb:
- vs **Redis**: is persistent (S3 is the new FS) and can offload cold data to local NVMe disk.
- vs embedded **RocksDB**: no need to build data sync between producer job and inference nodes in-house. Murrdb was built being distributed from start.
- vs **DynamoDB**: just 10x cheaper, as you only pay per CPU/RAM and not per query. 

Being designed not as a general-purpose database, it tries to be friendly to the PITAs of ML/AI engineers:
* **First-class Python support**: `pip install murrdb`, map to/from Numpy/Pandas/Polars/Pytorch arrays with zero copy.
* **Sparse columns**: when column has no data, it consumes zero bytes. Unlike packed feature blob approach, where null columns are not-actually-null.

## Why NOT Murr?

Murr is not a general-purpose database: 
* **OLTP workload**: When you have relations, transactions and do per-row reads and writes, choose [Postgres](todo)
* **Analytics**: You aggregate over whole table to produce a report? Choose [Clickhouse](todo), [Bigquery](todo) or [Snowflake](todo).
* **General-purpose caching**: You need to cache user session data for a web app? Use (Redis)[todo].

## Quickstart

TODO

### Benchmarks

Batch feature lookup: 10M rows, 10 Float32 columns, fetching 1000 random keys per request.

- **Murr HTTP**: full HTTP round-trip returning Arrow IPC streaming response
- **Murr Flight**: Arrow Flight gRPC round-trip
- **Redis MGET**: all columns packed into a single binary blob per key, fetched with pipelined MGET
- **Redis Feast**: one HSET per key with a field per column (standard Feast layout), fetched with pipelined HGETALL

| Approach | Latency (mean) | 95% CI | Throughput |
|----------|----------------|--------|------------|
| Murr (HTTP + Arrow IPC) | 104 µs | [103—104 µs] | 9.63 Mkeys/s |
| Murr (Flight gRPC) | 105 µs | [104—105 µs] | 9.53 Mkeys/s |
| Redis MGET (feature blobs) | 263 µs | [262—264 µs] | 3.80 Mkeys/s |
| Redis Feast (HSET per row) | 3.80 ms | [3.76—3.89 ms] | 263 Kkeys/s |

Murr is ~2.5x faster than the best Redis layout (MGET with packed blobs) and ~36x faster than Feast-style hash-per-row storage.


## Status

**Pre-alpha. Here be dragons.**

The storage engine, service layer, REST API, and Arrow Flight gRPC API are implemented and working. But only god knows how well.

## Architecture

### Storage Engine

The storage subsystem is a custom columnar format inspired by Apache Lucene's immutable segment model:

- **Segments** (`.seg` files) are the atomic unit of write — one batch of data becomes one immutable segment. Segments are never modified in place, which simplifies concurrency and maps naturally to object storage.
- **Directory abstraction** decouples logical data organization from physical storage (local filesystem now, S3 later).
- **Memory-mapped reads** via `memmap2` — the OS manages page caching, segment data is accessed as zero-copy byte slices.
- **Last-write-wins** key resolution: newer segments shadow older ones for the same key, enabling incremental updates without rewriting history.

Segment wire format:
```
[MURR magic (4B)][version u32 LE]
[column payloads, 4-byte aligned]
[footer entries: name_len|name|offset|size per column]
[footer_size u32 LE]
```

The footer-at-the-end layout (reader seeks to end first to find metadata) follows the same pattern as Lucene's compound file format.

### Column Types

Each column type has its own binary encoding optimized for scatter-gather reads:

| Type | Status | Description |
|------|--------|-------------|
| `float32` | Implemented | 16-byte header, 8-byte aligned f32 payload, optional null bitmap |
| `utf8` | Implemented | 20-byte header, i32 value offsets, concatenated strings, optional null bitmap |
| `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float64`, `bool` | Planned | Declared in config schema |

Null bitmaps use u64-word bit arrays (bit set = valid). Non-nullable columns skip bitmap checks entirely — benchmarks showed this returns performance to the no-nulls baseline.

### REST API (port 8080)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/openapi.json` | OpenAPI spec |
| GET | `/api/v1/table` | List all tables with schemas |
| GET | `/api/v1/table/{name}/schema` | Get table schema |
| PUT | `/api/v1/table/{name}` | Create a table |
| POST | `/api/v1/table/{name}/fetch` | Read data (JSON or Arrow IPC response) |
| PUT | `/api/v1/table/{name}/write` | Write data (JSON or Arrow IPC request) |

**Content negotiation**: Fetch responses use `Accept` header (`application/json` or `application/vnd.apache.arrow.stream`). Write requests use `Content-Type` header for the same formats.

### Arrow Flight gRPC API (port 8081)

A read-only [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) endpoint for native integration with Arrow-based data tools. Both APIs run concurrently via `tokio::try_join!`.

| RPC | Description |
|-----|-------------|
| `do_get` | Fetch rows by keys and columns (JSON-encoded `FetchTicket`) |
| `get_flight_info` | Get table schema and metadata |
| `get_schema` | Get schema in Arrow IPC format |
| `list_flights` | List all available tables |

Ticket format for `do_get`:
```json
{"table": "user_features", "keys": ["user_1", "user_2"], "columns": ["click_rate_7d"]}
```

### Performance

At 10M rows, 10 Float32 columns, 1000 random key lookups:

- Key index lookup: ~2-3us (AHash + string comparison in AHashMap)
- Value gather per column: ~3-4us (random memory access into mmapped segments)
- Null bitmap overhead: near-zero for non-nullable columns
- Total scatter-gather: ~30-40us

Key optimizations discovered through systematic benchmarking:
- **AHashMap** (AES-NI based) reduced hashing overhead from 26% to ~2% of query time
- **Two-loop gather** (values then bitmap separately) preserves `.collect()`/`extend_trusted` compiler optimization — fusing into one loop forces `push()` which is significantly slower
- **Branch-once null checking**: check nullability at segment level, not per element
- **`bytemuck`** for zero-copy casting of segment headers

## Quick Start

Query features:

```bash
# JSON format (for debugging)
curl -X POST http://localhost:8080/api/v1/table/user_features/fetch \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user_1", "user_2", "user_3"], "columns": ["click_rate_7d", "purchase_count_30d"]}'

# Arrow IPC format (for production)
curl -X POST http://localhost:8080/api/v1/table/user_features/fetch \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d '{"keys": ["user_1", "user_2", "user_3"], "columns": ["click_rate_7d", "purchase_count_30d"]}' \
  --output response.arrow
```

Arrow Flight gRPC (for Arrow-native clients):

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://localhost:8081")
ticket = json.dumps({"table": "user_features", "keys": ["user_1", "user_2", "user_3"], "columns": ["click_rate_7d", "purchase_count_30d"]})
reader = client.do_get(flight.Ticket(ticket.encode()))
table = reader.read_all()
```

Python client (planned):

```python
from murr import Client

client = Client("http://localhost:8080")
batch = client.get("user_features",
    keys=["user_1", "user_2", "user_3"],
    columns=["click_rate_7d", "purchase_count_30d"]
)

# Direct conversion to numpy/torch
features = batch.to_numpy()  # dict[str, np.ndarray]
tensor = batch.to_torch()    # dict[str, torch.Tensor]
```

## Development

```bash
cargo build                  # Build the project
cargo test                   # Run all tests
cargo check                  # Fast syntax/type check
cargo clippy                 # Linting
cargo fmt                    # Format code
cargo bench --bench <name>   # Run a benchmark (table_bench, http_bench, flight_bench, hashmap_bench, hashmap_row_bench, redis_feast_bench, redis_featureblob_bench)
```

## Roadmap

- Data loading from S3/local Parquet files
- Additional column types (int32, int64, float64, bool, etc.)
- Memory-mapped storage backend for larger-than-RAM datasets
- Python client library
- Prometheus metrics
- Iceberg catalog support (BigQuery, Snowflake, AWS Glue)

## License

Apache 2.0
