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
```shell
# yes this works for batch writes
curl -d @0000.parquet -H "Content-Type: application/vnd.apache.parquet" \
  -XPUT http://localhost:8080/api/v1/table/yolo/write
```
- **Zero-copy wire protocol**: zero conversion when building `np.ndarray`, `pd.DataFrame` and `pt.Tensor` from API replies. Yes, Redis is fast, but parsing its responses is not (especially in Python!).
```python
result = db.read("docs", keys=["doc_1", "doc_3", "doc_5"], columns=["score", "category"])
print(result.to_pandas()) # look mom, zero copy!
```
- **Stateless**: Murr is not a database, all state is persisted on S3. When a Redis node gets restarted, you're cooked -- but Murr always self-bootstraps from block store.

Murr shines when:
* **your data is heavy and tabular**: giant parquet dump on S3 your AI inference/ML prep offline job produces is a perfect fit.
* **reads are batched**: pull 100 columns per 1000 documents your agent wants to analyze? Great!
* **you care about costs**: yes Redis with 1TB RAM will work well, but disk/S3 offload makes things operationally easier and cheaper.

Short quickstart:
```shell
uv pip install murrdb
```
and then
```python
from murr import MurrClient

db = MurrClient("http://localhost:8080") # connect to a running murr instance

# fetch columns for a batch of document keys
result = db.read("docs", keys=["doc_1", "doc_3", "doc_5"], columns=["score", "category"])
print(result.to_pandas())

# Output:
#    score category
# 0   0.95       ml
# 1   0.72    infra
# 2   0.68      ops
```

## Why Murr?

TLDR: You have latency, simplicity, costs -- choose only two. Murrdb tries to nail all three: the fastest, cheapest, and easiest to operate at once. A bold claim, I know.

![comparison with competitors](doc/img/compare.png)

For a use case of `read N datapoints over M documents` (agent reading document attributes, ML ranker fetching feature values), apart from being the fastest, Murrdb:
- vs **Redis**: is persistent (S3 is the new FS) and can offload cold data to local NVMe disk.
- vs embedded **RocksDB**: no need to build data sync between the producer job and inference nodes in-house. Murrdb was built to be distributed from the start.
- vs **DynamoDB**: just 10x cheaper, as you only pay per CPU/RAM and not per query. 

Not being a general-purpose database, it tries to be friendly to the PITAs of ML/AI engineers:
* **First-class Python support**: `pip install murrdb`, map to/from Numpy/Pandas/Polars/Pytorch arrays with zero copy.
* **Sparse columns**: when column has no data, it consumes zero bytes. Unlike packed feature blob approach, where null columns are not-actually-null.

## Why NOT Murr?

Murr is not a general-purpose database: 
* **OLTP workload**: When you have relations, transactions, and do per-row reads and writes, choose [Postgres](https://www.postgresql.org/)
* **Analytics**: You aggregate over a whole table to produce a report? Choose [Clickhouse](https://clickhouse.com/), [BigQuery](https://cloud.google.com/bigquery), or [Snowflake](https://www.snowflake.com/).
* **General-purpose caching**: You need to cache user session data for a web app? Use [Redis](https://redis.io/).

When making a choice, also note that Murr is in its early days and might not be stable enough for you. But it's quickly improving.

## Quickstart

```python
import pandas as pd
import pyarrow as pa
from murr import LocalMurr, TableSchema, ColumnSchema, DType

db = LocalMurr(cache_dir="/tmp/murr")

# define table schema
schema = TableSchema(
    key="doc_id", # the key
    columns={
        "doc_id": ColumnSchema(dtype=DType.UTF8, nullable=False),
        "score": ColumnSchema(dtype=DType.FLOAT32),
        "category": ColumnSchema(dtype=DType.UTF8),
    },
)
db.create_table("docs", schema)

# write a batch of documents
df = pd.DataFrame.from_dict({
    "doc_id":   ["doc_1", "doc_2", "doc_3", "doc_4", "doc_5"],
    "score":    [0.95, 0.87, 0.72, 0.91, 0.68],
    "category": ["ml", "search", "infra", "ml", "ops"],
})
db.write("docs", pa.Table.from_pandas(df))

# fetch specific columns for a few keys
result = db.read("docs", keys=["doc_1", "doc_3", "doc_5"], columns=["score", "category"])
print(result.to_pandas())

# Output:
#   score category
# 0   0.95       ml
# 1   0.72    infra
# 2   0.68      ops

```

## Benchmarks

We benchmark a typical `ML Ranking` use case, where you have an ML scoring model running across `N=1000` documents each having `M=10` `float32` feature values. Key distribution is random, we have a tiny 10M row dataset.

* for **murrdb** we model it as a simple table with a `utf8` key and 10 `float32` non-nullable columns. We measure Flight gRPC and HTTP protocols.
* for **Redis** with feature-blob approach, we pack all 10 per-document features into a 40-byte blob. So it's basically a key-value lookup using `MGET`, all 1000 keys at once. Efficient, but good luck adding a new column.
* for **Redis** with Feast-style approach, each document is a HSET, where the key is the feature name and the value is its value. Each feature can be read/written separately, but requires pipelining to get close to MGET performance.

We measure last-byte latency and don't include protocol parsing overhead yet.

| Approach | Latency (mean) | 95% CI | Throughput |
|----------|----------------|--------|------------|
| Murr (HTTP + Arrow IPC) | 104 µs | [103—104 µs] | 9.63 Mkeys/s |
| Murr (Flight gRPC) | 105 µs | [104—105 µs] | 9.53 Mkeys/s |
| Redis MGET (feature blobs) | 263 µs | [262—264 µs] | 3.80 Mkeys/s |
| Redis Feast (HSET per row) | 3.80 ms | [3.76—3.89 ms] | 263 Kkeys/s |

Murr is ~2.5x faster than the best Redis layout (MGET with packed blobs) and ~36x faster than Feast-style hash-per-row storage.

## Roadmap

No ETAs here, but at least you can see how far we are right now:
- [x] HTTP API
- [x] Arrow Flight gRPC API
- [x] API for data ingestion
- [x] Storage Directory interface (which is heavily inspired by [Apache Lucene](https://lucene.apache.org/))
- [x] Segment read/writes (again, inspired by [Apache Lucene](https://lucene.apache.org/))
- [x] Python embedded murrdb, so we can make a cool demo
- [x] Benchmarking harness: Redis support, Feast and feature-blob styles
- [x] Win at your own benchmark (this was surprisingly hard btw)
- [x] Support for `utf8` and `float32` datatypes
- [ ] Python remote API client
- [ ] Docker image
- [ ] Support most popular Arrow numerical types (signed/unsigned int 8/16/32/64, float 16/64, date-time)
- [ ] Array datatypes (e.g. Arrow `list`), so you can store embeddings
- [ ] Sparse columns
- [ ] Add RocksDB and Postgres to the benchmark harness
- [ ] Apache Iceberg and the very popular `parquet dump on S3` data catalog support


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

The footer-at-the-end layout follows the same pattern as our favourite: Lucene's compound file format.

### Column Types

Each column type has its own binary encoding for scatter-gather reads. BTW we tried to use Arrow for in-memory representation in the past and it was surprisingly slow compared to a hand-rolled implementation:

| Type | Status | Description |
|------|--------|-------------|
| `float32` | Implemented | 16-byte header, 8-byte aligned f32 payload, optional null bitmap |
| `utf8` | Implemented | 20-byte header, i32 value offsets, concatenated strings, optional null bitmap |
| `int16`, `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float64`, `bool` | Planned |   |

Null bitmaps use u64-word bit arrays (bit set = valid). Non-nullable columns skip bitmap checks entirely.

### REST API (port 8080)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/openapi.json` | OpenAPI spec |
| GET | `/api/v1/table` | List all tables with schemas |
| GET | `/api/v1/table/{name}/schema` | Get table schema |
| PUT | `/api/v1/table/{name}` | Create a table |
| POST | `/api/v1/table/{name}/fetch` | Read data (JSON or Arrow IPC response) |
| PUT | `/api/v1/table/{name}/write` | Write data (JSON, Parquet or Arrow IPC request) |

**Content negotiation**: Fetch responses use `Accept` header (`application/json` or `application/vnd.apache.arrow.stream`). Write requests use `Content-Type` header for the same formats.

### Arrow Flight gRPC API (port 8081)

A read-only [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) endpoint for native integration with Arrow-based data tools. 

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

## Development

```bash
cargo build                  # Build the project
cargo test                   # Run all tests
cargo check                  # Fast syntax/type check
cargo clippy                 # Linting
cargo fmt                    # Format code
cargo bench --bench <name>   # Run a benchmark (table_bench, http_bench, flight_bench, hashmap_bench, hashmap_row_bench, redis_feast_bench, redis_featureblob_bench)
```

## License

Apache 2.0
