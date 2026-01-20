# Murr

[![CI Status](https://github.com/shuttie/murr/workflows/CI/badge.svg)](https://github.com/shuttie/murr/actions)
[![License: Apache 2](https://img.shields.io/badge/License-Apache2-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Last commit](https://img.shields.io/github/last-commit/shuttie/murr)
![Last release](https://img.shields.io/github/release/shuttie/murr)

Columnar in-memory cache for AI inference workloads. Think Redis but made for batch low-latency zero-copy reads directly into your `np.ndarray`.

## What is Murr?

![system diagram](doc/img/overview.png)

Murr is a caching layer for ML feature serving that sits between your batch data pipelines and inference services:

- **Parquet-native**: Reads directly from Parquet files in S3, local storage, or Iceberg catalogs (BigQuery, Snowflake)
- **Columnar queries**: Fetch specific columns for batches of keys in a single request
- **Zero-copy responses**: Arrow IPC RecordBatch maps directly to `np.ndarray` and `torch.Tensor` - no wire protocol decoding, no JSON parsing
- **Pull-based sync**: Workers poll S3 for new partitions and reload automatically
- **Stateless**: No primary/replica coordination, just point at S3 and scale horizontally
- **Single-binary**: Written in Rust, deploys as one binary with a YAML config

## Why Murr?

ML inference often requires fetching features for hundreds of documents per request. A ranking model scoring 200 candidates with 40 features each needs 8000 values in milliseconds. Existing solutions weren't built for this:

* **Redis** uses row-oriented storage. With Feast-style HSET layouts, each feature is a separate hash field, so fetching 40 features × 200 docs = 8000 hash lookups. Even with pipelining, this adds 50-100ms latency. Packing features into blobs helps reads but makes atomic updates complex.

* **DynamoDB** charges per request. High-throughput inference becomes expensive quickly.

* **Local RocksDB** is fast but operationally heavy. You need pipelines to build DB files, distribute them to pods, and coordinate reloads. Storage costs multiply with replica count.

Simple, fast, cheap - you can choose only two.

Murr is designed around the batch read pattern from the start. Data lives in columnar Arrow format, so "give me columns X, Y, Z for keys 1-200" is a single operation, not thousands. Workers pull Parquet files from S3 on startup - no ingestion pipelines, no coordination. Responses are Arrow IPC, which maps directly to NumPy arrays without re-encoding.

## Status

**Pre-alpha. Here be dragons.**

Murr is in early development. The configuration parsing and basic structure exist, but core functionality (data loading, query processing, API endpoints) is not yet implemented.

## Quick Start

Create a configuration file `murr.yml`:

```yaml
server:
  host: localhost
  port: 8080
  data_dir: /var/lib/murr

tables:
  user_features:
    source:
      s3:
        bucket: my-features
        prefix: user_features/
        region: us-east-1
    poll_interval: 5m
    parts: 8
    key: [user_id]
    columns:
      user_id:
        dtype: utf8
        nullable: false
      click_rate_7d:
        dtype: float32
        nullable: true
      purchase_count_30d:
        dtype: int32
        nullable: true
```

Run the server:

```bash
murr --config murr.yml
```

Query features (planned API):

```bash
# JSON format (for debugging)
curl -X POST http://localhost:8080/v1/user_features \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user_1", "user_2", "user_3"], "columns": ["click_rate_7d", "purchase_count_30d"]}'

# Arrow IPC format (for production)
curl -X POST http://localhost:8080/v1/user_features \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d '{"keys": ["user_1", "user_2", "user_3"], "columns": ["click_rate_7d", "purchase_count_30d"]}' \
  --output response.arrow
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

## Configuration Reference

```yaml
server:
  host: localhost        # Bind address (default: localhost)
  port: 8080             # HTTP port (default: 8080)
  data_dir: /var/lib/murr  # Directory for cached Arrow files

tables:
  <table_name>:
    source:
      # S3 source
      s3:
        bucket: my-bucket
        prefix: path/to/data/
        endpoint: https://custom-endpoint:9000  # Optional, for MinIO/etc
        region: us-east-1
      # Or local filesystem
      local:
        path: /data/features

    poll_interval: 5m    # How often to check for new data
    parts: 8             # Number of Arrow file partitions (default: 8)
    key: [id]            # Primary key column(s)

    columns:
      <column_name>:
        dtype: float32   # utf8, int16, int32, int64, uint16, uint32, uint64, float32, float64, bool
        nullable: true   # Whether nulls are allowed
```

### Data Layout Convention

Murr expects data in date-partitioned directories with a `_SUCCESS` marker:

```
s3://bucket/prefix/
  2026-01-19/
    part_0000.parquet
    part_0001.parquet
    _SUCCESS
  2026-01-20/
    part_0000.parquet
    part_0001.parquet
    _SUCCESS  <- Murr loads from the latest partition with _SUCCESS
```

## Roadmap

- Core data loading from S3/local Parquet files
- REST API with JSON and Arrow IPC responses
- Memory-mapped storage backend for larger-than-RAM datasets
- Python client library
- Prometheus metrics
- Iceberg catalog support (BigQuery, Snowflake, AWS Glue)

## License

Apache 2.0