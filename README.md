# Murr

Columnar in-memory cache for AI inference workloads. Think Redis but made for batch low-latency reads and writes.

## Why Murr?

ML inference systems often need to fetch features for hundreds of documents in a single request. Current solutions struggle with this pattern:

**Redis** is the most common choice, but its data model creates latency problems. With HSET-based layouts (Feast-style), you end up with thousands of hash lookups per request - fetching 40 features across 200 documents means 8000 operations, resulting in 50-100ms latency even with pipelining. Blob-based approaches avoid this but make atomic updates painful.

**DynamoDB** handles batch reads well but costs scale with request volume, making it expensive for high-throughput inference workloads.

**Local RocksDB** offers excellent read performance but requires complex machinery to build, distribute, and mount database files to inference pods. When datasets grow, you're duplicating storage costs across every replica.

![system diagram](doc/img/overview.png)

Murr takes a different approach. It's a columnar cache designed specifically for batch ML workloads:

- **Pull-based data loading**: Point Murr at your Parquet files in S3 and it handles the rest. No ingestion pipelines, no upload coordination. Workers pull data from block storage on startup.
- **Columnar storage**: Data is stored in Arrow RecordBatches, optimized for "give me columns X, Y, Z for these N document IDs" access patterns. One lookup per document, not per feature.
- **Zero-copy serialization**: Arrow IPC format means data can be mapped directly to NumPy arrays or PyTorch tensors without re-encoding. This eliminates one of the main latency sources in large batch fetches.
- **Stateless workers**: Since data is pulled from S3, scaling up or down is trivial. New replicas become ready without complex data synchronization.

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
- Iceberg table support
