import socket

import pyarrow as pa

from murr import ColumnSchema, Config, DType, StorageConfig, TableSchema


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def local_config(cache_dir, http_port: int | None = None) -> Config:
    config = Config(storage=StorageConfig(cache_dir=str(cache_dir)))
    if http_port is not None:
        config.server.http.host = "127.0.0.1"
        config.server.http.port = http_port
    return config


def user_schema() -> TableSchema:
    return TableSchema(
        key="id",
        columns={
            "id": ColumnSchema(dtype=DType.UTF8, nullable=False),
            "score": ColumnSchema(dtype=DType.FLOAT32, nullable=True),
        },
    )


def user_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {"id": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]},
        schema=pa.schema([
            pa.field("id", pa.utf8(), nullable=False),
            pa.field("score", pa.float32(), nullable=True),
        ]),
    )
