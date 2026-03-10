from __future__ import annotations

import pyarrow as pa

from murr.schema import TableSchema


def validate_and_convert_batch(batch: pa.RecordBatch | pa.Table) -> list[pa.RecordBatch]:
    if isinstance(batch, pa.Table):
        return batch.to_batches()
    return [batch]


def parse_table_schemas(raw: dict) -> dict[str, TableSchema]:
    return {name: TableSchema.model_validate(schema) for name, schema in raw.items()}


def batch_to_ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def ipc_to_batch(data: bytes) -> pa.RecordBatch:
    reader = pa.ipc.open_stream(data)
    return reader.read_next_batch()
