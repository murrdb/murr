from __future__ import annotations

import os

import pyarrow as pa

from murr.libmurr import LocalMurr as _LocalMurr
from murr.schema import TableSchema


class LocalMurr:
    """Embedded local Murr instance backed by on-disk segment files."""

    def __init__(self, cache_dir: str | os.PathLike[str]) -> None:
        self._inner = _LocalMurr(str(cache_dir))

    def create_table(self, name: str, schema: TableSchema) -> None:
        self._inner.create_table(name, schema)

    def write(self, table_name: str, batch: pa.RecordBatch | pa.Table) -> None:
        if isinstance(batch, pa.Table):
            for rb in batch.to_batches():
                self._inner.write(table_name, rb)
        else:
            self._inner.write(table_name, batch)

    def read(
        self, table_name: str, keys: list[str], columns: list[str]
    ) -> pa.RecordBatch:
        return self._inner.read(table_name, keys, columns)

    def list_tables(self) -> dict[str, TableSchema]:
        raw: dict = self._inner.list_tables()
        return {
            name: TableSchema.model_validate(schema)
            for name, schema in raw.items()
        }

    def get_schema(self, table_name: str) -> TableSchema:
        raw = self._inner.get_schema(table_name)
        return TableSchema.model_validate(raw)
