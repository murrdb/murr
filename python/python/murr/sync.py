from __future__ import annotations

import os

import pyarrow as pa

from murr._base import parse_table_schemas, validate_and_convert_batch
from murr.libmurr import MurrLocalSync as _MurrLocalSync
from murr.schema import TableSchema


class MurrLocalSync:
    """Synchronous local Murr instance backed by on-disk segment files."""

    def __init__(self, _inner: _MurrLocalSync) -> None:
        self._inner = _inner

    def create_table(self, name: str, schema: TableSchema) -> None:
        self._inner.create_table(name, schema)

    def write(self, table_name: str, batch: pa.RecordBatch | pa.Table) -> None:
        for rb in validate_and_convert_batch(batch):
            self._inner.write(table_name, rb)

    def read(
        self, table_name: str, keys: list[str], columns: list[str]
    ) -> pa.RecordBatch:
        return self._inner.read(table_name, keys, columns)

    def list_tables(self) -> dict[str, TableSchema]:
        return parse_table_schemas(self._inner.list_tables())

    def get_schema(self, table_name: str) -> TableSchema:
        return TableSchema.model_validate(self._inner.get_schema(table_name))


class Murr:
    """Factory for creating Murr client instances."""

    @classmethod
    def start_local(
        cls, cache_dir: str | os.PathLike[str], http_port: int | None = None
    ) -> MurrLocalSync:
        """Start an embedded local Murr instance backed by on-disk segment files.

        Args:
            cache_dir: Path to the on-disk cache directory.
            http_port: If set, starts the HTTP API on this port (bound to 127.0.0.1).
        """
        return MurrLocalSync(_MurrLocalSync(str(cache_dir), http_port))

    @classmethod
    def connect(cls, endpoint: str) -> MurrClientSync:
        """Connect to a remote Murr server over HTTP."""
        from murr.http import MurrClientSync

        return MurrClientSync(endpoint)
