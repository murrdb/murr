from __future__ import annotations

import os

import pyarrow as pa

from murr._base import parse_table_schemas, validate_and_convert_batch
from murr.config import Config
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
        cls,
        cache_dir: str | os.PathLike[str] | None = None,
        http_port: int | None = None,
        config: Config | None = None,
        serve_http: bool | None = None,
    ) -> MurrLocalSync:
        """Start an embedded local Murr instance backed by on-disk segment files.

        Two calling conventions:

          1. `Murr.start_local(cache_dir="/tmp/cache")` — legacy shorthand.
             Optionally pass `http_port=N` to also spawn the HTTP API on
             `127.0.0.1:N`.

          2. `Murr.start_local(config=Config(...))` — pass a full
             `murr.config.Config` (Pydantic) for every server and storage
             knob. Pass `serve_http=True` to additionally spawn the HTTP
             API on `config.server.http.host:port`.

        `cache_dir` and `config` are mutually exclusive; passing both
        raises `ValueError`. `http_port` belongs to the legacy path; to
        pick a port via `config`, set `config.server.http.port`.
        """
        dir_arg = str(cache_dir) if cache_dir is not None else None
        return MurrLocalSync(_MurrLocalSync(dir_arg, http_port, config, serve_http))

    @classmethod
    def connect(cls, endpoint: str) -> MurrClientSync:
        """Connect to a remote Murr server over HTTP."""
        from murr.http import MurrClientSync

        return MurrClientSync(endpoint)
