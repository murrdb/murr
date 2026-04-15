from __future__ import annotations

import os

import pyarrow as pa

from murr._base import parse_table_schemas, validate_and_convert_batch
from murr.config import Config
from murr.libmurr import MurrLocalAsync as _MurrLocalAsync
from murr.schema import TableSchema


class MurrLocalAsync:
    """Asynchronous local Murr instance backed by on-disk segment files."""

    def __init__(self, _inner: _MurrLocalAsync) -> None:
        self._inner = _inner

    async def create_table(self, name: str, schema: TableSchema) -> None:
        await self._inner.create_table(name, schema)

    async def write(self, table_name: str, batch: pa.RecordBatch | pa.Table) -> None:
        for rb in validate_and_convert_batch(batch):
            await self._inner.write(table_name, rb)

    async def read(
        self, table_name: str, keys: list[str], columns: list[str]
    ) -> pa.RecordBatch:
        return await self._inner.read(table_name, keys, columns)

    async def list_tables(self) -> dict[str, TableSchema]:
        raw = await self._inner.list_tables()
        return parse_table_schemas(raw)

    async def get_schema(self, table_name: str) -> TableSchema:
        raw = await self._inner.get_schema(table_name)
        return TableSchema.model_validate(raw)


class Murr:
    """Factory for creating Murr client instances."""

    @classmethod
    async def start_local(
        cls,
        cache_dir: str | os.PathLike[str] | None = None,
        http_port: int | None = None,
        config: Config | None = None,
        serve_http: bool | None = None,
    ) -> MurrLocalAsync:
        """Start an embedded local Murr instance backed by on-disk segment files.

        Two calling conventions:

          1. `await Murr.start_local(cache_dir="/tmp/cache")` — legacy
             shorthand. Optionally `http_port=N` to spawn the HTTP API
             on `127.0.0.1:N`.

          2. `await Murr.start_local(config=Config(...))` — pass a full
             `murr.config.Config`. Pass `serve_http=True` to additionally
             spawn the HTTP API on `config.server.http.host:port`.

        `cache_dir` and `config` are mutually exclusive; passing both
        raises `ValueError`. `http_port` belongs to the legacy path; to
        pick a port via `config`, set `config.server.http.port`.
        """
        dir_arg = str(cache_dir) if cache_dir is not None else None
        inner = await _MurrLocalAsync.create(dir_arg, http_port, config, serve_http)
        return MurrLocalAsync(inner)

    @classmethod
    async def connect(cls, endpoint: str) -> MurrClientAsync:
        """Connect to a remote Murr server over HTTP."""
        from murr.http import MurrClientAsync

        return MurrClientAsync(endpoint)
