from __future__ import annotations

import json

import httpx
import pyarrow as pa

from murr._base import batch_to_ipc, ipc_to_batch, parse_table_schemas, validate_and_convert_batch
from murr.schema import TableSchema

ARROW_IPC_MIME = "application/vnd.apache.arrow.stream"


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    try:
        msg = response.json()["error"]
    except Exception:
        msg = response.text or f"HTTP {response.status_code}"
    if response.status_code == 404:
        raise FileNotFoundError(msg)
    if response.status_code == 409:
        raise ValueError(msg)
    raise RuntimeError(msg)


class MurrClientSync:
    """Synchronous HTTP client for a remote Murr server."""

    def __init__(self, endpoint: str) -> None:
        self._client = httpx.Client(base_url=endpoint.rstrip("/"))

    def create_table(self, name: str, schema: TableSchema) -> None:
        resp = self._client.put(
            f"/api/v1/table/{name}",
            content=schema.model_dump_json(),
            headers={"content-type": "application/json"},
        )
        _raise_for_status(resp)

    def write(self, table_name: str, batch: pa.RecordBatch | pa.Table) -> None:
        for rb in validate_and_convert_batch(batch):
            resp = self._client.put(
                f"/api/v1/table/{table_name}/write",
                content=batch_to_ipc(rb),
                headers={"content-type": ARROW_IPC_MIME},
            )
            _raise_for_status(resp)

    def read(
        self, table_name: str, keys: list[str], columns: list[str]
    ) -> pa.RecordBatch:
        resp = self._client.post(
            f"/api/v1/table/{table_name}/fetch",
            content=json.dumps({"keys": keys, "columns": columns}),
            headers={"content-type": "application/json", "accept": ARROW_IPC_MIME},
        )
        _raise_for_status(resp)
        return ipc_to_batch(resp.content)

    def list_tables(self) -> dict[str, TableSchema]:
        resp = self._client.get("/api/v1/table")
        _raise_for_status(resp)
        return parse_table_schemas(resp.json())

    def get_schema(self, table_name: str) -> TableSchema:
        resp = self._client.get(f"/api/v1/table/{table_name}/schema")
        _raise_for_status(resp)
        return TableSchema.model_validate(resp.json())

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MurrClientSync:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class MurrClientAsync:
    """Asynchronous HTTP client for a remote Murr server."""

    def __init__(self, endpoint: str) -> None:
        self._client = httpx.AsyncClient(base_url=endpoint.rstrip("/"))

    async def create_table(self, name: str, schema: TableSchema) -> None:
        resp = await self._client.put(
            f"/api/v1/table/{name}",
            content=schema.model_dump_json(),
            headers={"content-type": "application/json"},
        )
        _raise_for_status(resp)

    async def write(self, table_name: str, batch: pa.RecordBatch | pa.Table) -> None:
        for rb in validate_and_convert_batch(batch):
            resp = await self._client.put(
                f"/api/v1/table/{table_name}/write",
                content=batch_to_ipc(rb),
                headers={"content-type": ARROW_IPC_MIME},
            )
            _raise_for_status(resp)

    async def read(
        self, table_name: str, keys: list[str], columns: list[str]
    ) -> pa.RecordBatch:
        resp = await self._client.post(
            f"/api/v1/table/{table_name}/fetch",
            content=json.dumps({"keys": keys, "columns": columns}),
            headers={"content-type": "application/json", "accept": ARROW_IPC_MIME},
        )
        _raise_for_status(resp)
        return ipc_to_batch(resp.content)

    async def list_tables(self) -> dict[str, TableSchema]:
        resp = await self._client.get("/api/v1/table")
        _raise_for_status(resp)
        return parse_table_schemas(resp.json())

    async def get_schema(self, table_name: str) -> TableSchema:
        resp = await self._client.get(f"/api/v1/table/{table_name}/schema")
        _raise_for_status(resp)
        return TableSchema.model_validate(resp.json())

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> MurrClientAsync:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
