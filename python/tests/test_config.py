"""Tests for the Config-based `Murr.start_local` path.

These cover the behaviours called out in issue #95 — the Python client
should accept a full `Config` (Pydantic) mirroring `murr::conf::Config`,
not just `cache_dir` and `http_port`.
"""

from __future__ import annotations

import time

import pyarrow as pa
import pytest

from murr import (
    ColumnSchema,
    Config,
    DType,
    GrpcConfig,
    HttpConfig,
    ServerConfig,
    StorageConfig,
    TableSchema,
)
from murr.aio import Murr as AsyncMurr
from murr.http import MurrClientSync
from murr.sync import Murr as SyncMurr

from conftest import free_port


def _user_schema() -> TableSchema:
    return TableSchema(
        key="user_id",
        columns={
            "user_id": ColumnSchema(dtype=DType.UTF8, nullable=False),
            "name": ColumnSchema(dtype=DType.UTF8, nullable=True),
        },
    )


def test_start_local_accepts_full_config(tmp_path):
    """A fully-specified Config should work end-to-end without any
    `cache_dir` argument — issue #95's main ask."""
    config = Config(storage=StorageConfig(cache_dir=str(tmp_path)))
    murr = SyncMurr.start_local(config=config)
    try:
        murr.create_table("users", _user_schema())
        schema = pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
            ]
        )
        batch = pa.RecordBatch.from_pydict(
            {"user_id": ["u1"], "name": ["Alice"]}, schema=schema
        )
        murr.write("users", batch)
        result = murr.read("users", ["u1"], ["name"])
        assert result.column("name").to_pylist() == ["Alice"]
    finally:
        del murr


def test_config_exposes_http_port_and_serves_http(tmp_path):
    """`config.server.http.port` is the config-mode equivalent of the
    legacy `http_port=` arg. `serve_http=True` opts in to spawning."""
    port = free_port()
    config = Config(
        server=ServerConfig(http=HttpConfig(host="127.0.0.1", port=port)),
        storage=StorageConfig(cache_dir=str(tmp_path)),
    )
    server = SyncMurr.start_local(config=config, serve_http=True)
    try:
        time.sleep(0.2)  # give axum a moment to bind
        client = MurrClientSync(f"http://127.0.0.1:{port}")
        try:
            client.create_table("users", _user_schema())
            assert "users" in client.list_tables()
        finally:
            client.close()
    finally:
        del server


def test_config_does_not_serve_http_by_default(tmp_path):
    """A Config without `serve_http=True` should NOT spawn the HTTP
    server — embedded callers don't need it."""
    port = free_port()
    config = Config(
        server=ServerConfig(http=HttpConfig(host="127.0.0.1", port=port)),
        storage=StorageConfig(cache_dir=str(tmp_path)),
    )
    server = SyncMurr.start_local(config=config)
    try:
        time.sleep(0.2)
        # Without serve_http=True, nothing should be listening.
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            with pytest.raises((ConnectionRefusedError, socket.timeout, OSError)):
                s.connect(("127.0.0.1", port))
    finally:
        del server


def test_cache_dir_and_config_are_mutually_exclusive(tmp_path):
    config = Config(storage=StorageConfig(cache_dir=str(tmp_path)))
    with pytest.raises(ValueError, match="mutually exclusive"):
        SyncMurr.start_local(cache_dir=str(tmp_path), config=config)


def test_http_port_with_config_rejected(tmp_path):
    config = Config(storage=StorageConfig(cache_dir=str(tmp_path)))
    with pytest.raises(ValueError, match="http_port cannot be combined with config"):
        SyncMurr.start_local(config=config, http_port=12345)


def test_missing_both_cache_dir_and_config_rejected():
    with pytest.raises(ValueError, match="either cache_dir or config"):
        SyncMurr.start_local()


def test_legacy_cache_dir_path_still_works(tmp_path):
    """Backward compat: the old `cache_dir=...` call shape must still
    produce a functional embedded client."""
    murr = SyncMurr.start_local(cache_dir=str(tmp_path))
    try:
        murr.create_table("users", _user_schema())
        assert "users" in murr.list_tables()
    finally:
        del murr


@pytest.mark.asyncio
async def test_async_start_local_with_config(tmp_path):
    config = Config(storage=StorageConfig(cache_dir=str(tmp_path)))
    murr = await AsyncMurr.start_local(config=config)
    try:
        await murr.create_table("users", _user_schema())
        tables = await murr.list_tables()
        assert "users" in tables
    finally:
        del murr


@pytest.mark.asyncio
async def test_async_cache_dir_and_config_mutually_exclusive(tmp_path):
    config = Config(storage=StorageConfig(cache_dir=str(tmp_path)))
    with pytest.raises(ValueError, match="mutually exclusive"):
        await AsyncMurr.start_local(cache_dir=str(tmp_path), config=config)


def test_config_defaults_match_rust():
    """Smoke test that the Pydantic defaults match the Rust defaults
    documented in `src/conf/server.rs`."""
    c = Config()
    assert c.server.http.host == "0.0.0.0"
    assert c.server.http.port == 8080
    assert c.server.http.max_payload_size == 1024 * 1024 * 1024
    assert c.server.grpc.host == "0.0.0.0"
    assert c.server.grpc.port == 8081
    assert c.storage.cache_dir is None


def test_grpc_config_exposed(tmp_path):
    """gRPC host/port are now configurable from Python (previously they
    were hard-coded to the Rust defaults)."""
    config = Config(
        server=ServerConfig(grpc=GrpcConfig(host="127.0.0.1", port=18081)),
        storage=StorageConfig(cache_dir=str(tmp_path)),
    )
    # We don't spin up a Flight server, but constructing a client with
    # custom gRPC settings must succeed and round-trip through PyO3.
    murr = SyncMurr.start_local(config=config)
    try:
        murr.create_table("users", _user_schema())
    finally:
        del murr
