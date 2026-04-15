"""Pydantic models mirroring the Rust `murr::conf::Config`.

These are the full set of knobs an embedded Murr instance accepts.
Pass an instance to `Murr.start_local(config=...)` to override the
server listen addresses, HTTP payload cap, or cache dir; omit it and
the Rust defaults apply (HTTP 0.0.0.0:8080, gRPC 0.0.0.0:8081,
`cache_dir` auto-resolved).

The field layout mirrors the YAML accepted by the standalone binary
(`--config path.yaml`), so the same shape works in both worlds.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class HttpConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    # 1 GB — matches HttpConfig::default_max_payload_size in Rust.
    max_payload_size: int = 1024 * 1024 * 1024


class GrpcConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8081


class ServerConfig(BaseModel):
    http: HttpConfig = Field(default_factory=HttpConfig)
    grpc: GrpcConfig = Field(default_factory=GrpcConfig)


class StorageConfig(BaseModel):
    """Cache directory location. Leave `cache_dir` unset to inherit the
    auto-resolution cascade (cwd/murr → /var/lib/murr/murr → /data/murr
    → tmpdir/murr, picking the first writable option)."""

    cache_dir: str | None = None


class Config(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
