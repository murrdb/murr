import csv
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import requests

REPO_ROOT = Path(__file__).parent.parent.parent
CSV_PATH = REPO_ROOT / "tests" / "fixtures" / "anime_info.csv"
BINARY = REPO_ROOT / "target" / "debug" / "murr"

FLOAT_COLUMNS = [
    "is_tv",
    "year_aired",
    "is_adult",
    "above_five_star_users",
    "above_five_star_ratings",
    "above_five_star_ratio",
]

TABLE_SCHEMA = {
    "key": "anime_id",
    "columns": {
        "anime_id": {"dtype": "utf8", "nullable": False},
        "Genres": {"dtype": "utf8", "nullable": True},
        "is_tv": {"dtype": "float32", "nullable": True},
        "year_aired": {"dtype": "float32", "nullable": True},
        "is_adult": {"dtype": "float32", "nullable": True},
        "above_five_star_users": {"dtype": "float32", "nullable": True},
        "above_five_star_ratings": {"dtype": "float32", "nullable": True},
        "above_five_star_ratio": {"dtype": "float32", "nullable": True},
    },
}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def load_csv() -> dict:
    """Return {anime_id: {"genres": str|None, "floats": {col: float|None}}}."""
    rows = {}
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for record in reader:
            anime_id = record["anime_id"]
            genres = record["Genres"] or None
            floats = {}
            for col in FLOAT_COLUMNS:
                raw = record[col]
                floats[col] = float(raw) if raw else None
            rows[anime_id] = {"genres": genres, "floats": floats}
    return rows


def csv_to_arrow(csv_data: dict) -> pa.RecordBatch:
    keys = sorted(csv_data.keys())
    key_arr = pa.array(keys, type=pa.utf8())
    genres_arr = pa.array([csv_data[k]["genres"] for k in keys], type=pa.utf8())
    arrays = [key_arr, genres_arr]
    fields = [
        pa.field("anime_id", pa.utf8(), nullable=False),
        pa.field("Genres", pa.utf8(), nullable=True),
    ]
    for col in FLOAT_COLUMNS:
        arr = pa.array([csv_data[k]["floats"][col] for k in keys], type=pa.float32())
        arrays.append(arr)
        fields.append(pa.field(col, pa.float32(), nullable=True))
    schema = pa.schema(fields)
    return pa.record_batch(arrays, schema=schema)


def _wait_for_http(url: str, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            requests.get(url, timeout=1)
            return
        except requests.exceptions.ConnectionError:
            time.sleep(0.2)
    raise RuntimeError(f"murr did not start within {timeout}s")


@pytest.fixture(scope="session")
def murr_server():
    """Start murr, load the anime table, yield base_url + csv_data, then stop."""
    http_port = _free_port()
    grpc_port = _free_port()

    with tempfile.TemporaryDirectory() as store_dir:
        config_path = os.path.join(store_dir, "murr.yaml")
        with open(config_path, "w") as f:
            f.write(f"""\
server:
  http:
    host: "127.0.0.1"
    port: {http_port}
  grpc:
    host: "127.0.0.1"
    port: {grpc_port}
storage:
  path: {store_dir}
  mmap: {{}}
""")
        proc = subprocess.Popen(
            [str(BINARY), "--config", config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        base_url = f"http://127.0.0.1:{http_port}"
        try:
            _wait_for_http(f"{base_url}/health")

            # Create table
            resp = requests.put(
                f"{base_url}/api/v1/table/anime",
                json=TABLE_SCHEMA,
                timeout=10,
            )
            assert resp.status_code == 201, f"create table failed: {resp.text}"

            # Write data as Arrow IPC
            csv_data = load_csv()
            batch = csv_to_arrow(csv_data)
            sink = pa.BufferOutputStream()
            writer = ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            body = sink.getvalue().to_pybytes()

            resp = requests.put(
                f"{base_url}/api/v1/table/anime/write",
                data=body,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                timeout=30,
            )
            assert resp.status_code == 200, f"write failed: {resp.text}"

            yield base_url, csv_data
        finally:
            proc.terminate()
            proc.wait(timeout=10)
