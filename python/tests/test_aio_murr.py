import pyarrow as pa
import pytest
import pytest_asyncio

from murr import ColumnSchema, DType, TableSchema
from murr.aio import Murr


@pytest_asyncio.fixture
async def murr_client(tmp_path):
    return await Murr.start_local(cache_dir=str(tmp_path))


def _user_schema() -> TableSchema:
    return TableSchema(
        key="id",
        columns={
            "id": ColumnSchema(dtype=DType.UTF8, nullable=False),
            "score": ColumnSchema(dtype=DType.FLOAT32, nullable=True),
        },
    )


def _user_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {"id": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]},
        schema=pa.schema([
            pa.field("id", pa.utf8(), nullable=False),
            pa.field("score", pa.float32(), nullable=True),
        ]),
    )


@pytest.mark.asyncio
async def test_create_and_read_roundtrip(murr_client):
    await murr_client.create_table("users", _user_schema())
    await murr_client.write("users", _user_batch())

    result = await murr_client.read("users", ["c", "a"], ["score"])
    assert result.num_rows == 2
    assert result.column("score").to_pylist() == [3.0, 1.0]


@pytest.mark.asyncio
async def test_read_all_columns(murr_client):
    await murr_client.create_table("users", _user_schema())
    await murr_client.write("users", _user_batch())

    result = await murr_client.read("users", ["b"], ["id", "score"])
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == ["b"]
    assert result.column("score").to_pylist() == [2.0]


@pytest.mark.asyncio
async def test_list_tables(murr_client):
    schema = TableSchema(
        key="id",
        columns={"id": ColumnSchema(dtype=DType.UTF8, nullable=False)},
    )
    await murr_client.create_table("t1", schema)

    tables = await murr_client.list_tables()
    assert "t1" in tables
    assert tables["t1"].key == "id"
    assert tables["t1"].columns["id"].dtype == DType.UTF8


@pytest.mark.asyncio
async def test_get_schema(murr_client):
    schema = _user_schema()
    await murr_client.create_table("users", schema)

    result = await murr_client.get_schema("users")
    assert result == schema


@pytest.mark.asyncio
async def test_create_duplicate_raises(murr_client):
    schema = TableSchema(
        key="id",
        columns={"id": ColumnSchema(dtype=DType.UTF8, nullable=False)},
    )
    await murr_client.create_table("t", schema)
    with pytest.raises(ValueError, match="already exists"):
        await murr_client.create_table("t", schema)


@pytest.mark.asyncio
async def test_read_nonexistent_table_raises(murr_client):
    with pytest.raises(FileNotFoundError, match="not found"):
        await murr_client.read("nope", ["a"], ["x"])


@pytest.mark.asyncio
async def test_get_schema_nonexistent_raises(murr_client):
    with pytest.raises(FileNotFoundError, match="not found"):
        await murr_client.get_schema("nope")


@pytest.mark.asyncio
async def test_persistence_across_instances(tmp_path):
    cache_dir = str(tmp_path)
    schema = _user_schema()

    client1 = await Murr.start_local(cache_dir=cache_dir)
    await client1.create_table("t", schema)
    await client1.write("t", _user_batch())
    del client1

    client2 = await Murr.start_local(cache_dir=cache_dir)
    result = await client2.read("t", ["c"], ["score"])
    assert result.column("score").to_pylist() == [3.0]


@pytest.mark.asyncio
async def test_start_local_with_http(tmp_path):
    import asyncio
    import urllib.request

    client = await Murr.start_local(cache_dir=str(tmp_path), http_port=19877)
    await asyncio.sleep(0.1)  # let the HTTP server bind

    resp = urllib.request.urlopen("http://127.0.0.1:19877/health")
    assert resp.status == 200

    del client


@pytest.mark.asyncio
async def test_connect_raises():
    with pytest.raises(NotImplementedError, match="not yet implemented"):
        await Murr.connect("grpc://localhost:8081")
