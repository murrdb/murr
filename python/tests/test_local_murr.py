import pyarrow as pa
import pytest

from murr import ColumnSchema, DType, LocalMurr, TableSchema


@pytest.fixture
def murr_client(tmp_path):
    return LocalMurr(cache_dir=str(tmp_path))


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


def test_create_and_read_roundtrip(murr_client):
    murr_client.create_table("users", _user_schema())
    murr_client.write("users", _user_batch())

    result = murr_client.read("users", ["c", "a"], ["score"])
    assert result.num_rows == 2
    assert result.column("score").to_pylist() == [3.0, 1.0]


def test_read_all_columns(murr_client):
    murr_client.create_table("users", _user_schema())
    murr_client.write("users", _user_batch())

    result = murr_client.read("users", ["b"], ["id", "score"])
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == ["b"]
    assert result.column("score").to_pylist() == [2.0]


def test_list_tables(murr_client):
    schema = TableSchema(
        key="id",
        columns={"id": ColumnSchema(dtype=DType.UTF8, nullable=False)},
    )
    murr_client.create_table("t1", schema)

    tables = murr_client.list_tables()
    assert "t1" in tables
    assert tables["t1"].key == "id"
    assert tables["t1"].columns["id"].dtype == DType.UTF8


def test_get_schema(murr_client):
    schema = _user_schema()
    murr_client.create_table("users", schema)

    result = murr_client.get_schema("users")
    assert result == schema


def test_create_duplicate_raises(murr_client):
    schema = TableSchema(
        key="id",
        columns={"id": ColumnSchema(dtype=DType.UTF8, nullable=False)},
    )
    murr_client.create_table("t", schema)
    with pytest.raises(ValueError, match="already exists"):
        murr_client.create_table("t", schema)


def test_read_nonexistent_table_raises(murr_client):
    with pytest.raises(FileNotFoundError, match="not found"):
        murr_client.read("nope", ["a"], ["x"])


def test_get_schema_nonexistent_raises(murr_client):
    with pytest.raises(FileNotFoundError, match="not found"):
        murr_client.get_schema("nope")


def test_persistence_across_instances(tmp_path):
    cache_dir = str(tmp_path)
    schema = _user_schema()

    client1 = LocalMurr(cache_dir=cache_dir)
    client1.create_table("t", schema)
    client1.write("t", _user_batch())
    del client1

    client2 = LocalMurr(cache_dir=cache_dir)
    result = client2.read("t", ["c"], ["score"])
    assert result.column("score").to_pylist() == [3.0]
