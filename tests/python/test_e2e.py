import math

import requests


def fetch(base_url: str, keys: list, columns: list) -> dict:
    resp = requests.post(
        f"{base_url}/api/v1/table/anime/fetch",
        json={"keys": keys, "columns": columns},
        headers={"Accept": "application/json"},
        timeout=30,
    )
    assert resp.status_code == 200, f"fetch failed: {resp.text}"
    return resp.json()


def assert_float_eq(actual, expected: float | None):
    if expected is None:
        assert actual is None, f"expected null, got {actual}"
    else:
        assert actual is not None, f"expected {expected}, got null"
        assert math.isclose(float(actual), expected, rel_tol=1e-5), (
            f"expected {expected}, got {actual}"
        )


def test_all_rows_all_columns(murr_server):
    base_url, csv_data = murr_server
    all_keys = list(csv_data.keys())
    all_columns = ["Genres", "is_tv", "year_aired", "is_adult",
                   "above_five_star_users", "above_five_star_ratings", "above_five_star_ratio"]

    result = fetch(base_url, all_keys, all_columns)
    columns = result["columns"]

    for col in all_columns:
        assert col in columns, f"missing column {col}"
        assert len(columns[col]) == len(all_keys), f"row count mismatch for {col}"

    for i, key in enumerate(all_keys):
        row = csv_data[key]
        genres_val = columns["Genres"][i]
        if row["genres"] is None:
            assert genres_val is None
        else:
            assert genres_val == row["genres"]
        for col in ["is_tv", "year_aired", "is_adult",
                    "above_five_star_users", "above_five_star_ratings", "above_five_star_ratio"]:
            assert_float_eq(columns[col][i], row["floats"][col])


def test_get_schema(murr_server):
    base_url, _ = murr_server
    resp = requests.get(f"{base_url}/api/v1/table/anime/schema", timeout=10)
    assert resp.status_code == 200
    schema = resp.json()

    assert schema["key"] == "anime_id"
    cols = schema["columns"]
    assert cols["anime_id"]["dtype"] == "utf8"
    assert cols["Genres"]["dtype"] == "utf8"
    for col in ["is_tv", "year_aired", "is_adult",
                "above_five_star_users", "above_five_star_ratings", "above_five_star_ratio"]:
        assert cols[col]["dtype"] == "float32", f"dtype mismatch for {col}"
        assert cols[col]["nullable"] is True, f"nullable mismatch for {col}"


def test_single_column(murr_server):
    base_url, csv_data = murr_server
    all_keys = list(csv_data.keys())

    result = fetch(base_url, all_keys, ["above_five_star_ratio"])
    values = result["columns"]["above_five_star_ratio"]
    assert len(values) == len(all_keys)

    for i, key in enumerate(all_keys):
        assert_float_eq(values[i], csv_data[key]["floats"]["above_five_star_ratio"])


def test_single_row_single_column(murr_server):
    base_url, csv_data = murr_server
    key = next(iter(csv_data))

    result = fetch(base_url, [key], ["above_five_star_ratio"])
    values = result["columns"]["above_five_star_ratio"]
    assert len(values) == 1
    assert_float_eq(values[0], csv_data[key]["floats"]["above_five_star_ratio"])


def test_mixed_existing_and_missing_keys(murr_server):
    base_url, csv_data = murr_server
    real_keys = list(csv_data.keys())[:5]
    fake_keys = [f"nonexistent_{i}" for i in range(5)]
    all_keys = real_keys + fake_keys

    result = fetch(base_url, all_keys, ["above_five_star_ratio"])
    values = result["columns"]["above_five_star_ratio"]
    assert len(values) == len(all_keys)

    for i, key in enumerate(real_keys):
        assert_float_eq(values[i], csv_data[key]["floats"]["above_five_star_ratio"])

    for val in values[5:]:
        assert val is None, f"expected null for missing key, got {val}"
