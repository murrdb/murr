use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use murr::api::MurrHttpService;
use murr::conf::{Config, StorageConfig};
use murr::service::MurrService;

const CSV_PATH: &str = "tests/fixtures/anime_info.csv";

const FLOAT_COLUMNS: &[&str] = &[
    "is_tv",
    "year_aired",
    "is_adult",
    "above_five_star_users",
    "above_five_star_ratings",
    "above_five_star_ratio",
];

/// A single row parsed from the CSV, keyed by anime_id (stringified).
struct AnimeRow {
    genres: Option<String>,
    floats: HashMap<String, Option<f32>>,
}

/// Parse anime_info.csv into a map of anime_id -> AnimeRow.
fn load_csv() -> HashMap<String, AnimeRow> {
    let mut reader = csv::Reader::from_path(CSV_PATH).expect("failed to open anime_info.csv");
    let mut rows = HashMap::new();

    for result in reader.records() {
        let record = result.expect("failed to read CSV record");

        let anime_id = record[0].to_string();
        let genres = if record[1].is_empty() {
            None
        } else {
            Some(record[1].to_string())
        };

        let mut floats = HashMap::new();
        for (i, col_name) in FLOAT_COLUMNS.iter().enumerate() {
            let raw = &record[i + 2];
            let value = if raw.is_empty() {
                None
            } else {
                Some(raw.parse::<f64>().expect("failed to parse float") as f32)
            };
            floats.insert(col_name.to_string(), value);
        }

        rows.insert(anime_id, AnimeRow { genres, floats });
    }
    rows
}

fn table_schema_json() -> Value {
    json!({
        "key": "anime_id",
        "columns": {
            "anime_id": {"dtype": "utf8", "nullable": false},
            "Genres": {"dtype": "utf8", "nullable": true},
            "is_tv": {"dtype": "float32", "nullable": true},
            "year_aired": {"dtype": "float32", "nullable": true},
            "is_adult": {"dtype": "float32", "nullable": true},
            "above_five_star_users": {"dtype": "float32", "nullable": true},
            "above_five_star_ratings": {"dtype": "float32", "nullable": true},
            "above_five_star_ratio": {"dtype": "float32", "nullable": true}
        }
    })
}

/// Build an Arrow RecordBatch from the CSV data.
fn csv_to_record_batch(csv_data: &HashMap<String, AnimeRow>) -> RecordBatch {
    let mut keys: Vec<String> = csv_data.keys().cloned().collect();
    keys.sort();

    let key_array: StringArray = keys.iter().map(|k| Some(k.as_str())).collect();
    let genres_array: StringArray = keys.iter().map(|k| csv_data[k].genres.as_deref()).collect();

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> =
        vec![Arc::new(key_array), Arc::new(genres_array)];

    for col in FLOAT_COLUMNS {
        let arr: Float32Array = keys.iter().map(|k| csv_data[k].floats[*col]).collect();
        arrays.push(Arc::new(arr));
    }

    let mut fields = vec![
        Field::new("anime_id", DataType::Utf8, false),
        Field::new("Genres", DataType::Utf8, true),
    ];
    for col in FLOAT_COLUMNS {
        fields.push(Field::new(*col, DataType::Float32, true));
    }
    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, arrays).expect("failed to build RecordBatch")
}

async fn setup() -> (TempDir, Router, HashMap<String, AnimeRow>) {
    let dir = TempDir::new().unwrap();
    let config = Config {
        storage: StorageConfig {
            cache_dir: dir.path().to_path_buf(),
        },
        ..Config::default()
    };
    let service = Arc::new(MurrService::new(config).await.unwrap());
    let api = MurrHttpService::new(service);
    let router = api.router();

    // Create table
    let schema = serde_json::to_vec(&table_schema_json()).unwrap();
    let req = Request::put("/api/v1/table/anime")
        .header("content-type", "application/json")
        .body(Body::from(schema))
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Write data as Arrow IPC
    let csv_data = load_csv();
    let batch = csv_to_record_batch(&csv_data);
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let req = Request::put("/api/v1/table/anime/write")
        .header("content-type", "application/vnd.apache.arrow.stream")
        .body(Body::from(buf))
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    (dir, router, csv_data)
}

async fn fetch_json(router: Router, body: Value) -> Value {
    let req = Request::post("/api/v1/table/anime/fetch")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn assert_float_eq(actual: &Value, expected: Option<f32>) {
    match expected {
        None => assert!(actual.is_null(), "expected null, got {actual}"),
        Some(v) => {
            let actual_f32 = actual.as_f64().expect("expected a number") as f32;
            assert!(
                (actual_f32 - v).abs() < 1e-6,
                "expected {v}, got {actual_f32}"
            );
        }
    }
}

#[tokio::test]
async fn test_all_rows_all_columns() {
    let (_dir, router, csv_data) = setup().await;

    let all_keys: Vec<&str> = csv_data.keys().map(|k| k.as_str()).collect();
    let all_columns: Vec<&str> = std::iter::once("Genres")
        .chain(FLOAT_COLUMNS.iter().copied())
        .collect();

    let body = json!({"keys": all_keys, "columns": all_columns});
    let json = fetch_json(router, body).await;

    let columns = json["columns"].as_object().unwrap();
    for col_name in &all_columns {
        let col_values = columns[*col_name].as_array().unwrap();
        assert_eq!(
            col_values.len(),
            all_keys.len(),
            "column {col_name} row count mismatch"
        );

        for (i, key) in all_keys.iter().enumerate() {
            let row = &csv_data[*key];
            if *col_name == "Genres" {
                match &row.genres {
                    None => assert!(col_values[i].is_null()),
                    Some(g) => assert_eq!(col_values[i].as_str().unwrap(), g),
                }
            } else {
                assert_float_eq(&col_values[i], row.floats[*col_name]);
            }
        }
    }
}

#[tokio::test]
async fn test_get_schema() {
    let (_dir, router, _csv_data) = setup().await;

    let req = Request::get("/api/v1/table/anime/schema")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(json["key"], "anime_id");

    let columns = json["columns"].as_object().unwrap();
    assert_eq!(columns["anime_id"]["dtype"], "utf8");
    assert_eq!(columns["Genres"]["dtype"], "utf8");
    for col in FLOAT_COLUMNS {
        assert_eq!(
            columns[*col]["dtype"], "float32",
            "dtype mismatch for {col}"
        );
        assert_eq!(
            columns[*col]["nullable"], true,
            "nullable mismatch for {col}"
        );
    }
}

#[tokio::test]
async fn test_single_column() {
    let (_dir, router, csv_data) = setup().await;

    let all_keys: Vec<&str> = csv_data.keys().map(|k| k.as_str()).collect();
    let body = json!({"keys": all_keys, "columns": ["above_five_star_ratio"]});
    let json = fetch_json(router, body).await;

    let values = json["columns"]["above_five_star_ratio"].as_array().unwrap();
    assert_eq!(values.len(), all_keys.len());

    for (i, key) in all_keys.iter().enumerate() {
        let expected = csv_data[*key].floats["above_five_star_ratio"];
        assert_float_eq(&values[i], expected);
    }
}

#[tokio::test]
async fn test_single_row_single_column() {
    let (_dir, router, csv_data) = setup().await;

    // Pick the first key from the dataset
    let key = csv_data.keys().next().unwrap().clone();
    let body = json!({"keys": [key], "columns": ["above_five_star_ratio"]});
    let json = fetch_json(router, body).await;

    let values = json["columns"]["above_five_star_ratio"].as_array().unwrap();
    assert_eq!(values.len(), 1);

    let expected = csv_data[&key].floats["above_five_star_ratio"];
    assert_float_eq(&values[0], expected);
}

#[tokio::test]
async fn test_mixed_existing_and_missing_keys() {
    let (_dir, router, csv_data) = setup().await;

    // Pick 5 real keys
    let real_keys: Vec<String> = csv_data.keys().take(5).cloned().collect();
    let fake_keys: Vec<String> = (0..5).map(|i| format!("nonexistent_{i}")).collect();

    let mut all_keys: Vec<String> = Vec::new();
    all_keys.extend(real_keys.clone());
    all_keys.extend(fake_keys.clone());

    let body = json!({"keys": all_keys, "columns": ["above_five_star_ratio"]});
    let json = fetch_json(router, body).await;

    let values = json["columns"]["above_five_star_ratio"].as_array().unwrap();
    assert_eq!(values.len(), all_keys.len());

    // Real keys should match CSV data
    for (i, key) in real_keys.iter().enumerate() {
        let expected = csv_data[key].floats["above_five_star_ratio"];
        assert_float_eq(&values[i], expected);
    }

    // Fake keys should be null
    for item in values.iter().take(10).skip(5) {
        assert!(
            item.is_null(),
            "expected null for missing key, got {}",
            item
        );
    }
}
