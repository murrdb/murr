use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use murr::conf::{Config, StorageConfig};
use murr::core::{ColumnSchema, DType, TableSchema};
use murr::service::MurrService;
use murr::testutil::{bench_column_names, generate_batch};

pub const NUM_ROWS: usize = 10_000_000;
pub const NUM_KEYS: usize = 1000;

#[allow(dead_code)]
pub fn make_schema(col_names: &[String]) -> (TableSchema, Arc<Schema>) {
    let mut columns = HashMap::new();
    columns.insert(
        "key".to_string(),
        ColumnSchema {
            dtype: DType::Utf8,
            nullable: false,
        },
    );
    let mut arrow_fields = vec![Field::new("key", DataType::Utf8, false)];

    for name in col_names {
        columns.insert(
            name.clone(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
        arrow_fields.push(Field::new(name, DataType::Float32, false));
    }

    let table_schema = TableSchema {
        name: "bench".to_string(),
        key: "key".to_string(),
        columns,
    };
    let arrow_schema = Arc::new(Schema::new(arrow_fields));
    (table_schema, arrow_schema)
}

/// Set up a MurrService with a "bench" table containing NUM_ROWS rows of Float32 data.
/// Returns the TempDir (must be kept alive) and the service.
#[allow(dead_code)]
pub fn setup_service(rt: &Runtime) -> (TempDir, MurrService) {
    let col_names = bench_column_names();
    let (table_schema, arrow_schema) = make_schema(&col_names);

    let dir = TempDir::new().unwrap();
    let config = Config {
        storage: StorageConfig {
            cache_dir: dir.path().to_path_buf(),
        },
        ..Config::default()
    };
    let svc = rt.block_on(async {
        let svc = MurrService::new(config).await.unwrap();
        svc.create(table_schema).await.unwrap();
        let batch = generate_batch(&arrow_schema, NUM_ROWS);
        svc.write("bench", &batch).await.unwrap();
        svc
    });

    (dir, svc)
}
