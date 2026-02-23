use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use murr::core::{ColumnConfig, DType, TableSchema};
use murr::service::MurrService;
use murr::testutil::{bench_column_names, bench_generate_keys, generate_batch};

const NUM_ROWS: usize = 10_000_000;
const NUM_KEYS: usize = 1000;

fn make_schema(col_names: &[String]) -> (TableSchema, Arc<Schema>) {
    let mut columns = HashMap::new();
    columns.insert(
        "key".to_string(),
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: false,
        },
    );
    let mut arrow_fields = vec![Field::new("key", DataType::Utf8, false)];

    for name in col_names {
        columns.insert(
            name.clone(),
            ColumnConfig {
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

fn bench_table_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let col_names = bench_column_names();
    let (table_schema, arrow_schema) = make_schema(&col_names);

    let dir = TempDir::new().unwrap();
    let svc = Arc::new(MurrService::new(dir.path().to_path_buf()));

    // Create table and write a single segment with 10M rows.
    rt.block_on(async {
        svc.create("bench", table_schema).await.unwrap();
        let batch = generate_batch(&arrow_schema, NUM_ROWS);
        svc.write("bench", &batch).await.unwrap();
    });

    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
    let keys = bench_generate_keys(NUM_KEYS, NUM_ROWS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group(format!("table/rows_{}", NUM_ROWS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_KEYS as u64));

    group.bench_with_input(BenchmarkId::new("keys", NUM_KEYS), &NUM_KEYS, |b, _| {
        b.to_async(&rt).iter(|| {
            let svc = svc.clone();
            let key_refs = key_refs.clone();
            let col_refs = col_refs.clone();
            async move {
                svc.read("bench", black_box(&key_refs), black_box(&col_refs))
                    .await
                    .unwrap()
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_table_get);
criterion_main!(benches);
