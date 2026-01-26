use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use murr::testutil::{bench_column_names, bench_generate_keys, setup_benchmark_table};

const ROW_COUNTS: &[usize] = &[100_000, 1_000_000, 10_000_000];
const KEY_COUNTS: &[usize] = &[10, 100, 1000];

fn bench_table_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let columns = bench_column_names();
    let column_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

    for &num_rows in ROW_COUNTS {
        let (state, _temp_dir) =
            rt.block_on(setup_benchmark_table(&format!("bench_{}", num_rows), num_rows));
        let table = &state.table;

        let mut group = c.benchmark_group(format!("table/rows_{}", num_rows));

        if num_rows >= 10_000_000 {
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(30));
            group.warm_up_time(Duration::from_secs(5));
        }

        for &num_keys in KEY_COUNTS {
            let keys = bench_generate_keys(num_keys, num_rows);
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

            group.throughput(Throughput::Elements(num_keys as u64));
            group.bench_with_input(BenchmarkId::new("keys", num_keys), &num_keys, |b, _| {
                b.iter(|| table.get(black_box(&key_refs), black_box(&column_refs)))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_table_get);
criterion_main!(benches);
