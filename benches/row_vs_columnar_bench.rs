use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use murr::testutil::bench_generate_keys;

const N_ROWS: usize = 2_000_000;
const TOTAL_COL_COUNTS: &[usize] = &[10, 100];
const KEY_COUNTS: &[usize] = &[100, 1000];

fn build_index(n_rows: usize) -> HashMap<String, usize> {
    let mut index = HashMap::with_capacity(n_rows);
    for i in 0..n_rows {
        index.insert(i.to_string(), i);
    }
    index
}

struct ColStore {
    index: HashMap<String, usize>,
    data: Vec<Vec<f32>>,
}

impl ColStore {
    fn build(n_rows: usize, n_cols: usize) -> Self {
        let data = (0..n_cols)
            .map(|c| (0..n_rows).map(|r| (r * n_cols + c) as f32).collect())
            .collect();
        ColStore { index: build_index(n_rows), data }
    }

    fn read(&self, keys: &[String]) -> Vec<f32> {
        let n_cols = self.data.len();
        let mut out = Vec::with_capacity(keys.len() * n_cols);
        for key in keys {
            if let Some(&row) = self.index.get(key) {
                for col in &self.data {
                    out.push(col[row]);
                }
            }
        }
        out
    }
}

struct RowStore {
    index: HashMap<String, usize>,
    data: Vec<f32>,
    n_cols: usize,
}

impl RowStore {
    fn build(n_rows: usize, n_cols: usize) -> Self {
        let data = (0..n_rows * n_cols).map(|i| i as f32).collect();
        RowStore { index: build_index(n_rows), data, n_cols }
    }

    fn read(&self, keys: &[String]) -> Vec<f32> {
        let mut out = Vec::with_capacity(keys.len() * self.n_cols);
        for key in keys {
            if let Some(&row) = self.index.get(key) {
                let start = row * self.n_cols;
                out.extend_from_slice(&self.data[start..start + self.n_cols]);
            }
        }
        out
    }
}

fn bench_read(c: &mut Criterion) {
    for &n_cols in TOTAL_COL_COUNTS {
        let col_store = ColStore::build(N_ROWS, n_cols);
        let row_store = RowStore::build(N_ROWS, n_cols);

        let mut group = c.benchmark_group(format!("row_vs_columnar/cols_{n_cols}"));
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(15));

        for &n_keys in KEY_COUNTS {
            let keys = bench_generate_keys(n_keys, N_ROWS);

            group.throughput(Throughput::Elements((n_keys * n_cols) as u64));

            group.bench_with_input(BenchmarkId::new("columnar", n_keys), &n_keys, |b, _| {
                b.iter(|| black_box(col_store.read(&keys)));
            });
            group.bench_with_input(BenchmarkId::new("row", n_keys), &n_keys, |b, _| {
                b.iter(|| black_box(row_store.read(&keys)));
            });
        }

        group.finish();
    }
}

criterion_group!(benches, bench_read);
criterion_main!(benches);
