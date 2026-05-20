use std::sync::{Arc, RwLock};

use criterion::{Criterion, criterion_group, criterion_main};

use murr::io::store::memory::MemoryStore;
use murr::io::table::Table;

mod common;
use common::dataset::Dataset;
use common::read_bench::{BenchOpts, run_read_bench};

fn bench(c: &mut Criterion) {
    let dataset = Dataset::new(100_000_000, 10);
    let store = Arc::new(RwLock::new(MemoryStore::new()));
    let table = Table::create(store, "bench", dataset.table_schema().clone()).unwrap();
    let opts = BenchOpts {
        key_counts: &[1000],
        sample_size: 100,
        write_batch_size: 1_000_000,
        group_name: "read_memory",
    };
    run_read_bench(c, &table, &dataset, &opts);
}

criterion_group!(benches, bench);
criterion_main!(benches);
