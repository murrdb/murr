#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::{Arc, RwLock};

use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use murr::io::store::rocksdb::RocksDBStore;
use murr::io::store::rocksdb::plain::PlainConfig;
use murr::io::table::Table;

mod common;
use common::dataset::Dataset;
use common::read_bench::{BenchOpts, run_read_bench};

fn bench(c: &mut Criterion) {
    let dataset = Dataset::new(10_000_000, 10);
    let tmp = TempDir::new().unwrap();
    let mut config = PlainConfig::default();
    config.read_method = murr::io::store::rocksdb::ReadMethod::ParGet;
    let store = RocksDBStore::open_plain(tmp.path(), &config).unwrap();
    let store = Arc::new(RwLock::new(store));
    let table = Table::create(store, "bench", dataset.table_schema().clone()).unwrap();
    let opts = BenchOpts {
        key_counts: &[1000],
        sample_size: 100,
        write_batch_size: 1_000_000,
        group_name: "read_plain",
    };
    run_read_bench(c, &table, &dataset, &opts);
    drop(tmp);
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(common::profiler::PProfProfiler::new());
    targets = bench
}
criterion_main!(benches);
