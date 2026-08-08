//! Backend-agnostic read benchmark scaffolding.
//!
//! Each backend implements `ReadBench` (or relies on the blanket impl for
//! `Table<S: Store>`); `run_read_bench` drives the Criterion loop.

use std::time::Instant;

use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion};

use murr::core::MurrError;
use murr::io::store::Store;
use murr::io::table::Table;

use super::dataset::Dataset;

pub trait ReadBench {
    fn write(&self, dataset: &Dataset, batch_size: usize) -> Result<(), MurrError>;
    fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError>;
}

impl<S: Store> ReadBench for Table<S> {
    fn write(&self, dataset: &Dataset, batch_size: usize) -> Result<(), MurrError> {
        let total_batches = dataset.num_rows().div_ceil(batch_size.max(1));
        let ingest_start = Instant::now();
        for (i, batch) in dataset.batches(batch_size).enumerate() {
            let rows = batch.num_rows();
            let batch_start = Instant::now();
            Table::write(self, &batch)?;
            let elapsed = batch_start.elapsed();
            let rate = rows as f64 / elapsed.as_secs_f64();
            eprintln!(
                "  batch {}/{}: {} rows in {:.2?} ({:.0} rows/s)",
                i + 1,
                total_batches,
                rows,
                elapsed,
                rate
            );
        }
        let total = ingest_start.elapsed();
        let total_rate = dataset.num_rows() as f64 / total.as_secs_f64();
        eprintln!(
            "  ingest total: {} rows in {:.2?} ({:.0} rows/s)",
            dataset.num_rows(),
            total,
            total_rate
        );
        Ok(())
    }

    fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        Table::read(self, keys, columns)
    }
}

pub struct BenchOpts<'a> {
    pub key_counts: &'a [usize],
    pub sample_size: usize,
    pub write_batch_size: usize,
    pub group_name: &'a str,
}

pub fn run_read_bench<B: ReadBench>(
    c: &mut Criterion,
    bench: &B,
    dataset: &Dataset,
    opts: &BenchOpts<'_>,
) {
    eprintln!(
        "[{}] writing {} rows in chunks of {}...",
        opts.group_name,
        dataset.num_rows(),
        opts.write_batch_size
    );
    bench
        .write(dataset, opts.write_batch_size)
        .expect("setup write failed");
    eprintln!("[{}] setup complete, starting benchmark", opts.group_name);

    let col_names = dataset.column_names();
    let col_refs: Vec<&str> = col_names.iter().map(String::as_str).collect();
    let col_refs_ref = &col_refs;

    let mut group = c.benchmark_group(opts.group_name);
    group.sample_size(opts.sample_size);

    for &num_keys in opts.key_counts {
        let mut seed: u64 = num_keys as u64 * 2_000_000;
        group.bench_with_input(
            BenchmarkId::new(opts.group_name, num_keys),
            &num_keys,
            |b, &n| {
                b.iter_batched(
                    || {
                        seed += 1;
                        dataset.generate_keys(n, seed)
                    },
                    |keys| {
                        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
                        std::hint::black_box(
                            bench
                                .read(std::hint::black_box(&key_refs), col_refs_ref)
                                .unwrap(),
                        )
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}
