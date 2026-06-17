#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::hint::black_box;
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use murr::core::TableSchema;
use murr::io::codec::ColumnDecoder;
use murr::io::row::write::WriteRow;
use murr::io::schema::SegmentSchema;
use murr::io::store::KeyValue;

mod common;
use common::dataset::Dataset;

const ROWS: usize = 1_000_000;
const COLS: usize = 10;

fn bench_write_encode(c: &mut Criterion) {
    let dataset = Dataset::new(ROWS, COLS);
    let table_schema: &TableSchema = dataset.table_schema();
    let segment = SegmentSchema::from(table_schema);
    let batch: RecordBatch = dataset.batches(ROWS).next().unwrap();

    let mut group = c.benchmark_group("write_encode");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("table_write_no_store", |b| {
        b.iter(|| {
            let canonical: Schema = table_schema.into();
            let indices: Vec<usize> = canonical
                .fields()
                .iter()
                .map(|f| batch.schema().index_of(f.name()).unwrap())
                .collect();
            let ordered = batch.project(&indices).unwrap();

            let key_idx = canonical.index_of(&table_schema.key).unwrap();
            let key_array = ordered
                .column(key_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let mut decoders: Vec<Box<dyn ColumnDecoder>> =
                Vec::with_capacity(segment.columns.len());
            for col in &segment.columns {
                let arr_idx = canonical.index_of(&col.name).unwrap();
                decoders.push(
                    col.dtype
                        .codec()
                        .make_decoder(col.clone(), ordered.column(arr_idx).as_ref())
                        .unwrap(),
                );
            }

            let n = ordered.num_rows();
            for i in 0..n {
                let mut row = WriteRow::new(&segment, key_array.value(i));
                for d in &decoders {
                    d.write_to_row(i, &mut row);
                }
                let kv: KeyValue = row.into();
                black_box(kv);
            }
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(common::profiler::PProfProfiler::new());
    targets = bench_write_encode
}
criterion_main!(benches);
