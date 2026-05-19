use arrow::array::{Array, StringArray};

use crate::core::DType;
use crate::io::row::read::ReadBatchBuilder;
use crate::io::row::write::WriteRow;
use crate::io::schema::{SegmentColumnSchema, SegmentSchema};
use crate::io::store::{KeyValue, Store};

pub fn payload_segment() -> SegmentSchema {
    SegmentSchema::new(&[SegmentColumnSchema {
        index: 0,
        dtype: DType::Utf8,
        name: "payload".into(),
        offset: 0,
    }])
}

pub fn put<S: Store>(store: &mut S, table: &str, rows: &[(&str, &[u8])]) {
    let segment = payload_segment();
    let col = &segment.columns[0];
    let kvs: Vec<KeyValue> = rows
        .iter()
        .map(|(k, v)| {
            let mut row = WriteRow::new(&segment, k);
            row.write_dynamic(col, v);
            row.into()
        })
        .collect();
    store.write(table, kvs).unwrap();
}

pub fn fetch<S: Store>(store: &S, table: &str, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let segment = payload_segment();
    let cols: Vec<&SegmentColumnSchema> = segment.columns.iter().collect();
    let builder = ReadBatchBuilder::new(&segment, cols, keys.len());
    let batch = store.read(table, keys, builder).unwrap();
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("payload column is Utf8");
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).as_bytes().to_vec())
            }
        })
        .collect()
}
