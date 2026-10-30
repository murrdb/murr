use arrow::array::{Array, StringArray, UInt32Array};

use crate::core::{DTypeName, IDX_COLUMN};
use crate::io::row::read::ReadBatchBuilder;
use crate::io::row::write::WriteRow;
use crate::io::schema::{SegmentColumnSchema, SegmentSchema};
use crate::io::store::{KeyValue, Store};

pub fn payload_segment() -> SegmentSchema {
    SegmentSchema::new(&[SegmentColumnSchema {
        index: 0,
        dtype: DTypeName::Utf8,
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
            let mut row = WriteRow::new(&segment);
            row.write_dynamic(col, v);
            KeyValue::new(*k, row.bytes)
        })
        .collect();
    store.write(table, kvs).unwrap();
}

/// Reads `keys` and scatters the found payloads back to request positions by `IDX_COLUMN`.
pub fn fetch<S: Store>(store: &S, table: &str, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let segment = payload_segment();
    let cols: Vec<&SegmentColumnSchema> = segment.columns.iter().collect();
    let builder = ReadBatchBuilder::new(&segment, cols, keys.len());
    let batch = store.read(table, keys, builder).unwrap();
    let idx = batch
        .column_by_name(IDX_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let arr = batch
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("payload column is Utf8");
    let mut out = vec![None; keys.len()];
    for i in 0..arr.len() {
        assert!(!arr.is_null(i), "found rows carry a payload");
        out[idx.value(i) as usize] = Some(arr.value(i).as_bytes().to_vec());
    }
    out
}
