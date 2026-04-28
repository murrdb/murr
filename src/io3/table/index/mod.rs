use hashbrown::HashMap;
use lean_string::LeanString;
use rustc_hash::FxBuildHasher;

use crate::io3::table::index::keys::{SegmentKey, SegmentKeyBytes};

pub mod keys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLocation {
    pub segment_id: u32,
    pub offset: u32,
    pub size: u32,
}

pub struct KeyIndex {
    map: HashMap<LeanString, RowLocation, FxBuildHasher>,
}

impl KeyIndex {
    pub fn empty() -> Self {
        KeyIndex {
            map: HashMap::with_capacity_and_hasher(1024, FxBuildHasher),
        }
    }

    pub fn add_segment(&mut self, segment_id: u32, bytes: &SegmentKeyBytes) {
        for SegmentKey { key, location } in bytes.read_keys(segment_id) {
            self.map.insert(key, location);
        }
    }

    pub fn get(&self, keys: &[&str]) -> Vec<Option<RowLocation>> {
        keys.iter().map(|k| self.map.get(*k).copied()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io3::table::segment::Segment;
    use arrow::array::{Float32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;

    fn table_schema() -> TableSchema {
        let mut columns = StdHashMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            key: "id".into(),
            columns,
        }
    }

    fn batch(keys: &[&str], scores: &[Option<f32>]) -> RecordBatch {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let id_array = StringArray::from(keys.to_vec());
        let score_array = Float32Array::from(scores.to_vec());
        RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(id_array), Arc::new(score_array)],
        )
        .unwrap()
    }

    // Keys added under one segment_id resolve under that segment_id; keys added
    // under another resolve under that other one; keys never added resolve to
    // None — and result order tracks the request order (so a missing key in the
    // middle does not shift its neighbors).
    #[test]
    fn get_routes_keys_to_the_segment_they_were_added_under() {
        let schema = table_schema();
        let seg7 =
            Segment::write(batch(&["a", "b", "c"], &[Some(1.0), None, Some(3.0)]), &schema)
                .unwrap();
        let seg11 =
            Segment::write(batch(&["d", "e"], &[Some(4.0), Some(5.0)]), &schema).unwrap();

        let mut index = KeyIndex::empty();
        index.add_segment(7, &seg7.keys);
        index.add_segment(11, &seg11.keys);

        let got = index.get(&["a", "missing", "e", "b"]);
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].map(|l| l.segment_id), Some(7));
        assert_eq!(got[1], None);
        assert_eq!(got[2].map(|l| l.segment_id), Some(11));
        assert_eq!(got[3].map(|l| l.segment_id), Some(7));
    }

    // Last-writer-wins on duplicate keys: the same key appearing in a later
    // add_segment must shadow the earlier one.
    #[test]
    fn duplicate_key_resolves_to_last_added_segment() {
        let schema = table_schema();
        let seg0 = Segment::write(batch(&["a"], &[Some(1.0)]), &schema).unwrap();
        let seg1 = Segment::write(batch(&["a"], &[None]), &schema).unwrap();

        let mut index = KeyIndex::empty();
        index.add_segment(0, &seg0.keys);
        index.add_segment(1, &seg1.keys);

        assert_eq!(index.get(&["a"])[0].map(|l| l.segment_id), Some(1));
    }
}
