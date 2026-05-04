use hashbrown::HashMap;
use lean_string::LeanString;
use rustc_hash::FxBuildHasher;

use crate::io::table::index::keys::{SegmentKey, SegmentKeyBytes};

pub mod keys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLocation {
    pub segment_id: u32,
    pub offset: u32,
    pub size: u32,
}

pub struct KeyIndex {
    max_segment_id: u32,
    map: HashMap<LeanString, RowLocation, FxBuildHasher>,
}

impl KeyIndex {
    pub fn empty() -> Self {
        KeyIndex {
            map: HashMap::with_capacity_and_hasher(1024, FxBuildHasher),
            max_segment_id: 0,
        }
    }

    pub fn add_segment(&mut self, segment_id: u32, bytes: &SegmentKeyBytes) {
        if segment_id > self.max_segment_id {
            self.max_segment_id = segment_id;
        }

        for SegmentKey { key, location } in bytes.read_keys(segment_id) {
            self.map.insert(key, location);
        }
    }

    pub fn prune_segments(&mut self, segments_to_delete: &[u32]) {
        let mut bitmap = vec![true; self.max_segment_id as usize + 1];
        for &id in segments_to_delete {
            bitmap[id as usize] = false;
        }
        self.map.retain(|_, loc| bitmap[loc.segment_id as usize]);
    }

    pub fn get(&self, keys: &[&str]) -> Vec<Option<RowLocation>> {
        keys.iter().map(|k| self.map.get(*k).copied()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::table::segment::Segment;
    use arrow::array::{Float32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use indexmap::IndexMap;
    use std::sync::Arc;

    fn table_schema() -> TableSchema {
        let mut columns = IndexMap::new();
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
        let seg7 = Segment::write(
            batch(&["a", "b", "c"], &[Some(1.0), None, Some(3.0)]),
            &schema,
        )
        .unwrap();
        let seg11 = Segment::write(batch(&["d", "e"], &[Some(4.0), Some(5.0)]), &schema).unwrap();

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

    // prune_segments drops every key whose segment_id is in the input list
    // and leaves the rest untouched. Used by TableReader::reopen when an
    // upstream metadata mutation drops a segment from the directory.
    #[test]
    fn prune_segments_drops_listed_segments_keys() {
        let schema = table_schema();
        let seg0 = Segment::write(batch(&["a"], &[Some(1.0)]), &schema).unwrap();
        let seg1 = Segment::write(batch(&["b"], &[Some(2.0)]), &schema).unwrap();
        let seg2 = Segment::write(batch(&["c"], &[Some(3.0)]), &schema).unwrap();

        let mut index = KeyIndex::empty();
        index.add_segment(0, &seg0.keys);
        index.add_segment(1, &seg1.keys);
        index.add_segment(2, &seg2.keys);

        index.prune_segments(&[1]);

        let got = index.get(&["a", "b", "c"]);
        assert_eq!(got[0].map(|l| l.segment_id), Some(0));
        assert_eq!(got[1], None);
        assert_eq!(got[2].map(|l| l.segment_id), Some(2));
    }

    // Empty input is a no-op.
    #[test]
    fn prune_segments_empty_input_is_noop() {
        let schema = table_schema();
        let seg = Segment::write(batch(&["a"], &[Some(1.0)]), &schema).unwrap();
        let mut index = KeyIndex::empty();
        index.add_segment(0, &seg.keys);

        index.prune_segments(&[]);

        assert_eq!(index.get(&["a"])[0].map(|l| l.segment_id), Some(0));
    }

    // Sparse segment ids: deleting a low id while higher ids exist must not
    // touch the higher ones.
    #[test]
    fn prune_segments_retains_keys_above_deleted_id_range() {
        let schema = table_schema();
        let seg2 = Segment::write(batch(&["a"], &[Some(1.0)]), &schema).unwrap();
        let seg9 = Segment::write(batch(&["b"], &[Some(2.0)]), &schema).unwrap();

        let mut index = KeyIndex::empty();
        index.add_segment(2, &seg2.keys);
        index.add_segment(9, &seg9.keys);

        index.prune_segments(&[2]);

        let got = index.get(&["a", "b"]);
        assert_eq!(got[0], None);
        assert_eq!(got[1].map(|l| l.segment_id), Some(9));
    }
}
