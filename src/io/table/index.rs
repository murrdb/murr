use arrow::array::{Array, StringArray};
use hashbrown::HashMap;
use lean_string::LeanString;
use rustc_hash::FxBuildHasher;

use crate::io::table::key_offset::KeyOffset;

pub struct SegmentOffset {
    pub segment_id: u32,
    pub segment_offset: u32,
}

pub struct KeyIndex {
    map: HashMap<LeanString, SegmentOffset, FxBuildHasher>,
}

impl KeyIndex {
    pub fn new() -> Self {
        KeyIndex {
            map: HashMap::with_hasher(FxBuildHasher),
        }
    }

    pub fn add_segment(&mut self, segment_id: u32, values: &StringArray) {
        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let key = unsafe { LeanString::from(values.value_unchecked(i)) };
            self.map.insert(
                key,
                SegmentOffset {
                    segment_id,
                    segment_offset: i as u32,
                },
            );
        }
    }

    pub fn get(&self, keys: &[&str]) -> Vec<KeyOffset> {
        keys.iter()
            .enumerate()
            .map(|(index, key)| {
                self.map
                    .get(*key)
                    .map(|seg_off| KeyOffset::new(index, seg_off.segment_id, seg_off.segment_offset))
                    .unwrap_or(KeyOffset::missing(index))
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_new_empty_index() {
        let index = KeyIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let results = index.get(&["a", "b"]);
        assert_eq!(results.len(), 2);
        assert!(results[0].is_missing());
        assert!(results[1].is_missing());
    }

    #[test]
    fn test_add_segment_and_get() {
        let mut index = KeyIndex::new();
        let keys: StringArray = vec![Some("a"), Some("b"), Some("c")].into();
        index.add_segment(0, &keys);

        let results = index.get(&["a", "c"]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], KeyOffset::new(0, 0, 0));
        assert_eq!(results[1], KeyOffset::new(1, 0, 2));
    }

    #[test]
    fn test_missing_keys() {
        let mut index = KeyIndex::new();
        let keys: StringArray = vec![Some("a")].into();
        index.add_segment(0, &keys);

        let results = index.get(&["missing"]);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_missing());
        assert_eq!(results[0].request_index, 0);
    }

    #[test]
    fn test_multiple_segments() {
        let mut index = KeyIndex::new();
        let keys0: StringArray = vec![Some("a"), Some("b")].into();
        let keys1: StringArray = vec![Some("c"), Some("d")].into();
        index.add_segment(0, &keys0);
        index.add_segment(1, &keys1);

        assert_eq!(index.len(), 4);

        let results = index.get(&["a", "d"]);
        assert_eq!(results[0], KeyOffset::new(0, 0, 0));
        assert_eq!(results[1], KeyOffset::new(1, 1, 1));
    }

    #[test]
    fn test_duplicate_key_last_segment_wins() {
        let mut index = KeyIndex::new();
        let keys0: StringArray = vec![Some("a"), Some("b")].into();
        let keys1: StringArray = vec![Some("a"), Some("c")].into();
        index.add_segment(0, &keys0);
        index.add_segment(1, &keys1);

        let results = index.get(&["a"]);
        assert_eq!(results[0], KeyOffset::new(0, 1, 0));
    }

    #[test]
    fn test_null_keys_are_skipped() {
        let mut index = KeyIndex::new();
        let keys: StringArray = vec![Some("a"), None, Some("c")].into();
        index.add_segment(0, &keys);

        assert_eq!(index.len(), 2);

        let results = index.get(&["a", "c"]);
        assert_eq!(results[0], KeyOffset::new(0, 0, 0));
        assert_eq!(results[1], KeyOffset::new(1, 0, 2));
    }

    #[test]
    fn test_empty_segment() {
        let mut index = KeyIndex::new();
        let keys: StringArray = Vec::<Option<&str>>::new().into();
        index.add_segment(0, &keys);

        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_get_empty_keys_slice() {
        let mut index = KeyIndex::new();
        let keys: StringArray = vec![Some("a")].into();
        index.add_segment(0, &keys);

        let results = index.get(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_request_index_tracks_input_position() {
        let mut index = KeyIndex::new();
        let keys: StringArray = vec![Some("x")].into();
        index.add_segment(0, &keys);

        let results = index.get(&["missing1", "x", "missing2"]);
        assert!(results[0].is_missing());
        assert_eq!(results[0].request_index, 0);
        assert_eq!(results[1], KeyOffset::new(1, 0, 0));
        assert!(results[2].is_missing());
        assert_eq!(results[2].request_index, 2);
    }
}
