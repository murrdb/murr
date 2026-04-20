use arrow::array::{Array, StringArray};
use hashbrown::HashMap;
use lean_string::LeanString;
use rustc_hash::FxBuildHasher;

use crate::io::table::key_offset::KeyOffset;

//Value payload from the index. 
struct SegmentRowOffset {
    segment_id: u32,
    offset: u32,
    size: u32
}

pub struct KeyIndex {
    map: HashMap<LeanString, SegmentRowOffset, FxBuildHasher>,
}

impl Default for KeyIndex {
    fn default() -> Self {
        Self::new()
    }
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
                SegmentRowOffset {
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
                    .map(|seg_off| {
                        KeyOffset::new(index, seg_off.segment_id, seg_off.segment_offset)
                    })
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

