use arrow::array::{Array, StringArray};
use hashbrown::HashMap;
use lean_string::LeanString;
use rustc_hash::FxBuildHasher;

//Value payload from the index.
struct RowLocation {
    segment_id: u32,
    offset: u32,
    size: u32,
}

struct RowKey {
    key: String,
    location: RowLocation,
}

pub struct KeyIndex {
    map: HashMap<LeanString, RowLocation, FxBuildHasher>,
}

impl Default for KeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyIndex {
    pub fn new() -> Self {
        KeyIndex {
            map: HashMap::with_capacity_and_hasher(1024, FxBuildHasher),
        }
    }

    pub fn add_segment(&mut self, entries: &impl Iterator<Item = RowKey>) {
        todo!()
    }

    pub fn get(&self, keys: &[&str]) -> Vec<Option<RowLocation>> {
        todo!()
    }
}
