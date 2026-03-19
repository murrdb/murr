use std::sync::Arc;

use ahash::AHashMap;

use crate::core::{ColumnSchema, DType, MurrError};

use super::column::KeyOffset;
use super::column::utf8::segment::Utf8Segment;
use super::column::ColumnSegment;
use super::view::TableView;

pub struct KeyIndex {
    map: AHashMap<String, KeyOffset>,
    max_segment_id: Option<u32>,
}

impl KeyIndex {
    /// Build a full index from scratch over all segments in the view.
    pub fn build(view: &TableView, key_column: &str) -> Result<Self, MurrError> {
        Self::build_incremental(view, key_column, None)
    }

    /// Build incrementally: reuse previous index, only scan new segments.
    /// If `previous` is provided, its Arc MUST have refcount 1 (sole owner).
    /// Returns error if Arc::try_unwrap fails.
    pub fn build_incremental(
        view: &TableView,
        key_column: &str,
        previous: Option<Arc<KeyIndex>>,
    ) -> Result<Self, MurrError> {
        let (mut map, max_old_id) = match previous {
            Some(arc) => {
                let prev = Arc::try_unwrap(arc).map_err(|_| {
                    MurrError::TableError(
                        "previous KeyIndex Arc has multiple owners (bug)".into(),
                    )
                })?;
                let max_id = prev.max_segment_id;
                (prev.map, max_id)
            }
            None => (AHashMap::new(), None),
        };

        let key_config = ColumnSchema {
            dtype: DType::Utf8,
            nullable: false,
        };
        let segment_ids = view.segment_ids();

        let new_ids: Vec<u32> = match max_old_id {
            Some(max_id) => segment_ids
                .iter()
                .filter(|&&id| id > max_id)
                .copied()
                .collect(),
            None => segment_ids,
        };

        let mut max_seg_id = max_old_id;

        for &seg_id in &new_ids {
            let seg = view.segment(seg_id).ok_or_else(|| {
                MurrError::TableError(format!("segment {} not found in view", seg_id))
            })?;
            let seg_data = seg.column(key_column).ok_or_else(|| {
                MurrError::TableError(format!(
                    "key column '{}' not found in segment {}",
                    key_column, seg_id
                ))
            })?;
            let key_seg = Utf8Segment::parse(key_column, &key_config, seg_data)?;

            for row in 0..key_seg.footer.num_values {
                let (start, end) = key_seg.string_range(row);
                let key = std::str::from_utf8(&key_seg.payload[start..end]).map_err(|e| {
                    MurrError::TableError(format!("invalid utf8 in key column: {e}"))
                })?;
                map.insert(
                    key.to_string(),
                    KeyOffset::SegmentOffset {
                        segment_id: seg_id,
                        segment_offset: row,
                    },
                );
            }

            max_seg_id = Some(seg_id);
        }

        Ok(Self {
            map,
            max_segment_id: max_seg_id,
        })
    }

    pub fn get(&self, key: &str) -> Option<KeyOffset> {
        self.map.get(key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::directory::SegmentInfo;
    use crate::io::segment::WriteSegment;
    use crate::io::table::column::float32::segment::Float32Segment;
    use crate::io::table::view::TableView;
    use arrow::array::{Float32Array, StringArray};
    use std::fs::File;
    use std::time::SystemTime;
    use tempfile::TempDir;

    fn non_nullable_utf8_config() -> ColumnSchema {
        ColumnSchema {
            dtype: DType::Utf8,
            nullable: false,
        }
    }

    fn non_nullable_float32_config() -> ColumnSchema {
        ColumnSchema {
            dtype: DType::Float32,
            nullable: false,
        }
    }

    fn write_segment(dir: &std::path::Path, id: u32, keys: &[&str], values: &[f32]) {
        let key_array: StringArray = keys.iter().map(|k| Some(*k)).collect();
        let val_array: Float32Array = values.iter().map(|v| Some(*v)).collect();

        let key_bytes = Utf8Segment::write(&non_nullable_utf8_config(), &key_array).unwrap();
        let val_bytes =
            Float32Segment::write(&non_nullable_float32_config(), &val_array).unwrap();

        let mut ws = WriteSegment::new();
        ws.add_column("key", key_bytes);
        ws.add_column("value", val_bytes);

        let path = dir.join(format!("{:08}.seg", id));
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();
    }

    fn seg_info(id: u32) -> SegmentInfo {
        SegmentInfo {
            id,
            size: 0,
            file_name: format!("{:08}.seg", id),
            last_modified: SystemTime::now(),
        }
    }

    #[test]
    fn test_build_from_scratch() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b", "c"], &[1.0, 2.0, 3.0]);

        let view =
            TableView::open(dir.path(), &[seg_info(0)], Vec::new()).unwrap();
        let index = KeyIndex::build(&view, "key").unwrap();

        assert_eq!(
            index.get("a"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0
            })
        );
        assert_eq!(
            index.get("c"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 2
            })
        );
        assert_eq!(index.get("missing"), None);
    }

    #[test]
    fn test_incremental_build() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b"], &[1.0, 2.0]);

        // Build initial index
        let view =
            TableView::open(dir.path(), &[seg_info(0)], Vec::new()).unwrap();
        let index = Arc::new(KeyIndex::build(&view, "key").unwrap());

        // Add a new segment
        write_segment(dir.path(), 1, &["c", "d"], &[3.0, 4.0]);

        let view = TableView::open(
            dir.path(),
            &[seg_info(0), seg_info(1)],
            view.into_segments(),
        )
        .unwrap();

        // Build incrementally
        let index =
            KeyIndex::build_incremental(&view, "key", Some(index)).unwrap();

        // Old keys still present
        assert_eq!(
            index.get("a"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0
            })
        );
        // New keys added
        assert_eq!(
            index.get("c"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 1,
                segment_offset: 0
            })
        );
    }

    #[test]
    fn test_incremental_overwrites_duplicate_keys() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b"], &[1.0, 2.0]);

        let view =
            TableView::open(dir.path(), &[seg_info(0)], Vec::new()).unwrap();
        let index = Arc::new(KeyIndex::build(&view, "key").unwrap());

        // New segment with overlapping key "a"
        write_segment(dir.path(), 1, &["a", "c"], &[100.0, 3.0]);

        let view = TableView::open(
            dir.path(),
            &[seg_info(0), seg_info(1)],
            view.into_segments(),
        )
        .unwrap();

        let index =
            KeyIndex::build_incremental(&view, "key", Some(index)).unwrap();

        // "a" should point to segment 1 (last write wins)
        assert_eq!(
            index.get("a"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 1,
                segment_offset: 0
            })
        );
        // "b" still from segment 0
        assert_eq!(
            index.get("b"),
            Some(KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 1
            })
        );
    }

    #[test]
    fn test_multiple_owners_returns_error() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a"], &[1.0]);

        let view =
            TableView::open(dir.path(), &[seg_info(0)], Vec::new()).unwrap();
        let index = Arc::new(KeyIndex::build(&view, "key").unwrap());
        let _clone = index.clone(); // second owner

        let result = KeyIndex::build_incremental(&view, "key", Some(index));
        assert!(result.is_err());
    }
}
