use std::collections::HashMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, StringArray, new_null_array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::conf::{ColumnConfig, DType};
use crate::core::MurrError;
use crate::directory::Directory;

use super::column::read_u32;
use super::column::{Column, Float32Column, KeyOffset, Utf8Column};

pub struct Table2<'a> {
    columns: AHashMap<String, Box<dyn Column + 'a>>,
    index: AHashMap<String, KeyOffset>,
}

impl<'a> Table2<'a> {
    /// Build a table from a directory of segments and an explicit column schema.
    ///
    /// The key column is always loaded as a non-nullable `Utf8Column` and
    /// is used to build the key index. It does not need to appear in `schema`,
    /// but if present it will also be queryable via `get()`.
    ///
    /// Segments are processed in directory order (sorted by filename). When
    /// multiple segments contain the same key, the last segment wins.
    pub fn from_directory(
        dir: &'a dyn Directory,
        key_column: &str,
        schema: &HashMap<String, ColumnConfig>,
    ) -> Result<Self, MurrError> {
        let segments = dir.segments();

        let mut columns: AHashMap<String, Box<dyn Column + 'a>> = AHashMap::new();

        for (col_name, col_config) in schema {
            let col_slices: Vec<&'a [u8]> = segments
                .iter()
                .map(|seg| {
                    seg.column(col_name).ok_or_else(|| {
                        MurrError::TableError(format!(
                            "column '{}' not found in segment {}",
                            col_name,
                            seg.id()
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;

            let column: Box<dyn Column + 'a> = match col_config.dtype {
                DType::Float32 => Box::new(Float32Column::new(col_name, col_config, &col_slices)?),
                DType::Utf8 => Box::new(Utf8Column::new(col_name, col_config, &col_slices)?),
                ref other => {
                    return Err(MurrError::TableError(format!(
                        "unsupported dtype {:?} for column '{}'",
                        other, col_name
                    )));
                }
            };

            columns.insert(col_name.to_string(), column);
        }

        // Collect key column slices and per-segment sizes.
        let key_slices: Vec<&'a [u8]> = segments
            .iter()
            .map(|seg| {
                seg.column(key_column).ok_or_else(|| {
                    MurrError::TableError(format!(
                        "key column '{}' not found in segment {}",
                        key_column,
                        seg.id()
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        // Read per-segment sizes from wire format headers (first u32 = num_values).
        let seg_sizes: Vec<u32> = key_slices
            .iter()
            .map(|data| {
                if data.len() < 4 {
                    return Err(MurrError::TableError(
                        "key column segment too small for num_values".into(),
                    ));
                }
                Ok(read_u32(data, 0))
            })
            .collect::<Result<_, _>>()?;

        let key_config = ColumnConfig {
            dtype: DType::Utf8,
            nullable: false,
        };
        let key_col = Utf8Column::new(key_column, &key_config, &key_slices)?;
        let key_array = key_col.get_all()?;
        let key_strings = key_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MurrError::TableError("key column must produce StringArray".into()))?;

        // Build index: walk the flat key array, tracking segment boundaries.
        let mut index: AHashMap<String, KeyOffset> = AHashMap::new();
        let mut flat_pos: usize = 0;

        for (seg_idx, &seg_size) in seg_sizes.iter().enumerate() {
            for row in 0..seg_size {
                if !key_strings.is_null(flat_pos) {
                    index.insert(
                        key_strings.value(flat_pos).to_string(),
                        KeyOffset::SegmentOffset {
                            segment_id: seg_idx as u32,
                            segment_offset: row,
                        },
                    );
                }
                flat_pos += 1;
            }
        }

        Ok(Self { columns, index })
    }

    pub fn get(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        let resolved: Vec<&Box<dyn Column + 'a>> = columns
            .iter()
            .map(|name| {
                self.columns
                    .get(*name)
                    .ok_or_else(|| MurrError::TableError(format!("column '{}' not found", name)))
            })
            .collect::<Result<_, _>>()?;

        let result_schema = Arc::new(Schema::new(
            resolved
                .iter()
                .map(|col| col.field().clone())
                .collect::<Vec<_>>(),
        ));

        if keys.is_empty() {
            let empty_arrays: Vec<_> = result_schema
                .fields()
                .iter()
                .map(|f| new_null_array(f.data_type(), 0))
                .collect();
            return Ok(RecordBatch::try_new(result_schema, empty_arrays)?);
        }

        let key_offsets: Vec<KeyOffset> = keys
            .iter()
            .map(|key| {
                self.index
                    .get(*key)
                    .copied()
                    .unwrap_or(KeyOffset::MissingKey)
            })
            .collect();

        let arrays: Result<Vec<_>, MurrError> = resolved
            .iter()
            .map(|col| col.get_indexes(&key_offsets))
            .collect();

        Ok(RecordBatch::try_new(result_schema, arrays?)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::LocalDirectory;
    use crate::segment::WriteSegment;
    use crate::table::column::ColumnSegment as _;
    use crate::table::column::float32::segment::Float32Segment;
    use crate::table::column::utf8::segment::Utf8Segment;
    use arrow::array::Float32Array;
    use std::fs::File;
    use tempfile::TempDir;

    fn non_nullable_utf8_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: false,
        }
    }

    fn non_nullable_float32_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Float32,
            nullable: false,
        }
    }

    fn write_segment(dir: &std::path::Path, id: u32, keys: &[&str], values: &[f32]) {
        let key_array: StringArray = keys.iter().map(|k| Some(*k)).collect();
        let val_array: Float32Array = values.iter().map(|v| Some(*v)).collect();

        let key_bytes = Utf8Segment::write(&non_nullable_utf8_config(), &key_array).unwrap();
        let val_bytes = Float32Segment::write(&non_nullable_float32_config(), &val_array).unwrap();

        let mut ws = WriteSegment::new();
        ws.add_column("key", key_bytes);
        ws.add_column("value", val_bytes);

        let path = dir.join(format!("{:08}.seg", id));
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();
    }

    fn make_schema() -> HashMap<String, ColumnConfig> {
        let mut schema = HashMap::new();
        schema.insert(
            "value".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        schema
    }

    #[test]
    fn test_single_segment_get() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b", "c"], &[1.0, 2.0, 3.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&["b", "a"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 2.0);
        assert_eq!(vals.value(1), 1.0);
    }

    #[test]
    fn test_missing_keys_produce_nulls() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b"], &[10.0, 20.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&["a", "missing", "b"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 3);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 10.0);
        assert!(vals.is_null(1));
        assert_eq!(vals.value(2), 20.0);
    }

    #[test]
    fn test_empty_keys_returns_empty_batch() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a"], &[1.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&[], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 1);
    }

    #[test]
    fn test_multi_segment_later_wins() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a", "b"], &[1.0, 2.0]);
        write_segment(dir.path(), 1, &["a", "c"], &[100.0, 3.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&["a", "b", "c"], &["value"]).unwrap();
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(vals.value(0), 100.0); // "a" from segment 1
        assert_eq!(vals.value(1), 2.0); // "b" from segment 0
        assert_eq!(vals.value(2), 3.0); // "c" from segment 1
    }

    #[test]
    fn test_order_preserved() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["x", "y", "z"], &[1.0, 2.0, 3.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&["z", "x", "y"], &["value"]).unwrap();
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(vals.value(0), 3.0);
        assert_eq!(vals.value(1), 1.0);
        assert_eq!(vals.value(2), 2.0);
    }

    #[test]
    fn test_mixed_string_and_float_columns() {
        let dir = TempDir::new().unwrap();

        let keys: StringArray = ["k1", "k2"].iter().map(|k| Some(*k)).collect();
        let floats: Float32Array = [10.0f32, 20.0].iter().map(|v| Some(*v)).collect();
        let names: StringArray = ["alice", "bob"].iter().map(|s| Some(*s)).collect();

        let key_bytes = Utf8Segment::write(&non_nullable_utf8_config(), &keys).unwrap();
        let float_bytes = Float32Segment::write(&non_nullable_float32_config(), &floats).unwrap();
        let name_bytes = Utf8Segment::write(&non_nullable_utf8_config(), &names).unwrap();

        let mut ws = WriteSegment::new();
        ws.add_column("key", key_bytes);
        ws.add_column("score", float_bytes);
        ws.add_column("name", name_bytes);

        let path = dir.path().join("00000000.seg");
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        let mut schema = HashMap::new();
        schema.insert(
            "score".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        schema.insert(
            "name".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: true,
            },
        );

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &schema).unwrap();

        let result = table.get(&["k2", "k1"], &["score", "name"]).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let scores = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(scores.value(0), 20.0);
        assert_eq!(scores.value(1), 10.0);

        let result_names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(result_names.value(0), "bob");
        assert_eq!(result_names.value(1), "alice");
    }

    #[test]
    fn test_all_missing_keys() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &["a"], &[1.0]);

        let local = LocalDirectory::open(dir.path()).unwrap();
        let table = Table2::from_directory(&local, "key", &make_schema()).unwrap();

        let result = table.get(&["x", "y", "z"], &["value"]).unwrap();
        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(vals.len(), 3);
        assert!(vals.is_null(0));
        assert!(vals.is_null(1));
        assert!(vals.is_null(2));
    }
}
