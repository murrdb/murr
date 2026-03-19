use std::collections::HashMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::new_null_array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::core::{ColumnSchema, DType};
use crate::core::MurrError;

use super::column::{Column, Float32Column, KeyOffset, Utf8Column};
use super::index::KeyIndex;
use super::view::TableView;

pub struct TableReader<'a> {
    columns: AHashMap<String, Box<dyn Column + 'a>>,
    index: Arc<KeyIndex>,
}

impl<'a> TableReader<'a> {
    /// Build a table reader from a TableView and an explicit column schema.
    ///
    /// If `previous_index` is provided, only new segments (those with IDs
    /// greater than the max segment ID in the previous index) will be scanned
    /// for key indexing. Columns are always rebuilt from all segments.
    pub fn from_table(
        table: &'a TableView,
        key_column: &str,
        schema: &HashMap<String, ColumnSchema>,
        previous_index: Option<Arc<KeyIndex>>,
    ) -> Result<Self, MurrError> {
        let segments = table.segments();
        let num_slots = segments.len();

        let mut columns: AHashMap<String, Box<dyn Column + 'a>> = AHashMap::new();

        for (col_name, col_config) in schema {
            let col_slices: Vec<(u32, &'a [u8])> = segments
                .iter()
                .enumerate()
                .filter_map(|(i, seg)| {
                    let seg = seg.as_ref()?;
                    let data = seg.column(col_name)?;
                    Some((i as u32, data))
                })
                .collect();

            let column: Box<dyn Column + 'a> = match col_config.dtype {
                DType::Float32 => {
                    Box::new(Float32Column::new(col_name, col_config, &col_slices, num_slots)?)
                }
                DType::Utf8 => {
                    Box::new(Utf8Column::new(col_name, col_config, &col_slices, num_slots)?)
                }
            };

            columns.insert(col_name.to_string(), column);
        }

        let index = Arc::new(KeyIndex::build_incremental(
            table,
            key_column,
            previous_index,
        )?);

        Ok(Self { columns, index })
    }

    pub fn index(&self) -> &Arc<KeyIndex> {
        &self.index
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
                    .get(key)
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
    use crate::io::directory::{Directory, LocalDirectory, TableSchema};
    use crate::io::segment::WriteSegment;
    use crate::io::table::column::ColumnSegment as _;
    use crate::io::table::column::float32::segment::Float32Segment;
    use crate::io::table::column::utf8::segment::Utf8Segment;
    use arrow::array::{Array, Float32Array, StringArray};
    use std::fs::File;
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
        let val_bytes = Float32Segment::write(&non_nullable_float32_config(), &val_array).unwrap();

        let mut ws = WriteSegment::new();
        ws.add_column("key", key_bytes);
        ws.add_column("value", val_bytes);

        let path = dir.join(format!("{:08}.seg", id));
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();
    }

    fn make_schema() -> HashMap<String, ColumnSchema> {
        let mut schema = HashMap::new();
        schema.insert(
            "value".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        schema
    }

    fn write_table_json(dir: &std::path::Path) {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "value".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        let schema = TableSchema { key: "key".to_string(), columns };
        let data = serde_json::to_vec_pretty(&schema).unwrap();
        std::fs::write(dir.join("table.json"), data).unwrap();
    }

    async fn open_view(dir: &std::path::Path) -> TableView {
        let local = LocalDirectory::new(dir);
        let index = local.index().await.unwrap().unwrap();
        TableView::open(dir, &index.segments, Vec::new()).unwrap()
    }

    #[tokio::test]
    async fn test_single_segment_get() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["a", "b", "c"], &[1.0, 2.0, 3.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

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

    #[tokio::test]
    async fn test_missing_keys_produce_nulls() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["a", "b"], &[10.0, 20.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

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

    #[tokio::test]
    async fn test_empty_keys_returns_empty_batch() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["a"], &[1.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

        let result = table.get(&[], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 1);
    }

    #[tokio::test]
    async fn test_multi_segment_later_wins() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["a", "b"], &[1.0, 2.0]);
        write_segment(dir.path(), 1, &["a", "c"], &[100.0, 3.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

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

    #[tokio::test]
    async fn test_order_preserved() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["x", "y", "z"], &[1.0, 2.0, 3.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

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

    #[tokio::test]
    async fn test_mixed_string_and_float_columns() {
        let dir = TempDir::new().unwrap();
        {
            let mut columns = HashMap::new();
            columns.insert(
                "key".to_string(),
                ColumnSchema {
                    dtype: DType::Utf8,
                    nullable: false,
                },
            );
            columns.insert(
                "score".to_string(),
                ColumnSchema {
                    dtype: DType::Float32,
                    nullable: true,
                },
            );
            columns.insert(
                "name".to_string(),
                ColumnSchema {
                    dtype: DType::Utf8,
                    nullable: true,
                },
            );
            let schema = TableSchema { key: "key".to_string(), columns };
            let data = serde_json::to_vec_pretty(&schema).unwrap();
            std::fs::write(dir.path().join("table.json"), data).unwrap();
        }

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
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        schema.insert(
            "name".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: true,
            },
        );

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &schema, None).unwrap();

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

    #[tokio::test]
    async fn test_all_missing_keys() {
        let dir = TempDir::new().unwrap();
        write_table_json(dir.path());
        write_segment(dir.path(), 0, &["a"], &[1.0]);

        let view = open_view(dir.path()).await;
        let table = TableReader::from_table(&view, "key", &make_schema(), None).unwrap();

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
