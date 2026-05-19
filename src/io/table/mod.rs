use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    core::{DType, MurrError, TableSchema},
    io::{
        column::{ColumnDecoder, decoder_for},
        row::{read::ReadBatchBuilder, write::WriteRow},
        schema::{SegmentColumnSchema, SegmentSchema},
        store::Store,
    },
};
use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::Schema,
};

pub struct Table<S: Store> {
    store: Arc<RwLock<S>>,
    name: String,
    table: TableSchema,
    segment: SegmentSchema,
    columns: HashMap<String, usize>,
}

impl<S: Store> Table<S> {
    pub fn create(
        store: Arc<RwLock<S>>,
        name: impl Into<String>,
        table: TableSchema,
    ) -> Result<Self, MurrError> {
        let name = name.into();
        store
            .write()
            .expect("store lock poisoned")
            .create_table(&name, &table)?;
        Self::build(store, name, table)
    }

    pub fn open(
        store: Arc<RwLock<S>>,
        name: impl Into<String>,
        table: TableSchema,
    ) -> Result<Self, MurrError> {
        Self::build(store, name.into(), table)
    }

    pub fn schema(&self) -> &TableSchema {
        &self.table
    }

    pub fn write(&self, batch: &RecordBatch) -> Result<(), MurrError> {
        let canonical: Schema = (&self.table).into();
        let indices: Vec<usize> = canonical
            .fields()
            .iter()
            .map(|f| {
                batch
                    .schema()
                    .index_of(f.name())
                    .map_err(|e| MurrError::ArrowError(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let ordered = batch
            .project(&indices)
            .map_err(|e| MurrError::ArrowError(e.to_string()))?;

        let key_idx = canonical
            .index_of(&self.table.key)
            .map_err(|e| MurrError::ArrowError(e.to_string()))?;
        let key_array = ordered
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!("key column '{}' must be Utf8", self.table.key))
            })?;
        if key_array.null_count() > 0 {
            return Err(MurrError::SegmentError("null in key column".into()));
        }

        let mut decoders: Vec<Box<dyn ColumnDecoder>> =
            Vec::with_capacity(self.segment.columns.len());
        for col in &self.segment.columns {
            let arr_idx = canonical
                .index_of(&col.name)
                .map_err(|e| MurrError::ArrowError(e.to_string()))?;
            decoders.push(decoder_for(col, ordered.column(arr_idx).as_ref())?);
        }

        let n = ordered.num_rows();
        let mut store = self.store.write().expect("store lock poisoned");

        store.write(
            &self.name,
            (0..n).into_iter().map(|i| {
                let mut row = WriteRow::new(&self.segment, key_array.value(i));
                for d in &decoders {
                    d.write_to_row(i, &mut row);
                }
                row.into()
            }),
        )?;

        Ok(())
    }

    pub fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        let req_cols: Vec<&SegmentColumnSchema> = columns
            .iter()
            .map(|name| {
                self.columns
                    .get(*name)
                    .map(|idx| &self.segment.columns[*idx])
                    .ok_or_else(|| MurrError::SegmentError(format!("column '{name}' not found")))
            })
            .collect::<Result<_, _>>()?;

        let builder = ReadBatchBuilder::new(&self.segment, req_cols, keys.len());
        let key_bytes: Vec<&[u8]> = keys.iter().map(|s| s.as_bytes()).collect();
        let store = self.store.read().expect("store lock poisoned");
        store.read(&self.name, &key_bytes, builder)
    }

    fn build(store: Arc<RwLock<S>>, name: String, table: TableSchema) -> Result<Self, MurrError> {
        let key_col = table.columns.get(&table.key).ok_or_else(|| {
            MurrError::TableError(format!("key column '{}' not in schema", table.key))
        })?;
        if key_col.dtype != DType::Utf8 {
            return Err(MurrError::TableError(
                "io currently supports Utf8 keys only".into(),
            ));
        }
        let segment = SegmentSchema::from(&table);
        let columns = segment
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Ok(Self {
            store,
            name,
            table,
            segment,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use arrow::array::{Float32Array, Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use indexmap::IndexMap;

    use super::*;
    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::store::memory::MemoryStore;

    fn store() -> Arc<RwLock<MemoryStore>> {
        Arc::new(RwLock::new(MemoryStore::new()))
    }

    fn schema_id_score() -> TableSchema {
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

    fn batch_id_score(ids: &[Option<&str>], scores: &[Option<f32>]) -> RecordBatch {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("score", DataType::Float32, true),
        ]));
        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Float32Array::from(scores.to_vec())),
            ],
        )
        .unwrap()
    }

    fn project_f32(batch: &RecordBatch, name: &str) -> Float32Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone()
    }

    fn project_string(batch: &RecordBatch, name: &str) -> StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    }

    #[test]
    fn roundtrip_writes_and_reads_back() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(
                &[Some("a"), Some("b"), Some("c")],
                &[Some(1.0), None, Some(3.0)],
            ))
            .unwrap();

        let out = table.read(&["a", "b", "c"], &["score"]).unwrap();
        assert_eq!(out.num_rows(), 3);
        let scores = project_f32(&out, "score");
        assert_eq!(scores.value(0), 1.0);
        assert!(scores.is_null(1));
        assert_eq!(scores.value(2), 3.0);
    }

    #[test]
    fn read_returns_columns_in_request_order() {
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
        columns.insert(
            "label".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: true,
            },
        );
        let schema = TableSchema {
            key: "id".into(),
            columns,
        };

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float32Array::from(vec![Some(1.0), Some(2.0)])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
            ],
        )
        .unwrap();

        let table = Table::create(store(), "t", schema).unwrap();
        table.write(&batch).unwrap();

        let out = table.read(&["a", "b"], &["label", "score"]).unwrap();
        assert_eq!(out.schema().field(0).name(), "label");
        assert_eq!(out.schema().field(1).name(), "score");

        let out = table.read(&["a", "b"], &["score", "label"]).unwrap();
        assert_eq!(out.schema().field(0).name(), "score");
        assert_eq!(out.schema().field(1).name(), "label");
    }

    #[test]
    fn read_subset_of_columns() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.5)]))
            .unwrap();
        let out = table.read(&["a"], &["score"]).unwrap();
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.schema().field(0).name(), "score");
        assert_eq!(project_f32(&out, "score").value(0), 1.5);
    }

    #[test]
    fn write_reorders_columns() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("score", DataType::Float32, true),
            Field::new("id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Float32Array::from(vec![Some(7.0)])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table.write(&batch).unwrap();

        let out = table.read(&["a"], &["score"]).unwrap();
        assert_eq!(project_f32(&out, "score").value(0), 7.0);
    }

    #[test]
    fn read_missing_keys_returns_nulls() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.0)]))
            .unwrap();

        let out = table.read(&["a", "missing"], &["score"]).unwrap();
        let scores = project_f32(&out, "score");
        assert_eq!(scores.value(0), 1.0);
        assert!(scores.is_null(1));
    }

    #[test]
    fn read_unknown_column_errors() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.0)]))
            .unwrap();
        let err = table.read(&["a"], &["nope"]).unwrap_err();
        assert!(matches!(err, MurrError::SegmentError(_)));
    }

    #[test]
    fn read_key_column_errors() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.0)]))
            .unwrap();
        let err = table.read(&["a"], &["id"]).unwrap_err();
        assert!(matches!(err, MurrError::SegmentError(_)));
    }

    #[test]
    fn write_with_null_key_errors() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        let err = table
            .write(&batch_id_score(&[None], &[Some(1.0)]))
            .unwrap_err();
        assert!(matches!(err, MurrError::SegmentError(_)));
    }

    #[test]
    fn mixed_dtypes_roundtrip() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "f32".into(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        columns.insert(
            "f64".into(),
            ColumnSchema {
                dtype: DType::Float64,
                nullable: true,
            },
        );
        columns.insert(
            "label".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: true,
            },
        );
        let schema = TableSchema {
            key: "id".into(),
            columns,
        };

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-2.5)])),
                Arc::new(Float64Array::from(vec![None, Some(2.0), Some(3.0)])),
                Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
            ],
        )
        .unwrap();

        let table = Table::create(store(), "t", schema).unwrap();
        table.write(&batch).unwrap();

        let out = table
            .read(&["a", "b", "c"], &["f32", "f64", "label"])
            .unwrap();
        let f32 = out
            .column_by_name("f32")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let f64 = out
            .column_by_name("f64")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let label = project_string(&out, "label");

        assert_eq!(f32.value(0), 1.5);
        assert!(f32.is_null(1));
        assert_eq!(f32.value(2), -2.5);
        assert!(f64.is_null(0));
        assert_eq!(f64.value(1), 2.0);
        assert_eq!(f64.value(2), 3.0);
        assert_eq!(label.value(0), "x");
        assert!(label.is_null(1));
        assert_eq!(label.value(2), "z");
    }

    #[test]
    fn create_then_open_roundtrip() {
        let s = store();
        {
            let table = Table::create(s.clone(), "t", schema_id_score()).unwrap();
            table
                .write(&batch_id_score(&[Some("a")], &[Some(9.0)]))
                .unwrap();
        }
        let table = Table::open(s.clone(), "t", schema_id_score()).unwrap();
        let out = table.read(&["a"], &["score"]).unwrap();
        assert_eq!(project_f32(&out, "score").value(0), 9.0);
    }

    #[test]
    fn create_duplicate_errors() {
        let s = store();
        Table::create(s.clone(), "t", schema_id_score()).unwrap();
        assert!(matches!(
            Table::create(s, "t", schema_id_score()),
            Err(MurrError::TableAlreadyExists(_))
        ));
    }

    #[test]
    fn non_utf8_key_rejected() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
        let schema = TableSchema {
            key: "id".into(),
            columns,
        };
        assert!(matches!(
            Table::create(store(), "t", schema),
            Err(MurrError::TableError(_))
        ));
    }
}
