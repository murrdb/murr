use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    core::{FetchRequest, MurrError, TableSchema},
    io::{
        codec::ColumnDecoder,
        key::KeySchema,
        row::{read::ReadBatchBuilder, write::WriteRow},
        schema::{SegmentColumnSchema, SegmentSchema},
        store::{KeyValue, Store},
    },
};
use arrow::{
    array::{Array, RecordBatch},
    datatypes::Schema,
};

pub struct Table<S: Store> {
    store: Arc<RwLock<S>>,
    name: String,
    table: TableSchema,
    key: KeySchema,
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

        let keys = self.key.encode(&ordered)?;

        let mut decoders: Vec<Box<dyn ColumnDecoder>> =
            Vec::with_capacity(self.segment.columns.len());
        for col in &self.segment.columns {
            let arr_idx = canonical
                .index_of(&col.name)
                .map_err(|e| MurrError::ArrowError(e.to_string()))?;
            decoders.push(
                col.dtype
                    .codec()
                    .make_decoder(col.clone(), ordered.column(arr_idx).as_ref())?,
            );
        }

        let n = ordered.num_rows();
        let mut store = self.store.write().expect("store lock poisoned");

        store.write(
            &self.name,
            (0..n).into_iter().map(|i| {
                let mut row = WriteRow::new(&self.segment);
                for d in &decoders {
                    d.write_to_row(i, &mut row);
                }
                KeyValue::new(keys.value(i), row.bytes)
            }),
        )?;

        Ok(())
    }

    pub fn read(&self, request: &FetchRequest) -> Result<RecordBatch, MurrError> {
        if request.keys.num_columns() != self.key.num_columns() {
            return Err(MurrError::TableError(format!(
                "expected {} key columns, got {}",
                self.key.num_columns(),
                request.keys.num_columns()
            )));
        }
        let keys = self.key.encode(&request.keys)?;

        let req_cols: Vec<&SegmentColumnSchema> = request
            .columns
            .iter()
            .map(|name| {
                self.columns
                    .get(name)
                    .map(|idx| &self.segment.columns[*idx])
                    .ok_or_else(|| MurrError::SegmentError(format!("column '{name}' not found")))
            })
            .collect::<Result<_, _>>()?;

        let builder = ReadBatchBuilder::new(&self.segment, req_cols, keys.len());
        let key_bytes: Vec<&[u8]> = (0..keys.len()).map(|i| keys.value(i)).collect();
        let store = self.store.read().expect("store lock poisoned");
        store.read(&self.name, &key_bytes, builder)
    }

    fn build(store: Arc<RwLock<S>>, name: String, table: TableSchema) -> Result<Self, MurrError> {
        let key = KeySchema::new(&table)?;
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
            key,
            segment,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch,
        StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use indexmap::IndexMap;
    use rstest::rstest;

    use super::*;
    use crate::core::{ColumnSchema, DTypeName, TableSchema};
    use crate::io::store::memory::MemoryStore;

    fn store() -> Arc<RwLock<MemoryStore>> {
        Arc::new(RwLock::new(MemoryStore::new()))
    }

    fn schema_id_score() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
                key: true,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
                key: false,
            },
        );
        TableSchema { columns }
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

    fn request(keys: Vec<(&str, ArrayRef)>, columns: &[&str]) -> FetchRequest {
        FetchRequest {
            keys: RecordBatch::try_from_iter(keys).unwrap(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
        }
    }

    fn fetch(ids: &[&str], columns: &[&str]) -> FetchRequest {
        request(
            vec![("id", Arc::new(StringArray::from(ids.to_vec())))],
            columns,
        )
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

        let out = table.read(&fetch(&["a", "b", "c"], &["score"])).unwrap();
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
                dtype: DTypeName::Utf8,
                nullable: false,
                key: true,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
                key: false,
            },
        );
        columns.insert(
            "label".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: true,
                key: false,
            },
        );
        let schema = TableSchema { columns };

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

        let out = table
            .read(&fetch(&["a", "b"], &["label", "score"]))
            .unwrap();
        assert_eq!(out.schema().field(0).name(), "label");
        assert_eq!(out.schema().field(1).name(), "score");

        let out = table
            .read(&fetch(&["a", "b"], &["score", "label"]))
            .unwrap();
        assert_eq!(out.schema().field(0).name(), "score");
        assert_eq!(out.schema().field(1).name(), "label");
    }

    #[test]
    fn read_subset_of_columns() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.5)]))
            .unwrap();
        let out = table.read(&fetch(&["a"], &["score"])).unwrap();
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

        let out = table.read(&fetch(&["a"], &["score"])).unwrap();
        assert_eq!(project_f32(&out, "score").value(0), 7.0);
    }

    #[test]
    fn read_missing_keys_returns_nulls() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.0)]))
            .unwrap();

        let out = table.read(&fetch(&["a", "missing"], &["score"])).unwrap();
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
        let err = table.read(&fetch(&["a"], &["nope"])).unwrap_err();
        assert!(matches!(err, MurrError::SegmentError(_)));
    }

    #[test]
    fn read_key_column_errors() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        table
            .write(&batch_id_score(&[Some("a")], &[Some(1.0)]))
            .unwrap();
        let err = table.read(&fetch(&["a"], &["id"])).unwrap_err();
        assert!(matches!(err, MurrError::SegmentError(_)));
    }

    #[test]
    fn write_with_null_key_errors() {
        let table = Table::create(store(), "t", schema_id_score()).unwrap();
        let err = table
            .write(&batch_id_score(&[None], &[Some(1.0)]))
            .unwrap_err();
        assert!(matches!(err, MurrError::TableError(_)));
    }

    #[test]
    fn mixed_dtypes_roundtrip() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
                key: true,
            },
        );
        columns.insert(
            "f32".into(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
                key: false,
            },
        );
        columns.insert(
            "f64".into(),
            ColumnSchema {
                dtype: DTypeName::Float64,
                nullable: true,
                key: false,
            },
        );
        columns.insert(
            "label".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: true,
                key: false,
            },
        );
        let schema = TableSchema { columns };

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
            .read(&fetch(&["a", "b", "c"], &["f32", "f64", "label"]))
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
        let out = table.read(&fetch(&["a"], &["score"])).unwrap();
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

    fn column(dtype: DTypeName, nullable: bool, key: bool) -> ColumnSchema {
        ColumnSchema {
            dtype,
            nullable,
            key,
        }
    }

    fn array<A: Array + From<Vec<T>> + 'static, T>(values: Vec<T>) -> ArrayRef {
        Arc::new(A::from(values))
    }

    #[rstest]
    #[case::float_key(column(DTypeName::Float32, false, true))]
    #[case::nullable_key(column(DTypeName::Utf8, true, true))]
    #[case::no_key(column(DTypeName::Utf8, false, false))]
    fn invalid_key_schema_rejected(#[case] id: ColumnSchema) {
        let schema = TableSchema {
            columns: IndexMap::from([("id".to_string(), id)]),
        };
        assert!(matches!(
            Table::create(store(), "t", schema),
            Err(MurrError::TableError(_))
        ));
    }

    /// Creates a table keyed by `keys` (name, dtype, values) with row i scored as i.
    fn keyed_table(keys: Vec<(&str, DTypeName, ArrayRef)>) -> Table<MemoryStore> {
        let rows = keys[0].2.len();
        let mut columns = IndexMap::new();
        let mut arrays = Vec::new();
        for (name, dtype, values) in keys {
            columns.insert(name.to_string(), column(dtype, false, true));
            arrays.push((name, values));
        }
        columns.insert("score".to_string(), column(DTypeName::Float32, true, false));
        let scores: Vec<f32> = (0..rows).map(|i| i as f32).collect();
        arrays.push(("score", array::<Float32Array, _>(scores)));

        let table = Table::create(store(), "t", TableSchema { columns }).unwrap();
        table
            .write(&RecordBatch::try_from_iter(arrays).unwrap())
            .unwrap();
        table
    }

    #[rstest]
    #[case::single_int(
        vec![("id", DTypeName::Int32, array::<Int32Array, _>(vec![-1, 0, i32::MAX]))],
        vec![("id", array::<Int32Array, _>(vec![i32::MAX, 5, -1]))],
        vec![Some(2.0), None, Some(0.0)],
    )]
    #[case::utf8_int(
        vec![
            ("user", DTypeName::Utf8, array::<StringArray, _>(vec!["u1", "u1", "u2"])),
            ("item", DTypeName::Int64, array::<Int64Array, _>(vec![1, -2, 1])),
        ],
        vec![
            ("user", array::<StringArray, _>(vec!["u2", "u1", "u2"])),
            ("item", array::<Int64Array, _>(vec![1, -2, -2])),
        ],
        vec![Some(2.0), Some(1.0), None],
    )]
    #[case::int_int(
        vec![
            ("shard", DTypeName::UInt8, array::<UInt8Array, _>(vec![1, 1, 2])),
            ("id", DTypeName::UInt64, array::<UInt64Array, _>(vec![0, u64::MAX, 0])),
        ],
        vec![
            ("shard", array::<UInt8Array, _>(vec![2, 1, 2])),
            ("id", array::<UInt64Array, _>(vec![0, u64::MAX, u64::MAX])),
        ],
        vec![Some(2.0), Some(1.0), None],
    )]
    // ("ab", "c") and ("a", "bc") concatenate to the same bytes unless components are delimited
    #[case::utf8_utf8_boundary(
        vec![
            ("a", DTypeName::Utf8, array::<StringArray, _>(vec!["ab"])),
            ("b", DTypeName::Utf8, array::<StringArray, _>(vec!["c"])),
        ],
        vec![
            ("a", array::<StringArray, _>(vec!["a", "ab"])),
            ("b", array::<StringArray, _>(vec!["bc", "c"])),
        ],
        vec![None, Some(0.0)],
    )]
    #[case::three_components(
        vec![
            ("a", DTypeName::Utf8, array::<StringArray, _>(vec!["x", "x"])),
            ("b", DTypeName::Int16, array::<Int16Array, _>(vec![7, 7])),
            ("c", DTypeName::Utf8, array::<StringArray, _>(vec!["", "y"])),
        ],
        vec![
            ("a", array::<StringArray, _>(vec!["x", "x", "x"])),
            ("b", array::<Int16Array, _>(vec![7, 7, 8])),
            ("c", array::<StringArray, _>(vec!["y", "", ""])),
        ],
        vec![Some(1.0), Some(0.0), None],
    )]
    fn key_roundtrip(
        #[case] keys: Vec<(&str, DTypeName, ArrayRef)>,
        #[case] lookup: Vec<(&str, ArrayRef)>,
        #[case] expected: Vec<Option<f32>>,
    ) {
        let table = keyed_table(keys);
        let out = table.read(&request(lookup, &["score"])).unwrap();
        assert_eq!(project_f32(&out, "score"), Float32Array::from(expected));
    }

    #[rstest]
    #[case::wrong_type(vec![
        ("user", array::<StringArray, _>(vec!["u1"])),
        ("item", array::<Int32Array, _>(vec![1])),
    ])]
    #[case::null_key(vec![
        ("user", array::<StringArray, _>(vec![None::<&str>])),
        ("item", array::<Int64Array, _>(vec![1])),
    ])]
    #[case::missing_column(vec![("user", array::<StringArray, _>(vec!["u1"]))])]
    #[case::extra_column(vec![
        ("user", array::<StringArray, _>(vec!["u1"])),
        ("item", array::<Int64Array, _>(vec![1])),
        ("score", array::<Float32Array, _>(vec![1.0])),
    ])]
    fn read_rejects_invalid_keys(#[case] lookup: Vec<(&str, ArrayRef)>) {
        let table = keyed_table(vec![
            ("user", DTypeName::Utf8, array::<StringArray, _>(vec!["u1"])),
            ("item", DTypeName::Int64, array::<Int64Array, _>(vec![1])),
        ]);
        let err = table.read(&request(lookup, &["score"])).unwrap_err();
        assert!(matches!(err, MurrError::TableError(_)));
    }
}
