use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::core::{DTypeName, MurrError, TableSchema};

/// For each column in `schema` that has `cast: true`, casts the corresponding column in `batch`
/// to the schema's dtype if the incoming Arrow type differs. Columns not present in `schema` or
/// already at the correct type are left unchanged.
pub fn apply_schema_casts(batch: RecordBatch, schema: &TableSchema) -> Result<RecordBatch, MurrError> {
    let arrow_schema = batch.schema();
    let mut new_fields: Vec<Field> = arrow_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();

    for (col_name, col_schema) in &schema.columns {
        if !col_schema.cast {
            continue;
        }
        let target = DataType::from(&col_schema.dtype);
        let idx = match arrow_schema.index_of(col_name) {
            Ok(i) => i,
            Err(_) => continue,
        };
        if new_columns[idx].data_type() == &target {
            continue;
        }
        new_columns[idx] = arrow::compute::cast(&new_columns[idx], &target)
            .map_err(|e| MurrError::TableError(format!("cast column '{col_name}': {e}")))?;
        new_fields[idx] = Field::new(col_name, target, col_schema.nullable);
    }

    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns).map_err(|e| e.into())
}

/// Newtype to implement From<&RecordBatch> (orphan rule prevents impl for serde_json::Value).
pub struct FetchResponse(pub Value);

impl TryFrom<&RecordBatch> for FetchResponse {
    type Error = MurrError;

    fn try_from(batch: &RecordBatch) -> Result<Self, MurrError> {
        let schema = batch.schema();
        let mut columns = Map::new();

        for (i, field) in schema.fields().iter().enumerate() {
            let column = batch.column(i);
            let dtype = DTypeName::try_from(field.data_type())?;
            let values = dtype.codec().to_json(column.as_ref())?;
            columns.insert(field.name().clone(), Value::Array(values));
        }

        let mut outer = Map::new();
        outer.insert("columns".to_string(), Value::Object(columns));
        Ok(FetchResponse(Value::Object(outer)))
    }
}

#[derive(Deserialize)]
pub struct WriteRequest {
    pub columns: HashMap<String, Vec<Value>>,
}

impl WriteRequest {
    pub fn into_record_batch(self, schema: &TableSchema) -> Result<RecordBatch, MurrError> {
        let mut fields = Vec::new();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        for (name, config) in &schema.columns {
            let values = self.columns.get(name).ok_or_else(|| {
                MurrError::TableError(format!("missing column '{}' in write payload", name))
            })?;

            let codec = config.dtype.codec();
            fields.push(Field::new(name, codec.arrow_dtype(), config.nullable));
            let array = codec
                .from_json(values)
                .map_err(|e| MurrError::TableError(format!("column '{name}': {e}")))?;
            arrays.push(array);
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DTypeName};
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    fn test_table_schema() -> TableSchema {
        let mut columns = indexmap::IndexMap::new();
        columns.insert(
            "name".to_string(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
                cast: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
                cast: false,
            },
        );
        columns.insert(
            "weight".to_string(),
            ColumnSchema {
                dtype: DTypeName::Float64,
                nullable: true,
                cast: false,
            },
        );
        TableSchema {
            key: "name".to_string(),
            columns,
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
            Field::new("weight", DataType::Float64, true),
        ]));
        let names: StringArray = vec![Some("alice"), Some("bob")].into_iter().collect();
        let scores: Float32Array = vec![Some(1.5), None].into_iter().collect();
        let weights: Float64Array = vec![Some(3.15), Some(2.72)].into_iter().collect();
        RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(scores), Arc::new(weights)],
        )
        .unwrap()
    }

    #[test]
    fn test_record_batch_to_json() {
        let batch = test_batch();
        let FetchResponse(json) = FetchResponse::try_from(&batch).unwrap();
        let cols = json.get("columns").unwrap().as_object().unwrap();

        let name_vals = cols.get("name").unwrap().as_array().unwrap();
        assert_eq!(name_vals[0], Value::String("alice".into()));
        assert_eq!(name_vals[1], Value::String("bob".into()));

        let score_vals = cols.get("score").unwrap().as_array().unwrap();
        assert_eq!(score_vals[0], Value::from(1.5f32));
        assert!(score_vals[1].is_null());

        let weight_vals = cols.get("weight").unwrap().as_array().unwrap();
        assert_eq!(weight_vals[0], Value::from(3.15f64));
        assert_eq!(weight_vals[1], Value::from(2.72f64));
    }

    #[test]
    fn test_columnar_write_to_record_batch() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec![Value::String("alice".into()), Value::String("bob".into())],
        );
        columns.insert("score".to_string(), vec![Value::from(1.5), Value::Null]);
        columns.insert(
            "weight".to_string(),
            vec![Value::from(3.15), Value::from(2.72)],
        );
        let write = WriteRequest { columns };
        let schema = test_table_schema();

        let batch = write.into_record_batch(&schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(scores.value(0), 1.5);
        assert!(scores.is_null(1));

        let weights = batch
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(weights.value(0), 3.15);
        assert_eq!(weights.value(1), 2.72);
    }

    #[test]
    fn test_round_trip_batch_to_json_to_batch() {
        let original = test_batch();
        let schema = test_table_schema();

        let FetchResponse(json) = FetchResponse::try_from(&original).unwrap();
        let write: WriteRequest = serde_json::from_value(json).unwrap();
        let restored = write.into_record_batch(&schema).unwrap();

        assert_eq!(restored.num_rows(), original.num_rows());

        let orig_names = original.column_by_name("name").unwrap();
        let rest_names = restored.column_by_name("name").unwrap();
        assert_eq!(
            orig_names
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            rest_names
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
        );

        let orig_scores = original.column_by_name("score").unwrap();
        let rest_scores = restored.column_by_name("score").unwrap();
        let orig_f = orig_scores.as_any().downcast_ref::<Float32Array>().unwrap();
        let rest_f = rest_scores.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(orig_f.value(0), rest_f.value(0));
        assert!(orig_f.is_null(1));
        assert!(rest_f.is_null(1));

        let orig_weights = original.column_by_name("weight").unwrap();
        let rest_weights = restored.column_by_name("weight").unwrap();
        let orig_w = orig_weights
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let rest_w = rest_weights
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(orig_w.value(0), rest_w.value(0));
        assert_eq!(orig_w.value(1), rest_w.value(1));
    }

    #[test]
    fn test_round_trip_json_to_batch_to_json() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec![Value::String("x".into()), Value::String("y".into())],
        );
        columns.insert("score".to_string(), vec![Value::from(2.72), Value::Null]);
        columns.insert("weight".to_string(), vec![Value::from(9.81), Value::Null]);
        let write = WriteRequest { columns };
        let schema = test_table_schema();

        let batch = write.into_record_batch(&schema).unwrap();
        let FetchResponse(json) = FetchResponse::try_from(&batch).unwrap();
        let cols = json.get("columns").unwrap().as_object().unwrap();

        let name_vals = cols.get("name").unwrap().as_array().unwrap();
        assert_eq!(name_vals[0], Value::String("x".into()));
        assert_eq!(name_vals[1], Value::String("y".into()));

        let score_vals = cols.get("score").unwrap().as_array().unwrap();
        // f64 2.72 → f32 → f64 round-trip may lose precision, so compare as f32
        let v = score_vals[0].as_f64().unwrap() as f32;
        assert!((v - 2.72f32).abs() < 1e-6);
        assert!(score_vals[1].is_null());

        let weight_vals = cols.get("weight").unwrap().as_array().unwrap();
        assert_eq!(weight_vals[0], Value::from(9.81));
        assert!(weight_vals[1].is_null());
    }

    fn cast_schema(dtype: DTypeName, cast: bool) -> TableSchema {
        let mut columns = indexmap::IndexMap::new();
        columns.insert("id".to_string(), ColumnSchema { dtype: DTypeName::Utf8, nullable: false, cast: false });
        columns.insert("val".to_string(), ColumnSchema { dtype, nullable: true, cast });
        TableSchema { key: "id".to_string(), columns }
    }

    fn batch_with_types(val_dtype: DataType, vals: Arc<dyn Array>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("val", val_dtype, true),
        ]));
        let ids: StringArray = vec![Some("a")].into_iter().collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), vals]).unwrap()
    }

    #[test]
    fn cast_float64_column_to_float32() {
        let input_vals: Arc<dyn Array> = Arc::new(Float64Array::from(vec![Some(1.5_f64)]));
        let batch = batch_with_types(DataType::Float64, input_vals);
        let schema = cast_schema(DTypeName::Float32, true);

        let result = apply_schema_casts(batch, &schema).unwrap();
        assert_eq!(result.schema().field_with_name("val").unwrap().data_type(), &DataType::Float32);
        let col = result.column_by_name("val").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((arr.value(0) - 1.5_f32).abs() < 1e-6);
    }

    #[test]
    fn cast_int32_column_to_int64() {
        let input_vals: Arc<dyn Array> = Arc::new(Int32Array::from(vec![Some(42_i32)]));
        let batch = batch_with_types(DataType::Int32, input_vals);
        let schema = cast_schema(DTypeName::Int64, true);

        let result = apply_schema_casts(batch, &schema).unwrap();
        assert_eq!(result.schema().field_with_name("val").unwrap().data_type(), &DataType::Int64);
        let col = result.column_by_name("val").unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 42_i64);
    }

    #[test]
    fn cast_disabled_leaves_type_unchanged() {
        let input_vals: Arc<dyn Array> = Arc::new(Float64Array::from(vec![Some(1.5_f64)]));
        let batch = batch_with_types(DataType::Float64, input_vals);
        let schema = cast_schema(DTypeName::Float32, false); // cast: false

        let result = apply_schema_casts(batch, &schema).unwrap();
        // type must remain Float64 — no cast was requested
        assert_eq!(result.schema().field_with_name("val").unwrap().data_type(), &DataType::Float64);
    }

    #[test]
    fn cast_already_matching_type_is_noop() {
        let input_vals: Arc<dyn Array> = Arc::new(Float32Array::from(vec![Some(3.14_f32)]));
        let batch = batch_with_types(DataType::Float32, input_vals);
        let schema = cast_schema(DTypeName::Float32, true);

        let result = apply_schema_casts(batch, &schema).unwrap();
        assert_eq!(result.schema().field_with_name("val").unwrap().data_type(), &DataType::Float32);
    }

    #[test]
    fn cast_incompatible_types_errors() {
        // Utf8 → Float32 is not a valid Arrow cast
        let input_vals: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("not-a-number")]));
        let batch = batch_with_types(DataType::Utf8, input_vals);
        let schema = cast_schema(DTypeName::Float32, true);

        assert!(apply_schema_casts(batch, &schema).is_err());
    }

    #[test]
    fn cast_preserves_nulls() {
        let input_vals: Arc<dyn Array> = Arc::new(Float64Array::from(vec![None, Some(2.0_f64)]));
        let batch = batch_with_types(DataType::Float64, input_vals);
        let schema = cast_schema(DTypeName::Float32, true);

        let result = apply_schema_casts(batch, &schema).unwrap();
        let col = result.column_by_name("val").unwrap();
        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!(arr.is_null(0));
        assert!((arr.value(1) - 2.0_f32).abs() < 1e-6);
    }
}
