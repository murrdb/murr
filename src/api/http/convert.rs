use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::{Map, Value};

use super::json::{downcast_array, JsonCodec};
use crate::core::{DType, MurrError, TableSchema};

/// Newtype to implement From<&RecordBatch> (orphan rule prevents impl for serde_json::Value).
pub struct FetchResponse(pub Value);

impl TryFrom<&RecordBatch> for FetchResponse {
    type Error = MurrError;

    fn try_from(batch: &RecordBatch) -> Result<Self, MurrError> {
        let schema = batch.schema();
        let mut columns = Map::new();

        for (i, field) in schema.fields().iter().enumerate() {
            let column = batch.column(i);
            let values = match field.data_type() {
                DataType::Float32 => f32::to_json(downcast_array(column)?),
                DataType::Float64 => f64::to_json(downcast_array(column)?),
                DataType::Utf8 => String::to_json(downcast_array(column)?),
                other => {
                    return Err(MurrError::ArrowError(format!(
                        "unsupported array type: {other:?}"
                    )))
                }
            };
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

            fields.push(Field::new(name, DataType::from(&config.dtype), config.nullable));
            let wrap = |e| MurrError::TableError(format!("column '{name}': {e}"));
            let array: Arc<dyn Array> = match config.dtype {
                DType::Float32 => Arc::new(f32::from_json(values).map_err(wrap)?),
                DType::Float64 => Arc::new(f64::from_json(values).map_err(wrap)?),
                DType::Utf8 => Arc::new(String::from_json(values).map_err(wrap)?),
            };
            arrays.push(array);
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, StringArray};
    use crate::core::{ColumnSchema, DType};

    fn test_table_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
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
            "weight".to_string(),
            ColumnSchema {
                dtype: DType::Float64,
                nullable: true,
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
        columns.insert("weight".to_string(), vec![Value::from(3.15), Value::from(2.72)]);
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

        // Batch → JSON
        let FetchResponse(json) = FetchResponse::try_from(&original).unwrap();

        // JSON → WriteRequest → Batch
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
        let orig_w = orig_weights.as_any().downcast_ref::<Float64Array>().unwrap();
        let rest_w = rest_weights.as_any().downcast_ref::<Float64Array>().unwrap();
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

        // JSON → Batch
        let batch = write.into_record_batch(&schema).unwrap();

        // Batch → JSON
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
}
