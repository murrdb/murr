use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::core::{DType, MurrError, TableSchema};

/// Newtype to implement From<&RecordBatch> (orphan rule prevents impl for serde_json::Value).
pub struct FetchResponse(pub Value);

impl TryFrom<&RecordBatch> for FetchResponse {
    type Error = MurrError;

    fn try_from(batch: &RecordBatch) -> Result<Self, MurrError> {
        let schema = batch.schema();
        let mut columns = Map::new();

        for (i, field) in schema.fields().iter().enumerate() {
            let values = array_to_json_values(batch.column(i))?;
            columns.insert(field.name().clone(), Value::Array(values));
        }

        let mut outer = Map::new();
        outer.insert("columns".to_string(), Value::Object(columns));
        Ok(FetchResponse(Value::Object(outer)))
    }
}

fn array_to_json_values(array: &dyn Array) -> Result<Vec<Value>, MurrError> {
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    Value::Null
                } else {
                    Value::from(arr.value(i))
                }
            })
            .collect())
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    Value::Null
                } else {
                    Value::String(arr.value(i).to_string())
                }
            })
            .collect())
    } else {
        Err(MurrError::ArrowError(format!(
            "unsupported array type: {:?}",
            array.data_type()
        )))
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

            match config.dtype {
                DType::Float32 => {
                    fields.push(Field::new(name, DataType::Float32, config.nullable));
                    let arr: Float32Array = values
                        .iter()
                        .map(|v| match v {
                            Value::Null => Ok(None),
                            Value::Number(n) => n
                                .as_f64()
                                .map(|f| Some(f as f32))
                                .ok_or_else(|| {
                                    MurrError::TableError(format!(
                                        "column '{}': expected number, got {}",
                                        name, v
                                    ))
                                }),
                            _ => Err(MurrError::TableError(format!(
                                "column '{}': expected number, got {}",
                                name, v
                            ))),
                        })
                        .collect::<Result<_, _>>()?;
                    arrays.push(Arc::new(arr));
                }
                DType::Utf8 => {
                    fields.push(Field::new(name, DataType::Utf8, config.nullable));
                    let arr: StringArray = values
                        .iter()
                        .map(|v| match v {
                            Value::Null => Ok(None),
                            Value::String(s) => Ok(Some(s.as_str())),
                            _ => Err(MurrError::TableError(format!(
                                "column '{}': expected string, got {}",
                                name, v
                            ))),
                        })
                        .collect::<Result<_, _>>()?;
                    arrays.push(Arc::new(arr));
                }
                ref other => {
                    return Err(MurrError::ArrowError(format!(
                        "unsupported dtype: {:?}",
                        other
                    )));
                }
            }
        }

        let arrow_schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnConfig, DType};

    fn test_table_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            name: "test".to_string(),
            key: "name".to_string(),
            columns,
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let names: StringArray = vec![Some("alice"), Some("bob")].into_iter().collect();
        let scores: Float32Array = vec![Some(1.5), None].into_iter().collect();
        RecordBatch::try_new(schema, vec![Arc::new(names), Arc::new(scores)]).unwrap()
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
    }

    #[test]
    fn test_columnar_write_to_record_batch() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec![Value::String("alice".into()), Value::String("bob".into())],
        );
        columns.insert(
            "score".to_string(),
            vec![Value::from(1.5), Value::Null],
        );
        let write = WriteRequest { columns };
        let schema = test_table_schema();

        let batch = write.into_record_batch(&schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

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
            orig_names.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            rest_names.as_any().downcast_ref::<StringArray>().unwrap().value(0),
        );

        let orig_scores = original.column_by_name("score").unwrap();
        let rest_scores = restored.column_by_name("score").unwrap();
        let orig_f = orig_scores.as_any().downcast_ref::<Float32Array>().unwrap();
        let rest_f = rest_scores.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(orig_f.value(0), rest_f.value(0));
        assert!(orig_f.is_null(1));
        assert!(rest_f.is_null(1));
    }

    #[test]
    fn test_round_trip_json_to_batch_to_json() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec![Value::String("x".into()), Value::String("y".into())],
        );
        columns.insert(
            "score".to_string(),
            vec![Value::from(3.14), Value::Null],
        );
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
        // f64 3.14 → f32 → f64 round-trip may lose precision, so compare as f32
        let v = score_vals[0].as_f64().unwrap() as f32;
        assert!((v - 3.14f32).abs() < 1e-6);
        assert!(score_vals[1].is_null());
    }
}
