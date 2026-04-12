use arrow::array::{Array, Float32Array, Float64Array, StringArray};
use serde_json::Value;

use crate::core::MurrError;

pub(crate) trait JsonCodec {
    type Array: Array + 'static;
    fn to_json(array: &Self::Array) -> Vec<Value>;
    fn from_json(values: &[Value]) -> Result<Self::Array, MurrError>;
}

impl JsonCodec for f32 {
    type Array = Float32Array;

    fn to_json(array: &Float32Array) -> Vec<Value> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    Value::Null
                } else {
                    Value::from(array.value(i))
                }
            })
            .collect()
    }

    fn from_json(values: &[Value]) -> Result<Float32Array, MurrError> {
        values
            .iter()
            .map(|v| match v {
                Value::Null => Ok(None),
                Value::Number(n) => n
                    .as_f64()
                    .map(|f| Some(f as f32))
                    .ok_or_else(|| MurrError::TableError(format!("expected number, got {v}"))),
                _ => Err(MurrError::TableError(format!("expected number, got {v}"))),
            })
            .collect()
    }
}

impl JsonCodec for f64 {
    type Array = Float64Array;

    fn to_json(array: &Float64Array) -> Vec<Value> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    Value::Null
                } else {
                    Value::from(array.value(i))
                }
            })
            .collect()
    }

    fn from_json(values: &[Value]) -> Result<Float64Array, MurrError> {
        values
            .iter()
            .map(|v| match v {
                Value::Null => Ok(None),
                Value::Number(n) => n
                    .as_f64()
                    .map(Some)
                    .ok_or_else(|| MurrError::TableError(format!("expected number, got {v}"))),
                _ => Err(MurrError::TableError(format!("expected number, got {v}"))),
            })
            .collect()
    }
}

impl JsonCodec for String {
    type Array = StringArray;

    fn to_json(array: &StringArray) -> Vec<Value> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    Value::Null
                } else {
                    Value::String(array.value(i).to_string())
                }
            })
            .collect()
    }

    fn from_json(values: &[Value]) -> Result<StringArray, MurrError> {
        values
            .iter()
            .map(|v| match v {
                Value::Null => Ok(None),
                Value::String(s) => Ok(Some(s.as_str())),
                _ => Err(MurrError::TableError(format!("expected string, got {v}"))),
            })
            .collect()
    }
}

pub(crate) fn downcast_array<A: Array + 'static>(array: &dyn Array) -> Result<&A, MurrError> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| MurrError::ArrowError(format!("downcast failed for {:?}", array.data_type())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f32_round_trip() {
        let original: Float32Array = vec![Some(1.5), None, Some(3.0)].into_iter().collect();
        let json = f32::to_json(&original);
        let restored = f32::from_json(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_f64_round_trip() {
        let original: Float64Array = vec![Some(3.15), None, Some(2.72)].into_iter().collect();
        let json = f64::to_json(&original);
        let restored = f64::from_json(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_string_round_trip() {
        let original: StringArray = vec![Some("hello"), None, Some("world")].into_iter().collect();
        let json = String::to_json(&original);
        let restored = String::from_json(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_f32_from_json_invalid_type() {
        let values = vec![Value::String("not a number".into())];
        assert!(f32::from_json(&values).is_err());
    }

    #[test]
    fn test_string_from_json_invalid_type() {
        let values = vec![Value::from(42)];
        assert!(String::from_json(&values).is_err());
    }
}
