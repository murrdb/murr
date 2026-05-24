use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::core::MurrError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    Utf8,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
}

impl DType {
    pub fn size(&self) -> usize {
        match self {
            DType::Utf8 => 4,
            DType::Bool => 1,
            DType::Int8 => 1,
            DType::Int16 => 2,
            DType::Int32 => 4,
            DType::Int64 => 8,
            DType::UInt8 => 1,
            DType::UInt16 => 2,
            DType::UInt32 => 4,
            DType::UInt64 => 8,
            DType::Float32 => 4,
            DType::Float64 => 8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub dtype: DType,
    #[serde(default = "ColumnSchema::default_nullable")]
    pub nullable: bool,
    /// When true, incoming data with a compatible but different Arrow type is silently cast
    /// to the schema dtype (e.g. Float64 → Float32). Defaults to false (strict).
    #[serde(default)]
    pub cast: bool,
}

impl ColumnSchema {
    pub fn default_nullable() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub key: String,
    pub columns: IndexMap<String, ColumnSchema>,
}

impl From<&DType> for DataType {
    fn from(dtype: &DType) -> Self {
        match dtype {
            DType::Utf8 => DataType::Utf8,
            DType::Bool => DataType::Boolean,
            DType::Int8 => DataType::Int8,
            DType::Int16 => DataType::Int16,
            DType::Int32 => DataType::Int32,
            DType::Int64 => DataType::Int64,
            DType::UInt8 => DataType::UInt8,
            DType::UInt16 => DataType::UInt16,
            DType::UInt32 => DataType::UInt32,
            DType::UInt64 => DataType::UInt64,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
        }
    }
}

impl TryFrom<&DataType> for DType {
    type Error = MurrError;
    fn try_from(dt: &DataType) -> Result<Self, Self::Error> {
        match dt {
            DataType::Utf8 => Ok(DType::Utf8),
            DataType::Boolean => Ok(DType::Bool),
            DataType::Int8 => Ok(DType::Int8),
            DataType::Int16 => Ok(DType::Int16),
            DataType::Int32 => Ok(DType::Int32),
            DataType::Int64 => Ok(DType::Int64),
            DataType::UInt8 => Ok(DType::UInt8),
            DataType::UInt16 => Ok(DType::UInt16),
            DataType::UInt32 => Ok(DType::UInt32),
            DataType::UInt64 => Ok(DType::UInt64),
            DataType::Float32 => Ok(DType::Float32),
            DataType::Float64 => Ok(DType::Float64),
            other => Err(MurrError::SegmentError(format!(
                "unsupported dtype {other:?}"
            ))),
        }
    }
}

impl From<&TableSchema> for Schema {
    fn from(schema: &TableSchema) -> Self {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|(name, config)| Field::new(name, DataType::from(&config.dtype), config.nullable))
            .collect();
        let metadata = HashMap::from([("key".to_string(), schema.key.clone())]);
        Schema::new_with_metadata(fields, metadata)
    }
}
