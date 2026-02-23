use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    Utf8,
    Int16,
    Int32,
    Int64,
    Uint16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    pub dtype: DType,
    #[serde(default = "ColumnConfig::default_nullable")]
    pub nullable: bool,
}

impl ColumnConfig {
    pub fn default_nullable() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    #[serde(default)]
    pub name: String,
    pub key: String,
    pub columns: HashMap<String, ColumnConfig>,
}

impl From<&DType> for DataType {
    fn from(dtype: &DType) -> Self {
        match dtype {
            DType::Utf8 => DataType::Utf8,
            DType::Int16 => DataType::Int16,
            DType::Int32 => DataType::Int32,
            DType::Int64 => DataType::Int64,
            DType::Uint16 => DataType::UInt16,
            DType::UInt32 => DataType::UInt32,
            DType::UInt64 => DataType::UInt64,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
            DType::Bool => DataType::Boolean,
        }
    }
}

impl From<&TableSchema> for Schema {
    fn from(schema: &TableSchema) -> Self {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|(name, config)| {
                Field::new(name, DataType::from(&config.dtype), config.nullable)
            })
            .collect();
        let metadata = HashMap::from([("key".to_string(), schema.key.clone())]);
        Schema::new_with_metadata(fields, metadata)
    }
}
