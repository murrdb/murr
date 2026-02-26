use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    Utf8,
    Float32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub dtype: DType,
    #[serde(default = "ColumnSchema::default_nullable")]
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn default_nullable() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    #[serde(default)]
    pub name: String,
    pub key: String,
    pub columns: HashMap<String, ColumnSchema>,
}

impl From<&DType> for DataType {
    fn from(dtype: &DType) -> Self {
        match dtype {
            DType::Utf8 => DataType::Utf8,
            DType::Float32 => DataType::Float32,
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
