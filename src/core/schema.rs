use std::collections::HashMap;

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
