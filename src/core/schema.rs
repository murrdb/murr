use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::core::MurrError;

/// Leading column of every fetch response: the row's 0-based position in the request keys.
pub const IDX_COLUMN: &str = "_idx";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum DTypeName {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub dtype: DTypeName,
    #[serde(default = "ColumnSchema::default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub key: bool,
    /// A strict column accepts only casts which cannot change a value.
    #[serde(default = "ColumnSchema::default_strict")]
    pub strict: bool,
}

impl ColumnSchema {
    pub fn default_nullable() -> bool {
        true
    }

    pub fn default_strict() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    pub columns: IndexMap<String, ColumnSchema>,
}

impl TableSchema {
    pub fn validate(&self) -> Result<(), MurrError> {
        if self.columns.contains_key(IDX_COLUMN) {
            return Err(MurrError::TableError(format!(
                "column name '{IDX_COLUMN}' is reserved"
            )));
        }
        Ok(())
    }

    /// Key columns in schema order, which is also the order of components in the encoded key.
    pub fn key_columns(&self) -> impl Iterator<Item = (&String, &ColumnSchema)> {
        self.columns.iter().filter(|(_, col)| col.key)
    }
}
