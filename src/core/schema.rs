use arrow::datatypes::DataType;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

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

impl From<DTypeName> for DataType {
    fn from(dtype: DTypeName) -> Self {
        match dtype {
            DTypeName::Utf8 => DataType::Utf8,
            DTypeName::Bool => DataType::Boolean,
            DTypeName::Int8 => DataType::Int8,
            DTypeName::Int16 => DataType::Int16,
            DTypeName::Int32 => DataType::Int32,
            DTypeName::Int64 => DataType::Int64,
            DTypeName::UInt8 => DataType::UInt8,
            DTypeName::UInt16 => DataType::UInt16,
            DTypeName::UInt32 => DataType::UInt32,
            DTypeName::UInt64 => DataType::UInt64,
            DTypeName::Float32 => DataType::Float32,
            DTypeName::Float64 => DataType::Float64,
        }
    }
}

impl From<&DTypeName> for DataType {
    fn from(dtype: &DTypeName) -> Self {
        (*dtype).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dtype_name_converts_to_arrow_data_type() {
        let cases = [
            (DTypeName::Utf8, DataType::Utf8),
            (DTypeName::Bool, DataType::Boolean),
            (DTypeName::Int8, DataType::Int8),
            (DTypeName::Int16, DataType::Int16),
            (DTypeName::Int32, DataType::Int32),
            (DTypeName::Int64, DataType::Int64),
            (DTypeName::UInt8, DataType::UInt8),
            (DTypeName::UInt16, DataType::UInt16),
            (DTypeName::UInt32, DataType::UInt32),
            (DTypeName::UInt64, DataType::UInt64),
            (DTypeName::Float32, DataType::Float32),
            (DTypeName::Float64, DataType::Float64),
        ];

        for (dtype, expected) in cases {
            assert_eq!(DataType::from(dtype), expected);
            assert_eq!(DataType::from(&dtype), expected);
        }
    }
}
