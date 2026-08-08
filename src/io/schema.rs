use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::core::{DTypeName, MurrError, TableSchema};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentColumnSchema {
    pub index: u32,
    pub dtype: DTypeName,
    pub name: String,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentSchema {
    pub capacity: usize,
    pub bitset_size: usize,
    pub columns: Vec<SegmentColumnSchema>,
}

impl SegmentSchema {
    pub fn new(columns: &[SegmentColumnSchema]) -> Self {
        SegmentSchema {
            columns: columns.to_vec(),
            capacity: columns.iter().map(|c| c.dtype.codec().size()).sum(),
            bitset_size: columns.len().div_ceil(8),
        }
    }
}

impl From<&TableSchema> for SegmentSchema {
    fn from(schema: &TableSchema) -> Self {
        let mut offset: u32 = 0;
        let columns: Vec<SegmentColumnSchema> = schema
            .columns
            .iter()
            .filter(|(name, _)| *name != &schema.key)
            .enumerate()
            .map(|(i, (name, col))| {
                let column = SegmentColumnSchema {
                    index: i as u32,
                    dtype: col.dtype,
                    name: name.clone(),
                    offset,
                };
                offset += col.dtype.codec().size() as u32;
                column
            })
            .collect();
        SegmentSchema::new(&columns)
    }
}

impl From<&TableSchema> for Schema {
    fn from(schema: &TableSchema) -> Self {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|(name, config)| {
                Field::new(name, config.dtype.codec().arrow_dtype(), config.nullable)
            })
            .collect();
        let metadata = HashMap::from([("key".to_string(), schema.key.clone())]);
        Schema::new_with_metadata(fields, metadata)
    }
}

impl TryFrom<&DataType> for DTypeName {
    type Error = MurrError;
    fn try_from(dt: &DataType) -> Result<Self, Self::Error> {
        match dt {
            DataType::Utf8 => Ok(DTypeName::Utf8),
            DataType::Boolean => Ok(DTypeName::Bool),
            DataType::Int8 => Ok(DTypeName::Int8),
            DataType::Int16 => Ok(DTypeName::Int16),
            DataType::Int32 => Ok(DTypeName::Int32),
            DataType::Int64 => Ok(DTypeName::Int64),
            DataType::UInt8 => Ok(DTypeName::UInt8),
            DataType::UInt16 => Ok(DTypeName::UInt16),
            DataType::UInt32 => Ok(DTypeName::UInt32),
            DataType::UInt64 => Ok(DTypeName::UInt64),
            DataType::Float32 => Ok(DTypeName::Float32),
            DataType::Float64 => Ok(DTypeName::Float64),
            other => Err(MurrError::SegmentError(format!(
                "unsupported dtype {other:?}"
            ))),
        }
    }
}
