use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::DType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    columns: HashMap<String, ColumnInfo>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    name: String,
    dtype: DType,
    nullable: bool,
    segments: HashMap<u32, ColumnSegment>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSegment {
    offset: u32,
    length: u32,
    num_values: u32,
}
