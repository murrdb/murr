use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::DType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub max_segment_id: u32,
    pub columns: HashMap<String, ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: DType,
    pub nullable: bool,
    pub segments: HashMap<u32, ColumnSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSegment {
    pub offset: u32,
    pub length: u32,
    pub num_values: u32,
}
