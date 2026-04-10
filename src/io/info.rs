use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{DType, TableSchema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: TableSchema,
    pub max_segment_id: u32,
    pub columns: HashMap<String, ColumnSegments>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: DType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSegments {
    pub column: ColumnInfo,
    pub segments: HashMap<u32, SegmentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub id: u32,
    pub offset: u32,
    pub length: u32,
    pub num_values: u32,
}
