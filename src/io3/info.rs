use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{DType, TableSchema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: TableSchema,
    pub segments: Vec<SegmentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub id: u32,
    pub size_bytes: u32,
    pub num_values: u32,
}
