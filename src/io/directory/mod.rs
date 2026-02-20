mod local;

pub use local::LocalDirectory;

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::conf::ColumnConfig;
use crate::core::MurrError;

#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub id: u32,
    pub size: u32,
    pub file_name: String,
    pub last_modified: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub key: String,
    pub columns: HashMap<String, ColumnConfig>,
}

pub struct IndexInfo {
    pub schema: TableSchema,
    pub segments: Vec<SegmentInfo>,
}

#[async_trait]
pub trait Directory {
    async fn index(&self) -> Result<Option<IndexInfo>, MurrError>;
    async fn write(&mut self, name: &str, data: &[u8]) -> Result<(), MurrError>;
}
