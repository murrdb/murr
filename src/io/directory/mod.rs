mod local;

pub use local::LocalDirectory;

use std::time::SystemTime;

use async_trait::async_trait;

pub use crate::core::TableSchema;
use crate::core::MurrError;

#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub id: u32,
    pub size: u32,
    pub file_name: String,
    pub last_modified: SystemTime,
}

pub struct IndexInfo {
    pub schema: TableSchema,
    pub segments: Vec<SegmentInfo>,
}

#[async_trait]
pub trait Directory: Sized {
    async fn from_storage(path: &std::path::Path) -> Result<Vec<Self>, MurrError>;
    async fn index(&self) -> Result<Option<IndexInfo>, MurrError>;
    async fn write(&mut self, name: &str, data: &[u8]) -> Result<(), MurrError>;
}
