mod local;

pub use local::LocalDirectory;

use std::time::SystemTime;

use async_trait::async_trait;

use crate::core::MurrError;

#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub id: u32,
    pub size: u32,
    pub file_name: String,
    pub last_modified: SystemTime,
}

#[async_trait]
pub trait Directory {
    async fn segments(&self) -> Result<Vec<SegmentInfo>, MurrError>;
}
