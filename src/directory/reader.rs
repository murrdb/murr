use async_trait::async_trait;

use crate::core::MurrError;

/// Random-read optimized reader for a single file within a segment.
#[async_trait]
pub trait Reader: Send + Sync {
    /// Read a batch of f32 values at given byte offsets.
    async fn read_f32_batch(&self, offsets: &[usize]) -> Result<Vec<f32>, MurrError>;

    /// Read a batch of u32 values at given byte offsets.
    async fn read_u32_batch(&self, offsets: &[usize]) -> Result<Vec<u32>, MurrError>;

    /// Read a vector of strings starting at the given byte offset.
    async fn read_vec_str(&self, offset: usize) -> Result<Vec<String>, MurrError>;
}
