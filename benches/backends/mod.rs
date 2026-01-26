//! Pluggable backend trait for benchmark comparisons.

use std::error::Error;

use async_trait::async_trait;

pub mod murr;
pub mod redis_blob;
pub mod redis_feast;
pub mod testdata;

/// Configuration for a benchmark run.
pub struct BenchConfig {
    pub table_name: String,
    pub num_rows: usize,
    pub num_columns: usize,
}

/// A backend that can be benchmarked for feature fetching.
///
/// Each backend returns its native result type, measured without
/// artificial conversion overhead.
#[async_trait]
pub trait BenchBackend: Send + Sync {
    /// The native result type this backend produces.
    type Result: Send;

    /// Human-readable name for benchmark identification.
    fn name(&self) -> &'static str;

    /// Initialize the backend with test data.
    /// Called once before benchmarks run, outside the timing loop.
    async fn init(&mut self, config: &BenchConfig) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Fetch features for the given keys and columns.
    /// This is the operation being benchmarked.
    async fn fetch(
        &self,
        keys: &[String],
        columns: &[String],
    ) -> Result<Self::Result, Box<dyn Error + Send + Sync>>;

    /// Cleanup resources (called after benchmarks complete).
    async fn cleanup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
}
