use std::collections::HashMap;

use async_trait::async_trait;

use crate::core::MurrError;

use super::reader::Reader;
use super::writer::Segment;

/// Snapshot of directory contents returned by `Directory::list()`.
pub struct DirectoryListing {
    /// segment_id -> list of file names
    pub segments: HashMap<usize, Vec<String>>,
}

/// Lucene-inspired storage abstraction.
///
/// Logical layout: `/<segment_id>/<file_name>`
///
/// One directory corresponds to one table. Segments are immutable once
/// committed — append-only, no in-place updates.
#[async_trait]
pub trait Directory: Send + Sync {
    /// List all segments and their files in one call.
    async fn list(&self) -> Result<DirectoryListing, MurrError>;

    /// Open a random-read optimized reader for a specific file in a segment.
    async fn reader(&self, segment: usize, file: &str) -> Result<Box<dyn Reader>, MurrError>;

    /// Atomically insert a finalized segment into the directory.
    /// Returns the assigned segment ID.
    async fn write(&self, segment: Segment) -> Result<usize, MurrError>;
}
