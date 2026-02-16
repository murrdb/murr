use crate::core::MurrError;

/// Buffered writer for building a new segment.
///
/// Owns an entire segment being constructed. Write methods take a file name
/// parameter and buffer data locally. Call `finish()` to produce a `Segment`
/// that can be atomically inserted into a Directory.
pub trait Writer: Send {
    /// Append f32 values to a file, returns the byte offset of the written block.
    fn write_f32(&mut self, file: &str, values: &[f32]) -> Result<usize, MurrError>;

    /// Append u32 values to a file, returns the byte offset of the written block.
    fn write_u32(&mut self, file: &str, values: &[u32]) -> Result<usize, MurrError>;

    /// Append string values to a file, returns the byte offset of the written block.
    fn write_str(&mut self, file: &str, values: &[&str]) -> Result<usize, MurrError>;

    /// Finalize and return the built segment.
    fn finish(self: Box<Self>) -> Result<Segment, MurrError>;
}

/// An opaque, finalized segment ready to be inserted into a Directory.
pub struct Segment {
    pub(crate) files: Vec<(String, Vec<u8>)>,
}
