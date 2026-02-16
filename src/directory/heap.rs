use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::core::MurrError;

use super::directory::{Directory, DirectoryListing};
use super::reader::Reader;
use super::writer::{Segment, Writer};

struct HeapFile {
    data: Vec<u8>,
}

/// In-memory Directory implementation backed by heap-allocated byte arrays.
///
/// Data layout:
/// - f32/u32: contiguous native-endian bytes, no length prefix
/// - strings: `<total_byte_size: u32>[<str_len: u32><str_bytes>]*`
pub struct HeapDirectory {
    segments: RwLock<HashMap<usize, HashMap<String, Arc<HeapFile>>>>,
    next_segment: AtomicUsize,
}

impl HeapDirectory {
    pub fn new() -> Self {
        Self {
            segments: RwLock::new(HashMap::new()),
            next_segment: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Directory for HeapDirectory {
    async fn list(&self) -> Result<DirectoryListing, MurrError> {
        let segments = self.segments.read().await;
        let listing = segments
            .iter()
            .map(|(&id, files)| {
                let names: Vec<String> = files.keys().cloned().collect();
                (id, names)
            })
            .collect();
        Ok(DirectoryListing { segments: listing })
    }

    async fn reader(&self, segment: usize, file: &str) -> Result<Box<dyn Reader>, MurrError> {
        let segments = self.segments.read().await;
        let heap_file = segments
            .get(&segment)
            .and_then(|f| f.get(file))
            .ok_or_else(|| {
                MurrError::DirectoryError(format!("file not found: {segment}/{file}"))
            })?;
        Ok(Box::new(HeapReader {
            file: Arc::clone(heap_file),
        }))
    }

    async fn write(&self, segment: Segment) -> Result<usize, MurrError> {
        let segment_id = self.next_segment.fetch_add(1, Ordering::Relaxed);
        let files: HashMap<String, Arc<HeapFile>> = segment
            .files
            .into_iter()
            .map(|(name, data)| (name, Arc::new(HeapFile { data })))
            .collect();
        let mut segments = self.segments.write().await;
        segments.insert(segment_id, files);
        Ok(segment_id)
    }
}

struct HeapReader {
    file: Arc<HeapFile>,
}

fn read_ne_u32(data: &[u8], offset: usize) -> Result<u32, MurrError> {
    let end = offset + 4;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| {
            MurrError::DirectoryError(format!(
                "read out of bounds: offset {offset}, len {}",
                data.len()
            ))
        })?
        .try_into()
        .map_err(|e| MurrError::DirectoryError(format!("slice conversion: {e}")))?;
    Ok(u32::from_ne_bytes(bytes))
}

fn read_ne_f32(data: &[u8], offset: usize) -> Result<f32, MurrError> {
    let end = offset + 4;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| {
            MurrError::DirectoryError(format!(
                "read out of bounds: offset {offset}, len {}",
                data.len()
            ))
        })?
        .try_into()
        .map_err(|e| MurrError::DirectoryError(format!("slice conversion: {e}")))?;
    Ok(f32::from_ne_bytes(bytes))
}

#[async_trait]
impl Reader for HeapReader {
    async fn read_f32_batch(&self, offsets: &[usize]) -> Result<Vec<f32>, MurrError> {
        let data = &self.file.data;
        offsets.iter().map(|&off| read_ne_f32(data, off)).collect()
    }

    async fn read_u32_batch(&self, offsets: &[usize]) -> Result<Vec<u32>, MurrError> {
        let data = &self.file.data;
        offsets.iter().map(|&off| read_ne_u32(data, off)).collect()
    }

    async fn read_vec_str(&self, offset: usize) -> Result<Vec<String>, MurrError> {
        let data = &self.file.data;

        let total_size = read_ne_u32(data, offset)? as usize;
        let mut pos = offset + 4;
        let end = offset + 4 + total_size;

        let mut strings = Vec::new();
        while pos < end {
            let str_len = read_ne_u32(data, pos)? as usize;
            pos += 4;
            let str_end = pos + str_len;
            let str_bytes = data.get(pos..str_end).ok_or_else(|| {
                MurrError::DirectoryError(format!(
                    "string read out of bounds: offset {pos}, len {str_len}, data len {}",
                    data.len()
                ))
            })?;
            let s = String::from_utf8(str_bytes.to_vec())
                .map_err(|e| MurrError::DirectoryError(e.to_string()))?;
            strings.push(s);
            pos = str_end;
        }

        Ok(strings)
    }
}

/// In-memory segment writer that buffers all writes into `Vec<u8>` per file.
pub struct HeapWriter {
    files: HashMap<String, Vec<u8>>,
}

impl HeapWriter {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }
}

impl Writer for HeapWriter {
    fn write_f32(&mut self, file: &str, values: &[f32]) -> Result<usize, MurrError> {
        let buf = self.files.entry(file.to_string()).or_default();
        let offset = buf.len();
        for &v in values {
            buf.extend_from_slice(&v.to_ne_bytes());
        }
        Ok(offset)
    }

    fn write_u32(&mut self, file: &str, values: &[u32]) -> Result<usize, MurrError> {
        let buf = self.files.entry(file.to_string()).or_default();
        let offset = buf.len();
        for &v in values {
            buf.extend_from_slice(&v.to_ne_bytes());
        }
        Ok(offset)
    }

    fn write_str(&mut self, file: &str, values: &[&str]) -> Result<usize, MurrError> {
        let buf = self.files.entry(file.to_string()).or_default();
        let offset = buf.len();

        // Reserve space for total_byte_size header
        buf.extend_from_slice(&0u32.to_ne_bytes());

        for s in values {
            let len = s.len() as u32;
            buf.extend_from_slice(&len.to_ne_bytes());
            buf.extend_from_slice(s.as_bytes());
        }

        // Patch total_byte_size (excludes the header itself)
        let total_size = (buf.len() - offset - 4) as u32;
        buf[offset..offset + 4].copy_from_slice(&total_size.to_ne_bytes());

        Ok(offset)
    }

    fn finish(self: Box<Self>) -> Result<Segment, MurrError> {
        let files: Vec<(String, Vec<u8>)> = self.files.into_iter().collect();
        Ok(Segment { files })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_f32_round_trip() {
        let dir = HeapDirectory::new();
        let values: Vec<f32> = vec![1.0, 2.5, -3.14, 0.0, f32::MAX];

        let mut writer = Box::new(HeapWriter::new());
        let offset = writer.write_f32("data.f32", &values).unwrap();
        assert_eq!(offset, 0);
        let seg = dir.write(writer.finish().unwrap()).await.unwrap();

        let reader = dir.reader(seg, "data.f32").await.unwrap();
        let offsets: Vec<usize> = (0..values.len()).map(|i| i * 4).collect();
        let result = reader.read_f32_batch(&offsets).await.unwrap();
        assert_eq!(result, values);
    }

    #[tokio::test]
    async fn test_u32_round_trip() {
        let dir = HeapDirectory::new();
        let values: Vec<u32> = vec![0, 1, 42, 1000, u32::MAX];

        let mut writer = Box::new(HeapWriter::new());
        let offset = writer.write_u32("data.u32", &values).unwrap();
        let seg = dir.write(writer.finish().unwrap()).await.unwrap();

        let reader = dir.reader(seg, "data.u32").await.unwrap();
        let offsets: Vec<usize> = (0..values.len()).map(|i| offset + i * 4).collect();
        let result = reader.read_u32_batch(&offsets).await.unwrap();
        assert_eq!(result, values);
    }

    #[tokio::test]
    async fn test_str_round_trip() {
        let dir = HeapDirectory::new();
        let values = vec!["hello", "world", "", "rust", "murr"];

        let mut writer = Box::new(HeapWriter::new());
        let offset = writer.write_str("data.str", &values).unwrap();
        let seg = dir.write(writer.finish().unwrap()).await.unwrap();

        let reader = dir.reader(seg, "data.str").await.unwrap();
        let result = reader.read_vec_str(offset).await.unwrap();
        assert_eq!(result, values);
    }

    #[tokio::test]
    async fn test_multiple_files_one_segment() {
        let dir = HeapDirectory::new();

        let mut writer = Box::new(HeapWriter::new());
        writer.write_f32("floats", &[1.0, 2.0]).unwrap();
        writer.write_u32("ints", &[10, 20]).unwrap();
        let seg = dir.write(writer.finish().unwrap()).await.unwrap();

        let listing = dir.list().await.unwrap();
        let mut files = listing.segments[&seg].clone();
        files.sort();
        assert_eq!(files, vec!["floats", "ints"]);

        let reader = dir.reader(seg, "floats").await.unwrap();
        let result = reader.read_f32_batch(&[0, 4]).await.unwrap();
        assert_eq!(result, vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn test_multiple_writes_single_file() {
        let dir = HeapDirectory::new();

        let mut writer = Box::new(HeapWriter::new());
        let off0 = writer.write_f32("data", &[1.0, 2.0]).unwrap();
        let off1 = writer.write_f32("data", &[3.0, 4.0]).unwrap();
        let seg = dir.write(writer.finish().unwrap()).await.unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 8);

        let reader = dir.reader(seg, "data").await.unwrap();
        let result = reader
            .read_f32_batch(&[off0, off0 + 4, off1, off1 + 4])
            .await
            .unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn test_multiple_segments() {
        let dir = HeapDirectory::new();

        let mut w1 = Box::new(HeapWriter::new());
        w1.write_f32("data", &[1.0]).unwrap();
        let seg1 = dir.write(w1.finish().unwrap()).await.unwrap();

        let mut w2 = Box::new(HeapWriter::new());
        w2.write_f32("data", &[2.0]).unwrap();
        let seg2 = dir.write(w2.finish().unwrap()).await.unwrap();

        assert_ne!(seg1, seg2);

        let listing = dir.list().await.unwrap();
        assert_eq!(listing.segments.len(), 2);
        assert!(listing.segments.contains_key(&seg1));
        assert!(listing.segments.contains_key(&seg2));

        let r1 = dir.reader(seg1, "data").await.unwrap();
        assert_eq!(r1.read_f32_batch(&[0]).await.unwrap(), vec![1.0]);

        let r2 = dir.reader(seg2, "data").await.unwrap();
        assert_eq!(r2.read_f32_batch(&[0]).await.unwrap(), vec![2.0]);
    }

    #[tokio::test]
    async fn test_reader_not_found() {
        let dir = HeapDirectory::new();
        assert!(dir.reader(0, "missing").await.is_err());
    }

    #[tokio::test]
    async fn test_empty_listing() {
        let dir = HeapDirectory::new();
        let listing = dir.list().await.unwrap();
        assert!(listing.segments.is_empty());
    }
}
