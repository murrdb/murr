use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::path::Path;

use memmap2::Mmap;

use crate::core::MurrError;

use super::format::{FOOTER_LEN_SIZE, HEADER_SIZE, MAGIC, VERSION, read_u16_le, read_u32_le};

/// Memory-mapped read handle for a `.seg` file. Provides zero-copy access
/// to named column payloads.
#[derive(Debug)]
pub struct Segment {
    id: u32,
    mmap: Mmap,
    columns: HashMap<String, Range<u32>>,
}

impl Segment {
    /// Open a `.seg` file, validate the header, parse the footer, and
    /// build column index. The segment id is parsed from the filename stem
    /// (e.g. `00000000.seg` → id 0).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MurrError> {
        let path = path.as_ref();
        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "cannot parse segment id from filename: {}",
                    path.display()
                ))
            })?;
        let file = File::open(path)?;
        // SAFETY: the file is opened read-only and we treat the mapping as immutable.
        let mmap = unsafe { Mmap::map(&file) }?;
        Self::from_mmap(id, mmap)
    }

    fn from_mmap(id: u32, mmap: Mmap) -> Result<Self, MurrError> {
        let len = mmap.len();
        let min_size = HEADER_SIZE + FOOTER_LEN_SIZE;
        if len < min_size {
            return Err(MurrError::SegmentError(format!(
                "file too small: {len} bytes, minimum {min_size}"
            )));
        }

        if &mmap[0..4] != MAGIC {
            return Err(MurrError::SegmentError(format!(
                "bad magic: expected MURR, got {:?}",
                &mmap[0..4]
            )));
        }

        let version = read_u32_le(&mmap, 4);
        if version != VERSION {
            return Err(MurrError::SegmentError(format!(
                "unsupported version: {version}, expected {VERSION}"
            )));
        }

        let footer_size = read_u32_le(&mmap, len - FOOTER_LEN_SIZE) as usize;
        let footer_end = len - FOOTER_LEN_SIZE;
        if footer_size > footer_end - HEADER_SIZE {
            return Err(MurrError::SegmentError(format!(
                "footer size {footer_size} exceeds available data"
            )));
        }
        let footer_start = footer_end - footer_size;

        let mut columns = HashMap::new();
        let mut pos = footer_start;
        while pos < footer_end {
            if pos + 2 > footer_end {
                return Err(MurrError::SegmentError(
                    "truncated footer entry: missing name length".into(),
                ));
            }
            let name_len = read_u16_le(&mmap, pos) as usize;
            pos += 2;

            if pos + name_len + 8 > footer_end {
                return Err(MurrError::SegmentError("truncated footer entry".into()));
            }
            let name = std::str::from_utf8(&mmap[pos..pos + name_len])
                .map_err(|e| MurrError::SegmentError(format!("invalid column name: {e}")))?
                .to_string();
            pos += name_len;

            let offset = read_u32_le(&mmap, pos);
            pos += 4;
            let size = read_u32_le(&mmap, pos);
            pos += 4;

            let end = offset + size;
            if end as usize > footer_start {
                return Err(MurrError::SegmentError(format!(
                    "column '{name}' payload range {offset}..{end} exceeds data region"
                )));
            }

            columns.insert(name, offset..end);
        }

        Ok(Self { id, mmap, columns })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    /// Iterator over column names in this segment.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|s| s.as_str())
    }

    /// Get a zero-copy slice of a column's payload.
    pub fn column(&self, name: &str) -> Option<&[u8]> {
        let range = self.columns.get(name)?;
        Some(&self.mmap[range.start as usize..range.end as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::write::WriteSegment;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write_to_dir(ws: &WriteSegment, id: u32) -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(format!("{:08}.seg", id));
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();
        (dir, path)
    }

    #[test]
    fn test_round_trip_single_column() {
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1, 2, 3, 4, 5]);
        let (_dir, path) = write_to_dir(&ws, 0);

        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.id(), 0);
        assert_eq!(seg.column("data").unwrap(), &[1, 2, 3, 4, 5]);
        assert!(seg.column("missing").is_none());
    }

    #[test]
    fn test_round_trip_multiple_columns() {
        let mut ws = WriteSegment::new();
        ws.add_column("floats", vec![0xAA; 16]);
        ws.add_column("ints", vec![0xBB; 8]);
        ws.add_column("strings", vec![0xCC; 32]);
        let (_dir, path) = write_to_dir(&ws, 5);

        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.id(), 5);
        assert_eq!(seg.column("floats").unwrap(), &[0xAA; 16]);
        assert_eq!(seg.column("ints").unwrap(), &[0xBB; 8]);
        assert_eq!(seg.column("strings").unwrap(), &[0xCC; 32]);
    }

    #[test]
    fn test_empty_segment() {
        let ws = WriteSegment::new();
        let (_dir, path) = write_to_dir(&ws, 0);

        let seg = Segment::open(&path).unwrap();
        assert!(seg.column("anything").is_none());
    }

    #[test]
    fn test_bad_magic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00000000.seg");
        std::fs::write(&path, b"BAAD\x01\x00\x00\x00\x00\x00\x00\x00").unwrap();

        let err = Segment::open(&path).unwrap_err();
        assert!(err.to_string().contains("bad magic"));
    }

    #[test]
    fn test_bad_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00000000.seg");
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MURR");
        buf.extend_from_slice(&99u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        std::fs::write(&path, &buf).unwrap();

        let err = Segment::open(&path).unwrap_err();
        assert!(err.to_string().contains("unsupported version"));
    }

    #[test]
    fn test_file_too_small() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00000000.seg");
        std::fs::write(&path, b"MURR").unwrap();

        let err = Segment::open(&path).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn test_empty_column_payload() {
        let mut ws = WriteSegment::new();
        ws.add_column("empty", vec![]);
        ws.add_column("notempty", vec![42]);
        let (_dir, path) = write_to_dir(&ws, 0);

        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.column("empty").unwrap(), &[] as &[u8]);
        assert_eq!(seg.column("notempty").unwrap(), &[42]);
    }

    #[test]
    fn test_segment_id_parsed_from_filename() {
        let ws = WriteSegment::new();
        let (_dir, path) = write_to_dir(&ws, 42);
        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.id(), 42);
    }
}
