use std::path::Path;

use crate::core::MurrError;
use crate::segment::Segment;

use super::Directory;

pub struct LocalDirectory {
    segments: Vec<Segment>,
}

impl LocalDirectory {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MurrError> {
        let path = path.as_ref();
        let mut entries: Vec<_> = std::fs::read_dir(path)
            .map_err(|e| {
                MurrError::IoError(format!("reading directory {}: {}", path.display(), e))
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("seg") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();

        entries.sort();

        let segments: Result<Vec<_>, _> = entries.iter().map(|p| Segment::open(p)).collect();

        Ok(Self {
            segments: segments?,
        })
    }
}

impl Directory for LocalDirectory {
    fn segments(&self) -> &[Segment] {
        &self.segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::WriteSegment;
    use std::fs::File;
    use tempfile::TempDir;

    #[test]
    fn test_open_empty_dir() {
        let dir = TempDir::new().unwrap();
        let local = LocalDirectory::open(dir.path()).unwrap();
        assert_eq!(local.segments().len(), 0);
    }

    #[test]
    fn test_open_dir_with_segments() {
        let dir = TempDir::new().unwrap();

        for id in [0u32, 1, 2] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let mut ws = WriteSegment::new();
            ws.add_column("data", vec![id as u8]);
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::open(dir.path()).unwrap();
        assert_eq!(local.segments().len(), 3);
        assert_eq!(local.segments()[0].id(), 0);
        assert_eq!(local.segments()[1].id(), 1);
        assert_eq!(local.segments()[2].id(), 2);
    }

    #[test]
    fn test_ignores_non_seg_files() {
        let dir = TempDir::new().unwrap();

        // Write a valid segment
        let path = dir.path().join("00000000.seg");
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1]);
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        // Write a non-seg file
        std::fs::write(dir.path().join("readme.txt"), "hello").unwrap();

        let local = LocalDirectory::open(dir.path()).unwrap();
        assert_eq!(local.segments().len(), 1);
    }

    #[test]
    fn test_segments_sorted_by_name() {
        let dir = TempDir::new().unwrap();

        // Write out of order
        for id in [5u32, 2, 8] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let ws = WriteSegment::new();
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::open(dir.path()).unwrap();
        assert_eq!(local.segments()[0].id(), 2);
        assert_eq!(local.segments()[1].id(), 5);
        assert_eq!(local.segments()[2].id(), 8);
    }
}
