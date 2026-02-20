use std::path::PathBuf;

use async_trait::async_trait;

use crate::core::MurrError;

use super::{Directory, SegmentInfo};

pub struct LocalDirectory {
    path: PathBuf,
}

impl LocalDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[async_trait]
impl Directory for LocalDirectory {
    async fn segments(&self) -> Result<Vec<SegmentInfo>, MurrError> {
        let mut entries: Vec<_> = std::fs::read_dir(&self.path)
            .map_err(|e| {
                MurrError::IoError(format!(
                    "reading directory {}: {}",
                    self.path.display(),
                    e
                ))
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("seg") {
                    Some((p, entry))
                } else {
                    None
                }
            })
            .map(|(path, entry)| {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| {
                        MurrError::IoError(format!(
                            "invalid filename: {}",
                            path.display()
                        ))
                    })?
                    .to_string();

                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u32>().ok())
                    .ok_or_else(|| {
                        MurrError::IoError(format!(
                            "cannot parse segment id from filename: {}",
                            path.display()
                        ))
                    })?;

                let metadata = entry.metadata().map_err(|e| {
                    MurrError::IoError(format!(
                        "reading metadata for {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                let size = metadata.len() as u32;
                let last_modified = metadata.modified().map_err(|e| {
                    MurrError::IoError(format!(
                        "reading modified time for {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                Ok(SegmentInfo {
                    id,
                    size,
                    file_name,
                    last_modified,
                })
            })
            .collect::<Result<Vec<_>, MurrError>>()?;

        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::WriteSegment;
    use std::fs::File;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_empty_dir() {
        let dir = TempDir::new().unwrap();
        let local = LocalDirectory::new(dir.path());
        let segments = local.segments().await.unwrap();
        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn test_dir_with_segments() {
        let dir = TempDir::new().unwrap();

        for id in [0u32, 1, 2] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let mut ws = WriteSegment::new();
            ws.add_column("data", vec![id as u8]);
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::new(dir.path());
        let segments = local.segments().await.unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id, 0);
        assert_eq!(segments[1].id, 1);
        assert_eq!(segments[2].id, 2);
        assert_eq!(segments[0].file_name, "00000000.seg");
    }

    #[tokio::test]
    async fn test_ignores_non_seg_files() {
        let dir = TempDir::new().unwrap();

        let path = dir.path().join("00000000.seg");
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1]);
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        std::fs::write(dir.path().join("readme.txt"), "hello").unwrap();

        let local = LocalDirectory::new(dir.path());
        let segments = local.segments().await.unwrap();
        assert_eq!(segments.len(), 1);
    }

    #[tokio::test]
    async fn test_segments_sorted_by_name() {
        let dir = TempDir::new().unwrap();

        for id in [5u32, 2, 8] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let ws = WriteSegment::new();
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::new(dir.path());
        let segments = local.segments().await.unwrap();
        assert_eq!(segments[0].id, 2);
        assert_eq!(segments[1].id, 5);
        assert_eq!(segments[2].id, 8);
    }

    #[tokio::test]
    async fn test_segment_info_has_size() {
        let dir = TempDir::new().unwrap();

        let path = dir.path().join("00000000.seg");
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1, 2, 3]);
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        let local = LocalDirectory::new(dir.path());
        let segments = local.segments().await.unwrap();
        assert!(segments[0].size > 0);
    }
}
