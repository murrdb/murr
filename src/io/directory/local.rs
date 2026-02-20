use std::path::PathBuf;

use async_trait::async_trait;

use crate::core::MurrError;

use super::{Directory, IndexInfo, SegmentInfo, TableSchema};

const TABLE_JSON: &str = "table.json";

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

    fn scan_segments(&self) -> Result<Vec<SegmentInfo>, MurrError> {
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

#[async_trait]
impl Directory for LocalDirectory {
    async fn index(&self) -> Result<Option<IndexInfo>, MurrError> {
        let table_json_path = self.path.join(TABLE_JSON);
        if !table_json_path.exists() {
            return Ok(None);
        }

        let data = std::fs::read(&table_json_path).map_err(|e| {
            MurrError::IoError(format!(
                "reading {}: {}",
                table_json_path.display(),
                e
            ))
        })?;

        let schema: TableSchema = serde_json::from_slice(&data).map_err(|e| {
            MurrError::IoError(format!(
                "parsing {}: {}",
                table_json_path.display(),
                e
            ))
        })?;

        let segments = self.scan_segments()?;

        Ok(Some(IndexInfo { schema, segments }))
    }

    async fn write(&mut self, name: &str, data: &[u8]) -> Result<(), MurrError> {
        let file_path = self.path.join(name);
        std::fs::write(&file_path, data).map_err(|e| {
            MurrError::IoError(format!(
                "writing {}: {}",
                file_path.display(),
                e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::{ColumnConfig, DType};
    use crate::io::segment::WriteSegment;
    use std::collections::HashMap;
    use std::fs::File;
    use tempfile::TempDir;

    fn test_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema { columns }
    }

    fn write_table_json(dir: &std::path::Path, schema: &TableSchema) {
        let data = serde_json::to_vec_pretty(schema).unwrap();
        std::fs::write(dir.join(TABLE_JSON), data).unwrap();
    }

    #[tokio::test]
    async fn test_empty_dir_returns_none() {
        let dir = TempDir::new().unwrap();
        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap();
        assert!(index.is_none());
    }

    #[tokio::test]
    async fn test_table_json_only_returns_empty_segments() {
        let dir = TempDir::new().unwrap();
        let schema = test_schema();
        write_table_json(dir.path(), &schema);

        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.schema, schema);
        assert_eq!(index.segments.len(), 0);
    }

    #[tokio::test]
    async fn test_table_json_with_segments() {
        let dir = TempDir::new().unwrap();
        let schema = test_schema();
        write_table_json(dir.path(), &schema);

        for id in [0u32, 1, 2] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let mut ws = WriteSegment::new();
            ws.add_column("data", vec![id as u8]);
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.schema, schema);
        assert_eq!(index.segments.len(), 3);
        assert_eq!(index.segments[0].id, 0);
        assert_eq!(index.segments[1].id, 1);
        assert_eq!(index.segments[2].id, 2);
    }

    #[tokio::test]
    async fn test_ignores_non_seg_files() {
        let dir = TempDir::new().unwrap();
        let schema = test_schema();
        write_table_json(dir.path(), &schema);

        let path = dir.path().join("00000000.seg");
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1]);
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        std::fs::write(dir.path().join("readme.txt"), "hello").unwrap();

        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.segments.len(), 1);
    }

    #[tokio::test]
    async fn test_segments_sorted_by_name() {
        let dir = TempDir::new().unwrap();
        let schema = test_schema();
        write_table_json(dir.path(), &schema);

        for id in [5u32, 2, 8] {
            let path = dir.path().join(format!("{:08}.seg", id));
            let ws = WriteSegment::new();
            let mut file = File::create(&path).unwrap();
            ws.write(&mut file).unwrap();
        }

        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.segments[0].id, 2);
        assert_eq!(index.segments[1].id, 5);
        assert_eq!(index.segments[2].id, 8);
    }

    #[tokio::test]
    async fn test_segment_info_has_size() {
        let dir = TempDir::new().unwrap();
        let schema = test_schema();
        write_table_json(dir.path(), &schema);

        let path = dir.path().join("00000000.seg");
        let mut ws = WriteSegment::new();
        ws.add_column("data", vec![1, 2, 3]);
        let mut file = File::create(&path).unwrap();
        ws.write(&mut file).unwrap();

        let local = LocalDirectory::new(dir.path());
        let index = local.index().await.unwrap().unwrap();
        assert!(index.segments[0].size > 0);
    }

    #[tokio::test]
    async fn test_write_creates_file() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());

        local.write("test.txt", b"hello world").await.unwrap();

        let content = std::fs::read(dir.path().join("test.txt")).unwrap();
        assert_eq!(content, b"hello world");
    }

    #[tokio::test]
    async fn test_write_table_json_via_directory() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());

        let schema = test_schema();
        let data = serde_json::to_vec_pretty(&schema).unwrap();
        local.write(TABLE_JSON, &data).await.unwrap();

        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.schema, schema);
    }
}
