use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::debug;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::column::ColumnSegmentBytes;
use crate::io2::directory::DirectoryWriter;
use crate::io2::directory::mmap::directory::MMapDirectory;
use crate::io2::info::{ColumnSegments, SegmentInfo, TableInfo};

pub struct MMapWriter {
    dir: Arc<MMapDirectory>,
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

impl MMapWriter {
    fn load_existing_info(&self) -> Option<TableInfo> {
        let path = self.dir.metadata_path();
        std::fs::read(&path)
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())
    }

    fn next_segment_id(&self) -> u32 {
        self.load_existing_info()
            .map(|info| info.max_segment_id + 1)
            .unwrap_or(0)
    }

    fn flush_info(&self, info: &TableInfo) -> Result<(), MurrError> {
        let path = self.dir.metadata_path();
        let data = serde_json::to_vec_pretty(info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;

        let tmp = tmp_path(&path);
        std::fs::write(&tmp, &data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, &path).map_err(|e| {
            MurrError::IoError(format!(
                "renaming {} to {}: {e}",
                tmp.display(),
                path.display()
            ))
        })?;
        Ok(())
    }

    fn ensure_dir(&self) -> Result<(), MurrError> {
        let path = self.dir.path();
        std::fs::create_dir_all(&path)
            .map_err(|e| MurrError::IoError(format!("creating dir {}: {e}", path.display())))
    }

    fn flush_segment(&self, segment_id: u32, data: &[u8]) -> Result<(), MurrError> {
        let seg_path = self.dir.segment_path(segment_id);
        let tmp = tmp_path(&seg_path);

        let mut file = std::fs::File::create(&tmp)
            .map_err(|e| MurrError::IoError(format!("creating {}: {e}", tmp.display())))?;
        file.write_all(data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", tmp.display())))?;
        file.sync_all()
            .map_err(|e| MurrError::IoError(format!("syncing {}: {e}", tmp.display())))?;
        drop(file);

        std::fs::rename(&tmp, &seg_path).map_err(|e| {
            MurrError::IoError(format!(
                "renaming {} to {}: {e}",
                tmp.display(),
                seg_path.display()
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl DirectoryWriter for MMapWriter {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        Ok(MMapWriter { dir })
    }

    async fn write(&self, columns: &[ColumnSegmentBytes]) -> Result<(), MurrError> {
        let segment_id = self.next_segment_id();

        // Concatenate all column bytes, tracking offsets
        let mut combined = Vec::new();
        let mut column_infos = Vec::new();

        for col in columns {
            let offset = combined.len() as u32;
            let length = col.bytes.len() as u32;
            combined.extend_from_slice(&col.bytes);
            column_infos.push((
                col.column.clone(),
                SegmentInfo {
                    id: segment_id,
                    offset,
                    length,
                    num_values: col.num_values,
                },
            ));
        }

        // Build/merge TableInfo
        let mut info = self.load_existing_info().unwrap_or_else(|| TableInfo {
            max_segment_id: 0,
            columns: HashMap::new(),
        });
        info.max_segment_id = segment_id;

        for (col_info, seg_info) in column_infos {
            let entry = info
                .columns
                .entry(col_info.name.clone())
                .or_insert_with(|| ColumnSegments {
                    column: col_info.clone(),
                    segments: HashMap::new(),
                });
            entry.segments.insert(segment_id, seg_info);
        }

        let seg_path = self.dir.segment_path(segment_id);
        debug!(
            "mmap write: segment={segment_id} path={} columns={} bytes={}",
            seg_path.display(),
            columns.len(),
            combined.len()
        );

        self.ensure_dir()?;
        self.flush_segment(segment_id, &combined)?;
        self.flush_info(&info)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::directory::{Directory, DirectoryWriter};
    use crate::io2::info::ColumnInfo;
    use crate::io2::url::LocalUrl;

    fn test_dir(tmp: &tempfile::TempDir) -> Arc<MMapDirectory> {
        let url = LocalUrl {
            path: tmp.path().to_path_buf(),
        };
        Arc::new(MMapDirectory::open(&url, "default", 4096, false))
    }

    fn column_bytes(name: &str, payload: Vec<u8>, num_values: u32) -> ColumnSegmentBytes {
        ColumnSegmentBytes::new(
            ColumnInfo {
                name: name.to_string(),
                dtype: DType::Float32,
                nullable: false,
            },
            payload,
            num_values,
        )
    }

    #[tokio::test]
    async fn write_first_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        writer
            .write(&[column_bytes("score", vec![1, 2, 3, 4], 1)])
            .await
            .unwrap();

        let idx = tmp.path().join("default");
        let seg_path = idx.join("00000000.seg");
        assert!(seg_path.exists());
        assert_eq!(std::fs::read(&seg_path).unwrap(), vec![1, 2, 3, 4]);
        assert!(idx.join("_metadata.json").exists());
    }

    #[tokio::test]
    async fn write_sequential_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        for i in 0..3u32 {
            writer
                .write(&[column_bytes("score", vec![i as u8; 4], 1)])
                .await
                .unwrap();
        }

        assert!(tmp.path().join("00000000.seg").exists());
        assert!(tmp.path().join("00000001.seg").exists());
        assert!(tmp.path().join("00000002.seg").exists());
        assert_eq!(
            std::fs::read(tmp.path().join("00000002.seg")).unwrap(),
            vec![2, 2, 2, 2]
        );
    }

    #[tokio::test]
    async fn write_persists_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        // Write two segments for the same column
        writer
            .write(&[column_bytes("score", vec![1; 16], 4)])
            .await
            .unwrap();
        writer
            .write(&[column_bytes("score", vec![2; 16], 4)])
            .await
            .unwrap();

        let meta_path = tmp.path().join("_metadata.json");
        let data = std::fs::read_to_string(&meta_path).unwrap();
        let parsed: TableInfo = serde_json::from_str(&data).unwrap();

        assert_eq!(parsed.max_segment_id, 1);
        assert!(parsed.columns.contains_key("score"));
        assert_eq!(parsed.columns["score"].segments.len(), 2);
    }
}
