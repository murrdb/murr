use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::MurrError;
use crate::io2::directory::mmap::directory::MMapDirectory;
use crate::io2::directory::{SegmentBytes, Writer};
use crate::io2::info::TableInfo;

pub struct MMapWriter<'a> {
    dir: &'a MMapDirectory,
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

impl MMapWriter<'_> {
    fn flush_info(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        let info = TableInfo {
            max_segment_id: segment.segment_id,
            columns: segment
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.clone()))
                .collect(),
        };

        let path = self.dir.metadata_path();
        let data = serde_json::to_vec_pretty(&info)
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

    fn flush_segment(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        let seg_path = self.dir.segment_path(segment.segment_id);
        let tmp = tmp_path(&seg_path);

        let mut file = std::fs::File::create(&tmp)
            .map_err(|e| MurrError::IoError(format!("creating {}: {e}", tmp.display())))?;
        file.write_all(&segment.bytes)
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

impl<'a> Writer<'a> for MMapWriter<'a> {
    type D = MMapDirectory;

    async fn new(dir: &'a Self::D) -> Result<Self, MurrError> {
        Ok(MMapWriter { dir })
    }

    async fn write(&self, segment: &SegmentBytes) -> Result<(), MurrError> {
        self.flush_segment(segment)?;
        self.flush_info(segment)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::directory::Directory;
    use crate::io2::info::{ColumnInfo, SegmentInfo};
    use crate::io2::url::LocalUrl;
    use std::collections::HashMap;

    fn test_dir(tmp: &tempfile::TempDir) -> MMapDirectory {
        let url = LocalUrl {
            path: tmp.path().to_path_buf(),
        };
        MMapDirectory::open(&url, 4096, false)
    }

    fn segment_with_column(id: u32, payload: Vec<u8>) -> SegmentBytes {
        let mut col_segments = HashMap::new();
        col_segments.insert(
            id,
            SegmentInfo {
                offset: 0,
                length: payload.len() as u32,
                num_values: payload.len() as u32 / 4,
            },
        );
        SegmentBytes {
            segment_id: id,
            bytes: payload,
            columns: vec![ColumnInfo {
                name: "score".to_string(),
                dtype: DType::Float32,
                nullable: false,
                segments: col_segments,
            }],
        }
    }

    #[tokio::test]
    async fn write_first_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        let segment = segment_with_column(0, vec![1, 2, 3, 4]);
        writer.write(&segment).await.unwrap();

        let seg_path = tmp.path().join("00000000.seg");
        assert!(seg_path.exists());
        assert_eq!(std::fs::read(&seg_path).unwrap(), vec![1, 2, 3, 4]);
        assert!(tmp.path().join("_metadata.json").exists());
    }

    #[tokio::test]
    async fn write_sequential_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        for i in 0..3u32 {
            let segment = segment_with_column(i, vec![i as u8; 4]);
            writer.write(&segment).await.unwrap();
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

        let mut col_segments = HashMap::new();
        col_segments.insert(
            0,
            SegmentInfo {
                offset: 0,
                length: 16,
                num_values: 4,
            },
        );
        col_segments.insert(
            1,
            SegmentInfo {
                offset: 0,
                length: 16,
                num_values: 4,
            },
        );

        let segment = SegmentBytes {
            segment_id: 1,
            bytes: vec![1; 16],
            columns: vec![ColumnInfo {
                name: "score".to_string(),
                dtype: DType::Float32,
                nullable: false,
                segments: col_segments,
            }],
        };
        writer.write(&segment).await.unwrap();

        let meta_path = tmp.path().join("_metadata.json");
        let data = std::fs::read_to_string(&meta_path).unwrap();
        let parsed: TableInfo = serde_json::from_str(&data).unwrap();

        assert_eq!(parsed.max_segment_id, 1);
        assert!(parsed.columns.contains_key("score"));
        assert_eq!(parsed.columns["score"].segments.len(), 2);
    }
}
