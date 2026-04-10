use std::sync::Arc;

use log::debug;
use memmap2::Mmap;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::bytes::FromBytes;
use crate::io2::directory::mmap::directory::MMapDirectory;
use crate::io2::directory::{DirectoryReader, SegmentReadRequest};
use crate::io2::info::TableInfo;

pub struct MMapReader {
    dir: Arc<MMapDirectory>,
    info: TableInfo,
    mmaps: Vec<Option<Arc<Mmap>>>,
}

impl MMapReader {
    fn load_info(dir: &MMapDirectory) -> Result<TableInfo, MurrError> {
        let path = dir.metadata_path();
        let data = std::fs::read(&path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", path.display())))
    }

    fn segment_ids(info: &TableInfo) -> std::collections::BTreeSet<u32> {
        let mut segment_ids = std::collections::BTreeSet::new();
        for col in info.columns.values() {
            for &seg_id in col.segments.keys() {
                segment_ids.insert(seg_id);
            }
        }
        segment_ids
    }

    fn load_mmaps(
        dir: &MMapDirectory,
        info: &TableInfo,
        existing: &[Option<Arc<Mmap>>],
    ) -> Result<Vec<Option<Arc<Mmap>>>, MurrError> {
        let max_id = info.max_segment_id as usize;
        let mut mmaps: Vec<Option<Arc<Mmap>>> = (0..=max_id).map(|_| None).collect();

        for seg_id in Self::segment_ids(info) {
            let idx = seg_id as usize;
            if let Some(existing_mmap) = existing.get(idx).and_then(|m| m.as_ref()) {
                mmaps[idx] = Some(Arc::clone(existing_mmap));
                continue;
            }
            let path = dir.segment_path(seg_id);
            let file = std::fs::File::open(&path)
                .map_err(|e| MurrError::IoError(format!("opening {}: {e}", path.display())))?;
            let mmap = unsafe { Mmap::map(&file) }
                .map_err(|e| MurrError::IoError(format!("mmapping {}: {e}", path.display())))?;
            if idx < mmaps.len() {
                mmaps[idx] = Some(Arc::new(mmap));
            }
        }

        Ok(mmaps)
    }
}

#[async_trait]
impl DirectoryReader for MMapReader {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = Self::load_info(&dir)?;
        let mmaps = Self::load_mmaps(&dir, &info, &[])?;
        Ok(MMapReader { dir, info, mmaps })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        let info = Self::load_info(&self.dir)?;
        let mmaps = Self::load_mmaps(&self.dir, &info, &self.mmaps)?;
        Ok(MMapReader {
            dir: self.dir.clone(),
            info,
            mmaps,
        })
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read<T, C: FromBytes<T>>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError> {
        debug!("mmap read: {} requests", requests.len());
        let mut results = Vec::with_capacity(requests.len());
        for req in requests {
            let mmap = self
                .mmaps
                .get(req.segment as usize)
                .and_then(|m| m.as_ref())
                .ok_or_else(|| {
                    MurrError::SegmentError(format!("segment {} not loaded", req.segment))
                })?;
            let value = C::from_bytes(&mmap[..], req.read.offset, req.read.size);
            results.push(value);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::column::ColumnSegmentBytes;
    use crate::io2::directory::{Directory, DirectoryReader, DirectoryWriter, ReadRequest};
    use crate::io2::info::ColumnInfo;
    use crate::io2::url::LocalUrl;

    fn test_dir(tmp: &tempfile::TempDir) -> Arc<MMapDirectory> {
        let url = LocalUrl {
            path: tmp.path().to_path_buf(),
        };
        Arc::new(MMapDirectory::open(&url, 4096, false))
    }

    fn column_bytes(
        name: &str,
        dtype: DType,
        payload: Vec<u8>,
        num_values: u32,
    ) -> ColumnSegmentBytes {
        ColumnSegmentBytes::new(
            ColumnInfo {
                name: name.to_string(),
                dtype,
                nullable: false,
            },
            payload,
            num_values,
        )
    }

    #[tokio::test]
    async fn read_bytes_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);

        // Write a segment with known bytes
        let writer = dir.open_writer().await.unwrap();
        let payload = b"hello world!".to_vec();
        writer
            .write(&[column_bytes("data", DType::Utf8, payload, 1)])
            .await
            .unwrap();

        // Read back
        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 0,
            read: ReadRequest {
                offset: 0,
                size: 12,
            },
        }];
        let results: Vec<Vec<u8>> = reader.read::<Vec<u8>, Vec<u8>>(&requests).await.unwrap();
        assert_eq!(results, vec![b"hello world!".to_vec()]);
    }

    #[tokio::test]
    async fn read_f32_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);

        let writer = dir.open_writer().await.unwrap();
        let value: f32 = 42.5;
        let payload = value.to_ne_bytes().to_vec();
        writer
            .write(&[column_bytes("score", DType::Float32, payload, 1)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 0,
            read: ReadRequest { offset: 0, size: 4 },
        }];
        let results: Vec<f32> = reader.read::<f32, f32>(&requests).await.unwrap();
        assert_eq!(results, vec![42.5_f32]);
    }

    #[tokio::test]
    async fn read_multi_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        // Write two segments
        for i in 0..2u32 {
            let value: f32 = i as f32 * 10.0;
            let payload = value.to_ne_bytes().to_vec();
            writer
                .write(&[column_bytes("val", DType::Float32, payload, 1)])
                .await
                .unwrap();
        }

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![
            SegmentReadRequest {
                segment: 1,
                read: ReadRequest { offset: 0, size: 4 },
            },
            SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size: 4 },
            },
        ];
        let results: Vec<f32> = reader.read::<f32, f32>(&requests).await.unwrap();
        assert_eq!(results, vec![10.0_f32, 0.0_f32]);
    }

    #[tokio::test]
    async fn read_missing_segment_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        writer
            .write(&[column_bytes("x", DType::Float32, vec![0; 4], 1)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 99,
            read: ReadRequest { offset: 0, size: 4 },
        }];
        let result = reader.read::<f32, f32>(&requests).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn info_returns_cached_table_info() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        writer
            .write(&[column_bytes("x", DType::Float32, vec![0; 4], 1)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let info = reader.info();
        assert_eq!(info.max_segment_id, 0);
        assert!(info.columns.contains_key("x"));
    }
}
