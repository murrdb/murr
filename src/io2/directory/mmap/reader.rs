use std::sync::Arc;

use memmap2::Mmap;

use crate::core::MurrError;
use crate::io2::bytes::FromBytes;
use crate::io2::directory::mmap::directory::MMapDirectory;
use crate::io2::directory::{SegmentReadRequest, TableReader};
use crate::io2::info::TableInfo;

pub struct MMapReader {
    dir: Arc<MMapDirectory>,
    info: TableInfo,
    mmaps: Vec<Option<Mmap>>,
}

impl MMapReader {
    fn load_info(dir: &MMapDirectory) -> Result<TableInfo, MurrError> {
        let path = dir.metadata_path();
        let data = std::fs::read(&path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", path.display())))
    }

    fn load_mmaps(dir: &MMapDirectory, info: &TableInfo) -> Result<Vec<Option<Mmap>>, MurrError> {
        let max_id = info.max_segment_id as usize;
        let mut mmaps: Vec<Option<Mmap>> = (0..=max_id).map(|_| None).collect();

        // Collect all segment IDs from column metadata
        let mut segment_ids = std::collections::BTreeSet::new();
        for col in info.columns.values() {
            for &seg_id in col.segments.keys() {
                segment_ids.insert(seg_id);
            }
        }

        for seg_id in segment_ids {
            let path = dir.segment_path(seg_id);
            let file = std::fs::File::open(&path)
                .map_err(|e| MurrError::IoError(format!("opening {}: {e}", path.display())))?;
            let mmap = unsafe { Mmap::map(&file) }
                .map_err(|e| MurrError::IoError(format!("mmapping {}: {e}", path.display())))?;
            if (seg_id as usize) < mmaps.len() {
                mmaps[seg_id as usize] = Some(mmap);
            }
        }

        Ok(mmaps)
    }
}

impl TableReader for MMapReader {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = Self::load_info(&dir)?;
        let mmaps = Self::load_mmaps(&dir, &info)?;
        Ok(MMapReader { dir, info, mmaps })
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read<T, C: FromBytes<T>>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError> {
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
    use crate::io2::directory::{Directory, ReadRequest, SegmentBytes, TableWriter};
    use crate::io2::info::{ColumnInfo, ColumnSegment};
    use crate::io2::url::LocalUrl;
    use std::collections::HashMap;

    fn test_dir(tmp: &tempfile::TempDir) -> Arc<MMapDirectory> {
        let url = LocalUrl {
            path: tmp.path().to_path_buf(),
        };
        Arc::new(MMapDirectory::open(&url, 4096, false))
    }

    #[tokio::test]
    async fn read_bytes_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);

        // Write a segment with known bytes
        let writer = dir.open_writer().await.unwrap();
        let payload = b"hello world!".to_vec();
        let mut col_segments = HashMap::new();
        col_segments.insert(
            0,
            ColumnSegment {
                offset: 0,
                length: payload.len() as u32,
                num_values: 1,
            },
        );
        let segment = SegmentBytes {
            id: 0,
            payload,
            columns: vec![ColumnInfo {
                name: "data".to_string(),
                dtype: DType::Utf8,
                nullable: false,
                segments: col_segments,
            }],
        };
        writer.write(&segment).await.unwrap();

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
        let mut col_segments = HashMap::new();
        col_segments.insert(
            0,
            ColumnSegment {
                offset: 0,
                length: 4,
                num_values: 1,
            },
        );
        let segment = SegmentBytes {
            id: 0,
            payload,
            columns: vec![ColumnInfo {
                name: "score".to_string(),
                dtype: DType::Float32,
                nullable: false,
                segments: col_segments,
            }],
        };
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 0,
            read: ReadRequest {
                offset: 0,
                size: 4,
            },
        }];
        let results: Vec<f32> = reader.read::<f32, f32>(&requests).await.unwrap();
        assert_eq!(results, vec![3.14_f32]);
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
            let mut col_segments = HashMap::new();
            // Full view: include all segments written so far
            for j in 0..=i {
                col_segments.insert(
                    j,
                    ColumnSegment {
                        offset: 0,
                        length: 4,
                        num_values: 1,
                    },
                );
            }
            let segment = SegmentBytes {
                id: i,
                payload,
                columns: vec![ColumnInfo {
                    name: "val".to_string(),
                    dtype: DType::Float32,
                    nullable: false,
                    segments: col_segments,
                }],
            };
            writer.write(&segment).await.unwrap();
        }

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![
            SegmentReadRequest {
                segment: 1,
                read: ReadRequest {
                    offset: 0,
                    size: 4,
                },
            },
            SegmentReadRequest {
                segment: 0,
                read: ReadRequest {
                    offset: 0,
                    size: 4,
                },
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

        let mut col_segments = HashMap::new();
        col_segments.insert(0, ColumnSegment { offset: 0, length: 4, num_values: 1 });
        let segment = SegmentBytes {
            id: 0,
            payload: vec![0; 4],
            columns: vec![ColumnInfo {
                name: "x".to_string(),
                dtype: DType::Float32,
                nullable: false,
                segments: col_segments,
            }],
        };
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 99,
            read: ReadRequest {
                offset: 0,
                size: 4,
            },
        }];
        let result = reader.read::<f32, f32>(&requests).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn info_returns_cached_table_info() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        let mut col_segments = HashMap::new();
        col_segments.insert(0, ColumnSegment { offset: 0, length: 4, num_values: 1 });
        let segment = SegmentBytes {
            id: 0,
            payload: vec![0; 4],
            columns: vec![ColumnInfo {
                name: "x".to_string(),
                dtype: DType::Float32,
                nullable: false,
                segments: col_segments,
            }],
        };
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let info = reader.info();
        assert_eq!(info.max_segment_id, 0);
        assert!(info.columns.contains_key("x"));
    }
}
