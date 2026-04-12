use std::sync::Arc;

use log::debug;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io::bytes::FromBytes;
use crate::io::directory::mem::directory::MemDirectory;
use crate::io::directory::{DirectoryReader, SegmentReadRequest, METADATA_JSON};
use crate::io::info::TableInfo;

pub struct MemReader {
    dir: Arc<MemDirectory>,
    info: TableInfo,
}

#[async_trait]
impl DirectoryReader for MemReader {
    type D = MemDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        let info = {
            let files = dir
                .files
                .read()
                .map_err(|e| MurrError::IoError(format!("lock poisoned: {e}")))?;
            let data = files
                .get(METADATA_JSON)
                .ok_or_else(|| MurrError::IoError("no metadata found".to_string()))?;
            serde_json::from_slice(data)
                .map_err(|e| MurrError::IoError(format!("parsing metadata: {e}")))?
        };
        Ok(MemReader { dir, info })
    }

    async fn reopen_reader(&self) -> Result<Self, MurrError> {
        Self::new(self.dir.clone()).await
    }

    fn info(&self) -> &TableInfo {
        &self.info
    }

    async fn read<T: FromBytes<T> + Send>(
        &self,
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError> {
        debug!("mem read: {} requests", requests.len());
        let files = self
            .dir
            .files
            .read()
            .map_err(|e| MurrError::IoError(format!("lock poisoned: {e}")))?;
        let mut results = Vec::with_capacity(requests.len());
        for req in requests {
            let name = MemDirectory::segment_name(req.segment);
            let data = files.get(&name).ok_or_else(|| {
                MurrError::SegmentError(format!("segment {} not loaded", req.segment))
            })?;
            let value = T::from_bytes(data, req.read.offset, req.read.size);
            results.push(value);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::column::{ColumnSegmentBytes, PayloadBytes};
    use crate::io::directory::{Directory, DirectoryReader, DirectoryWriter, ReadRequest};
    use crate::io::info::ColumnInfo;
    use crate::io::url::MemUrl;
    use std::collections::HashMap;

    fn test_dir() -> Arc<MemDirectory> {
        let mut columns = HashMap::new();
        columns.insert("key".to_string(), ColumnSchema { dtype: DType::Utf8, nullable: false });
        let schema = TableSchema { key: "key".to_string(), columns };
        Arc::new(MemDirectory::create(&MemUrl, "default", schema, 4096, false).unwrap())
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
            vec![PayloadBytes::new(payload)],
            Vec::new(),
            num_values,
        )
    }

    #[tokio::test]
    async fn read_bytes_roundtrip() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        let payload = b"hello world!".to_vec();
        writer
            .write(&[column_bytes("data", DType::Utf8, payload, 1)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let requests = vec![SegmentReadRequest {
            segment: 0,
            read: ReadRequest {
                offset: 0,
                size: 12,
            },
        }];
        let results: Vec<Vec<u8>> = reader.read::<Vec<u8>>(&requests).await.unwrap();
        assert_eq!(results, vec![b"hello world!".to_vec()]);
    }

    #[tokio::test]
    async fn read_f32_roundtrip() {
        let dir = test_dir();
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
        let results: Vec<f32> = reader.read::<f32>(&requests).await.unwrap();
        assert_eq!(results, vec![42.5_f32]);
    }

    #[tokio::test]
    async fn read_multi_segment() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

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
        let results: Vec<f32> = reader.read::<f32>(&requests).await.unwrap();
        assert_eq!(results, vec![10.0_f32, 0.0_f32]);
    }

    #[tokio::test]
    async fn read_missing_segment_errors() {
        let dir = test_dir();
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
        let result = reader.read::<f32>(&requests).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn info_returns_table_info() {
        let dir = test_dir();
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
