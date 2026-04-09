use std::collections::HashMap;
use std::sync::Arc;

use log::debug;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::column::ColumnSegmentBytes;
use crate::io2::directory::mem::directory::MemDirectory;
use crate::io2::directory::{METADATA_JSON, Writer};
use crate::io2::info::{ColumnSegments, SegmentInfo, TableInfo};

pub struct MemWriter {
    dir: Arc<MemDirectory>,
}

impl MemWriter {
    fn load_existing_info(&self) -> Result<Option<TableInfo>, MurrError> {
        let files = self
            .dir
            .files
            .read()
            .map_err(|e| MurrError::IoError(format!("lock poisoned: {e}")))?;
        match files.get(METADATA_JSON) {
            None => Ok(None),
            Some(data) => {
                let info = serde_json::from_slice(data)
                    .map_err(|e| MurrError::IoError(format!("parsing metadata: {e}")))?;
                Ok(Some(info))
            }
        }
    }

    fn next_segment_id(&self) -> Result<u32, MurrError> {
        Ok(self
            .load_existing_info()?
            .map(|info| info.max_segment_id + 1)
            .unwrap_or(0))
    }
}

#[async_trait]
impl Writer for MemWriter {
    type D = MemDirectory;

    async fn new(dir: Arc<Self::D>) -> Result<Self, MurrError> {
        Ok(MemWriter { dir })
    }

    async fn write(&self, columns: &[ColumnSegmentBytes]) -> Result<(), MurrError> {
        let segment_id = self.next_segment_id()?;

        // Concatenate all column bytes, tracking offsets
        let mut combined = Vec::new();
        let mut column_infos = Vec::new();

        for col in columns {
            let offset = combined.len() as u32;
            let length = col.bytes.bytes.len() as u32;
            combined.extend_from_slice(&col.bytes.bytes);
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
        let mut info = self.load_existing_info()?.unwrap_or_else(|| TableInfo {
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

        let metadata = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;

        debug!(
            "mem write: segment={segment_id} columns={} bytes={}",
            columns.len(),
            combined.len()
        );

        let mut files = self
            .dir
            .files
            .write()
            .map_err(|e| MurrError::IoError(format!("lock poisoned: {e}")))?;
        files.insert(MemDirectory::segment_name(segment_id), combined);
        files.insert(METADATA_JSON.to_string(), metadata);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::directory::Directory;
    use crate::io2::info::ColumnInfo;
    use crate::io2::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::open(&MemUrl, 4096, false))
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
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        writer
            .write(&[column_bytes("score", vec![1, 2, 3, 4], 1)])
            .await
            .unwrap();

        let files = dir.files.read().unwrap();
        assert_eq!(files.get("00000000.seg").unwrap(), &vec![1, 2, 3, 4]);
        assert!(files.contains_key("_metadata.json"));
    }

    #[tokio::test]
    async fn write_sequential_segments() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        for i in 0..3u32 {
            writer
                .write(&[column_bytes("score", vec![i as u8; 4], 1)])
                .await
                .unwrap();
        }

        let files = dir.files.read().unwrap();
        assert!(files.contains_key("00000000.seg"));
        assert!(files.contains_key("00000001.seg"));
        assert!(files.contains_key("00000002.seg"));
        assert_eq!(files.get("00000002.seg").unwrap(), &vec![2, 2, 2, 2]);
    }

    #[tokio::test]
    async fn write_persists_metadata() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        writer
            .write(&[column_bytes("score", vec![1; 16], 4)])
            .await
            .unwrap();
        writer
            .write(&[column_bytes("score", vec![2; 16], 4)])
            .await
            .unwrap();

        let files = dir.files.read().unwrap();
        let data = files.get("_metadata.json").unwrap();
        let parsed: TableInfo = serde_json::from_slice(data).unwrap();

        assert_eq!(parsed.max_segment_id, 1);
        assert!(parsed.columns.contains_key("score"));
        assert_eq!(parsed.columns["score"].segments.len(), 2);
    }
}
