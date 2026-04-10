use std::sync::Arc;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::bitmap::NullBitmap;
use crate::io2::column::utf8::footer::{StringOffsetPair, Utf8ColumnFooter};
use crate::io2::column::{ColumnReader, OffsetSize, MAX_COLUMN_HEADER_SIZE};
use crate::io2::directory::{Directory, ReadRequest, Reader, SegmentReadRequest};
use crate::io2::info::ColumnInfo;
use crate::io2::table::key_offset::KeyOffset;

pub struct Utf8ColumnReader<D: Directory> {
    dir: Arc<D>,
    column: Arc<ColumnInfo>,
    segments: Vec<Option<Utf8ColumnFooter>>,
    bitmap: NullBitmap<D>,
}

impl<D: Directory> Utf8ColumnReader<D> {
    fn footer(&self, segment: u32) -> Result<&Utf8ColumnFooter, MurrError> {
        self.segments
            .get(segment as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "segment {} not found for column '{}'",
                    segment, self.column.name
                ))
            })
    }
}

#[async_trait]
impl<D: Directory> ColumnReader<D> for Utf8ColumnReader<D> {
    async fn open(dir: Arc<D>, column: Arc<ColumnInfo>) -> Result<Self, MurrError> {
        let reader = Arc::new(dir.open_reader().await?);
        let info = reader.info();
        let col_segments = info.columns.get(&column.name).ok_or_else(|| {
            MurrError::TableError(format!("column '{}' not found in metadata", column.name))
        })?;

        let segment_ids: Vec<u32> = col_segments.segments.keys().copied().collect();
        if segment_ids.is_empty() {
            return Ok(Utf8ColumnReader {
                dir,
                column,
                segments: Vec::new(),
                bitmap: NullBitmap::new(Vec::new(), reader),
            });
        }

        // Read footer region from tail of each column blob
        let requests: Vec<SegmentReadRequest> = segment_ids
            .iter()
            .map(|&seg_id| {
                let seg_info = &col_segments.segments[&seg_id];
                let read_size = MAX_COLUMN_HEADER_SIZE.min(seg_info.length);
                let read_offset = seg_info.offset + seg_info.length - read_size;
                SegmentReadRequest {
                    segment: seg_id,
                    read: ReadRequest {
                        offset: read_offset,
                        size: read_size,
                    },
                }
            })
            .collect();

        let footers: Vec<Utf8ColumnFooter> = reader
            .read::<Utf8ColumnFooter, Utf8ColumnFooter>(&requests)
            .await?;

        let max_seg_id = segment_ids
            .iter()
            .copied()
            .max()
            .ok_or_else(|| MurrError::SegmentError("no segment ids".into()))?
            as usize;
        let mut segments: Vec<Option<Utf8ColumnFooter>> = vec![None; max_seg_id + 1];
        let mut bitmap_segments: Vec<Option<OffsetSize>> = vec![None; max_seg_id + 1];

        for (i, &seg_id) in segment_ids.iter().enumerate() {
            let seg_info = &col_segments.segments[&seg_id];
            let mut footer = footers[i].clone();
            footer.offsets.offset += seg_info.offset;
            footer.payload.offset += seg_info.offset;
            if footer.bitmap.size > 0 {
                footer.bitmap.offset += seg_info.offset;
                bitmap_segments[seg_id as usize] = Some(footer.bitmap.clone());
            }
            segments[seg_id as usize] = Some(footer);
        }

        Ok(Utf8ColumnReader {
            dir,
            column,
            segments,
            bitmap: NullBitmap::new(bitmap_segments, reader),
        })
    }

    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            return Ok(Arc::new(StringArray::from(Vec::<&str>::new())));
        }

        let mut values: Vec<Option<String>> = vec![None; num_keys];

        // Collect non-missing keys
        let mut non_missing_keys: Vec<KeyOffset> = Vec::with_capacity(num_keys);
        for key in keys {
            if !key.is_missing() {
                non_missing_keys.push(*key);
            }
        }

        if !non_missing_keys.is_empty() {
            let reader = self.dir.open_reader().await?;

            // Phase 1: Read i32 offset pairs (8 bytes each: offsets[i] and offsets[i+1])
            let mut offset_requests: Vec<SegmentReadRequest> =
                Vec::with_capacity(non_missing_keys.len());
            for key in &non_missing_keys {
                let footer = self.footer(key.segment)?;
                offset_requests.push(SegmentReadRequest {
                    segment: key.segment,
                    read: ReadRequest {
                        offset: footer.offsets.offset + key.segment_index * 4,
                        size: 8,
                    },
                });
            }

            let offset_pairs: Vec<StringOffsetPair> = reader
                .read::<StringOffsetPair, StringOffsetPair>(&offset_requests)
                .await?;

            // Phase 2: Read actual string bytes
            let mut payload_requests: Vec<SegmentReadRequest> =
                Vec::with_capacity(non_missing_keys.len());
            let mut payload_indices: Vec<usize> = Vec::with_capacity(non_missing_keys.len());

            for (i, key) in non_missing_keys.iter().enumerate() {
                let footer = self.footer(key.segment)?;
                let pair = &offset_pairs[i];
                let len = (pair.end - pair.start) as u32;
                // Set empty string as default for non-missing keys
                values[key.request_index] = Some(String::new());
                if len > 0 {
                    payload_requests.push(SegmentReadRequest {
                        segment: key.segment,
                        read: ReadRequest {
                            offset: footer.payload.offset + pair.start as u32,
                            size: len,
                        },
                    });
                    payload_indices.push(i);
                }
            }

            if !payload_requests.is_empty() {
                let string_values: Vec<String> =
                    reader.read::<String, String>(&payload_requests).await?;

                for (j, &orig_idx) in payload_indices.iter().enumerate() {
                    let key = &non_missing_keys[orig_idx];
                    values[key.request_index] = Some(string_values[j].clone());
                }
            }

            // Check null bitmap for nullable columns
            if self.column.nullable {
                let null_indices = self.bitmap.get_nulls(&non_missing_keys).await?;
                for idx in null_indices {
                    values[idx] = None;
                }
            }
        }

        let array: StringArray = values.iter().map(|v| v.as_deref()).collect();
        Ok(Arc::new(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::column::utf8::writer::Utf8ColumnWriter;
    use crate::io2::column::ColumnWriter;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::directory::{Directory, Writer};
    use crate::io2::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::open(&MemUrl, 4096, false))
    }

    fn non_nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "name".to_string(),
            dtype: DType::Utf8,
            nullable: false,
        })
    }

    fn nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "name".to_string(),
            dtype: DType::Utf8,
            nullable: true,
        })
    }

    fn make_array(values: &[Option<&str>]) -> Arc<dyn Array> {
        Arc::new(values.iter().copied().collect::<StringArray>())
    }

    fn make_non_null_array(values: &[&str]) -> Arc<dyn Array> {
        Arc::new(StringArray::from(values.to_vec()))
    }

    async fn write_segment(
        dir: &Arc<MemDirectory>,
        col_info: &Arc<ColumnInfo>,
        values: Arc<dyn Array>,
    ) {
        let writer = Utf8ColumnWriter::new(dir.clone(), col_info.clone());
        let segment_bytes = writer.write(values).await.unwrap();
        let dir_writer = dir.open_writer().await.unwrap();
        dir_writer.write(&[segment_bytes]).await.unwrap();
    }

    #[tokio::test]
    async fn read_write_roundtrip_non_nullable() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&["hello", "world", "!"])).await;

        let reader = Utf8ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 0,
                segment_index: 2,
            },
            KeyOffset {
                request_index: 1,
                segment: 0,
                segment_index: 0,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "!");
        assert_eq!(arr.value(1), "hello");
        assert_eq!(arr.null_count(), 0);
    }

    #[tokio::test]
    async fn read_write_multi_segment() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&["a", "bb"])).await;
        write_segment(&dir, &col_info, make_non_null_array(&["ccc", "dddd"])).await;

        let reader = Utf8ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 1,
                segment_index: 0,
            },
            KeyOffset {
                request_index: 1,
                segment: 0,
                segment_index: 1,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "ccc");
        assert_eq!(arr.value(1), "bb");
    }

    #[tokio::test]
    async fn read_missing_keys_produce_nulls() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&["foo", "bar"])).await;

        let reader = Utf8ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 0,
                segment_index: 0,
            },
            KeyOffset::missing(1),
            KeyOffset {
                request_index: 2,
                segment: 0,
                segment_index: 1,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), "foo");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "bar");
    }

    #[tokio::test]
    async fn read_nullable_roundtrip() {
        let dir = test_dir();
        let col_info = nullable_info();

        write_segment(
            &dir,
            &col_info,
            make_array(&[Some("a"), None, Some("bc"), None, Some("d")]),
        )
        .await;

        let reader = Utf8ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 5);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), "a");
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(2), "bc");
        assert!(arr.is_null(3));
        assert!(!arr.is_null(4));
        assert_eq!(arr.value(4), "d");
    }

    #[tokio::test]
    async fn read_empty_keys() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&["x"])).await;

        let reader = Utf8ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let result = reader.read(&[]).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 0);
    }
}
