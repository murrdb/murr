use std::sync::Arc;

use arrow::array::{Array, Float32Array};
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::{NullBuffer, ScalarBuffer};
use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::bitmap::NullBitmap;
use crate::io2::column::float32::footer::Float32ColumnFooter;
use crate::io2::column::{ColumnReader, OffsetSize, MAX_COLUMN_HEADER_SIZE};
use crate::io2::directory::{Directory, ReadRequest, Reader, SegmentReadRequest};
use crate::io2::info::ColumnInfo;
use crate::io2::table::key_offset::KeyOffset;

pub struct Float32ColumnReader<D: Directory> {
    dir: Arc<D>,
    column: Arc<ColumnInfo>,
    segments: Vec<Option<Float32ColumnFooter>>,
    bitmap: NullBitmap<D>,
}

impl<D: Directory> Float32ColumnReader<D> {
    fn footer(&self, segment: u32) -> Result<&Float32ColumnFooter, MurrError> {
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
impl<D: Directory> ColumnReader<D> for Float32ColumnReader<D> {
    async fn open(dir: Arc<D>, column: Arc<ColumnInfo>) -> Result<Self, MurrError> {
        let reader = Arc::new(dir.open_reader().await?);
        let info = reader.info();
        let col_segments = info.columns.get(&column.name).ok_or_else(|| {
            MurrError::TableError(format!("column '{}' not found in metadata", column.name))
        })?;

        let segment_ids: Vec<u32> = col_segments.segments.keys().copied().collect();
        if segment_ids.is_empty() {
            return Ok(Float32ColumnReader {
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

        let footers: Vec<Float32ColumnFooter> = reader
            .read::<Float32ColumnFooter, Float32ColumnFooter>(&requests)
            .await?;

        let max_seg_id = *segment_ids.iter().max().unwrap() as usize;
        let mut segments: Vec<Option<Float32ColumnFooter>> = vec![None; max_seg_id + 1];
        let mut bitmap_segments: Vec<Option<OffsetSize>> = vec![None; max_seg_id + 1];

        for (i, &seg_id) in segment_ids.iter().enumerate() {
            let seg_info = &col_segments.segments[&seg_id];
            let mut footer = footers[i].clone();
            footer.payload.offset += seg_info.offset;
            if footer.bitmap.size > 0 {
                footer.bitmap.offset += seg_info.offset;
                bitmap_segments[seg_id as usize] = Some(footer.bitmap.clone());
            }
            segments[seg_id as usize] = Some(footer);
        }

        Ok(Float32ColumnReader {
            dir,
            column,
            segments,
            bitmap: NullBitmap::new(bitmap_segments, reader),
        })
    }

    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            return Ok(Arc::new(Float32Array::from(Vec::<f32>::new())));
        }

        let mut values = vec![0.0f32; num_keys];
        let mut validity = BooleanBufferBuilder::new(num_keys);
        validity.append_n(num_keys, true);
        let mut has_nulls = false;

        // Single pass: build data requests and collect non-missing keys for bitmap
        let mut data_requests: Vec<SegmentReadRequest> = Vec::with_capacity(num_keys);
        let mut request_indices: Vec<usize> = Vec::with_capacity(num_keys);
        let mut non_missing_keys: Vec<KeyOffset> = Vec::with_capacity(num_keys);

        for key in keys {
            if key.is_missing() {
                validity.set_bit(key.request_index, false);
                has_nulls = true;
            } else {
                let footer = self.footer(key.segment)?;
                data_requests.push(SegmentReadRequest {
                    segment: key.segment,
                    read: ReadRequest {
                        offset: footer.payload.offset + key.segment_index * 4,
                        size: 4,
                    },
                });
                request_indices.push(key.request_index);
                non_missing_keys.push(*key);
            }
        }

        if !data_requests.is_empty() {
            let reader = self.dir.open_reader().await?;
            let data_values: Vec<f32> = reader.read::<f32, f32>(&data_requests).await?;

            for (i, &request_index) in request_indices.iter().enumerate() {
                values[request_index] = data_values[i];
            }

            if self.column.nullable {
                let null_indices = self.bitmap.get_nulls(&non_missing_keys).await?;
                for idx in null_indices {
                    validity.set_bit(idx, false);
                    has_nulls = true;
                }
            }
        }

        let values_buffer = ScalarBuffer::from(values);
        if has_nulls {
            let null_buffer = NullBuffer::new(validity.finish());
            Ok(Arc::new(Float32Array::new(values_buffer, Some(null_buffer))))
        } else {
            Ok(Arc::new(Float32Array::new(values_buffer, None)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::column::float32::writer::Float32ColumnWriter;
    use crate::io2::column::ColumnWriter;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::directory::{Directory, Writer};
    use crate::io2::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::open(&MemUrl, 4096, false))
    }

    fn non_nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: false,
        })
    }

    fn nullable_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: true,
        })
    }

    fn make_array(values: &[Option<f32>]) -> Arc<dyn Array> {
        Arc::new(values.iter().copied().collect::<Float32Array>())
    }

    fn make_non_null_array(values: &[f32]) -> Arc<dyn Array> {
        Arc::new(Float32Array::from(values.to_vec()))
    }

    async fn write_segment(
        dir: &Arc<MemDirectory>,
        col_info: &Arc<ColumnInfo>,
        values: Arc<dyn Array>,
    ) {
        let writer = Float32ColumnWriter::new(dir.clone(), col_info.clone());
        let segment_bytes = writer.write(values).await.unwrap();
        let dir_writer = dir.open_writer().await.unwrap();
        dir_writer.write(&[segment_bytes]).await.unwrap();
    }

    #[tokio::test]
    async fn read_write_roundtrip_non_nullable() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&[10.0, 20.0, 30.0])).await;

        let reader = Float32ColumnReader::open(dir.clone(), col_info).await.unwrap();

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.value(0), 30.0);
        assert_eq!(arr.value(1), 10.0);
        assert_eq!(arr.null_count(), 0);
    }

    #[tokio::test]
    async fn read_write_multi_segment() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&[1.0, 2.0])).await;
        write_segment(&dir, &col_info, make_non_null_array(&[10.0, 20.0])).await;

        let reader = Float32ColumnReader::open(dir.clone(), col_info).await.unwrap();

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.value(0), 10.0);
        assert_eq!(arr.value(1), 2.0);
    }

    #[tokio::test]
    async fn read_missing_keys_produce_nulls() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&[5.0, 6.0])).await;

        let reader = Float32ColumnReader::open(dir.clone(), col_info).await.unwrap();

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 5.0);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 6.0);
    }

    #[tokio::test]
    async fn read_nullable_roundtrip() {
        let dir = test_dir();
        let col_info = nullable_info();

        write_segment(
            &dir,
            &col_info,
            make_array(&[Some(1.0), None, Some(3.0), None, Some(5.0)]),
        )
        .await;

        let reader = Float32ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 5);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 1.0);
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(2), 3.0);
        assert!(arr.is_null(3));
        assert!(!arr.is_null(4));
        assert_eq!(arr.value(4), 5.0);
    }

    #[tokio::test]
    async fn read_empty_keys() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, make_non_null_array(&[1.0])).await;

        let reader = Float32ColumnReader::open(dir.clone(), col_info).await.unwrap();

        let result = reader.read(&[]).await.unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 0);
    }
}
