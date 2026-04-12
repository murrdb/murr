use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use async_trait::async_trait;

use crate::core::MurrError;
use crate::io::bitmap::NullBitmap;
use crate::io::column::ColumnReader;
use crate::io::column::reopen::open_segments;
use crate::io::column::scalar::ScalarCodec;
use crate::io::column::scalar::footer::ScalarColumnFooter;
use crate::io::directory::{DirectoryReader, ReadRequest, SegmentReadRequest};
use crate::io::info::{ColumnInfo, ColumnSegments};
use crate::io::table::key_offset::KeyOffset;

pub struct ScalarColumnReader<R: DirectoryReader, S: ScalarCodec> {
    reader: Arc<R>,
    column: ColumnInfo,
    segments: Vec<Option<ScalarColumnFooter>>,
    bitmap: NullBitmap,
    _codec: PhantomData<S>,
}

impl<R: DirectoryReader, S: ScalarCodec> ScalarColumnReader<R, S> {
    fn footer(&self, segment: u32) -> Result<&ScalarColumnFooter, MurrError> {
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
impl<R: DirectoryReader, S: ScalarCodec> ColumnReader<R> for ScalarColumnReader<R, S> {
    async fn open(
        reader: Arc<R>,
        column: &ColumnSegments,
        previous: &Option<Self>,
    ) -> Result<Self, MurrError> {
        let opened = open_segments::<ScalarColumnFooter, _>(
            &reader,
            column,
            previous.as_ref().map(|p| &p.segments),
            previous.as_ref().map(|p| &p.bitmap),
        )
        .await?;
        Ok(ScalarColumnReader {
            reader,
            column: column.column.clone(),
            segments: opened.segments,
            bitmap: opened.bitmap,
            _codec: PhantomData,
        })
    }

    async fn reopen(
        &self,
        reader: Arc<R>,
        column: &ColumnSegments,
    ) -> Result<Arc<dyn ColumnReader<R>>, MurrError> {
        let prev = Self {
            reader: self.reader.clone(),
            column: self.column.clone(),
            segments: self.segments.clone(),
            bitmap: self.bitmap.clone(),
            _codec: PhantomData,
        };
        Ok(Arc::new(Self::open(reader, column, &Some(prev)).await?))
    }

    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            let empty = ScalarBuffer::from(Vec::<S::Native>::new());
            return Ok(Arc::new(PrimitiveArray::<S::ArrowType>::new(empty, None)));
        }

        let mut values = vec![S::Native::default(); num_keys];
        let mut validity = BooleanBufferBuilder::new(num_keys);
        validity.append_n(num_keys, true);
        let mut has_nulls = false;

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
                        offset: footer.payload.offset + key.segment_index * S::ELEMENT_SIZE,
                        size: S::ELEMENT_SIZE,
                    },
                });
                request_indices.push(key.request_index);
                non_missing_keys.push(*key);
            }
        }

        if !data_requests.is_empty() {
            let data_values: Vec<S::Native> = self.reader.read(&data_requests).await?;

            for (i, &request_index) in request_indices.iter().enumerate() {
                values[request_index] = data_values[i];
            }

            if self.column.nullable {
                let null_indices = self
                    .bitmap
                    .get_nulls(&*self.reader, &non_missing_keys)
                    .await?;
                for idx in null_indices {
                    validity.set_bit(idx, false);
                    has_nulls = true;
                }
            }
        }

        let values_buffer = ScalarBuffer::from(values);
        if has_nulls {
            let null_buffer = NullBuffer::new(validity.finish());
            Ok(Arc::new(PrimitiveArray::<S::ArrowType>::new(
                values_buffer,
                Some(null_buffer),
            )))
        } else {
            Ok(Arc::new(PrimitiveArray::<S::ArrowType>::new(
                values_buffer,
                None,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::core::{ColumnSchema, TableSchema};
    use crate::io::column::float32::Float32Codec;
    use crate::io::column::scalar::writer::write_scalar;
    use crate::io::directory::mem::directory::MemDirectory;
    use crate::io::directory::mem::reader::MemReader;
    use crate::io::directory::{Directory, DirectoryWriter};
    use crate::io::url::MemUrl;
    use arrow::array::Float32Array;
    use std::collections::HashMap;

    type Float32Reader = ScalarColumnReader<MemReader, Float32Codec>;

    fn test_dir() -> Arc<MemDirectory> {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
        let schema = TableSchema {
            key: "key".to_string(),
            columns,
        };
        Arc::new(MemDirectory::create(&MemUrl, "default", schema, 4096, false).unwrap())
    }

    fn non_nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: false,
        }
    }

    fn nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: true,
        }
    }

    fn make_array(values: &[Option<f32>]) -> Float32Array {
        values.iter().copied().collect::<Float32Array>()
    }

    fn make_non_null_array(values: &[f32]) -> Float32Array {
        Float32Array::from(values.to_vec())
    }

    async fn write_segment(dir: &Arc<MemDirectory>, col_info: &ColumnInfo, values: &Float32Array) {
        let segment_bytes = write_scalar::<Float32Codec>(col_info, values).unwrap();
        let dir_writer = dir.open_writer().await.unwrap();
        dir_writer.write(&[segment_bytes]).await.unwrap();
    }

    async fn open_reader(dir: &Arc<MemDirectory>, col_name: &str) -> Float32Reader {
        let reader: Arc<MemReader> = Arc::new(dir.open_reader().await.unwrap());
        let col_segments = reader.info().columns.get(col_name).unwrap().clone();
        Float32Reader::open(reader, &col_segments, &None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn read_write_roundtrip_non_nullable() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&[10.0, 20.0, 30.0])).await;

        let reader = open_reader(&dir, "score").await;

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

        write_segment(&dir, &col_info, &make_non_null_array(&[1.0, 2.0])).await;
        write_segment(&dir, &col_info, &make_non_null_array(&[10.0, 20.0])).await;

        let reader = open_reader(&dir, "score").await;

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

        write_segment(&dir, &col_info, &make_non_null_array(&[5.0, 6.0])).await;

        let reader = open_reader(&dir, "score").await;

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
            &make_array(&[Some(1.0), None, Some(3.0), None, Some(5.0)]),
        )
        .await;

        let reader = open_reader(&dir, "score").await;

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

        write_segment(&dir, &col_info, &make_non_null_array(&[1.0])).await;

        let reader = open_reader(&dir, "score").await;

        let result = reader.read(&[]).await.unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 0);
    }
}
