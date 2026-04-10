use std::sync::Arc;

use arrow::array::Array;
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io2::column::OffsetSize;
use crate::io2::{
    directory::{Directory, ReadRequest, Reader, SegmentReadRequest},
    table::key_offset::KeyOffset,
};

pub struct NullBitmap<D: Directory> {
    pub segments: Vec<Option<OffsetSize>>,
    pub reader: Arc<D::ReaderType>,
}

impl<D: Directory> Clone for NullBitmap<D> {
    fn clone(&self) -> Self {
        NullBitmap {
            segments: self.segments.clone(),
            reader: self.reader.clone(),
        }
    }
}

impl<D: Directory> NullBitmap<D> {
    pub fn new(segments: Vec<Option<OffsetSize>>, reader: Arc<D::ReaderType>) -> Self {
        NullBitmap { segments, reader }
    }

    pub async fn get_nulls(&self, keys: &[KeyOffset]) -> Result<Vec<usize>, MurrError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut bitmap_keys: Vec<&KeyOffset> = Vec::new();
        let mut requests: Vec<SegmentReadRequest> = Vec::new();

        for key in keys {
            let seg = key.segment as usize;
            match self.segments.get(seg).and_then(|s| s.as_ref()) {
                Some(os) if os.size > 0 => {
                    let word_index = key.segment_index / 64;
                    let word_byte_offset = os.offset + word_index * 8;
                    requests.push(SegmentReadRequest {
                        segment: key.segment,
                        read: ReadRequest {
                            offset: word_byte_offset,
                            size: 8,
                        },
                    });
                    bitmap_keys.push(key);
                }
                _ => {}
            }
        }

        if bitmap_keys.is_empty() {
            return Ok(Vec::new());
        }

        let words: Vec<u64> = self.reader.read::<u64, u64>(&requests).await?;

        let nulls = bitmap_keys
            .iter()
            .zip(words.iter())
            .filter_map(|(key, &word)| {
                let bit_idx = key.segment_index % 64;
                if (word >> bit_idx) & 1 == 0 {
                    Some(key.request_index)
                } else {
                    None
                }
            })
            .collect();

        Ok(nulls)
    }

    pub fn write(values: &dyn Array) -> Vec<u8> {
        let len = values.len();
        let word_count = len.div_ceil(64);
        let mut words = vec![0u64; word_count];
        let mut has_nulls = false;

        for i in 0..len {
            if values.is_null(i) {
                has_nulls = true;
            } else {
                words[i / 64] |= 1 << (i % 64);
            }
        }

        if has_nulls {
            cast_slice(&words).to_vec()
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    use std::sync::Arc;

    use crate::core::DType;
    use crate::io2::column::ColumnSegmentBytes;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::directory::{Directory, Writer};
    use crate::io2::info::ColumnInfo;
    use crate::io2::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::open(&MemUrl, 4096, false))
    }

    fn bitmap_column(payload: Vec<u8>, num_values: u32) -> ColumnSegmentBytes {
        ColumnSegmentBytes::new(
            ColumnInfo {
                name: "bitmap".to_string(),
                dtype: DType::Float32,
                nullable: true,
            },
            payload,
            num_values,
        )
    }

    #[test]
    fn write_no_nulls() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let bytes = NullBitmap::<MemDirectory>::write(&array);
        assert!(bytes.is_empty());
    }

    #[test]
    fn write_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bytes.len(), 8);
        let words: &[u64] = cast_slice(&bytes);
        // bit 0 set, bit 1 clear, bit 2 set, bit 3 set = 0b1101 = 13
        assert_eq!(words[0], 0b1101);
    }

    #[test]
    fn write_all_nulls() {
        let array = Int32Array::from(vec![None, None, None]);
        let bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bytes.len(), 8);
        let words: &[u64] = cast_slice(&bytes);
        assert_eq!(words[0], 0);
    }

    #[test]
    fn write_boundary_64_values() {
        let values: Vec<Option<i32>> = (0..64)
            .map(|i| if i == 63 { None } else { Some(i) })
            .collect();
        let array = Int32Array::from(values);
        let bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bytes.len(), 8);
        let words: &[u64] = cast_slice(&bytes);
        assert_eq!(words[0], u64::MAX ^ (1 << 63));
    }

    #[test]
    fn write_boundary_65_values() {
        let values: Vec<Option<i32>> = (0..65)
            .map(|i| if i == 64 { None } else { Some(i) })
            .collect();
        let array = Int32Array::from(values);
        let bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bytes.len(), 16);
        let words: &[u64] = cast_slice(&bytes);
        assert_eq!(words[0], u64::MAX);
        assert_eq!(words[1], 0);
    }

    #[tokio::test]
    async fn get_nulls_empty_bitmap() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer.write(&[bitmap_column(vec![0; 8], 4)]).await.unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());
        let bitmap: NullBitmap<MemDirectory> = NullBitmap::new(vec![None], reader);
        let keys = vec![KeyOffset {
            request_index: 0,
            segment: 0,
            segment_index: 0,
        }];
        let nulls = bitmap.get_nulls(&keys).await.unwrap();
        assert!(nulls.is_empty());
    }

    #[tokio::test]
    async fn get_nulls_empty_keys() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer.write(&[bitmap_column(vec![0; 8], 4)]).await.unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());
        let bitmap: NullBitmap<MemDirectory> = NullBitmap::new(
            vec![Some(OffsetSize { offset: 0, size: 8 })],
            reader,
        );
        let nulls = bitmap.get_nulls(&[]).await.unwrap();
        assert!(nulls.is_empty());
    }

    #[tokio::test]
    async fn get_nulls_roundtrip() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let bitmap_bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bitmap_bytes.len(), 8);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 5)])
            .await
            .unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());
        let bitmap: NullBitmap<MemDirectory> = NullBitmap::new(
            vec![Some(OffsetSize { offset: 0, size: bitmap_bytes.len() as u32 })],
            reader,
        );

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let nulls = bitmap.get_nulls(&keys).await.unwrap();
        assert_eq!(nulls, vec![1, 3]);
    }

    #[tokio::test]
    async fn get_nulls_sparse_keys() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let bitmap_bytes = NullBitmap::<MemDirectory>::write(&array);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 5)])
            .await
            .unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());
        let bitmap: NullBitmap<MemDirectory> = NullBitmap::new(
            vec![Some(OffsetSize { offset: 0, size: bitmap_bytes.len() as u32 })],
            reader,
        );

        // Only query indices 1 and 4 (request_index differs from segment_index)
        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 0,
                segment_index: 1,
            },
            KeyOffset {
                request_index: 1,
                segment: 0,
                segment_index: 4,
            },
        ];

        let nulls = bitmap.get_nulls(&keys).await.unwrap();
        // segment_index 1 is null -> request_index 0
        // segment_index 4 is valid -> not included
        assert_eq!(nulls, vec![0]);
    }

    #[tokio::test]
    async fn get_nulls_multi_word() {
        let values: Vec<Option<i32>> = (0..65)
            .map(|i| if i == 64 { None } else { Some(i) })
            .collect();
        let array = Int32Array::from(values);
        let bitmap_bytes = NullBitmap::<MemDirectory>::write(&array);
        assert_eq!(bitmap_bytes.len(), 16);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 65)])
            .await
            .unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());
        let bitmap: NullBitmap<MemDirectory> = NullBitmap::new(
            vec![Some(OffsetSize { offset: 0, size: bitmap_bytes.len() as u32 })],
            reader,
        );

        let keys: Vec<KeyOffset> = (0..65)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let nulls = bitmap.get_nulls(&keys).await.unwrap();
        assert_eq!(nulls, vec![64]);
    }
}
