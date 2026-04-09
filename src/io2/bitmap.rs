use arrow::array::Array;
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io2::{
    directory::{Directory, ReadRequest, Reader, SegmentReadRequest},
    table::key_offset::KeyOffset,
};

pub struct NullBitmap {
    pub segment: u32,
    pub offset: u32,
    pub size: u32,
}

impl NullBitmap {
    pub fn new(segment: u32, offset: u32, size: u32) -> Self {
        NullBitmap {
            segment,
            offset,
            size,
        }
    }

    pub async fn get_nulls<D: Directory>(
        &self,
        reader: &D::ReaderType<'_>,
        keys: &[KeyOffset],
    ) -> Result<Vec<usize>, MurrError> {
        if self.size == 0 || keys.is_empty() {
            return Ok(Vec::new());
        }

        let requests: Vec<SegmentReadRequest> = keys
            .iter()
            .map(|key| {
                let word_index = key.segment_index / 64;
                let word_byte_offset = self.offset + word_index * 8;
                SegmentReadRequest {
                    segment: self.segment,
                    read: ReadRequest {
                        offset: word_byte_offset,
                        size: 8,
                    },
                }
            })
            .collect();

        let words: Vec<u64> = reader.read::<u64, u64>(&requests).await?;

        let nulls = keys
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

    use crate::core::DType;
    use crate::io2::column::ColumnSegmentBytes;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::directory::{Directory, Writer};
    use crate::io2::info::ColumnInfo;
    use crate::io2::url::MemUrl;

    fn test_dir() -> MemDirectory {
        MemDirectory::open(&MemUrl, 4096, false)
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
        let bytes = NullBitmap::write(&array);
        assert!(bytes.is_empty());
    }

    #[test]
    fn write_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 8);
        let words: &[u64] = cast_slice(&bytes);
        // bit 0 set, bit 1 clear, bit 2 set, bit 3 set = 0b1101 = 13
        assert_eq!(words[0], 0b1101);
    }

    #[test]
    fn write_all_nulls() {
        let array = Int32Array::from(vec![None, None, None]);
        let bytes = NullBitmap::write(&array);
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
        let bytes = NullBitmap::write(&array);
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
        let bytes = NullBitmap::write(&array);
        assert_eq!(bytes.len(), 16);
        let words: &[u64] = cast_slice(&bytes);
        assert_eq!(words[0], u64::MAX);
        assert_eq!(words[1], 0);
    }

    #[tokio::test]
    async fn get_nulls_empty_bitmap() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(vec![0; 8], 4)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let bitmap = NullBitmap::new(0, 0, 0);
        let keys = vec![KeyOffset {
            request_index: 0,
            segment: 0,
            segment_index: 0,
        }];
        let nulls = bitmap.get_nulls::<MemDirectory>(&reader, &keys).await.unwrap();
        assert!(nulls.is_empty());
    }

    #[tokio::test]
    async fn get_nulls_empty_keys() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(vec![0; 8], 4)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let bitmap = NullBitmap::new(0, 0, 8);
        let nulls = bitmap.get_nulls::<MemDirectory>(&reader, &[]).await.unwrap();
        assert!(nulls.is_empty());
    }

    #[tokio::test]
    async fn get_nulls_roundtrip() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let bitmap_bytes = NullBitmap::write(&array);
        assert_eq!(bitmap_bytes.len(), 8);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 5)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let bitmap = NullBitmap::new(0, 0, bitmap_bytes.len() as u32);

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let nulls = bitmap.get_nulls::<MemDirectory>(&reader, &keys).await.unwrap();
        assert_eq!(nulls, vec![1, 3]);
    }

    #[tokio::test]
    async fn get_nulls_sparse_keys() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let bitmap_bytes = NullBitmap::write(&array);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 5)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let bitmap = NullBitmap::new(0, 0, bitmap_bytes.len() as u32);

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

        let nulls = bitmap.get_nulls::<MemDirectory>(&reader, &keys).await.unwrap();
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
        let bitmap_bytes = NullBitmap::write(&array);
        assert_eq!(bitmap_bytes.len(), 16);

        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();
        writer
            .write(&[bitmap_column(bitmap_bytes.clone(), 65)])
            .await
            .unwrap();

        let reader = dir.open_reader().await.unwrap();
        let bitmap = NullBitmap::new(0, 0, bitmap_bytes.len() as u32);

        let keys: Vec<KeyOffset> = (0..65)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let nulls = bitmap.get_nulls::<MemDirectory>(&reader, &keys).await.unwrap();
        assert_eq!(nulls, vec![64]);
    }
}
