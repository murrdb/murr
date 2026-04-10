use std::sync::Arc;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io2::bitmap::NullBitmap;
use crate::io2::column::utf8::footer::{align8_padding, encode_footer, Utf8ColumnFooter};
use crate::io2::column::{ColumnSegmentBytes, ColumnWriter, OffsetSize};
use crate::io2::directory::Directory;
use crate::io2::info::ColumnInfo;

pub struct Utf8ColumnWriter<D: Directory> {
    dir: Arc<D>,
    column: Arc<ColumnInfo>,
}

impl<D: Directory> Utf8ColumnWriter<D> {
    pub fn new(dir: Arc<D>, column: Arc<ColumnInfo>) -> Self {
        Utf8ColumnWriter { dir, column }
    }
}

#[async_trait]
impl<D: Directory> ColumnWriter<D> for Utf8ColumnWriter<D> {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError> {
        let array = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MurrError::TableError("expected StringArray".into()))?;

        let num_values = array.len() as u32;

        // Build i32 offset array (num_values + 1 entries) and concatenated payload
        let mut offsets: Vec<i32> = Vec::with_capacity(array.len() + 1);
        let mut payload = Vec::new();
        for i in 0..array.len() {
            offsets.push(payload.len() as i32);
            if !array.is_null(i) {
                payload.extend_from_slice(array.value(i).as_bytes());
            }
        }
        offsets.push(payload.len() as i32);

        let offsets_bytes: &[u8] = cast_slice(&offsets);
        let offsets_size = offsets_bytes.len() as u32;

        // Build null bitmap
        let bitmap_bytes = if self.column.nullable {
            NullBitmap::<D>::write(values.as_ref())
        } else {
            Vec::new()
        };

        // Layout: [offsets][pad8][payload][pad8][bitmap][pad8][footer]
        let padding1 = align8_padding(offsets_size);

        let payload_size = payload.len() as u32;
        let payload_offset = offsets_size + padding1;
        let padding2 = align8_padding(payload_size);

        let (bitmap_offset, bitmap_size) = if bitmap_bytes.is_empty() {
            (0u32, 0u32)
        } else {
            (
                payload_offset + payload_size + padding2,
                bitmap_bytes.len() as u32,
            )
        };
        let padding3 = if bitmap_bytes.is_empty() {
            0
        } else {
            align8_padding(bitmap_size)
        };

        let footer = Utf8ColumnFooter {
            offsets: OffsetSize {
                offset: 0,
                size: offsets_size,
            },
            payload: OffsetSize {
                offset: payload_offset,
                size: payload_size,
            },
            bitmap: OffsetSize {
                offset: bitmap_offset,
                size: bitmap_size,
            },
        };
        let footer_bytes = encode_footer(&footer)?;

        let total_size =
            offsets_size + padding1 + payload_size + padding2 + bitmap_size + padding3
                + footer_bytes.len() as u32;
        let mut buf = Vec::with_capacity(total_size as usize);
        buf.extend_from_slice(offsets_bytes);
        buf.extend_from_slice(&vec![0u8; padding1 as usize]);
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&vec![0u8; padding2 as usize]);
        if !bitmap_bytes.is_empty() {
            buf.extend_from_slice(&bitmap_bytes);
            buf.extend_from_slice(&vec![0u8; padding3 as usize]);
        }
        buf.extend_from_slice(&footer_bytes);

        Ok(ColumnSegmentBytes::new(
            (*self.column).clone(),
            buf,
            num_values,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io2::bytes::FromBytes;
    use crate::io2::column::utf8::footer::Utf8ColumnFooter;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::url::MemUrl;
    use bytemuck::cast_slice;

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

    #[tokio::test]
    async fn write_non_nullable() {
        let dir = test_dir();
        let writer = Utf8ColumnWriter::new(dir, non_nullable_info());

        let result = writer
            .write(make_non_null_array(&["hello", "world", "!"]))
            .await
            .unwrap();
        assert_eq!(result.num_values, 3);

        let bytes = &result.bytes.bytes;
        let footer = Utf8ColumnFooter::from_bytes(bytes, 0, bytes.len() as u32);
        assert_eq!(footer.offsets.offset, 0);
        assert_eq!(footer.offsets.size, 16); // (3 + 1) * 4
        assert_eq!(footer.payload.size, 11); // "hello" + "world" + "!"
        assert_eq!(footer.bitmap.size, 0);

        let offsets: &[i32] = cast_slice(&bytes[0..16]);
        assert_eq!(offsets, &[0, 5, 10, 11]);

        let payload_start = footer.payload.offset as usize;
        let payload_end = payload_start + footer.payload.size as usize;
        let payload = std::str::from_utf8(&bytes[payload_start..payload_end]).unwrap();
        assert_eq!(payload, "helloworld!");
    }

    #[tokio::test]
    async fn write_nullable_with_nulls() {
        let dir = test_dir();
        let writer = Utf8ColumnWriter::new(dir, nullable_info());

        let result = writer
            .write(make_array(&[Some("a"), None, Some("bc"), None]))
            .await
            .unwrap();
        assert_eq!(result.num_values, 4);

        let bytes = &result.bytes.bytes;
        let footer = Utf8ColumnFooter::from_bytes(bytes, 0, bytes.len() as u32);
        assert_eq!(footer.offsets.size, 20); // (4 + 1) * 4
        assert_eq!(footer.payload.size, 3); // "a" + "bc"
        assert!(footer.bitmap.size > 0);

        let bitmap_start = footer.bitmap.offset as usize;
        let bitmap_end = bitmap_start + footer.bitmap.size as usize;
        let bitmap_words: &[u64] = cast_slice(&bytes[bitmap_start..bitmap_end]);
        // bit0=1, bit1=0, bit2=1, bit3=0 => 0b0101 = 5
        assert_eq!(bitmap_words[0], 0b0101);
    }

    #[tokio::test]
    async fn write_nullable_no_nulls() {
        let dir = test_dir();
        let writer = Utf8ColumnWriter::new(dir, nullable_info());

        let result = writer
            .write(make_array(&[Some("x"), Some("y")]))
            .await
            .unwrap();

        let bytes = &result.bytes.bytes;
        let footer = Utf8ColumnFooter::from_bytes(bytes, 0, bytes.len() as u32);
        assert_eq!(footer.bitmap.offset, 0);
        assert_eq!(footer.bitmap.size, 0);
    }

    #[tokio::test]
    async fn write_empty() {
        let dir = test_dir();
        let writer = Utf8ColumnWriter::new(dir, non_nullable_info());

        let result = writer.write(make_non_null_array(&[])).await.unwrap();
        assert_eq!(result.num_values, 0);
    }
}
