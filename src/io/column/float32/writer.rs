use std::sync::Arc;

use arrow::array::{Array, Float32Array};
use async_trait::async_trait;
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io::bitmap::NullBitmap;
use crate::io::column::float32::footer::Float32ColumnFooter;
use crate::io::column::{
    ColumnFooter, ColumnSegmentBytes, ColumnWriter, OffsetSize, align8_padding,
};
use crate::io::info::ColumnInfo;

pub struct Float32ColumnWriter {
    column: Arc<ColumnInfo>,
}

impl Float32ColumnWriter {
    pub fn new(column: Arc<ColumnInfo>) -> Self {
        Float32ColumnWriter { column }
    }
}

#[async_trait]
impl ColumnWriter for Float32ColumnWriter {
    async fn write(&self, values: Arc<dyn Array>) -> Result<ColumnSegmentBytes, MurrError> {
        let array = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| MurrError::TableError("expected Float32Array".into()))?;

        let num_values = array.len() as u32;

        // Build payload: 0.0 for nulls
        let payload: Vec<f32> = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    0.0f32
                } else {
                    array.value(i)
                }
            })
            .collect();
        let payload_bytes: &[u8] = cast_slice(&payload);

        // Build null bitmap
        let bitmap_bytes = if self.column.nullable {
            NullBitmap::write(values.as_ref())
        } else {
            Vec::new()
        };

        // Layout
        let payload_size = num_values * 4;
        let padding1 = align8_padding(payload_size);

        let (bitmap_offset, bitmap_size) = if bitmap_bytes.is_empty() {
            (0u32, 0u32)
        } else {
            (payload_size + padding1, bitmap_bytes.len() as u32)
        };
        let padding2 = if bitmap_bytes.is_empty() {
            0
        } else {
            align8_padding(bitmap_size)
        };

        let footer = Float32ColumnFooter {
            payload: OffsetSize {
                offset: 0,
                size: payload_size,
            },
            bitmap: OffsetSize {
                offset: bitmap_offset,
                size: bitmap_size,
            },
        };
        let footer_bytes = footer.encode();

        let total_size =
            payload_size + padding1 + bitmap_size + padding2 + footer_bytes.len() as u32;
        let mut buf = Vec::with_capacity(total_size as usize);
        buf.extend_from_slice(payload_bytes);
        buf.resize(buf.len() + padding1 as usize, 0);
        if !bitmap_bytes.is_empty() {
            buf.extend_from_slice(&bitmap_bytes);
            buf.resize(buf.len() + padding2 as usize, 0);
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
    use crate::io::column::ColumnFooter;
    use crate::io::column::float32::footer::Float32ColumnFooter;
    use bytemuck::cast_slice;

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

    #[tokio::test]
    async fn write_non_nullable() {
        let writer = Float32ColumnWriter::new(non_nullable_info());

        let result = writer
            .write(make_non_null_array(&[1.0, 2.5, 3.0]))
            .await
            .unwrap();
        assert_eq!(result.num_values, 3);

        let bytes = &result.bytes;
        let footer = Float32ColumnFooter::parse(bytes, 0).unwrap();
        assert_eq!(footer.payload.offset, 0);
        assert_eq!(footer.payload.size, 12);
        assert_eq!(footer.bitmap.size, 0);

        let payload: &[f32] = cast_slice(&bytes[0..12]);
        assert_eq!(payload, &[1.0, 2.5, 3.0]);
    }

    #[tokio::test]
    async fn write_nullable_with_nulls() {
        let writer = Float32ColumnWriter::new(nullable_info());

        let result = writer
            .write(make_array(&[Some(1.0), None, Some(3.0), None]))
            .await
            .unwrap();
        assert_eq!(result.num_values, 4);

        let bytes = &result.bytes;
        let footer = Float32ColumnFooter::parse(bytes, 0).unwrap();
        assert_eq!(footer.payload.size, 16);
        assert!(footer.bitmap.size > 0);

        let bitmap_start = footer.bitmap.offset as usize;
        let bitmap_end = bitmap_start + footer.bitmap.size as usize;
        let bitmap_words: &[u64] = cast_slice(&bytes[bitmap_start..bitmap_end]);
        // bit0=1, bit1=0, bit2=1, bit3=0 => 0b0101 = 5
        assert_eq!(bitmap_words[0], 0b0101);
    }

    #[tokio::test]
    async fn write_nullable_no_nulls() {
        let writer = Float32ColumnWriter::new(nullable_info());

        let result = writer
            .write(make_array(&[Some(1.0), Some(2.0)]))
            .await
            .unwrap();

        let bytes = &result.bytes;
        let footer = Float32ColumnFooter::parse(bytes, 0).unwrap();
        assert_eq!(footer.bitmap.offset, 0);
        assert_eq!(footer.bitmap.size, 0);
    }

    #[tokio::test]
    async fn write_empty() {
        let writer = Float32ColumnWriter::new(non_nullable_info());

        let result = writer.write(make_non_null_array(&[])).await.unwrap();
        assert_eq!(result.num_values, 0);
    }
}
