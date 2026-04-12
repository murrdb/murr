use std::sync::Arc;

use arrow::array::{Array, StringArray};
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io::bitmap::NullBitmap;
use crate::io::column::utf8::footer::Utf8ColumnFooter;
use crate::io::column::{ColumnFooter, ColumnSegmentBytes, ColumnWriter, OffsetSize, PayloadBytes};
use crate::io::info::ColumnInfo;

pub struct Utf8ColumnWriter {
    column: Arc<ColumnInfo>,
}

impl Utf8ColumnWriter {
    pub fn new(column: Arc<ColumnInfo>) -> Self {
        Utf8ColumnWriter { column }
    }
}

impl ColumnWriter<StringArray> for Utf8ColumnWriter {
    fn write(&self, array: &StringArray) -> Result<ColumnSegmentBytes, MurrError> {
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
        let offsets_buf = PayloadBytes::new(offsets_bytes.to_vec());

        let payload_buf = PayloadBytes::new(payload);

        // Build null bitmap
        let bitmap_bytes = if self.column.nullable {
            NullBitmap::write(array)
        } else {
            Vec::new()
        };
        let bitmap_buf = PayloadBytes::new(bitmap_bytes);

        // Compute footer offsets from padded buffer sizes
        let offsets_size = offsets_buf.bytes.len() as u32;
        let payload_offset = offsets_buf.padded_len();
        let payload_size = payload_buf.bytes.len() as u32;
        let bitmap_offset = payload_offset + payload_buf.padded_len();
        let bitmap_size = bitmap_buf.bytes.len() as u32;

        let footer = Utf8ColumnFooter {
            offsets: OffsetSize {
                offset: 0,
                size: offsets_size,
            },
            payload: OffsetSize {
                offset: payload_offset,
                size: payload_size,
            },
            bitmap: if bitmap_size > 0 {
                OffsetSize {
                    offset: bitmap_offset,
                    size: bitmap_size,
                }
            } else {
                OffsetSize { offset: 0, size: 0 }
            },
        };
        let footer_bytes = footer.encode();

        Ok(ColumnSegmentBytes::new(
            (*self.column).clone(),
            vec![offsets_buf, payload_buf, bitmap_buf],
            footer_bytes,
            num_values,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io::column::ColumnFooter;
    use crate::io::column::utf8::footer::Utf8ColumnFooter;
    use bytemuck::cast_slice;

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

    fn make_array(values: &[Option<&str>]) -> StringArray {
        values.iter().copied().collect::<StringArray>()
    }

    fn make_non_null_array(values: &[&str]) -> StringArray {
        StringArray::from(values.to_vec())
    }

    #[test]
    fn write_non_nullable() {
        let writer = Utf8ColumnWriter::new(non_nullable_info());
        let array = make_non_null_array(&["hello", "world", "!"]);

        let result = writer.write(&array).unwrap();
        assert_eq!(result.num_values, 3);

        let bytes = result.to_bytes();
        let footer = Utf8ColumnFooter::parse(&bytes, 0).unwrap();
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

    #[test]
    fn write_nullable_with_nulls() {
        let writer = Utf8ColumnWriter::new(nullable_info());
        let array = make_array(&[Some("a"), None, Some("bc"), None]);

        let result = writer.write(&array).unwrap();
        assert_eq!(result.num_values, 4);

        let bytes = result.to_bytes();
        let footer = Utf8ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.offsets.size, 20); // (4 + 1) * 4
        assert_eq!(footer.payload.size, 3); // "a" + "bc"
        assert!(footer.bitmap.size > 0);

        let bitmap_start = footer.bitmap.offset as usize;
        let bitmap_end = bitmap_start + footer.bitmap.size as usize;
        let bitmap_words: &[u64] = cast_slice(&bytes[bitmap_start..bitmap_end]);
        // bit0=1, bit1=0, bit2=1, bit3=0 => 0b0101 = 5
        assert_eq!(bitmap_words[0], 0b0101);
    }

    #[test]
    fn write_nullable_no_nulls() {
        let writer = Utf8ColumnWriter::new(nullable_info());
        let array = make_array(&[Some("x"), Some("y")]);

        let result = writer.write(&array).unwrap();

        let bytes = result.to_bytes();
        let footer = Utf8ColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.bitmap.offset, 0);
        assert_eq!(footer.bitmap.size, 0);
    }

    #[test]
    fn write_empty() {
        let writer = Utf8ColumnWriter::new(non_nullable_info());
        let array = make_non_null_array(&[]);

        let result = writer.write(&array).unwrap();
        assert_eq!(result.num_values, 0);
    }
}
