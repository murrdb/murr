use arrow::array::{Array, PrimitiveArray};
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::io::bitmap::NullBitmap;
use crate::io::column::scalar::ScalarCodec;
use crate::io::column::scalar::footer::ScalarColumnFooter;
use crate::io::column::{ColumnFooter, ColumnSegmentBytes, OffsetSize, PayloadBytes};
use crate::io::info::ColumnInfo;

pub fn write_scalar<S: ScalarCodec>(
    column: &ColumnInfo,
    array: &PrimitiveArray<S::ArrowType>,
) -> Result<ColumnSegmentBytes, MurrError> {
    let num_values = array.len() as u32;

    // Build payload: ZERO for nulls
    let payload: Vec<S::Native> = (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                S::ZERO
            } else {
                array.value(i)
            }
        })
        .collect();
    let payload_bytes: &[u8] = cast_slice(&payload);
    let payload_buf = PayloadBytes::new(payload_bytes.to_vec());

    // Build null bitmap
    let bitmap_bytes = if column.nullable {
        NullBitmap::write(array)
    } else {
        Vec::new()
    };
    let bitmap_buf = PayloadBytes::new(bitmap_bytes);

    // Compute footer offsets from padded buffer sizes
    let payload_size = num_values * S::ELEMENT_SIZE;
    let bitmap_offset = payload_buf.padded_len();
    let bitmap_size = bitmap_buf.bytes.len() as u32;

    let footer = ScalarColumnFooter {
        payload: OffsetSize {
            offset: 0,
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
        column.clone(),
        vec![payload_buf, bitmap_buf],
        footer_bytes,
        num_values,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::io::column::ColumnFooter;
    use crate::io::column::float32::Float32Codec;
    use crate::io::column::scalar::footer::ScalarColumnFooter;
    use arrow::array::Float32Array;
    use bytemuck::cast_slice;

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

    #[test]
    fn write_non_nullable() {
        let array = make_non_null_array(&[1.0, 2.5, 3.0]);

        let result = write_scalar::<Float32Codec>(&non_nullable_info(), &array).unwrap();
        assert_eq!(result.num_values, 3);

        let bytes = result.to_bytes();
        let footer = ScalarColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.payload.offset, 0);
        assert_eq!(footer.payload.size, 12);
        assert_eq!(footer.bitmap.size, 0);

        let payload: &[f32] = cast_slice(&bytes[0..12]);
        assert_eq!(payload, &[1.0, 2.5, 3.0]);
    }

    #[test]
    fn write_nullable_with_nulls() {
        let array = make_array(&[Some(1.0), None, Some(3.0), None]);

        let result = write_scalar::<Float32Codec>(&nullable_info(), &array).unwrap();
        assert_eq!(result.num_values, 4);

        let bytes = result.to_bytes();
        let footer = ScalarColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.payload.size, 16);
        assert!(footer.bitmap.size > 0);

        let bitmap_start = footer.bitmap.offset as usize;
        let bitmap_end = bitmap_start + footer.bitmap.size as usize;
        let bitmap_words: &[u64] = cast_slice(&bytes[bitmap_start..bitmap_end]);
        assert_eq!(bitmap_words[0], 0b0101);
    }

    #[test]
    fn write_nullable_no_nulls() {
        let array = make_array(&[Some(1.0), Some(2.0)]);

        let result = write_scalar::<Float32Codec>(&nullable_info(), &array).unwrap();

        let bytes = result.to_bytes();
        let footer = ScalarColumnFooter::parse(&bytes, 0).unwrap();
        assert_eq!(footer.bitmap.offset, 0);
        assert_eq!(footer.bitmap.size, 0);
    }

    #[test]
    fn write_empty() {
        let array = make_non_null_array(&[]);

        let result = write_scalar::<Float32Codec>(&non_nullable_info(), &array).unwrap();
        assert_eq!(result.num_values, 0);
    }
}
