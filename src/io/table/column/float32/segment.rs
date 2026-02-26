use arrow::array::{Array, Float32Array};
use bincode::{Decode, Encode};
use bytemuck::cast_slice;

use crate::core::ColumnConfig;
use crate::core::MurrError;
use crate::io::segment::format::{align8_padding, decode_footer, encode_footer};
use crate::io::table::column::ColumnSegment;
use crate::io::table::column::bitmap::NullBitmap;

/// Footer at the end of a dense float32 column segment.
///
/// All byte offsets are relative to the start of the column data.
#[derive(Clone, Debug, Encode, Decode)]
pub(crate) struct Float32Footer {
    pub(crate) num_values: u32,
    pub(crate) payload_offset: u32,
    pub(crate) null_bitmap_offset: u32,
    pub(crate) null_bitmap_size: u32,
}

/// Parsed zero-copy view over a single segment's dense float32 column data.
///
/// Wire format:
/// ```text
/// [payload: [f32; num_values]]        // at payload_offset
/// [padding to 8-byte align]
/// [null_bitmap: [u64; bitmap_words]]   // at null_bitmap_offset
/// [padding to 8-byte align]
/// [bincode footer: Float32Footer]
/// [footer_len: u32 LE]                // last 4 bytes
/// ```
pub(crate) struct Float32Segment<'a> {
    pub(super) footer: Float32Footer,
    pub(super) payload: &'a [f32],
    pub(super) nulls: Option<NullBitmap<'a>>,
}

impl<'a> ColumnSegment<'a> for Float32Segment<'a> {
    type ArrayType = Float32Array;

    fn parse(_name: &str, config: &ColumnConfig, data: &'a [u8]) -> Result<Self, MurrError> {
        let footer: Float32Footer = decode_footer(data, "float32 segment")?;

        let payload_byte_len = footer.num_values as usize * 4;
        let payload_end = footer.payload_offset as usize + payload_byte_len;
        if payload_end > data.len() {
            return Err(MurrError::TableError(
                "float32 segment truncated at payload".into(),
            ));
        }
        let payload: &[f32] = cast_slice(&data[footer.payload_offset as usize..payload_end]);

        let nulls = NullBitmap::parse(
            data,
            footer.null_bitmap_offset as usize,
            footer.null_bitmap_size,
            config.nullable,
            "float32",
        )?;

        Ok(Self {
            footer,
            payload,
            nulls,
        })
    }

    fn write(config: &ColumnConfig, values: &Float32Array) -> Result<Vec<u8>, MurrError> {
        let num_values = values.len() as u32;

        // Build payload
        let payload: Vec<f32> = (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    0.0f32
                } else {
                    values.value(i)
                }
            })
            .collect();
        let payload_bytes: &[u8] = cast_slice(&payload);

        // Build bitmap
        let bitmap_bytes: Vec<u8> = if config.nullable {
            NullBitmap::write(values)
        } else {
            Vec::new()
        };

        // Layout: payload, pad, bitmap, pad, footer, footer_len
        let payload_offset = 0u32;
        let payload_padding = align8_padding(payload_bytes.len());
        let null_bitmap_offset = (payload_bytes.len() + payload_padding) as u32;
        let bitmap_padding = align8_padding(bitmap_bytes.len());

        let footer = Float32Footer {
            num_values,
            payload_offset,
            null_bitmap_offset,
            null_bitmap_size: bitmap_bytes.len() as u32,
        };

        let mut buf = Vec::new();
        buf.extend_from_slice(payload_bytes);
        buf.extend_from_slice(&[0u8; 7][..payload_padding]);
        buf.extend_from_slice(&bitmap_bytes);
        buf.extend_from_slice(&[0u8; 7][..bitmap_padding]);
        encode_footer(&mut buf, &footer)?;

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;

    fn make_float32_array(values: &[Option<f32>]) -> Float32Array {
        values.iter().copied().collect::<Float32Array>()
    }

    fn non_nullable_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Float32,
            nullable: false,
        }
    }

    fn nullable_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Float32,
            nullable: true,
        }
    }

    #[test]
    fn test_round_trip_non_nullable() {
        let config = non_nullable_config();
        let array = make_float32_array(&[Some(1.0), Some(2.5), Some(0.0)]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let seg = Float32Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 3);
        assert_eq!(seg.payload[0], 1.0);
        assert_eq!(seg.payload[1], 2.5);
        assert_eq!(seg.payload[2], 0.0);
    }

    #[test]
    fn test_round_trip_nullable_no_nulls() {
        let config = nullable_config();
        let array = make_float32_array(&[Some(1.0), Some(2.0)]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let seg = Float32Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 2);
        assert_eq!(seg.payload[0], 1.0);
        assert_eq!(seg.payload[1], 2.0);
        assert!(seg.nulls.is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let config = nullable_config();
        let array = make_float32_array(&[Some(1.5), None, Some(3.25), None]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let seg = Float32Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 4);
        assert_eq!(seg.payload[0], 1.5);
        let nulls = seg.nulls.as_ref().unwrap();
        assert!(nulls.is_valid(0));
        assert!(!nulls.is_valid(1));
        assert_eq!(seg.payload[2], 3.25);
        assert!(nulls.is_valid(2));
        assert!(!nulls.is_valid(3));
    }

    #[test]
    fn test_empty_segment() {
        let config = non_nullable_config();
        let array = make_float32_array(&[]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let seg = Float32Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 0);
    }

    #[test]
    fn test_many_values_bitmap_spans_multiple_words() {
        let config = nullable_config();
        let values: Vec<Option<f32>> = (0..64)
            .map(|i| if i % 3 == 0 { None } else { Some(i as f32) })
            .collect();
        let array = make_float32_array(&values);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let seg = Float32Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 64);

        let nulls = seg.nulls.as_ref().unwrap();
        for i in 0..64u64 {
            if i % 3 == 0 {
                assert!(!nulls.is_valid(i), "expected null at index {i}");
            } else {
                assert!(nulls.is_valid(i), "expected valid at index {i}");
                assert_eq!(seg.payload[i as usize], i as f32);
            }
        }
    }
}
