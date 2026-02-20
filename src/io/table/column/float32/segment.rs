use arrow::array::{Array, Float32Array};
use bytemuck::{Pod, Zeroable, cast_slice};

use crate::conf::ColumnConfig;
use crate::core::MurrError;
use crate::io::table::column::ColumnSegment;
use crate::io::table::column::bitmap::{NullBitmap, align8_padding};

/// Fixed-size header at the start of a dense float32 column segment.
///
/// All byte offsets are relative to the start of the column data.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Float32Header {
    pub(super) num_values: u32,
    pub(super) payload_offset: u32,
    pub(super) null_bitmap_offset: u32,
    pub(super) null_bitmap_size: u32,
}

pub(super) const HEADER_SIZE: usize = std::mem::size_of::<Float32Header>();

impl Float32Header {
    /// Parse the header from the beginning of a column data slice.
    pub(super) fn parse(data: &[u8]) -> Result<&Float32Header, MurrError> {
        if data.len() < HEADER_SIZE {
            return Err(MurrError::TableError(
                "dense float32 segment too small for header".into(),
            ));
        }
        Ok(bytemuck::from_bytes(&data[..HEADER_SIZE]))
    }
}

/// Parsed zero-copy view over a single segment's dense float32 column data.
///
/// Wire format:
/// ```text
/// [header: Float32Header]             // 16 bytes
/// [payload: [f32; num_values]]        // at payload_offset
/// [null_bitmap: [u32; bitmap_size]]   // at null_bitmap_offset
/// ```
pub(crate) struct Float32Segment<'a> {
    pub(super) header: &'a Float32Header,
    pub(super) payload: &'a [f32],
    pub(super) nulls: Option<NullBitmap<'a>>,
}

impl<'a> ColumnSegment<'a> for Float32Segment<'a> {
    type ArrayType = Float32Array;

    fn parse(_name: &str, config: &ColumnConfig, data: &'a [u8]) -> Result<Self, MurrError> {
        let header = Float32Header::parse(data)?;

        let payload_byte_len = header.num_values as usize * 4;
        let payload_end = header.payload_offset as usize + payload_byte_len;
        if payload_end > data.len() {
            return Err(MurrError::TableError(
                "dense float32 segment truncated at payload".into(),
            ));
        }
        let payload: &[f32] = cast_slice(&data[header.payload_offset as usize..payload_end]);

        let nulls = NullBitmap::parse(
            data,
            header.null_bitmap_offset as usize,
            header.null_bitmap_size,
            config.nullable,
            "dense float32",
        )?;

        Ok(Self {
            header,
            payload,
            nulls,
        })
    }

    fn write(config: &ColumnConfig, values: &Float32Array) -> Result<Vec<u8>, MurrError> {
        let num_values = values.len() as u32;

        let bitmap_bytes: Vec<u8> = if config.nullable {
            NullBitmap::write(values)
        } else {
            Vec::new()
        };

        let payload_offset = HEADER_SIZE as u32;
        let payload_byte_len = num_values as usize * 4;
        let payload_padding = align8_padding(HEADER_SIZE + payload_byte_len);
        let null_bitmap_offset = (HEADER_SIZE + payload_byte_len + payload_padding) as u32;

        let total_size = HEADER_SIZE + payload_byte_len + payload_padding + bitmap_bytes.len();
        let mut buf = Vec::with_capacity(total_size);

        let header = Float32Header {
            num_values,
            payload_offset,
            null_bitmap_offset,
            null_bitmap_size: bitmap_bytes.len() as u32,
        };
        buf.extend_from_slice(bytemuck::bytes_of(&header));

        let payload: Vec<f32> = (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    0.0f32
                } else {
                    values.value(i)
                }
            })
            .collect();
        buf.extend_from_slice(cast_slice(&payload));
        buf.extend_from_slice(&[0u8; 7][..payload_padding]);
        buf.extend_from_slice(&bitmap_bytes);

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::DType;

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
        assert_eq!(seg.header.num_values, 3);
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
        assert_eq!(seg.header.num_values, 2);
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
        assert_eq!(seg.header.num_values, 4);
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
        assert_eq!(seg.header.num_values, 0);
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
        assert_eq!(seg.header.num_values, 64);

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
