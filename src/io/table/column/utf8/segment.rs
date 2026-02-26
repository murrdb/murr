use arrow::array::{Array, StringArray};
use bincode::{Decode, Encode};
use bytemuck::cast_slice;

use crate::core::ColumnConfig;
use crate::core::MurrError;
use crate::io::segment::format::{Footer, align8_padding, decode_footer, encode_footer};
use crate::io::table::column::ColumnSegment;
use crate::io::table::column::bitmap::NullBitmap;

/// Footer at the end of a dense string column segment.
///
/// All byte offsets are relative to the start of the column data.
#[derive(Clone, Debug, Encode, Decode)]
pub(crate) struct Utf8Footer {
    pub(crate) num_values: u32,
    pub(crate) offsets_offset: u32,
    pub(crate) payload_offset: u32,
    pub(crate) payload_size: u32,
    pub(crate) null_bitmap_offset: u32,
    pub(crate) null_bitmap_size: u32,
}

impl Footer for Utf8Footer {}

/// Parsed zero-copy view over a single segment's dense string column data.
///
/// Wire format:
/// ```text
/// [value_offsets: [i32; num_values]]   // at offsets_offset
/// [padding to 8-byte align]
/// [payload: [u8; payload_size]]        // at payload_offset
/// [padding to 8-byte align]
/// [null_bitmap: [u64; bitmap_words]]   // at null_bitmap_offset
/// [padding to 8-byte align]
/// [bincode footer: Utf8Footer]
/// [footer_len: u32 LE]                // last 4 bytes
/// ```
pub(crate) struct Utf8Segment<'a> {
    pub(super) footer: Utf8Footer,
    pub(super) value_offsets: &'a [i32],
    pub(super) payload: &'a [u8],
    pub(super) nulls: Option<NullBitmap<'a>>,
}

impl<'a> Utf8Segment<'a> {
    /// Get the string byte range for value at `idx`.
    pub(super) fn string_range(&self, idx: u32) -> (usize, usize) {
        let start = self.value_offsets[idx as usize] as usize;
        let end = if idx + 1 < self.footer.num_values {
            self.value_offsets[(idx + 1) as usize] as usize
        } else {
            self.footer.payload_size as usize
        };
        (start, end)
    }
}

impl<'a> ColumnSegment<'a> for Utf8Segment<'a> {
    type ArrayType = StringArray;

    fn parse(_name: &str, config: &ColumnConfig, data: &'a [u8]) -> Result<Self, MurrError> {
        let footer: Utf8Footer = decode_footer(data, "utf8 segment")?;

        // Value offsets
        let offsets_byte_len = footer.num_values as usize * 4;
        let offsets_end = footer.offsets_offset as usize + offsets_byte_len;
        if offsets_end > data.len() {
            return Err(MurrError::TableError(
                "utf8 segment truncated at value_offsets".into(),
            ));
        }
        let value_offsets: &[i32] =
            cast_slice(&data[footer.offsets_offset as usize..offsets_end]);

        // Payload
        let payload_end = footer.payload_offset as usize + footer.payload_size as usize;
        if payload_end > data.len() {
            return Err(MurrError::TableError(
                "utf8 segment truncated at payload".into(),
            ));
        }
        let payload = &data[footer.payload_offset as usize..payload_end];

        // Null bitmap
        let nulls = NullBitmap::parse(
            data,
            footer.null_bitmap_offset as usize,
            footer.null_bitmap_size,
            config.nullable,
            "utf8",
        )?;

        Ok(Self {
            footer,
            value_offsets,
            payload,
            nulls,
        })
    }

    fn write(config: &ColumnConfig, values: &StringArray) -> Result<Vec<u8>, MurrError> {
        let num_values = values.len() as u32;

        // Compute payload: concatenated string bytes and their offsets.
        let mut string_payload = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(values.len());

        for i in 0..values.len() {
            offsets.push(string_payload.len() as i32);
            if !values.is_null(i) {
                string_payload.extend_from_slice(values.value(i).as_bytes());
            }
        }

        let payload_size = string_payload.len() as u32;

        let bitmap_bytes: Vec<u8> = if config.nullable {
            NullBitmap::write(values)
        } else {
            Vec::new()
        };

        // Layout: offsets, pad, string payload, pad, bitmap, pad, footer, footer_len
        let offsets_bytes: &[u8] = cast_slice(&offsets);
        let offsets_offset = 0u32;
        let offsets_padding = align8_padding(offsets_bytes.len());
        let payload_offset = (offsets_bytes.len() + offsets_padding) as u32;
        let payload_padding = align8_padding(string_payload.len());
        let null_bitmap_offset =
            (payload_offset as usize + string_payload.len() + payload_padding) as u32;
        let bitmap_padding = align8_padding(bitmap_bytes.len());

        let footer = Utf8Footer {
            num_values,
            offsets_offset,
            payload_offset,
            payload_size,
            null_bitmap_offset,
            null_bitmap_size: bitmap_bytes.len() as u32,
        };

        let mut buf = Vec::new();
        buf.extend_from_slice(offsets_bytes);
        buf.extend_from_slice(&[0u8; 7][..offsets_padding]);
        buf.extend_from_slice(&string_payload);
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

    fn make_string_array(values: &[Option<&str>]) -> StringArray {
        values.iter().collect::<StringArray>()
    }

    fn non_nullable_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: false,
        }
    }

    fn nullable_config() -> ColumnConfig {
        ColumnConfig {
            dtype: DType::Utf8,
            nullable: true,
        }
    }

    #[test]
    fn test_round_trip_non_nullable() {
        let config = non_nullable_config();
        let array = make_string_array(&[Some("hello"), Some("world"), Some("")]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 3);

        let (s0, e0) = seg.string_range(0);
        assert_eq!(std::str::from_utf8(&seg.payload[s0..e0]).unwrap(), "hello");
        let (s1, e1) = seg.string_range(1);
        assert_eq!(std::str::from_utf8(&seg.payload[s1..e1]).unwrap(), "world");
        let (s2, e2) = seg.string_range(2);
        assert_eq!(std::str::from_utf8(&seg.payload[s2..e2]).unwrap(), "");
    }

    #[test]
    fn test_round_trip_nullable_no_nulls() {
        let config = nullable_config();
        let array = make_string_array(&[Some("a"), Some("b")]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 2);
        assert!(seg.nulls.is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let config = nullable_config();
        let array = make_string_array(&[Some("hello"), None, Some("world"), None]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 4);
        let nulls = seg.nulls.as_ref().unwrap();
        assert!(nulls.is_valid(0));
        assert!(!nulls.is_valid(1));
        assert!(nulls.is_valid(2));
        assert!(!nulls.is_valid(3));
    }

    #[test]
    fn test_empty_segment() {
        let config = non_nullable_config();
        let array = make_string_array(&[]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 0);
    }

    #[test]
    fn test_many_values_bitmap_spans_multiple_words() {
        let config = nullable_config();
        let values: Vec<Option<&str>> = (0..64)
            .map(|i| if i % 3 == 0 { None } else { Some("v") })
            .collect();
        let array = make_string_array(&values);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.footer.num_values, 64);

        let nulls = seg.nulls.as_ref().unwrap();
        for i in 0..64u64 {
            if i % 3 == 0 {
                assert!(!nulls.is_valid(i), "expected null at index {i}");
            } else {
                assert!(nulls.is_valid(i), "expected valid at index {i}");
            }
        }
    }
}
