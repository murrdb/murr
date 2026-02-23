use arrow::array::{Array, StringArray};
use bytemuck::{Pod, Zeroable, cast_slice};

use crate::core::ColumnConfig;
use crate::core::MurrError;
use crate::io::table::column::ColumnSegment;
use crate::io::table::column::bitmap::{NullBitmap, align8_padding};

/// Fixed-size header at the start of a dense string column segment.
///
/// All byte offsets are relative to the start of the column data.
/// Value offsets (`[i32; num_values]`) immediately follow the header.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Utf8Header {
    pub(super) num_values: u32,
    pub(super) payload_offset: u32,
    pub(super) payload_size: u32,
    pub(super) null_bitmap_offset: u32,
    pub(super) null_bitmap_size: u32,
}

pub(super) const HEADER_SIZE: usize = std::mem::size_of::<Utf8Header>();

impl Utf8Header {
    /// Parse the header from the beginning of a column data slice.
    pub(super) fn parse(data: &[u8]) -> Result<&Utf8Header, MurrError> {
        if data.len() < HEADER_SIZE {
            return Err(MurrError::TableError(
                "dense string segment too small for header".into(),
            ));
        }
        Ok(bytemuck::from_bytes(&data[..HEADER_SIZE]))
    }
}

/// Parsed zero-copy view over a single segment's dense string column data.
///
/// Wire format:
/// ```text
/// [header: Utf8Header]                   // 20 bytes
/// [value_offsets: [i32; num_values]]     // immediately after header (4-byte aligned)
/// [payload: [u8; payload_size]]          // at payload_offset
/// [padding: 0..3 bytes]                  // align to 4 bytes
/// [null_bitmap: [u32; bitmap_size]]      // at null_bitmap_offset (4-byte aligned)
/// ```
pub(crate) struct Utf8Segment<'a> {
    pub(super) header: &'a Utf8Header,
    pub(super) value_offsets: &'a [i32],
    pub(super) payload: &'a [u8],
    pub(super) nulls: Option<NullBitmap<'a>>,
}

impl<'a> Utf8Segment<'a> {
    /// Get the string byte range for value at `idx`.
    pub(super) fn string_range(&self, idx: u32) -> (usize, usize) {
        let start = self.value_offsets[idx as usize] as usize;
        let end = if idx + 1 < self.header.num_values {
            self.value_offsets[(idx + 1) as usize] as usize
        } else {
            self.header.payload_size as usize
        };
        (start, end)
    }
}

impl<'a> ColumnSegment<'a> for Utf8Segment<'a> {
    type ArrayType = StringArray;

    fn parse(_name: &str, config: &ColumnConfig, data: &'a [u8]) -> Result<Self, MurrError> {
        let header = Utf8Header::parse(data)?;

        let offsets_start = HEADER_SIZE;
        let offsets_byte_len = header.num_values as usize * 4;
        let offsets_end = offsets_start + offsets_byte_len;
        if offsets_end > data.len() {
            return Err(MurrError::TableError(
                "dense string segment truncated at value_offsets".into(),
            ));
        }
        let value_offsets: &[i32] = cast_slice(&data[offsets_start..offsets_end]);

        let payload_end = header.payload_offset as usize + header.payload_size as usize;
        if payload_end > data.len() {
            return Err(MurrError::TableError(
                "dense string segment truncated at payload".into(),
            ));
        }
        let payload = &data[header.payload_offset as usize..payload_end];

        let nulls = NullBitmap::parse(
            data,
            header.null_bitmap_offset as usize,
            header.null_bitmap_size,
            config.nullable,
            "dense string",
        )?;

        Ok(Self {
            header,
            value_offsets,
            payload,
            nulls,
        })
    }

    fn write(config: &ColumnConfig, values: &StringArray) -> Result<Vec<u8>, MurrError> {
        let num_values = values.len() as u32;

        // Compute payload: concatenated string bytes and their offsets.
        let mut payload = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(values.len());

        for i in 0..values.len() {
            offsets.push(payload.len() as i32);
            if !values.is_null(i) {
                payload.extend_from_slice(values.value(i).as_bytes());
            }
        }

        let payload_size = payload.len() as u32;

        let bitmap_bytes: Vec<u8> = if config.nullable {
            NullBitmap::write(values)
        } else {
            Vec::new()
        };

        // Compute offsets for header.
        let offsets_byte_len = num_values as usize * 4;
        let payload_offset = (HEADER_SIZE + offsets_byte_len) as u32;
        let bitmap_unpadded = HEADER_SIZE + offsets_byte_len + payload_size as usize;
        let payload_padding = align8_padding(bitmap_unpadded);
        let null_bitmap_offset = (bitmap_unpadded + payload_padding) as u32;

        let total_size = HEADER_SIZE
            + offsets_byte_len
            + payload_size as usize
            + payload_padding
            + bitmap_bytes.len();

        let mut buf = Vec::with_capacity(total_size);

        let header = Utf8Header {
            num_values,
            payload_offset,
            payload_size,
            null_bitmap_offset,
            null_bitmap_size: bitmap_bytes.len() as u32,
        };
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(cast_slice(&offsets));
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&[0u8; 7][..payload_padding]);
        buf.extend_from_slice(&bitmap_bytes);

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
        assert_eq!(seg.header.num_values, 3);

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
        assert_eq!(seg.header.num_values, 2);
        assert!(seg.nulls.is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let config = nullable_config();
        let array = make_string_array(&[Some("hello"), None, Some("world"), None]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let seg = Utf8Segment::parse("test", &config, &bytes).unwrap();
        assert_eq!(seg.header.num_values, 4);
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
        assert_eq!(seg.header.num_values, 0);
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
        assert_eq!(seg.header.num_values, 64);

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
