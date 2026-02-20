mod header;

use std::sync::Arc;

use arrow::array::{Array, StringBuilder, StringArray};
use arrow::datatypes::{DataType, Field};
use bytemuck::cast_slice;

use crate::core::MurrError;
use crate::table::column::bitmap::{
    align4_padding, build_bitmap_words, parse_null_bitmap, NullBitmap,
};
use crate::table::column::{Column, KeyOffset};

use header::{Utf8Header, HEADER_SIZE};

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
struct Utf8Segment<'a> {
    header: &'a Utf8Header,
    value_offsets: &'a [i32],
    payload: &'a [u8],
    nulls: NullBitmap<'a>,
}

impl<'a> Utf8Segment<'a> {
    fn parse(data: &'a [u8], nullable: bool) -> Result<Self, MurrError> {
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

        let nulls = parse_null_bitmap(
            data,
            header.null_bitmap_offset as usize,
            header.null_bitmap_size,
            nullable,
            "dense string",
        )?;

        Ok(Self {
            header,
            value_offsets,
            payload,
            nulls,
        })
    }

    /// Get the i32 offset at the given value index.
    fn offset_at(&self, idx: u32) -> i32 {
        self.value_offsets[idx as usize]
    }

    /// Get the string byte range for value at `idx`.
    fn string_range(&self, idx: u32) -> (usize, usize) {
        let start = self.offset_at(idx) as usize;
        let end = if idx + 1 < self.header.num_values {
            self.offset_at(idx + 1) as usize
        } else {
            self.header.payload_size as usize
        };
        (start, end)
    }
}

pub struct Utf8Column<'a> {
    segments: Vec<Utf8Segment<'a>>,
    nullable: bool,
    field: Field,
}

impl<'a> Utf8Column<'a> {
    pub fn new(name: &str, segments: &[&'a [u8]], nullable: bool) -> Result<Self, MurrError> {
        let parsed: Result<Vec<_>, _> = segments
            .iter()
            .map(|data| Utf8Segment::parse(data, nullable))
            .collect();
        Ok(Self {
            segments: parsed?,
            nullable,
            field: Field::new(name, DataType::Utf8, nullable),
        })
    }

    /// Serialize an Arrow StringArray into the dense string wire format.
    pub fn write(values: &StringArray, nullable: bool) -> Result<Vec<u8>, MurrError> {
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
        let payload_padding = align4_padding(payload.len());

        let bitmap_words: Vec<u32> = if nullable {
            build_bitmap_words(values.len(), |i| values.is_null(i))
        } else {
            Vec::new()
        };

        // Compute offsets for header.
        let offsets_byte_len = num_values as usize * 4;
        let payload_offset = (HEADER_SIZE + offsets_byte_len) as u32;
        let null_bitmap_offset = payload_offset + payload_size + payload_padding as u32;

        let total_size = HEADER_SIZE
            + offsets_byte_len
            + payload_size as usize
            + payload_padding
            + bitmap_words.len() * 4;

        let mut buf = Vec::with_capacity(total_size);

        let header = Utf8Header {
            num_values,
            payload_offset,
            payload_size,
            null_bitmap_offset,
            null_bitmap_size: bitmap_words.len() as u32,
        };
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(cast_slice(&offsets));
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&[0u8; 3][..payload_padding]);
        buf.extend_from_slice(cast_slice(&bitmap_words));

        Ok(buf)
    }
}

impl<'a> Column for Utf8Column<'a> {
    fn field(&self) -> &Field {
        &self.field
    }

    fn get_indexes(&self, indexes: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let mut builder = StringBuilder::with_capacity(indexes.len(), 0);

        for idx in indexes {
            match idx {
                KeyOffset::MissingKey => {
                    builder.append_null();
                }
                KeyOffset::SegmentOffset {
                    segment_id,
                    segment_offset,
                } => {
                    let seg = self
                        .segments
                        .get(*segment_id as usize)
                        .ok_or_else(|| {
                            MurrError::TableError(format!(
                                "segment_id {} out of range (have {})",
                                segment_id,
                                self.segments.len()
                            ))
                        })?;

                    if *segment_offset >= seg.header.num_values {
                        return Err(MurrError::TableError(format!(
                            "segment_offset {} out of range (segment has {} values)",
                            segment_offset, seg.header.num_values
                        )));
                    }

                    if !seg.nulls.is_valid(*segment_offset) {
                        builder.append_null();
                    } else {
                        let (start, end) = seg.string_range(*segment_offset);
                        let s =
                            std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                                MurrError::TableError(format!(
                                    "invalid utf8 in string column: {e}"
                                ))
                            })?;
                        builder.append_value(s);
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn get_all(&self) -> Result<Arc<dyn Array>, MurrError> {
        let total = self.size() as usize;
        let mut builder = StringBuilder::with_capacity(total, 0);

        for seg in &self.segments {
            for i in 0..seg.header.num_values {
                if !seg.nulls.is_valid(i) {
                    builder.append_null();
                } else {
                    let (start, end) = seg.string_range(i);
                    let s = std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                        MurrError::TableError(format!("invalid utf8 in string column: {e}"))
                    })?;
                    builder.append_value(s);
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn size(&self) -> u32 {
        self.segments.iter().map(|s| s.header.num_values).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_array(values: &[Option<&str>]) -> StringArray {
        values.iter().collect::<StringArray>()
    }

    #[test]
    fn test_round_trip_non_nullable() {
        let array = make_string_array(&[Some("hello"), Some("world"), Some("")]);
        let bytes = Utf8Column::write(&array, false).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], false).unwrap();
        assert_eq!(col.size(), 3);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "hello");
        assert_eq!(result.value(1), "world");
        assert_eq!(result.value(2), "");
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
        assert!(!result.is_null(2));
        assert!(result.nulls().is_none());
    }

    #[test]
    fn test_round_trip_nullable_no_nulls() {
        let array = make_string_array(&[Some("a"), Some("b")]);
        let bytes = Utf8Column::write(&array, true).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], true).unwrap();
        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "a");
        assert_eq!(result.value(1), "b");
        assert!(result.nulls().is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let array = make_string_array(&[Some("hello"), None, Some("world"), None]);
        let bytes = Utf8Column::write(&array, true).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], true).unwrap();
        assert_eq!(col.size(), 4);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), "hello");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "world");
        assert!(result.is_null(3));
    }

    #[test]
    fn test_get_indexes() {
        let array = make_string_array(&[Some("a"), Some("b"), Some("c"), Some("d")]);
        let bytes = Utf8Column::write(&array, false).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], false).unwrap();

        let indexes = vec![
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 2,
            },
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0,
            },
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 3,
            },
        ];

        let result = col.get_indexes(&indexes).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "c");
        assert_eq!(result.value(1), "a");
        assert_eq!(result.value(2), "d");
    }

    #[test]
    fn test_get_indexes_with_nulls() {
        let array = make_string_array(&[Some("x"), None, Some("z")]);
        let bytes = Utf8Column::write(&array, true).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], true).unwrap();

        let indexes = vec![
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 1,
            },
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0,
            },
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 2,
            },
        ];

        let result = col.get_indexes(&indexes).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert_eq!(result.value(1), "x");
        assert_eq!(result.value(2), "z");
    }

    #[test]
    fn test_multiple_segments() {
        let array1 = make_string_array(&[Some("seg0_a"), Some("seg0_b")]);
        let array2 = make_string_array(&[Some("seg1_a"), Some("seg1_b"), Some("seg1_c")]);

        let bytes1 = Utf8Column::write(&array1, false).unwrap();
        let bytes2 = Utf8Column::write(&array2, false).unwrap();

        let col = Utf8Column::new("test", &[&bytes1[..], &bytes2[..]], false).unwrap();
        assert_eq!(col.size(), 5);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), "seg0_a");
        assert_eq!(result.value(1), "seg0_b");
        assert_eq!(result.value(2), "seg1_a");
        assert_eq!(result.value(3), "seg1_b");
        assert_eq!(result.value(4), "seg1_c");

        let indexes = vec![
            KeyOffset::SegmentOffset {
                segment_id: 1,
                segment_offset: 2,
            },
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0,
            },
        ];
        let result = col.get_indexes(&indexes).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "seg1_c");
        assert_eq!(result.value(1), "seg0_a");
    }

    #[test]
    fn test_empty_segment() {
        let array = make_string_array(&[]);
        let bytes = Utf8Column::write(&array, false).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], false).unwrap();
        assert_eq!(col.size(), 0);

        let result = col.get_all().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_indexes_with_missing_keys() {
        let array = make_string_array(&[Some("hello"), Some("world"), Some("foo")]);
        let bytes = Utf8Column::write(&array, false).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], false).unwrap();

        let indexes = vec![
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 0,
            },
            KeyOffset::MissingKey,
            KeyOffset::SegmentOffset {
                segment_id: 0,
                segment_offset: 2,
            },
            KeyOffset::MissingKey,
        ];

        let result = col.get_indexes(&indexes).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), "hello");
        assert!(!result.is_null(0));
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "foo");
        assert!(!result.is_null(2));
        assert!(result.is_null(3));
    }

    #[test]
    fn test_many_values_bitmap_spans_multiple_words() {
        let values: Vec<Option<&str>> = (0..64)
            .map(|i| if i % 3 == 0 { None } else { Some("v") })
            .collect();
        let array = make_string_array(&values);
        let bytes = Utf8Column::write(&array, true).unwrap();

        let col = Utf8Column::new("test", &[&bytes[..]], true).unwrap();
        assert_eq!(col.size(), 64);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..64 {
            if i % 3 == 0 {
                assert!(result.is_null(i), "expected null at index {i}");
            } else {
                assert_eq!(result.value(i), "v", "expected 'v' at index {i}");
            }
        }
    }
}
