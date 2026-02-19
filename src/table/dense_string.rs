use std::sync::Arc;

use arrow::array::{Array, StringBuilder, StringArray};

use crate::core::MurrError;

use super::bitmap::{
    build_bitmap_words, parse_null_bitmap, read_i32_le, read_u32_le, write_bitmap, NullBitmap,
};
use super::column::{Column, KeyOffset};

/// Parsed zero-copy view over a single segment's dense string column data.
///
/// Wire format:
/// ```text
/// [num_values: u32]
/// [value_offsets: [i32; num_values]]
/// [payload_size: u32]
/// [payload: [u8; payload_size]]
/// [null_bitmap_size: u32]            // count of u32 words (0 if non-nullable)
/// [null_bitmap: [u32; null_bitmap_size]]
/// ```
struct DenseStringSegment<'a> {
    size: u32,
    value_offsets: &'a [u8], // raw bytes, read as i32 LE per element
    payload: &'a [u8],
    payload_size: u32,
    nulls: NullBitmap<'a>,
}

impl<'a> DenseStringSegment<'a> {
    fn parse(data: &'a [u8], nullable: bool) -> Result<Self, MurrError> {
        let mut pos: usize = 0;

        if data.len() < 4 {
            return Err(MurrError::TableError(
                "dense string segment too small for num_values".into(),
            ));
        }

        let num_values = read_u32_le(data, pos);
        pos += 4;

        let offsets_byte_len = num_values as usize * 4;
        if pos + offsets_byte_len > data.len() {
            return Err(MurrError::TableError(
                "dense string segment truncated at value_offsets".into(),
            ));
        }
        let value_offsets = &data[pos..pos + offsets_byte_len];
        pos += offsets_byte_len;

        if pos + 4 > data.len() {
            return Err(MurrError::TableError(
                "dense string segment truncated at payload_size".into(),
            ));
        }
        let payload_size = read_u32_le(data, pos);
        pos += 4;

        if pos + payload_size as usize > data.len() {
            return Err(MurrError::TableError(
                "dense string segment truncated at payload".into(),
            ));
        }
        let payload = &data[pos..pos + payload_size as usize];
        pos += payload_size as usize;

        let (nulls, _) = parse_null_bitmap(data, pos, nullable, "dense string")?;

        Ok(Self {
            size: num_values,
            value_offsets,
            payload,
            payload_size,
            nulls,
        })
    }

    /// Get the i32 offset at the given value index.
    fn offset_at(&self, idx: u32) -> i32 {
        let byte_pos = idx as usize * 4;
        read_i32_le(self.value_offsets, byte_pos)
    }

    /// Get the string byte range for value at `idx`.
    fn string_range(&self, idx: u32) -> (usize, usize) {
        let start = self.offset_at(idx) as usize;
        let end = if idx + 1 < self.size {
            self.offset_at(idx + 1) as usize
        } else {
            self.payload_size as usize
        };
        (start, end)
    }
}

pub struct DenseStringColumn<'a> {
    segments: Vec<DenseStringSegment<'a>>,
    nullable: bool,
}

impl<'a> DenseStringColumn<'a> {
    pub fn new(segments: &[&'a [u8]], nullable: bool) -> Result<Self, MurrError> {
        let parsed: Result<Vec<_>, _> = segments
            .iter()
            .map(|data| DenseStringSegment::parse(data, nullable))
            .collect();
        Ok(Self {
            segments: parsed?,
            nullable,
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
            // Null values get same offset as next value (zero-length string in payload).
        }

        let payload_size = payload.len() as u32;

        let bitmap_words: Vec<u32> = if nullable {
            build_bitmap_words(values.len(), |i| values.is_null(i))
        } else {
            Vec::new()
        };

        // Calculate total size and write.
        let total_size = 4 // num_values
            + (num_values as usize * 4) // offsets
            + 4 // payload_size
            + payload_size as usize // payload
            + 4 // null_bitmap_size
            + (bitmap_words.len() * 4); // bitmap words

        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&num_values.to_le_bytes());
        for off in &offsets {
            buf.extend_from_slice(&off.to_le_bytes());
        }
        buf.extend_from_slice(&payload_size.to_le_bytes());
        buf.extend_from_slice(&payload);
        write_bitmap(&mut buf, &bitmap_words);

        Ok(buf)
    }
}

impl<'a> Column for DenseStringColumn<'a> {
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

                    if *segment_offset >= seg.size {
                        return Err(MurrError::TableError(format!(
                            "segment_offset {} out of range (segment has {} values)",
                            segment_offset, seg.size
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
            for i in 0..seg.size {
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
        self.segments.iter().map(|s| s.size).sum()
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
        let bytes = DenseStringColumn::write(&array, false).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], false).unwrap();
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
        let bytes = DenseStringColumn::write(&array, true).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], true).unwrap();
        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "a");
        assert_eq!(result.value(1), "b");
        // No append_null was called, so Arrow shouldn't allocate a null buffer.
        assert!(result.nulls().is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let array = make_string_array(&[Some("hello"), None, Some("world"), None]);
        let bytes = DenseStringColumn::write(&array, true).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], true).unwrap();
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
        let bytes = DenseStringColumn::write(&array, false).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], false).unwrap();

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
        let bytes = DenseStringColumn::write(&array, true).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], true).unwrap();

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

        let bytes1 = DenseStringColumn::write(&array1, false).unwrap();
        let bytes2 = DenseStringColumn::write(&array2, false).unwrap();

        let col = DenseStringColumn::new(&[&bytes1[..], &bytes2[..]], false).unwrap();
        assert_eq!(col.size(), 5);

        // get_all across segments
        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), "seg0_a");
        assert_eq!(result.value(1), "seg0_b");
        assert_eq!(result.value(2), "seg1_a");
        assert_eq!(result.value(3), "seg1_b");
        assert_eq!(result.value(4), "seg1_c");

        // get_indexes across segments
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
        let bytes = DenseStringColumn::write(&array, false).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], false).unwrap();
        assert_eq!(col.size(), 0);

        let result = col.get_all().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_indexes_with_missing_keys() {
        let array = make_string_array(&[Some("hello"), Some("world"), Some("foo")]);
        let bytes = DenseStringColumn::write(&array, false).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], false).unwrap();

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
        // 64 values to exercise 2 u32 words in null bitmap
        let values: Vec<Option<&str>> = (0..64)
            .map(|i| if i % 3 == 0 { None } else { Some("v") })
            .collect();
        let array = make_string_array(&values);
        let bytes = DenseStringColumn::write(&array, true).unwrap();

        let col = DenseStringColumn::new(&[&bytes[..]], true).unwrap();
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
