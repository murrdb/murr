use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float32Builder};
use arrow::datatypes::{DataType, Field};

use crate::core::MurrError;

use super::bitmap::{
    build_bitmap_words, parse_null_bitmap, read_f32_le, read_u32_le, write_bitmap, NullBitmap,
};
use super::column::{Column, KeyOffset};

/// Parsed zero-copy view over a single segment's dense float32 column data.
///
/// Wire format:
/// ```text
/// [num_values: u32]
/// [payload: [f32; num_values]]
/// [null_bitmap_size: u32]            // count of u32 words (0 if non-nullable)
/// [null_bitmap: [u32; null_bitmap_size]]
/// ```
struct DenseFloat32Segment<'a> {
    size: u32,
    payload: &'a [u8], // raw bytes, read as f32 LE per element
    nulls: NullBitmap<'a>,
}

impl<'a> DenseFloat32Segment<'a> {
    fn parse(data: &'a [u8], nullable: bool) -> Result<Self, MurrError> {
        let mut pos: usize = 0;

        if data.len() < 4 {
            return Err(MurrError::TableError(
                "dense float32 segment too small for num_values".into(),
            ));
        }

        let num_values = read_u32_le(data, pos);
        pos += 4;

        let payload_byte_len = num_values as usize * 4;
        if pos + payload_byte_len > data.len() {
            return Err(MurrError::TableError(
                "dense float32 segment truncated at payload".into(),
            ));
        }
        let payload = &data[pos..pos + payload_byte_len];
        pos += payload_byte_len;

        let (nulls, _) = parse_null_bitmap(data, pos, nullable, "dense float32")?;

        Ok(Self {
            size: num_values,
            payload,
            nulls,
        })
    }

    fn value_at(&self, idx: u32) -> f32 {
        let byte_pos = idx as usize * 4;
        read_f32_le(self.payload, byte_pos)
    }
}

pub struct DenseFloat32Column<'a> {
    segments: Vec<DenseFloat32Segment<'a>>,
    nullable: bool,
    field: Field,
}

impl<'a> DenseFloat32Column<'a> {
    pub fn new(name: &str, segments: &[&'a [u8]], nullable: bool) -> Result<Self, MurrError> {
        let parsed: Result<Vec<_>, _> = segments
            .iter()
            .map(|data| DenseFloat32Segment::parse(data, nullable))
            .collect();
        Ok(Self {
            segments: parsed?,
            nullable,
            field: Field::new(name, DataType::Float32, nullable),
        })
    }

    /// Serialize an Arrow Float32Array into the dense float32 wire format.
    pub fn write(values: &Float32Array, nullable: bool) -> Result<Vec<u8>, MurrError> {
        let num_values = values.len() as u32;

        let bitmap_words: Vec<u32> = if nullable {
            build_bitmap_words(values.len(), |i| values.is_null(i))
        } else {
            Vec::new()
        };

        let total_size = 4 // num_values
            + (num_values as usize * 4) // payload
            + 4 // null_bitmap_size
            + (bitmap_words.len() * 4); // bitmap words

        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&num_values.to_le_bytes());
        for i in 0..values.len() {
            let v = if values.is_null(i) { 0.0f32 } else { values.value(i) };
            buf.extend_from_slice(&v.to_le_bytes());
        }
        write_bitmap(&mut buf, &bitmap_words);

        Ok(buf)
    }
}

impl<'a> Column for DenseFloat32Column<'a> {
    fn field(&self) -> &Field {
        &self.field
    }

    fn get_indexes(&self, indexes: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let mut builder = Float32Builder::with_capacity(indexes.len());

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
                        builder.append_value(seg.value_at(*segment_offset));
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn get_all(&self) -> Result<Arc<dyn Array>, MurrError> {
        let total = self.size() as usize;
        let mut builder = Float32Builder::with_capacity(total);

        for seg in &self.segments {
            for i in 0..seg.size {
                if !seg.nulls.is_valid(i) {
                    builder.append_null();
                } else {
                    builder.append_value(seg.value_at(i));
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

    fn make_float32_array(values: &[Option<f32>]) -> Float32Array {
        values.iter().copied().collect::<Float32Array>()
    }

    #[test]
    fn test_round_trip_non_nullable() {
        let array = make_float32_array(&[Some(1.0), Some(2.5), Some(0.0)]);
        let bytes = DenseFloat32Column::write(&array, false).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], false).unwrap();
        assert_eq!(col.size(), 3);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 1.0);
        assert_eq!(result.value(1), 2.5);
        assert_eq!(result.value(2), 0.0);
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
        assert!(!result.is_null(2));
        assert!(result.nulls().is_none());
    }

    #[test]
    fn test_round_trip_nullable_no_nulls() {
        let array = make_float32_array(&[Some(1.0), Some(2.0)]);
        let bytes = DenseFloat32Column::write(&array, true).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], true).unwrap();
        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), 1.0);
        assert_eq!(result.value(1), 2.0);
        assert!(result.nulls().is_none());
    }

    #[test]
    fn test_round_trip_nullable_with_nulls() {
        let array = make_float32_array(&[Some(1.5), None, Some(3.14), None]);
        let bytes = DenseFloat32Column::write(&array, true).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], true).unwrap();
        assert_eq!(col.size(), 4);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 1.5);
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 3.14);
        assert!(result.is_null(3));
    }

    #[test]
    fn test_get_indexes() {
        let array = make_float32_array(&[Some(10.0), Some(20.0), Some(30.0), Some(40.0)]);
        let bytes = DenseFloat32Column::write(&array, false).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], false).unwrap();

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
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 30.0);
        assert_eq!(result.value(1), 10.0);
        assert_eq!(result.value(2), 40.0);
    }

    #[test]
    fn test_get_indexes_with_nulls() {
        let array = make_float32_array(&[Some(1.0), None, Some(3.0)]);
        let bytes = DenseFloat32Column::write(&array, true).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], true).unwrap();

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
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert_eq!(result.value(1), 1.0);
        assert_eq!(result.value(2), 3.0);
    }

    #[test]
    fn test_multiple_segments() {
        let array1 = make_float32_array(&[Some(1.0), Some(2.0)]);
        let array2 = make_float32_array(&[Some(3.0), Some(4.0), Some(5.0)]);

        let bytes1 = DenseFloat32Column::write(&array1, false).unwrap();
        let bytes2 = DenseFloat32Column::write(&array2, false).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes1[..], &bytes2[..]], false).unwrap();
        assert_eq!(col.size(), 5);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), 1.0);
        assert_eq!(result.value(1), 2.0);
        assert_eq!(result.value(2), 3.0);
        assert_eq!(result.value(3), 4.0);
        assert_eq!(result.value(4), 5.0);

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
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(result.value(0), 5.0);
        assert_eq!(result.value(1), 1.0);
    }

    #[test]
    fn test_empty_segment() {
        let array = make_float32_array(&[]);
        let bytes = DenseFloat32Column::write(&array, false).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], false).unwrap();
        assert_eq!(col.size(), 0);

        let result = col.get_all().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_indexes_with_missing_keys() {
        let array = make_float32_array(&[Some(10.0), Some(20.0), Some(30.0)]);
        let bytes = DenseFloat32Column::write(&array, false).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], false).unwrap();

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
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 10.0);
        assert!(!result.is_null(0));
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 30.0);
        assert!(!result.is_null(2));
        assert!(result.is_null(3));
    }

    #[test]
    fn test_many_values_bitmap_spans_multiple_words() {
        let values: Vec<Option<f32>> = (0..64)
            .map(|i| if i % 3 == 0 { None } else { Some(i as f32) })
            .collect();
        let array = make_float32_array(&values);
        let bytes = DenseFloat32Column::write(&array, true).unwrap();

        let col = DenseFloat32Column::new("test", &[&bytes[..]], true).unwrap();
        assert_eq!(col.size(), 64);

        let result = col.get_all().unwrap();
        let result = result.as_any().downcast_ref::<Float32Array>().unwrap();

        for i in 0..64 {
            if i % 3 == 0 {
                assert!(result.is_null(i), "expected null at index {i}");
            } else {
                assert_eq!(result.value(i), i as f32, "expected {} at index {i}", i as f32);
            }
        }
    }
}
