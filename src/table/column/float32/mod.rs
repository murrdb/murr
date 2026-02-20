pub(crate) mod segment;

use std::sync::Arc;

use arrow::array::{Array, Float32Builder};
use arrow::datatypes::{DataType, Field};

use crate::conf::ColumnConfig;
use crate::core::MurrError;
use crate::table::column::ColumnSegment;
use crate::table::column::{Column, KeyOffset};

use segment::Float32Segment;

pub struct Float32Column<'a> {
    segments: Vec<Float32Segment<'a>>,
    field: Field,
    nullable: bool,
}

impl<'a> Float32Column<'a> {
    pub fn new(
        name: &str,
        config: &ColumnConfig,
        segments: &[&'a [u8]],
    ) -> Result<Self, MurrError> {
        let parsed: Result<Vec<_>, _> = segments
            .iter()
            .map(|data| Float32Segment::parse(name, config, data))
            .collect();
        Ok(Self {
            segments: parsed?,
            field: Field::new(name, DataType::Float32, config.nullable),
            nullable: config.nullable,
        })
    }
}

impl<'a> Column for Float32Column<'a> {
    fn field(&self) -> &Field {
        &self.field
    }

    fn get_indexes(&self, indexes: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let mut builder = Float32Builder::with_capacity(indexes.len());

        if self.nullable {
            for idx in indexes {
                match idx {
                    KeyOffset::MissingKey => {
                        builder.append_null();
                    }
                    KeyOffset::SegmentOffset {
                        segment_id,
                        segment_offset,
                    } => {
                        let seg =
                            self.segments.get(*segment_id as usize).ok_or_else(|| {
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

                        if let Some(ref nulls) = seg.nulls {
                            if !nulls.is_valid(*segment_offset as u64) {
                                builder.append_null();
                                continue;
                            }
                        }
                        builder.append_value(seg.payload[*segment_offset as usize]);
                    }
                }
            }
        } else {
            for idx in indexes {
                match idx {
                    KeyOffset::MissingKey => {
                        builder.append_null();
                    }
                    KeyOffset::SegmentOffset {
                        segment_id,
                        segment_offset,
                    } => {
                        let seg =
                            self.segments.get(*segment_id as usize).ok_or_else(|| {
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

                        builder.append_value(seg.payload[*segment_offset as usize]);
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn get_all(&self) -> Result<Arc<dyn Array>, MurrError> {
        let total = self.size() as usize;
        let mut builder = Float32Builder::with_capacity(total);

        if self.nullable {
            for seg in &self.segments {
                if let Some(ref nulls) = seg.nulls {
                    for i in 0..seg.header.num_values {
                        if !nulls.is_valid(i as u64) {
                            builder.append_null();
                        } else {
                            builder.append_value(seg.payload[i as usize]);
                        }
                    }
                } else {
                    for i in 0..seg.header.num_values {
                        builder.append_value(seg.payload[i as usize]);
                    }
                }
            }
        } else {
            for seg in &self.segments {
                for i in 0..seg.header.num_values {
                    builder.append_value(seg.payload[i as usize]);
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
    use crate::conf::DType;
    use arrow::array::Float32Array;
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
    fn test_get_indexes() {
        let config = non_nullable_config();
        let array = make_float32_array(&[Some(10.0), Some(20.0), Some(30.0), Some(40.0)]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let col = Float32Column::new("test", &config, &[&bytes[..]]).unwrap();

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
        let config = nullable_config();
        let array = make_float32_array(&[Some(1.0), None, Some(3.0)]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let col = Float32Column::new("test", &config, &[&bytes[..]]).unwrap();

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
        let config = non_nullable_config();
        let array1 = make_float32_array(&[Some(1.0), Some(2.0)]);
        let array2 = make_float32_array(&[Some(3.0), Some(4.0), Some(5.0)]);

        let bytes1 = Float32Segment::write(&config, &array1).unwrap();
        let bytes2 = Float32Segment::write(&config, &array2).unwrap();

        let col = Float32Column::new("test", &config, &[&bytes1[..], &bytes2[..]]).unwrap();
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
    fn test_get_indexes_with_missing_keys() {
        let config = non_nullable_config();
        let array = make_float32_array(&[Some(10.0), Some(20.0), Some(30.0)]);
        let bytes = Float32Segment::write(&config, &array).unwrap();

        let col = Float32Column::new("test", &config, &[&bytes[..]]).unwrap();

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
}
