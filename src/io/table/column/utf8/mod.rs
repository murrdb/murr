pub(crate) mod segment;

use std::sync::Arc;

use arrow::array::{Array, StringBuilder};
use arrow::datatypes::{DataType, Field};

use crate::core::ColumnConfig;
use crate::core::MurrError;
use crate::io::table::column::ColumnSegment;
use crate::io::table::column::{Column, KeyOffset};

use segment::Utf8Segment;

pub struct Utf8Column<'a> {
    segments: Vec<Utf8Segment<'a>>,
    field: Field,
    nullable: bool,
}

impl<'a> Utf8Column<'a> {
    pub fn new(
        name: &str,
        config: &ColumnConfig,
        segments: &[&'a [u8]],
    ) -> Result<Self, MurrError> {
        let parsed: Result<Vec<_>, _> = segments
            .iter()
            .map(|data| Utf8Segment::parse(name, config, data))
            .collect();
        Ok(Self {
            segments: parsed?,
            field: Field::new(name, DataType::Utf8, config.nullable),
            nullable: config.nullable,
        })
    }
}

impl<'a> Column for Utf8Column<'a> {
    fn field(&self) -> &Field {
        &self.field
    }

    fn get_indexes(&self, indexes: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let mut builder = StringBuilder::with_capacity(indexes.len(), 0);

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
                        let (start, end) = seg.string_range(*segment_offset);
                        let s = std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                            MurrError::TableError(format!("invalid utf8 in string column: {e}"))
                        })?;
                        builder.append_value(s);
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

                        let (start, end) = seg.string_range(*segment_offset);
                        let s = std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                            MurrError::TableError(format!("invalid utf8 in string column: {e}"))
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

        if self.nullable {
            for seg in &self.segments {
                if let Some(ref nulls) = seg.nulls {
                    for i in 0..seg.header.num_values {
                        if !nulls.is_valid(i as u64) {
                            builder.append_null();
                        } else {
                            let (start, end) = seg.string_range(i);
                            let s =
                                std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                                    MurrError::TableError(format!(
                                        "invalid utf8 in string column: {e}"
                                    ))
                                })?;
                            builder.append_value(s);
                        }
                    }
                } else {
                    for i in 0..seg.header.num_values {
                        let (start, end) = seg.string_range(i);
                        let s = std::str::from_utf8(&seg.payload[start..end]).map_err(|e| {
                            MurrError::TableError(format!("invalid utf8 in string column: {e}"))
                        })?;
                        builder.append_value(s);
                    }
                }
            }
        } else {
            for seg in &self.segments {
                for i in 0..seg.header.num_values {
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
    use crate::core::DType;
    use arrow::array::StringArray;
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
    fn test_get_indexes() {
        let config = non_nullable_config();
        let array = make_string_array(&[Some("a"), Some("b"), Some("c"), Some("d")]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let col = Utf8Column::new("test", &config, &[&bytes[..]]).unwrap();

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
        let config = nullable_config();
        let array = make_string_array(&[Some("x"), None, Some("z")]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let col = Utf8Column::new("test", &config, &[&bytes[..]]).unwrap();

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
        let config = non_nullable_config();
        let array1 = make_string_array(&[Some("seg0_a"), Some("seg0_b")]);
        let array2 = make_string_array(&[Some("seg1_a"), Some("seg1_b"), Some("seg1_c")]);

        let bytes1 = Utf8Segment::write(&config, &array1).unwrap();
        let bytes2 = Utf8Segment::write(&config, &array2).unwrap();

        let col = Utf8Column::new("test", &config, &[&bytes1[..], &bytes2[..]]).unwrap();
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
    fn test_get_indexes_with_missing_keys() {
        let config = non_nullable_config();
        let array = make_string_array(&[Some("hello"), Some("world"), Some("foo")]);
        let bytes = Utf8Segment::write(&config, &array).unwrap();

        let col = Utf8Column::new("test", &config, &[&bytes[..]]).unwrap();

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
}
