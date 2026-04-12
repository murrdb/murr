use std::sync::Arc;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;

use crate::core::MurrError;
use crate::io::bitmap::NullBitmap;
use crate::io::bytes::StringOffsetPair;
use crate::io::column::reopen::open_segments;
use crate::io::column::utf8::footer::Utf8ColumnFooter;
use crate::io::column::ColumnReader;
use crate::io::directory::{DirectoryReader, ReadRequest, SegmentReadRequest};
use crate::io::info::{ColumnInfo, ColumnSegments};
use crate::io::table::key_offset::KeyOffset;

pub struct Utf8ColumnReader<R: DirectoryReader> {
    reader: Arc<R>,
    column: ColumnInfo,
    segments: Vec<Option<Utf8ColumnFooter>>,
    bitmap: NullBitmap,
}

impl<R: DirectoryReader> Utf8ColumnReader<R> {
    fn footer(&self, segment: u32) -> Result<&Utf8ColumnFooter, MurrError> {
        self.segments
            .get(segment as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "segment {} not found for column '{}'",
                    segment, self.column.name
                ))
            })
    }
}

#[async_trait]
impl<R: DirectoryReader> ColumnReader<R> for Utf8ColumnReader<R> {
    async fn open(
        reader: Arc<R>,
        column: &ColumnSegments,
        previous: &Option<Self>,
    ) -> Result<Self, MurrError> {
        let opened = open_segments::<Utf8ColumnFooter, _>(
            &reader,
            column,
            previous.as_ref().map(|p| &p.segments),
            previous.as_ref().map(|p| &p.bitmap),
        )
        .await?;
        Ok(Utf8ColumnReader {
            reader,
            column: column.column.clone(),
            segments: opened.segments,
            bitmap: opened.bitmap,
        })
    }

    async fn reopen(
        &self,
        reader: Arc<R>,
        column: &ColumnSegments,
    ) -> Result<Arc<dyn ColumnReader<R>>, MurrError> {
        let prev = Self {
            reader: self.reader.clone(),
            column: self.column.clone(),
            segments: self.segments.clone(),
            bitmap: self.bitmap.clone(),
        };
        Ok(Arc::new(Self::open(reader, column, &Some(prev)).await?))
    }

    async fn read(&self, keys: &[KeyOffset]) -> Result<Arc<dyn Array>, MurrError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            return Ok(Arc::new(StringArray::from(Vec::<&str>::new())));
        }

        let mut values: Vec<Option<String>> = vec![None; num_keys];

        // Collect non-missing keys
        let mut non_missing_keys: Vec<KeyOffset> = Vec::with_capacity(num_keys);
        for key in keys {
            if !key.is_missing() {
                non_missing_keys.push(*key);
            }
        }

        if !non_missing_keys.is_empty() {
            let reader = &self.reader;

            // Phase 1: Read i32 offset pairs (8 bytes each: offsets[i] and offsets[i+1])
            let mut offset_requests: Vec<SegmentReadRequest> =
                Vec::with_capacity(non_missing_keys.len());
            for key in &non_missing_keys {
                let footer = self.footer(key.segment)?;
                offset_requests.push(SegmentReadRequest {
                    segment: key.segment,
                    read: ReadRequest {
                        offset: footer.offsets.offset + key.segment_index * 4,
                        size: 8,
                    },
                });
            }

            let offset_pairs: Vec<StringOffsetPair> =
                reader.read(&offset_requests).await?;

            // Phase 2: Read actual string bytes
            let mut payload_requests: Vec<SegmentReadRequest> =
                Vec::with_capacity(non_missing_keys.len());
            let mut payload_indices: Vec<usize> = Vec::with_capacity(non_missing_keys.len());

            for (i, key) in non_missing_keys.iter().enumerate() {
                let footer = self.footer(key.segment)?;
                let pair = &offset_pairs[i];
                let len = (pair.end - pair.start) as u32;
                // Set empty string as default for non-missing keys
                values[key.request_index] = Some(String::new());
                if len > 0 {
                    payload_requests.push(SegmentReadRequest {
                        segment: key.segment,
                        read: ReadRequest {
                            offset: footer.payload.offset + pair.start as u32,
                            size: len,
                        },
                    });
                    payload_indices.push(i);
                }
            }

            if !payload_requests.is_empty() {
                let string_values: Vec<String> =
                    reader.read(&payload_requests).await?;

                for (j, &orig_idx) in payload_indices.iter().enumerate() {
                    let key = &non_missing_keys[orig_idx];
                    values[key.request_index] = Some(string_values[j].clone());
                }
            }

            // Check null bitmap for nullable columns
            if self.column.nullable {
                let null_indices = self.bitmap.get_nulls(&*self.reader, &non_missing_keys).await?;
                for idx in null_indices {
                    values[idx] = None;
                }
            }
        }

        let array: StringArray = values.iter().map(|v| v.as_deref()).collect();
        Ok(Arc::new(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::column::ColumnWriter;
    use crate::io::directory::mem::directory::MemDirectory;
    use crate::io::directory::mem::reader::MemReader;
    use crate::io::directory::{Directory, DirectoryWriter};
    use crate::io::url::MemUrl;
    use std::collections::HashMap;

    fn test_dir() -> Arc<MemDirectory> {
        let mut columns = HashMap::new();
        columns.insert("key".to_string(), ColumnSchema { dtype: DType::Utf8, nullable: false });
        columns.insert("name".to_string(), ColumnSchema { dtype: DType::Utf8, nullable: false });
        let schema = TableSchema { key: "key".to_string(), columns };
        Arc::new(MemDirectory::create(&MemUrl, "default", schema, 4096, false).unwrap())
    }

    fn non_nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "name".to_string(),
            dtype: DType::Utf8,
            nullable: false,
        }
    }

    fn nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "name".to_string(),
            dtype: DType::Utf8,
            nullable: true,
        }
    }

    fn make_array(values: &[Option<&str>]) -> StringArray {
        values.iter().copied().collect::<StringArray>()
    }

    fn make_non_null_array(values: &[&str]) -> StringArray {
        StringArray::from(values.to_vec())
    }

    async fn write_segment(
        dir: &Arc<MemDirectory>,
        col_info: &ColumnInfo,
        values: &StringArray,
    ) {
        let segment_bytes = values.write_column(col_info).unwrap();
        let dir_writer = dir.open_writer().await.unwrap();
        dir_writer.write(&[segment_bytes]).await.unwrap();
    }

    async fn open_reader(
        dir: &Arc<MemDirectory>,
        col_name: &str,
    ) -> Utf8ColumnReader<MemReader> {
        let reader: Arc<MemReader> = Arc::new(dir.open_reader().await.unwrap());
        let col_segments = reader.info().columns.get(col_name).unwrap().clone();
        Utf8ColumnReader::open(reader, &col_segments, &None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn read_write_roundtrip_non_nullable() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&["hello", "world", "!"])).await;

        let reader = open_reader(&dir, "name").await;

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 0,
                segment_index: 2,
            },
            KeyOffset {
                request_index: 1,
                segment: 0,
                segment_index: 0,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "!");
        assert_eq!(arr.value(1), "hello");
        assert_eq!(arr.null_count(), 0);
    }

    #[tokio::test]
    async fn read_write_multi_segment() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&["a", "bb"])).await;
        write_segment(&dir, &col_info, &make_non_null_array(&["ccc", "dddd"])).await;

        let reader = open_reader(&dir, "name").await;

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 1,
                segment_index: 0,
            },
            KeyOffset {
                request_index: 1,
                segment: 0,
                segment_index: 1,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "ccc");
        assert_eq!(arr.value(1), "bb");
    }

    #[tokio::test]
    async fn read_missing_keys_produce_nulls() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&["foo", "bar"])).await;

        let reader = open_reader(&dir, "name").await;

        let keys = vec![
            KeyOffset {
                request_index: 0,
                segment: 0,
                segment_index: 0,
            },
            KeyOffset::missing(1),
            KeyOffset {
                request_index: 2,
                segment: 0,
                segment_index: 1,
            },
        ];
        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), "foo");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "bar");
    }

    #[tokio::test]
    async fn read_nullable_roundtrip() {
        let dir = test_dir();
        let col_info = nullable_info();

        write_segment(
            &dir,
            &col_info,
            &make_array(&[Some("a"), None, Some("bc"), None, Some("d")]),
        )
        .await;

        let reader = open_reader(&dir, "name").await;

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 5);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), "a");
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(2), "bc");
        assert!(arr.is_null(3));
        assert!(!arr.is_null(4));
        assert_eq!(arr.value(4), "d");
    }

    #[tokio::test]
    async fn read_empty_keys() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&["x"])).await;

        let reader = open_reader(&dir, "name").await;

        let result = reader.read(&[]).await.unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 0);
    }
}
