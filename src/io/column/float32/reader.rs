use crate::io::codec::Float32Codec;
use crate::io::column::scalar::ScalarColumnReader;

pub type Float32ColumnReader<R> = ScalarColumnReader<R, Float32Codec>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DType;
    use crate::core::{ColumnSchema, TableSchema};
    use crate::io::column::float32::writer::Float32ColumnWriter;
    use crate::io::column::ColumnWriter;
    use crate::io::column::ColumnReader;
    use crate::io::directory::mem::directory::MemDirectory;
    use crate::io::directory::mem::reader::MemReader;
    use crate::io::directory::{Directory, DirectoryReader, DirectoryWriter};
    use crate::io::info::ColumnInfo;
    use crate::io::table::key_offset::KeyOffset;
    use crate::io::url::MemUrl;
    use arrow::array::{Array, Float32Array};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_dir() -> Arc<MemDirectory> {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
        let schema = TableSchema {
            key: "key".to_string(),
            columns,
        };
        Arc::new(MemDirectory::create(&MemUrl, "default", schema, 4096, false).unwrap())
    }

    fn non_nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: false,
        }
    }

    fn nullable_info() -> ColumnInfo {
        ColumnInfo {
            name: "score".to_string(),
            dtype: DType::Float32,
            nullable: true,
        }
    }

    fn make_array(values: &[Option<f32>]) -> Float32Array {
        values.iter().copied().collect::<Float32Array>()
    }

    fn make_non_null_array(values: &[f32]) -> Float32Array {
        Float32Array::from(values.to_vec())
    }

    async fn write_segment(
        dir: &Arc<MemDirectory>,
        col_info: &ColumnInfo,
        values: &Float32Array,
    ) {
        let writer = Float32ColumnWriter::new(Arc::new(col_info.clone()));
        let segment_bytes = writer.write(values).unwrap();
        let dir_writer = dir.open_writer().await.unwrap();
        dir_writer.write(&[segment_bytes]).await.unwrap();
    }

    async fn open_reader(
        dir: &Arc<MemDirectory>,
        col_name: &str,
    ) -> Float32ColumnReader<MemReader> {
        let reader: Arc<MemReader> = Arc::new(dir.open_reader().await.unwrap());
        let col_segments = reader.info().columns.get(col_name).unwrap().clone();
        Float32ColumnReader::open(reader, &col_segments, &None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn read_write_roundtrip_non_nullable() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&[10.0, 20.0, 30.0])).await;

        let reader = open_reader(&dir, "score").await;

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.value(0), 30.0);
        assert_eq!(arr.value(1), 10.0);
        assert_eq!(arr.null_count(), 0);
    }

    #[tokio::test]
    async fn read_write_multi_segment() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&[1.0, 2.0])).await;
        write_segment(&dir, &col_info, &make_non_null_array(&[10.0, 20.0])).await;

        let reader = open_reader(&dir, "score").await;

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.value(0), 10.0);
        assert_eq!(arr.value(1), 2.0);
    }

    #[tokio::test]
    async fn read_missing_keys_produce_nulls() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&[5.0, 6.0])).await;

        let reader = open_reader(&dir, "score").await;

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
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 5.0);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 6.0);
    }

    #[tokio::test]
    async fn read_nullable_roundtrip() {
        let dir = test_dir();
        let col_info = nullable_info();

        write_segment(
            &dir,
            &col_info,
            &make_array(&[Some(1.0), None, Some(3.0), None, Some(5.0)]),
        )
        .await;

        let reader = open_reader(&dir, "score").await;

        let keys: Vec<KeyOffset> = (0..5)
            .map(|i| KeyOffset {
                request_index: i,
                segment: 0,
                segment_index: i as u32,
            })
            .collect();

        let result = reader.read(&keys).await.unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 5);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 1.0);
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(2), 3.0);
        assert!(arr.is_null(3));
        assert!(!arr.is_null(4));
        assert_eq!(arr.value(4), 5.0);
    }

    #[tokio::test]
    async fn read_empty_keys() {
        let dir = test_dir();
        let col_info = non_nullable_info();

        write_segment(&dir, &col_info, &make_non_null_array(&[1.0])).await;

        let reader = open_reader(&dir, "score").await;

        let result = reader.read(&[]).await.unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.len(), 0);
    }
}
