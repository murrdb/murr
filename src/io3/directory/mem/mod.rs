pub mod directory;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io3::directory::mem::directory::{MemConfig, MemDirectory};
    use crate::io3::directory::{
        Directory, DirectoryReader, DirectoryWriter, ReadRequest, SegmentReadRequest,
    };
    use crate::io3::table::segment::{Segment, SegmentBytes};
    use crate::io3::url::MemUrl;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use indexmap::IndexMap;

    fn test_schema() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );
        TableSchema {
            key: "id".to_string(),
            columns,
        }
    }

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::create(&MemUrl, "default", test_schema(), MemConfig).unwrap())
    }

    fn make_segment(keys: &[&str], scores: &[Option<f32>]) -> SegmentBytes {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let ids = StringArray::from(keys.to_vec());
        let scores_arr = Float32Array::from(scores.to_vec());
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(ids), Arc::new(scores_arr)]).unwrap();
        Segment::write(batch, &test_schema()).unwrap()
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        let segment = make_segment(&["k0", "k1"], &[Some(1.0), None]);
        let expected = segment.to_bytes().unwrap();
        let size = expected.len() as u32;

        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let result = reader
            .read(&[SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size },
            }])
            .await
            .unwrap();

        assert_eq!(result[0].bytes, expected);
    }

    #[tokio::test]
    async fn two_segments_are_independently_readable() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        let seg0 = make_segment(&["a"], &[Some(1.0)]);
        let seg1 = make_segment(&["b"], &[Some(2.0)]);
        let bytes0 = seg0.to_bytes().unwrap();
        let bytes1 = seg1.to_bytes().unwrap();

        writer.write(&seg0).await.unwrap();
        writer.write(&seg1).await.unwrap();

        let reader = dir.open_reader().await.unwrap();

        let results = reader
            .read(&[
                SegmentReadRequest {
                    segment: 0,
                    read: ReadRequest {
                        offset: 0,
                        size: bytes0.len() as u32,
                    },
                },
                SegmentReadRequest {
                    segment: 1,
                    read: ReadRequest {
                        offset: 0,
                        size: bytes1.len() as u32,
                    },
                },
            ])
            .await
            .unwrap();

        assert_eq!(results[0].bytes, bytes0);
        assert_eq!(results[1].bytes, bytes1);
    }

    #[tokio::test]
    async fn reopen_reader_reflects_new_segments() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        let reader_v1 = dir.open_reader().await.unwrap();
        assert_eq!(reader_v1.info().segments.len(), 0);

        writer
            .write(&make_segment(&["a"], &[Some(1.0)]))
            .await
            .unwrap();

        let reader_v2 = reader_v1.reopen_reader().await.unwrap();
        assert_eq!(reader_v2.info().segments.len(), 1);

        let size = reader_v2.info().segments[0].size_bytes;
        let result = reader_v2
            .read(&[SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size },
            }])
            .await
            .unwrap();
        assert_eq!(result[0].bytes.len(), size as usize);
    }

    #[tokio::test]
    async fn partial_read_returns_matching_slice() {
        let dir = test_dir();
        let writer = dir.open_writer().await.unwrap();

        let segment = make_segment(&["k0"], &[Some(42.0)]);
        let full_bytes = segment.to_bytes().unwrap();

        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let result = reader
            .read(&[SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 4, size: 8 },
            }])
            .await
            .unwrap();

        assert_eq!(result[0].bytes, full_bytes[4..12]);
    }
}
