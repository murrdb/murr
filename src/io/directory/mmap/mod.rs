pub mod directory;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::directory::mmap::directory::{MMapConfig, MMapDirectory};
    use crate::io::directory::{Directory, DirectoryReader, DirectoryWriter, ReadRequest, SegmentReadRequest};
    use crate::io::table::segment::Segment;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use indexmap::IndexMap;

    fn test_schema() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnSchema { dtype: DType::Utf8, nullable: false },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema { dtype: DType::Float32, nullable: true },
        );
        TableSchema { key: "id".to_string(), columns }
    }

    fn test_dir(tmp: &tempfile::TempDir) -> Arc<MMapDirectory> {
        let cfg = MMapConfig::new(tmp.path().to_path_buf());
        Arc::new(MMapDirectory::create("default", test_schema(), cfg).unwrap())
    }

    fn make_segment(keys: &[&str], scores: &[Option<f32>]) -> crate::io::table::segment::SegmentBytes {
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
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
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
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
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
                    read: ReadRequest { offset: 0, size: bytes0.len() as u32 },
                },
                SegmentReadRequest {
                    segment: 1,
                    read: ReadRequest { offset: 0, size: bytes1.len() as u32 },
                },
            ])
            .await
            .unwrap();

        assert_eq!(results[0].bytes, bytes0);
        assert_eq!(results[1].bytes, bytes1);
    }

    #[tokio::test]
    async fn partial_read_returns_matching_slice() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
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

    #[tokio::test]
    async fn reopen_reader_reflects_new_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp);
        let writer = dir.open_writer().await.unwrap();

        let reader_v1 = dir.open_reader().await.unwrap();
        assert_eq!(reader_v1.info().segments.len(), 0);

        writer.write(&make_segment(&["a"], &[Some(1.0)])).await.unwrap();

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
    async fn open_reads_segments_written_by_prior_instance() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = MMapConfig::new(tmp.path().to_path_buf());

        let dir = Arc::new(MMapDirectory::create("idx", test_schema(), cfg.clone()).unwrap());
        let seg = make_segment(&["x"], &[Some(3.14)]);
        let expected = seg.to_bytes().unwrap();
        let size = expected.len() as u32;
        dir.open_writer().await.unwrap().write(&seg).await.unwrap();

        let dir2 = Arc::new(MMapDirectory::open("idx", cfg).unwrap());
        let reader = dir2.open_reader().await.unwrap();
        assert_eq!(reader.info().segments.len(), 1);
        let result = reader
            .read(&[SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size },
            }])
            .await
            .unwrap();
        assert_eq!(result[0].bytes, expected);
    }

    #[test]
    fn list_indexes_returns_index_names() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = MMapConfig::new(tmp.path().to_path_buf());
        MMapDirectory::create("alpha", test_schema(), cfg.clone()).unwrap();
        MMapDirectory::create("beta", test_schema(), cfg.clone()).unwrap();
        let mut indexes = MMapDirectory::list_indexes(&cfg);
        indexes.sort();
        assert_eq!(indexes, vec!["alpha", "beta"]);
    }
}
