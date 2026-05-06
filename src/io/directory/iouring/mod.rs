use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(not(target_os = "linux"))]
use crate::core::MurrError;
use crate::io::directory::DirectoryConfig;
use crate::io::directory::mmap::directory::resolve_cache_dir;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IoUringConfig {
    #[serde(default = "IoUringConfig::default_cache_dir")]
    pub cache_dir: PathBuf,
    #[serde(default = "IoUringConfig::default_ring_size")]
    pub ring_size: u32,
    #[serde(default)]
    pub direct: bool,
    #[serde(default = "IoUringConfig::default_page_size")]
    pub page_size: u32,
    #[serde(default)]
    pub sqpoll: bool,
    #[serde(default = "IoUringConfig::default_workers")]
    pub workers: usize,
    #[serde(default = "IoUringConfig::default_buffer_slots")]
    pub buffer_slots: u32,
    /// When true, the pool's arenas are pinned via
    /// `Submitter::register_buffers` and reads use `IORING_OP_READ_FIXED`.
    /// When false, the arenas stay unpinned and all reads use the regular
    /// `Read` opcode — useful in test environments where the per-user
    /// `RLIMIT_MEMLOCK` budget is shared across many concurrent test
    /// binaries and the RCU release grace period would otherwise stack
    /// pinned pages from back-to-back tests.
    #[serde(default = "IoUringConfig::default_register_buffers")]
    pub register_buffers: bool,
    #[serde(default = "IoUringConfig::default_coalesce_window")]
    pub coalesce_window: u32,
    #[serde(default = "IoUringConfig::default_coalesce_slots")]
    pub coalesce_slots: u32,
}

impl IoUringConfig {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            ring_size: Self::default_ring_size(),
            direct: false,
            page_size: Self::default_page_size(),
            sqpoll: false,
            workers: Self::default_workers(),
            buffer_slots: Self::default_buffer_slots(),
            register_buffers: Self::default_register_buffers(),
            coalesce_window: Self::default_coalesce_window(),
            coalesce_slots: Self::default_coalesce_slots(),
        }
    }

    fn default_cache_dir() -> PathBuf {
        resolve_cache_dir().expect(
            "failed to resolve cache dir — set storage.backend.cache_dir or MURR_STORAGE_BACKEND__CACHE__DIR",
        )
    }

    fn default_ring_size() -> u32 {
        256
    }

    fn default_page_size() -> u32 {
        4096
    }

    fn default_workers() -> usize {
        4
    }

    fn default_buffer_slots() -> u32 {
        Self::default_ring_size()
    }

    fn default_register_buffers() -> bool {
        true
    }

    fn default_coalesce_window() -> u32 {
        128 * 1024
    }

    fn default_coalesce_slots() -> u32 {
        32
    }
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self::new(Self::default_cache_dir())
    }
}

impl DirectoryConfig for IoUringConfig {}

#[cfg(not(target_os = "linux"))]
pub(crate) fn unsupported_platform_error() -> MurrError {
    MurrError::ConfigParsingError(
        "io_uring backend is only supported on Linux".to_string(),
    )
}

core::cfg_select! {
    target_os = "linux" => {
        pub mod directory;
        pub mod reader;
        pub mod writer;
        pub(crate) mod pool;
    }
    _ => {}
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use std::sync::Arc;

    use crate::core::{ColumnSchema, DType, TableSchema};
    use crate::io::directory::iouring::IoUringConfig;
    use crate::io::directory::iouring::directory::IoUringDirectory;
    use crate::io::directory::{
        Directory, DirectoryReader, DirectoryWriter, ReadRequest, SegmentReadRequest,
    };
    use crate::io::table::segment::{Segment, SegmentBytes};
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use serial_test::file_serial;
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

    fn test_dir(tmp: &tempfile::TempDir, direct: bool) -> Arc<IoUringDirectory> {
        test_dir_with(tmp, direct, IoUringConfig::default_coalesce_window())
    }

    fn test_dir_with(
        tmp: &tempfile::TempDir,
        direct: bool,
        coalesce_window: u32,
    ) -> Arc<IoUringDirectory> {
        // `register_buffers: false` keeps tests off the per-user
        // RLIMIT_MEMLOCK budget entirely — the kernel releases io_uring
        // memory asynchronously over an RCU grace period, and back-to-back
        // tests would otherwise stack pinned pages and exhaust the typical
        // 8 MiB limit shared across concurrent test binaries.
        let cfg = IoUringConfig {
            cache_dir: tmp.path().to_path_buf(),
            direct,
            workers: 1,
            ring_size: 8,
            buffer_slots: 8,
            register_buffers: false,
            coalesce_window,
            coalesce_slots: 4,
            ..IoUringConfig::default()
        };
        Arc::new(IoUringDirectory::create("default", test_schema(), cfg).unwrap())
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

    #[file_serial]
    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
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

    #[file_serial]
    #[tokio::test]
    async fn two_segments_are_independently_readable() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
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

    #[file_serial]
    #[tokio::test]
    async fn partial_read_returns_matching_slice() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
        let writer = dir.open_writer().await.unwrap();

        let segment = make_segment(&["k0"], &[Some(42.0)]);
        let full_bytes = segment.to_bytes().unwrap();
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        // Unaligned offset + size — exercises the page-alignment math.
        let result = reader
            .read(&[SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 4, size: 8 },
            }])
            .await
            .unwrap();
        assert_eq!(result[0].bytes, full_bytes[4..12]);
    }

    #[file_serial]
    #[tokio::test]
    async fn reopen_reader_reflects_new_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
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

    #[file_serial]
    #[tokio::test]
    async fn direct_mode_reads_unaligned_window() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, true);
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

    #[file_serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_distribute_across_workers() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
        let writer = dir.open_writer().await.unwrap();

        let seg0 = make_segment(&["a"], &[Some(1.0)]);
        let seg1 = make_segment(&["b"], &[Some(2.0)]);
        let bytes0 = seg0.to_bytes().unwrap();
        let bytes1 = seg1.to_bytes().unwrap();
        writer.write(&seg0).await.unwrap();
        writer.write(&seg1).await.unwrap();

        let reader = Arc::new(dir.open_reader().await.unwrap());

        let mut handles = Vec::new();
        for i in 0..32 {
            let reader = Arc::clone(&reader);
            let bytes0 = bytes0.clone();
            let bytes1 = bytes1.clone();
            handles.push(tokio::spawn(async move {
                let segment = (i % 2) as u32;
                let expected = if segment == 0 { &bytes0 } else { &bytes1 };
                let size = expected.len() as u32;
                let result = reader
                    .read(&[SegmentReadRequest {
                        segment,
                        read: ReadRequest { offset: 0, size },
                    }])
                    .await
                    .unwrap();
                assert_eq!(&result[0].bytes, expected);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    fn make_big_segment(n: usize) -> SegmentBytes {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let keys: Vec<String> = (0..n).map(|i| format!("k{i:08}")).collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let scores: Vec<Option<f32>> = (0..n).map(|i| Some(i as f32)).collect();
        let ids = StringArray::from(key_refs);
        let scores_arr = Float32Array::from(scores);
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(ids), Arc::new(scores_arr)]).unwrap();
        Segment::write(batch, &test_schema()).unwrap()
    }

    #[file_serial]
    #[tokio::test]
    async fn coalesce_merges_reads_in_same_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        // Default coalesce window (128 KiB) — all four reads land in bucket 0.
        let dir = test_dir(&tmp, false);
        let writer = dir.open_writer().await.unwrap();

        let segment = make_big_segment(64);
        let full = segment.to_bytes().unwrap();
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let reqs: Vec<SegmentReadRequest> = [(0u32, 16u32), (32, 16), (64, 16), (128, 16)]
            .into_iter()
            .map(|(offset, size)| SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset, size },
            })
            .collect();
        let results = reader.read(&reqs).await.unwrap();

        assert_eq!(results.len(), 4);
        for (i, req) in reqs.iter().enumerate() {
            let off = req.read.offset as usize;
            let sz = req.read.size as usize;
            assert_eq!(results[i].bytes, full[off..off + sz]);
            assert_eq!(results[i].request, *req);
        }
    }

    #[file_serial]
    #[tokio::test]
    async fn coalesce_keeps_reads_in_different_buckets_separate() {
        let tmp = tempfile::tempdir().unwrap();
        // Tiny window (256 B) so a single segment spans many buckets.
        let dir = test_dir_with(&tmp, false, 256);
        let writer = dir.open_writer().await.unwrap();

        let segment = make_big_segment(128);
        let full = segment.to_bytes().unwrap();
        assert!(full.len() >= 600, "test segment too small: {}", full.len());
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        // Offsets 0 and 512 → buckets 0 and 2 respectively.
        let reqs = vec![
            SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size: 32 },
            },
            SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 512, size: 32 },
            },
        ];
        let results = reader.read(&reqs).await.unwrap();

        assert_eq!(results[0].bytes, full[0..32]);
        assert_eq!(results[1].bytes, full[512..544]);
    }

    #[file_serial]
    #[tokio::test]
    async fn coalesce_does_not_merge_across_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir(&tmp, false);
        let writer = dir.open_writer().await.unwrap();

        let seg0 = make_segment(&["a"], &[Some(1.0)]);
        let seg1 = make_segment(&["b"], &[Some(2.0)]);
        let bytes0 = seg0.to_bytes().unwrap();
        let bytes1 = seg1.to_bytes().unwrap();
        writer.write(&seg0).await.unwrap();
        writer.write(&seg1).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        // Both at offset 0 — same bucket id within each segment, but
        // different segments must never merge.
        let reqs = vec![
            SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset: 0, size: bytes0.len() as u32 },
            },
            SegmentReadRequest {
                segment: 1,
                read: ReadRequest { offset: 0, size: bytes1.len() as u32 },
            },
        ];
        let results = reader.read(&reqs).await.unwrap();

        assert_eq!(results[0].bytes, bytes0);
        assert_eq!(results[1].bytes, bytes1);
    }

    #[file_serial]
    #[tokio::test]
    async fn coalesce_disabled_when_window_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = test_dir_with(&tmp, false, 0);
        let writer = dir.open_writer().await.unwrap();

        let segment = make_big_segment(64);
        let full = segment.to_bytes().unwrap();
        writer.write(&segment).await.unwrap();

        let reader = dir.open_reader().await.unwrap();
        let reqs: Vec<SegmentReadRequest> = [(0u32, 16u32), (32, 16), (64, 16), (128, 16)]
            .into_iter()
            .map(|(offset, size)| SegmentReadRequest {
                segment: 0,
                read: ReadRequest { offset, size },
            })
            .collect();
        let results = reader.read(&reqs).await.unwrap();

        for (i, req) in reqs.iter().enumerate() {
            let off = req.read.offset as usize;
            let sz = req.read.size as usize;
            assert_eq!(results[i].bytes, full[off..off + sz]);
        }
    }
}
