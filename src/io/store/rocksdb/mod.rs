use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use rocksdb::{DB, Options, ReadOptions, WriteBatch, WriteOptions};

use crate::core::{MurrError, TableSchema};
use crate::io::row::read::ReadBatchBuilder;
use crate::io::store::rocksdb::block::BlockConfig;
use crate::io::store::rocksdb::plain::PlainConfig;
use crate::io::store::{KeyValue, Manifest, Store};
use itertools::Itertools;
pub mod block;
pub mod plain;

const MANIFEST_FILE: &str = "manifest.json";

pub struct RocksDBStore {
    db: DB,
    cf_opts: Options,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
    path: PathBuf,
    manifest: Manifest,
    write_buffer_size: usize,
    sort_keys: bool,
}

impl RocksDBStore {
    pub fn open_plain(path: &Path, config: &PlainConfig) -> Result<Self, MurrError> {
        let cf_opts: Options = config.into();
        Self::open_inner(
            path,
            cf_opts,
            ReadOptions::default(),
            config.write_buffer_size,
            false,
        )
    }

    pub fn open_block(path: &Path, config: &BlockConfig) -> Result<Self, MurrError> {
        let cf_opts: Options = config.into();
        let mut read_opts = ReadOptions::default();
        read_opts.set_async_io(config.async_io);
        read_opts.set_verify_checksums(config.verify_checksums);
        Self::open_inner(
            path,
            cf_opts,
            read_opts,
            config.write_buffer_size,
            true,
        )
    }

    fn open_inner(
        path: &Path,
        cf_opts: Options,
        read_opts: ReadOptions,
        write_buffer_size: usize,
        sort_keys: bool,
    ) -> Result<Self, MurrError> {
        let cfs = DB::list_cf(&cf_opts, path).unwrap_or_default();
        let cf_descriptors = cfs.iter().map(|name| (name.as_str(), cf_opts.clone()));
        let db = DB::open_cf_with_opts(&cf_opts, path, cf_descriptors)?;
        let manifest = Manifest::from_file(&path.join(MANIFEST_FILE))?;
        Ok(Self {
            db,
            cf_opts,
            write_opts: WriteOptions::default(),
            read_opts,
            path: path.to_path_buf(),
            manifest,
            write_buffer_size,
            sort_keys,
        })
    }

    fn manifest_path(&self) -> PathBuf {
        self.path.join(MANIFEST_FILE)
    }
}

impl Store for RocksDBStore {
    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError> {
        self.manifest.add_table(table, schema)?;
        self.db.create_cf(table, &self.cf_opts)?;
        self.manifest.to_file(&self.manifest_path())?;
        Ok(())
    }

    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn write(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = KeyValue>,
    ) -> Result<(), MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;

        for chunk in &rows.into_iter().chunks(self.write_buffer_size) {
            let mut batch = WriteBatch::default();
            for kv in chunk {
                batch.put_cf(cf, kv.key, kv.value);
            }
            self.db.write_opt(batch, &self.write_opts)?;
            self.db.flush_cf(cf)?;
        }
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        keys: &[&[u8]],
        mut builder: ReadBatchBuilder<'_>,
    ) -> Result<RecordBatch, MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;

        if self.sort_keys {
            let n = keys.len();
            let mut order: Vec<usize> = (0..n).collect();
            order.sort_unstable_by_key(|&i| keys[i]);
            let sorted: Vec<&[u8]> = order.iter().map(|&i| keys[i]).collect();
            let raw = self
                .db
                .batched_multi_get_cf_opt(&cf, &sorted, true, &self.read_opts);
            // raw[i] is the result for keys[order[i]]; pos[order[i]] = i so we can
            // walk caller order by indexing into raw with pos[i].
            let mut pos = vec![0usize; n];
            for (i, &o) in order.iter().enumerate() {
                pos[o] = i;
            }
            for i in 0..n {
                match &raw[pos[i]] {
                    Ok(Some(v)) => builder.add_row(v.as_ref())?,
                    Ok(None) => builder.add_empty()?,
                    Err(e) => return Err(MurrError::IoError(e.to_string())),
                }
            }
        } else {
            let raw = self
                .db
                .batched_multi_get_cf_opt(&cf, keys, false, &self.read_opts);
            for r in &raw {
                match r {
                    Ok(Some(v)) => builder.add_row(v.as_ref())?,
                    Ok(None) => builder.add_empty()?,
                    Err(e) => return Err(MurrError::IoError(e.to_string())),
                }
            }
        }
        builder.build()
    }

    fn compact(&self, table: &str) -> Result<(), MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}

#[cfg(all(test, feature = "testutil"))]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};
    use crate::io::row::write::WriteRow;
    use crate::io::schema::{SegmentColumnSchema, SegmentSchema};
    use arrow::array::{Array, StringArray};
    use indexmap::IndexMap;
    use rstest::rstest;
    use std::path::Path;
    use tempfile::TempDir;

    type Opener = fn(&Path) -> RocksDBStore;

    fn open_plain(path: &Path) -> RocksDBStore {
        RocksDBStore::open_plain(path, &PlainConfig::default()).expect("open plain")
    }

    fn open_block(path: &Path) -> RocksDBStore {
        RocksDBStore::open_block(path, &BlockConfig::default()).expect("open block")
    }

    fn schema(key: &str) -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            key.to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "payload".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: true,
            },
        );
        TableSchema {
            key: key.to_string(),
            columns,
        }
    }

    fn payload_segment() -> SegmentSchema {
        SegmentSchema::new(&[SegmentColumnSchema {
            index: 0,
            dtype: DType::Utf8,
            name: "payload".into(),
            offset: 0,
        }])
    }

    fn put(store: &mut RocksDBStore, table: &str, rows: &[(&str, &[u8])]) {
        let segment = payload_segment();
        let col = &segment.columns[0];
        let kvs: Vec<KeyValue> = rows
            .iter()
            .map(|(k, v)| {
                let mut row = WriteRow::new(&segment, k);
                row.write_dynamic(col, v);
                row.into()
            })
            .collect();
        store.write(table, kvs).unwrap();
    }

    fn fetch(store: &RocksDBStore, table: &str, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let segment = payload_segment();
        let cols: Vec<&SegmentColumnSchema> = segment.columns.iter().collect();
        let builder = ReadBatchBuilder::new(&segment, cols, keys.len());
        let batch = store.read(table, keys, builder).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column is Utf8");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).as_bytes().to_vec())
                }
            })
            .collect()
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn round_trip(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        store.create_table("users", &schema("id")).unwrap();

        let keys: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        put(
            &mut store,
            "users",
            &[
                ("alice", b"a-payload"),
                ("bob", b"b-payload"),
                ("carol", b"c-payload"),
            ],
        );

        let got = fetch(&store, "users", &keys);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].as_deref(), Some(&b"a-payload"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"b-payload"[..]));
        assert_eq!(got[2].as_deref(), Some(&b"c-payload"[..]));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn read_preserves_caller_key_order(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        store.create_table("users", &schema("id")).unwrap();

        put(
            &mut store,
            "users",
            &[("alice", b"a"), ("bob", b"b"), ("carol", b"c"), ("dave", b"d")],
        );

        // Mix sorted/unsorted keys, including a miss in the middle.
        let lookup: [&[u8]; 5] = [b"dave", b"alice", b"zzz", b"carol", b"bob"];
        let got = fetch(&store, "users", &lookup);
        assert_eq!(got.len(), 5);
        assert_eq!(got[0].as_deref(), Some(&b"d"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"a"[..]));
        assert_eq!(got[2], None);
        assert_eq!(got[3].as_deref(), Some(&b"c"[..]));
        assert_eq!(got[4].as_deref(), Some(&b"b"[..]));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn missing_key_yields_none(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        store.create_table("users", &schema("id")).unwrap();

        put(
            &mut store,
            "users",
            &[("alice", b"a-payload"), ("carol", b"c-payload")],
        );

        let lookup: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        let got = fetch(&store, "users", &lookup);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].as_deref(), Some(&b"a-payload"[..]));
        assert_eq!(got[1], None);
        assert_eq!(got[2].as_deref(), Some(&b"c-payload"[..]));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn reopen_persists_data(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        {
            let mut store = open(dir.path());
            store.create_table("users", &schema("id")).unwrap();
            put(&mut store, "users", &[("alice", b"v1"), ("bob", b"v2")]);
        }

        let store = open(dir.path());
        let lookup: [&[u8]; 2] = [b"alice", b"bob"];
        let got = fetch(&store, "users", &lookup);
        assert_eq!(got[0].as_deref(), Some(&b"v1"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"v2"[..]));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn write_to_unknown_table_fails(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        let err = store
            .write("nope", [KeyValue::new(*b"x", *b"y")])
            .unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn create_duplicate_table_fails(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        store.create_table("users", &schema("id")).unwrap();
        let err = store.create_table("users", &schema("id")).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn created_table_persists_after_reopen(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        {
            let mut store = open(dir.path());
            store.create_table("users", &schema("id")).unwrap();
        }

        let mut store = open(dir.path());
        let err = store.create_table("users", &schema("id")).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));

        store
            .write("users", [KeyValue::new(*b"alice", *b"v1")])
            .unwrap();
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn manifest_persists_across_reopen(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let users = schema("id");
        let products = schema("sku");
        {
            let mut store = open(dir.path());
            store.create_table("users", &users).unwrap();
            store.create_table("products", &products).unwrap();
        }

        let store = open(dir.path());
        assert_eq!(store.manifest().schema("users"), Some(&users));
        assert_eq!(store.manifest().schema("products"), Some(&products));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn compact_after_write(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let mut store = open(dir.path());
        store.create_table("users", &schema("id")).unwrap();
        put(
            &mut store,
            "users",
            &[("alice", b"a"), ("bob", b"b"), ("carol", b"c")],
        );

        store.compact("users").unwrap();

        let lookup: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        let got = fetch(&store, "users", &lookup);
        assert_eq!(got[0].as_deref(), Some(&b"a"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"b"[..]));
        assert_eq!(got[2].as_deref(), Some(&b"c"[..]));
    }

    #[rstest]
    #[case::plain(open_plain)]
    #[case::block(open_block)]
    fn compact_unknown_table_fails(#[case] open: Opener) {
        let dir = TempDir::new().unwrap();
        let store = open(dir.path());
        let err = store.compact("nope").unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }
}
