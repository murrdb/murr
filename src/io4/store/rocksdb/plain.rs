use std::path::{Path, PathBuf};

use itertools::Itertools;
use rocksdb::{
    DB, KeyEncodingType, MemtableFactory, Options, PlainTableFactoryOptions, ReadOptions,
    SliceTransform, WriteBatch, WriteOptions,
};
use serde::Deserialize;

use crate::core::{MurrError, TableSchema};
use crate::io4::store::{Manifest, Store, rocksdb::MultiGetResult};

const MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone, Deserialize)]
pub struct PlainConfig {
    #[serde(default = "default_bloom_bits")]
    pub bloom_bits_per_key: i32,
    /// PlainTable: hash table utilization ratio (lower = fewer collisions, more space).
    #[serde(default = "default_data_block_hash_ratio")]
    pub hash_table_ratio: f64,
    /// PlainTable: stride between sparse-index entries (lower = faster lookup, more memory).
    #[serde(default = "default_index_sparseness")]
    pub index_sparseness: usize,
    /// PlainTable: store the full index in the file instead of rebuilding at open.
    #[serde(default)]
    pub store_index_in_file: bool,
    #[serde(default)]
    pub huge_page_tlb_size: usize,
    /// Rows per WriteBatch before forcing a flush. Each chunk produces one
    /// PlainTable SST, which is hard-capped at 2 GiB by the format — too many
    /// rows in a single batch overflows that limit at flush time.
    #[serde(default = "default_write_batch_size")]
    pub write_batch_size: usize,
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    #[serde(default = "default_target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default_disable_auto_compactions")]
    pub disable_auto_compactions: bool,
}

impl Default for PlainConfig {
    fn default() -> Self {
        Self {
            bloom_bits_per_key: default_bloom_bits(),
            hash_table_ratio: default_data_block_hash_ratio(),
            index_sparseness: default_index_sparseness(),
            store_index_in_file: false,
            huge_page_tlb_size: 0,
            write_batch_size: default_write_batch_size(),
            write_buffer_size: default_write_buffer_size(),
            target_file_size_base: default_target_file_size_base(),
            disable_auto_compactions: default_disable_auto_compactions(),
        }
    }
}

fn default_bloom_bits() -> i32 {
    16
}
fn default_index_sparseness() -> usize {
    4
}
fn default_data_block_hash_ratio() -> f64 {
    0.75
}
fn default_write_batch_size() -> usize {
    5 * 1024 * 1024
}
fn default_write_buffer_size() -> usize {
    256 * 1024 * 1024
}
fn default_target_file_size_base() -> u64 {
    1024 * 1024 * 1024
}
fn default_disable_auto_compactions() -> bool {
    false
}

pub struct PlainRocksDBStore {
    db: DB,
    cf_opts: Options,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
    path: PathBuf,
    manifest: Manifest,
    config: PlainConfig,
}

impl PlainRocksDBStore {
    pub fn open(path: &Path, config: &PlainConfig) -> Result<Self, MurrError> {
        let cf_opts: Options = config.into();
        let cfs = DB::list_cf(&cf_opts, path).unwrap_or_default();
        let db = DB::open_cf(&cf_opts, path, cfs)?;
        let manifest = Manifest::from_file(&path.join(MANIFEST_FILE))?;
        Ok(Self {
            db,
            cf_opts,
            write_opts: WriteOptions::default(),
            read_opts: ReadOptions::default(),
            path: path.to_path_buf(),
            manifest,
            config: config.clone(),
        })
    }

    fn manifest_path(&self) -> PathBuf {
        self.path.join(MANIFEST_FILE)
    }

    /// Block-compact a table's column family into a stable post-compaction
    /// shape. Useful after a bulk write to collapse many small L0 SSTs into
    /// a few larger SSTs before reads start.
    pub fn compact(&self, table: &str) -> Result<(), MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}

impl Into<Options> for &PlainConfig {
    fn into(self) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_allow_mmap_reads(true);
        opts.set_prefix_extractor(SliceTransform::create_noop());
        opts.set_memtable_factory(MemtableFactory::Vector);
        opts.set_write_buffer_size(self.write_buffer_size);
        opts.set_target_file_size_base(self.target_file_size_base);
        opts.set_disable_auto_compactions(self.disable_auto_compactions);
        opts.set_plain_table_factory(&PlainTableFactoryOptions {
            user_key_length: 0,
            bloom_bits_per_key: self.bloom_bits_per_key,
            hash_table_ratio: self.hash_table_ratio,
            index_sparseness: self.index_sparseness,
            huge_page_tlb_size: self.huge_page_tlb_size,
            encoding_type: KeyEncodingType::Plain,
            full_scan_mode: false,
            store_index_in_file: self.store_index_in_file,
        });
        opts
    }
}

impl Store for PlainRocksDBStore {
    type R<'a> = MultiGetResult<'a>;

    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError> {
        self.manifest.add_table(table, schema)?;
        self.db.create_cf(table, &self.cf_opts)?;
        self.manifest.to_file(&self.manifest_path())?;
        Ok(())
    }

    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn write<'a>(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Result<(), MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;

        for chunk in &rows.into_iter().chunks(self.config.write_batch_size) {
            let mut batch = WriteBatch::default();
            for (k, v) in chunk {
                batch.put_cf(cf, k, v);
            }
            self.db.write_opt(batch, &self.write_opts)?;
            self.db.flush_cf(cf)?;
        }
        Ok(())
    }

    fn read<'a>(&'a self, table: &str, keys: &[&[u8]]) -> Result<MultiGetResult<'a>, MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;

        let values = self
            .db
            .batched_multi_get_cf_opt(cf, keys, false, &self.read_opts);
        Ok(MultiGetResult { values })
    }
}

#[cfg(all(test, feature = "testutil"))]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};
    use crate::io4::store::ReadResult;
    use indexmap::IndexMap;
    use tempfile::TempDir;

    fn open_store(dir: &TempDir) -> PlainRocksDBStore {
        PlainRocksDBStore::open(dir.path(), &PlainConfig::default()).expect("open")
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
        TableSchema {
            key: key.to_string(),
            columns,
        }
    }

    #[test]
    fn round_trip() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users", &schema("id")).unwrap();

        let keys: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        let rows: [(&[u8], &[u8]); 3] = [
            (b"alice", b"a-payload"),
            (b"bob", b"b-payload"),
            (b"carol", b"c-payload"),
        ];
        store.write("users", rows.iter().copied()).unwrap();

        let result = store.read("users", &keys).unwrap();
        let got: Vec<Option<Vec<u8>>> = result
            .bytes()
            .map(|r| r.unwrap().map(|b| b.to_vec()))
            .collect();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].as_deref(), Some(&b"a-payload"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"b-payload"[..]));
        assert_eq!(got[2].as_deref(), Some(&b"c-payload"[..]));
    }

    #[test]
    fn read_preserves_caller_key_order() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users", &schema("id")).unwrap();

        let rows: [(&[u8], &[u8]); 4] = [
            (b"alice", b"a"),
            (b"bob", b"b"),
            (b"carol", b"c"),
            (b"dave", b"d"),
        ];
        store.write("users", rows.iter().copied()).unwrap();

        // Mix sorted/unsorted keys, including a miss in the middle.
        let lookup: [&[u8]; 5] = [b"dave", b"alice", b"zzz", b"carol", b"bob"];
        let result = store.read("users", &lookup).unwrap();
        let got: Vec<Option<Vec<u8>>> = result
            .bytes()
            .map(|r| r.unwrap().map(|b| b.to_vec()))
            .collect();
        assert_eq!(got.len(), 5);
        assert_eq!(got[0].as_deref(), Some(&b"d"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"a"[..]));
        assert_eq!(got[2], None);
        assert_eq!(got[3].as_deref(), Some(&b"c"[..]));
        assert_eq!(got[4].as_deref(), Some(&b"b"[..]));
    }

    #[test]
    fn missing_key_yields_none() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users", &schema("id")).unwrap();

        let written: [(&[u8], &[u8]); 2] = [(b"alice", b"a-payload"), (b"carol", b"c-payload")];
        store.write("users", written.iter().copied()).unwrap();

        let lookup: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        let result = store.read("users", &lookup).unwrap();
        let got: Vec<Option<Vec<u8>>> = result
            .bytes()
            .map(|r| r.unwrap().map(|b| b.to_vec()))
            .collect();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].as_deref(), Some(&b"a-payload"[..]));
        assert_eq!(got[1], None);
        assert_eq!(got[2].as_deref(), Some(&b"c-payload"[..]));
    }

    #[test]
    fn reopen_persists_data() {
        let dir = TempDir::new().unwrap();
        {
            let mut store = open_store(&dir);
            store.create_table("users", &schema("id")).unwrap();
            let rows: [(&[u8], &[u8]); 2] = [(b"alice", b"v1"), (b"bob", b"v2")];
            store.write("users", rows.iter().copied()).unwrap();
        }

        let store = open_store(&dir);
        let lookup: [&[u8]; 2] = [b"alice", b"bob"];
        let result = store.read("users", &lookup).unwrap();
        let got: Vec<Option<Vec<u8>>> = result
            .bytes()
            .map(|r| r.unwrap().map(|b| b.to_vec()))
            .collect();
        assert_eq!(got[0].as_deref(), Some(&b"v1"[..]));
        assert_eq!(got[1].as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn write_to_unknown_table_fails() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        let rows: [(&[u8], &[u8]); 1] = [(b"x", b"y")];
        let err = store.write("nope", rows.iter().copied()).unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[test]
    fn create_duplicate_table_fails() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users", &schema("id")).unwrap();
        let err = store.create_table("users", &schema("id")).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }

    #[test]
    fn created_table_persists_after_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let mut store = open_store(&dir);
            store.create_table("users", &schema("id")).unwrap();
        }

        let mut store = open_store(&dir);
        let err = store.create_table("users", &schema("id")).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));

        let rows: [(&[u8], &[u8]); 1] = [(b"alice", b"v1")];
        store.write("users", rows.iter().copied()).unwrap();
    }

    #[test]
    fn manifest_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let users = schema("id");
        let products = schema("sku");
        {
            let mut store = open_store(&dir);
            store.create_table("users", &users).unwrap();
            store.create_table("products", &products).unwrap();
        }

        let store = open_store(&dir);
        assert_eq!(store.manifest().schema("users"), Some(&users));
        assert_eq!(store.manifest().schema("products"), Some(&products));
    }
}
