use std::path::Path;

use rocksdb::{
    DB, KeyEncodingType, MemtableFactory, Options, PlainTableFactoryOptions, ReadOptions,
    SliceTransform, WriteBatch, WriteOptions,
};
use serde::Deserialize;

use crate::core::MurrError;
use crate::io4::store::{Store, rocksdb::MultiGetResult};

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
}

impl Default for PlainConfig {
    fn default() -> Self {
        Self {
            bloom_bits_per_key: default_bloom_bits(),
            hash_table_ratio: default_data_block_hash_ratio(),
            index_sparseness: default_index_sparseness(),
            store_index_in_file: false,
            huge_page_tlb_size: 0,
        }
    }
}

fn default_bloom_bits() -> i32 {
    10
}
fn default_index_sparseness() -> usize {
    16
}
fn default_data_block_hash_ratio() -> f64 {
    0.75
}

pub struct PlainRocksDBStore {
    db: DB,
    cf_opts: Options,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}

impl PlainRocksDBStore {
    pub fn open(path: &Path, config: &PlainConfig) -> Result<Self, MurrError> {
        let cf_opts: Options = config.into();
        let cfs = DB::list_cf(&cf_opts, path).unwrap_or_default();
        let db = DB::open_cf(&cf_opts, path, cfs)?;
        Ok(Self {
            db,
            cf_opts,
            write_opts: WriteOptions::default(),
            read_opts: ReadOptions::default(),
        })
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

    fn create_table(&mut self, table: &str) -> Result<(), MurrError> {
        if self.db.cf_handle(table).is_some() {
            return Err(MurrError::TableAlreadyExists(table.to_string()));
        }
        self.db.create_cf(table, &self.cf_opts)?;
        Ok(())
    }

    fn write<'k, 'v>(
        &mut self,
        table: &str,
        keys: impl Iterator<Item = &'k [u8]>,
        values: impl Iterator<Item = &'v [u8]>,
    ) -> Result<(), MurrError> {
        let cf = self
            .db
            .cf_handle(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        let mut batch = WriteBatch::default();
        for (k, v) in keys.zip(values) {
            batch.put_cf(cf, k, v);
        }
        self.db.write_opt(batch, &self.write_opts)?;
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
    use crate::io4::store::ReadResult;
    use tempfile::TempDir;

    fn open_store(dir: &TempDir) -> PlainRocksDBStore {
        PlainRocksDBStore::open(dir.path(), &PlainConfig::default()).expect("open")
    }

    #[test]
    fn round_trip() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users").unwrap();

        let keys: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        let vals: [&[u8]; 3] = [b"a-payload", b"b-payload", b"c-payload"];
        store
            .write("users", keys.iter().copied(), vals.iter().copied())
            .unwrap();

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
    fn missing_key_yields_none() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users").unwrap();

        let written_keys: [&[u8]; 2] = [b"alice", b"carol"];
        let vals: [&[u8]; 2] = [b"a-payload", b"c-payload"];
        store
            .write("users", written_keys.iter().copied(), vals.iter().copied())
            .unwrap();

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
            store.create_table("users").unwrap();
            let keys: [&[u8]; 2] = [b"alice", b"bob"];
            let vals: [&[u8]; 2] = [b"v1", b"v2"];
            store
                .write("users", keys.iter().copied(), vals.iter().copied())
                .unwrap();
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
        let keys: [&[u8]; 1] = [b"x"];
        let vals: [&[u8]; 1] = [b"y"];
        let err = store
            .write("nope", keys.iter().copied(), vals.iter().copied())
            .unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[test]
    fn create_duplicate_table_fails() {
        let dir = TempDir::new().unwrap();
        let mut store = open_store(&dir);
        store.create_table("users").unwrap();
        let err = store.create_table("users").unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }

    #[test]
    fn created_table_persists_after_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let mut store = open_store(&dir);
            store.create_table("users").unwrap();
        }

        let mut store = open_store(&dir);
        let err = store.create_table("users").unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));

        let keys: [&[u8]; 1] = [b"alice"];
        let vals: [&[u8]; 1] = [b"v1"];
        store
            .write("users", keys.iter().copied(), vals.iter().copied())
            .unwrap();
    }
}
