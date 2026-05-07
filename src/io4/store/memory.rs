use std::collections::HashMap;

use crate::core::MurrError;
use crate::io4::store::{ReadResult, Store};

#[derive(Default)]
pub struct MemoryStore {
    pub tables: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct MemoryReadResult<'a> {
    pub values: Vec<Option<&'a [u8]>>,
}

impl ReadResult for MemoryReadResult<'_> {
    fn bytes(&self) -> impl Iterator<Item = Result<Option<&[u8]>, MurrError>> {
        self.values.iter().map(|v| Ok(*v))
    }
}

impl Store for MemoryStore {
    type R<'a> = MemoryReadResult<'a>;

    fn create_table(&mut self, table: &str) -> Result<(), MurrError> {
        if self.tables.contains_key(table) {
            return Err(MurrError::TableAlreadyExists(table.to_string()));
        }
        self.tables.insert(table.to_string(), HashMap::new());
        Ok(())
    }

    fn read<'a>(
        &'a self,
        table: &str,
        keys: &[&[u8]],
    ) -> Result<Self::R<'a>, MurrError> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        let values = keys
            .iter()
            .map(|k| rows.get(*k).map(|v| v.as_slice()))
            .collect();
        Ok(MemoryReadResult { values })
    }

    fn write<'k, 'v>(
        &mut self,
        table: &str,
        keys: impl Iterator<Item = &'k [u8]>,
        values: impl Iterator<Item = &'v [u8]>,
    ) -> Result<(), MurrError> {
        let rows = self
            .tables
            .get_mut(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        for (k, v) in keys.zip(values) {
            rows.insert(k.to_vec(), v.to_vec());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut store = MemoryStore::new();
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
        let mut store = MemoryStore::new();
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
    fn write_to_unknown_table_fails() {
        let mut store = MemoryStore::new();
        let keys: [&[u8]; 1] = [b"x"];
        let vals: [&[u8]; 1] = [b"y"];
        let err = store
            .write("nope", keys.iter().copied(), vals.iter().copied())
            .unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[test]
    fn create_duplicate_table_fails() {
        let mut store = MemoryStore::new();
        store.create_table("users").unwrap();
        let err = store.create_table("users").unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }
}
