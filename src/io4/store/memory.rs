use std::collections::HashMap;

use crate::core::{MurrError, TableSchema};
use crate::io4::store::{KeyValue, Manifest, ReadResult, Store};

#[derive(Default)]
pub struct MemoryStore {
    pub tables: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
    manifest: Manifest,
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

    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError> {
        self.manifest.add_table(table, schema)?;
        self.tables.insert(table.to_string(), HashMap::new());
        Ok(())
    }

    fn read<'a>(&'a self, table: &str, keys: &[&[u8]]) -> Result<Self::R<'a>, MurrError> {
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

    fn write(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = KeyValue>,
    ) -> Result<(), MurrError> {
        let entries = self
            .tables
            .get_mut(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        for row in rows {
            entries.insert(row.key, row.value);
        }
        Ok(())
    }

    fn compact(&self, _table: &str) -> Result<(), MurrError> {
        Ok(())
    }

    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};
    use indexmap::IndexMap;

    fn schema() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        TableSchema {
            key: "id".into(),
            columns,
        }
    }

    #[test]
    fn round_trip() {
        let mut store = MemoryStore::new();
        store.create_table("users", &schema()).unwrap();

        let keys: [&[u8]; 3] = [b"alice", b"bob", b"carol"];
        store
            .write(
                "users",
                [
                    KeyValue::new(*b"alice", *b"a-payload"),
                    KeyValue::new(*b"bob", *b"b-payload"),
                    KeyValue::new(*b"carol", *b"c-payload"),
                ],
            )
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
        store.create_table("users", &schema()).unwrap();

        store
            .write(
                "users",
                [
                    KeyValue::new(*b"alice", *b"a-payload"),
                    KeyValue::new(*b"carol", *b"c-payload"),
                ],
            )
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
        let err = store
            .write("nope", [KeyValue::new(*b"x", *b"y")])
            .unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[test]
    fn create_duplicate_table_fails() {
        let mut store = MemoryStore::new();
        store.create_table("users", &schema()).unwrap();
        let err = store.create_table("users", &schema()).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }

    #[test]
    fn manifest_tracks_created_tables() {
        let mut store = MemoryStore::new();
        store.create_table("users", &schema()).unwrap();
        assert!(store.manifest().contains("users"));
        assert_eq!(store.manifest().schema("users"), Some(&schema()));
    }
}
