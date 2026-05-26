use std::collections::HashMap;

use arrow::array::RecordBatch;

use crate::core::{MurrError, TableSchema};
use crate::io::row::read::ReadBatchBuilder;
use crate::io::store::{KeyValue, Manifest, Store};

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

impl Store for MemoryStore {
    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError> {
        self.manifest.add_table(table, schema)?;
        self.tables.insert(table.to_string(), HashMap::new());
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        keys: &[&[u8]],
        mut builder: ReadBatchBuilder<'_>,
    ) -> Result<RecordBatch, MurrError> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| MurrError::TableNotFound(table.to_string()))?;
        for k in keys {
            match rows.get(*k) {
                Some(v) => builder.add_row(v.as_slice())?,
                None => builder.add_empty()?,
            }
        }
        builder.build()
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
    use crate::core::{ColumnSchema, DTypeName};
    use crate::io::store::test_util::{fetch, put};
    use indexmap::IndexMap;

    fn schema() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "payload".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: true,
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

    #[test]
    fn missing_key_yields_none() {
        let mut store = MemoryStore::new();
        store.create_table("users", &schema()).unwrap();

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
