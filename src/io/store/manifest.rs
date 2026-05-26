use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::core::{MurrError, TableSchema};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub version: u64,
    pub updated_at: u64,
    pub tables: HashMap<String, TableSchema>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: 1,
            updated_at: now_secs(),
            tables: HashMap::new(),
        }
    }
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_file(path: &Path) -> Result<Self, MurrError> {
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| MurrError::IoError(format!("manifest parse: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(MurrError::IoError(e.to_string())),
        }
    }

    pub fn to_file(&self, path: &Path) -> Result<(), MurrError> {
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| MurrError::IoError(format!("manifest serialize: {e}")))?;
        let tmp = match path.extension() {
            Some(ext) => {
                let mut s = ext.to_os_string();
                s.push(".tmp");
                path.with_extension(s)
            }
            None => path.with_extension("tmp"),
        };
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn add_table(&mut self, name: &str, schema: &TableSchema) -> Result<(), MurrError> {
        if self.tables.contains_key(name) {
            return Err(MurrError::TableAlreadyExists(name.to_string()));
        }
        self.tables.insert(name.to_string(), schema.clone());
        self.updated_at = now_secs();
        Ok(())
    }

    pub fn del_table(&mut self, name: &str) -> Result<(), MurrError> {
        if self.tables.remove(name).is_none() {
            return Err(MurrError::TableNotFound(name.to_string()));
        }
        self.updated_at = now_secs();
        Ok(())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn schema(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(all(test, feature = "testutil"))]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DTypeName, TableSchema};
    use indexmap::IndexMap;
    use tempfile::TempDir;

    fn schema_id_score() -> TableSchema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".into(),
            ColumnSchema {
                dtype: DTypeName::Float32,
                nullable: true,
            },
        );
        TableSchema {
            key: "id".into(),
            columns,
        }
    }

    #[test]
    fn round_trip_through_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");

        let mut m = Manifest::new();
        m.add_table("users", &schema_id_score()).unwrap();
        m.to_file(&path).unwrap();

        let loaded = Manifest::from_file(&path).unwrap();
        assert_eq!(loaded.tables.len(), 1);
        assert_eq!(loaded.schema("users"), Some(&schema_id_score()));
    }

    #[test]
    fn from_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.json");
        let m = Manifest::from_file(&path).unwrap();
        assert!(m.tables.is_empty());
    }

    #[test]
    fn add_duplicate_errors() {
        let mut m = Manifest::new();
        m.add_table("t", &schema_id_score()).unwrap();
        let err = m.add_table("t", &schema_id_score()).unwrap_err();
        assert!(matches!(err, MurrError::TableAlreadyExists(_)));
    }

    #[test]
    fn del_missing_errors() {
        let mut m = Manifest::new();
        let err = m.del_table("nope").unwrap_err();
        assert!(matches!(err, MurrError::TableNotFound(_)));
    }

    #[test]
    fn add_then_del() {
        let mut m = Manifest::new();
        m.add_table("t", &schema_id_score()).unwrap();
        assert!(m.contains("t"));
        m.del_table("t").unwrap();
        assert!(!m.contains("t"));
    }
}
