use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::table::Table;

use super::state::TableState;

/// Central registry for all loaded tables.
pub struct TableManager {
    tables: RwLock<HashMap<String, TableState>>,
    data_dir: PathBuf,
}

impl TableManager {
    /// Create a new TableManager with the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            data_dir,
        }
    }

    /// Get a read-only reference to a table by name.
    pub async fn get(&self, name: &str) -> Option<Arc<Table>> {
        let tables = self.tables.read().await;
        tables.get(name).map(|state| Arc::clone(&state.table))
    }

    /// Insert or update a table state.
    pub async fn insert(&self, name: String, state: TableState) {
        let mut tables = self.tables.write().await;
        tables.insert(name, state);
    }

    /// Get the current partition date for a table (for change detection).
    pub async fn current_partition(&self, name: &str) -> Option<String> {
        let tables = self.tables.read().await;
        tables.get(name).map(|state| state.partition_date.clone())
    }

    /// Get the path for a table's IPC file: data_dir/<table_name>.ipc
    pub fn ipc_path(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.ipc", name))
    }
}
