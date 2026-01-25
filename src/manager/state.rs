use std::path::PathBuf;
use std::sync::Arc;

use crate::table::Table;

/// Represents the current state of a loaded table.
pub struct TableState {
    /// The loaded Table wrapped in Arc for thread-safe sharing
    pub table: Arc<Table>,
    /// The partition date string (e.g., "2024-01-14") for detecting new partitions
    pub partition_date: String,
    /// The path to the IPC file on disk
    pub ipc_path: PathBuf,
}
