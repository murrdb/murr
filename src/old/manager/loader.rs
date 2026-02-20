use std::path::Path;
use std::sync::Arc;

use log::info;

use crate::conf::TableConfig;
use crate::core::MurrError;
use crate::old::discovery::{Discovery, DiscoveryKind, DiscoveryResult};
use crate::old::parquet::convert_parquet_to_ipc;
use crate::table::Table;

use super::state::TableState;

/// Orchestrates the discovery -> convert -> load pipeline for a single table.
pub struct TableLoader {
    name: String,
    config: TableConfig,
    discovery: DiscoveryKind,
}

impl TableLoader {
    /// Create a new TableLoader for a given table configuration.
    pub fn new(name: String, config: TableConfig) -> Result<Self, MurrError> {
        let discovery = DiscoveryKind::new(&config.source)?;
        Ok(Self {
            name,
            config,
            discovery,
        })
    }

    /// Discover the latest valid partition.
    pub async fn discover(&self) -> Result<DiscoveryResult, MurrError> {
        self.discovery.discover().await
    }

    /// Load the table from a previously discovered partition.
    ///
    /// # Arguments
    /// * `discovery_result` - The result from a prior `discover()` call
    /// * `output_dir` - Directory where the IPC file should be written
    pub async fn load(
        &self,
        discovery_result: DiscoveryResult,
        output_dir: &Path,
    ) -> Result<TableState, MurrError> {
        info!(
            "Table '{}': loading partition {}",
            self.name, discovery_result.partition_date
        );

        // 1. Convert parquet to Arrow IPC
        let ipc_path = output_dir.join(format!("{}.ipc", self.name));
        convert_parquet_to_ipc(
            discovery_result.store,
            &discovery_result.parquet_paths,
            &ipc_path,
            &self.config,
        )
        .await?;

        // 2. Load the Table (using first key column)
        let key_column = self.config.key.first().ok_or_else(|| {
            MurrError::ConfigParsingError("Table config must have at least one key column".into())
        })?;

        let table = Table::open(&ipc_path, key_column)?;

        // 3. Create and return TableState
        Ok(TableState {
            table: Arc::new(table),
            partition_date: discovery_result.partition_date,
            ipc_path,
        })
    }

    /// Get the poll interval from the table config.
    pub fn poll_interval(&self) -> std::time::Duration {
        self.config.poll_interval
    }
}
