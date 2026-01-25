mod api;
mod conf;
mod core;
mod discovery;
mod manager;
mod parquet;
mod table;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use log::{error, info};
use tokio::time::interval;

use crate::conf::{Config, TableConfig};
use crate::core::{CliArgs, MurrError, setup_logging};
use crate::manager::{TableLoader, TableManager};

#[tokio::main]
async fn main() -> Result<(), MurrError> {
    setup_logging();
    info!("Murr started.");

    let args = CliArgs::parse();
    info!("Cli args: {args:?}");
    let config = Config::from_args(&args)?;

    // Ensure data_dir exists
    let data_dir: PathBuf = config.server.data_dir.clone().into();
    std::fs::create_dir_all(&data_dir)?;

    let manager = Arc::new(TableManager::new(data_dir.clone()));

    // Spawn discovery loop for each table
    let mut handles = Vec::new();
    for (name, table_config) in config.tables {
        let manager = Arc::clone(&manager);
        let data_dir = data_dir.clone();
        handles.push(tokio::spawn(run_discovery_loop(
            name,
            table_config,
            manager,
            data_dir,
        )));
    }

    // Wait for all (they run forever)
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn run_discovery_loop(
    name: String,
    config: TableConfig,
    manager: Arc<TableManager>,
    data_dir: PathBuf,
) {
    let poll_interval = config.poll_interval;

    let loader = match TableLoader::new(name.clone(), config) {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to create loader for '{}': {}", name, e);
            return;
        }
    };

    // Immediate first run
    match try_load(&loader, &manager, &name, &data_dir).await {
        Ok(true) => info!("Table '{}': initial load complete", name),
        Ok(false) => info!("Table '{}': no valid partition found", name),
        Err(e) => error!("Initial load failed for '{}': {}", name, e),
    }

    // Periodic polling
    let mut ticker = interval(poll_interval);
    ticker.tick().await; // Skip first tick (we already did initial load)

    loop {
        ticker.tick().await;
        match try_load(&loader, &manager, &name, &data_dir).await {
            Ok(true) => info!("Table '{}': reload complete", name),
            Ok(false) => info!("Table '{}': no new partition", name),
            Err(e) => error!("Discovery failed for '{}': {}", name, e),
        }
    }
}

/// Try to discover and load the table. Returns Ok(true) if loaded, Ok(false) if unchanged.
async fn try_load(
    loader: &TableLoader,
    manager: &TableManager,
    name: &str,
    data_dir: &PathBuf,
) -> Result<bool, MurrError> {
    // 1. Discover the latest partition
    let discovery_result = loader.discover().await?;

    // 2. Check if partition has changed
    let current_partition = manager.current_partition(name).await;
    if current_partition.as_ref() == Some(&discovery_result.partition_date) {
        return Ok(false);
    }

    // 3. Load the new partition
    let state = loader.load(discovery_result, data_dir).await?;
    info!(
        "Table '{}': loaded partition {}",
        name, state.partition_date
    );

    // 4. Insert into manager
    manager.insert(name.to_string(), state).await;

    Ok(true)
}
