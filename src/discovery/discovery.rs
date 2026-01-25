use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use tokio_stream::StreamExt;

use crate::conf::SourceConfig;
use crate::core::MurrError;

use super::partition::{filter_parquet_files, find_date_partitions, success_marker_path};
use super::store::{create_local_store, create_s3_store};

/// Result of a successful discovery operation.
pub struct DiscoveryResult {
    /// The ObjectStore configured for the source
    pub store: Arc<dyn ObjectStore>,
    /// List of Parquet file paths within the latest valid partition
    pub parquet_paths: Vec<ObjectPath>,
    /// The partition date that was discovered (e.g., "2024-01-14")
    pub partition_date: String,
}

/// Trait for discovering Parquet files from different storage backends.
#[async_trait]
pub trait Discovery: Send + Sync {
    /// Discover the latest valid partition and return its Parquet files.
    ///
    /// A valid partition is defined as a date directory (YYYY-MM-DD) that:
    /// 1. Contains a `_SUCCESS` marker file
    /// 2. Contains at least one `.parquet` file
    async fn discover(&self) -> Result<DiscoveryResult, MurrError>;

    /// Get the underlying ObjectStore (useful for reloads).
    fn store(&self) -> Arc<dyn ObjectStore>;
}

/// Enum dispatch for Discovery implementations.
/// This allows static dispatch while supporting extensibility.
pub enum DiscoveryKind {
    /// Uses object_store trait - works for both LocalFileSystem and S3
    ObjectStore(ObjectStoreDiscovery),
    // Future: Iceberg(IcebergDiscovery),
}

impl DiscoveryKind {
    /// Create a Discovery implementation from a SourceConfig.
    pub fn new(source: &SourceConfig) -> Result<Self, MurrError> {
        let discovery = ObjectStoreDiscovery::new(source)?;
        Ok(DiscoveryKind::ObjectStore(discovery))
    }
}

#[async_trait]
impl Discovery for DiscoveryKind {
    async fn discover(&self) -> Result<DiscoveryResult, MurrError> {
        match self {
            DiscoveryKind::ObjectStore(d) => d.discover().await,
        }
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        match self {
            DiscoveryKind::ObjectStore(d) => d.store(),
        }
    }
}

/// Discovery implementation using object_store trait.
/// Works with both LocalFileSystem and S3.
pub struct ObjectStoreDiscovery {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreDiscovery {
    /// Create a new ObjectStoreDiscovery from a SourceConfig.
    pub fn new(source: &SourceConfig) -> Result<Self, MurrError> {
        match source {
            SourceConfig::Local(config) => {
                let store = create_local_store(config)?;
                // LocalFileSystem uses paths without leading slash, so we strip it
                let prefix = config.path.strip_prefix('/').unwrap_or(&config.path);
                Ok(Self {
                    store,
                    prefix: prefix.to_string(),
                })
            }
            SourceConfig::S3(config) => {
                let store = create_s3_store(config)?;
                Ok(Self {
                    store,
                    prefix: config.prefix.clone(),
                })
            }
        }
    }

    /// Create from an existing store and prefix (useful for testing).
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// List all objects under the prefix.
    async fn list_all(&self) -> Result<Vec<ObjectPath>, MurrError> {
        let prefix_path = if self.prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(self.prefix.clone()))
        };

        let mut paths = Vec::new();
        let mut stream = self.store.list(prefix_path.as_ref());

        while let Some(result) = stream.next().await {
            let meta = result?;
            paths.push(meta.location);
        }

        Ok(paths)
    }

    /// Check if a _SUCCESS marker exists for a partition.
    async fn has_success_marker(&self, partition_path: &ObjectPath) -> bool {
        let marker = success_marker_path(partition_path);
        self.store.head(&marker).await.is_ok()
    }
}

#[async_trait]
impl Discovery for ObjectStoreDiscovery {
    async fn discover(&self) -> Result<DiscoveryResult, MurrError> {
        log::info!("Starting discovery for prefix: '{}'", self.prefix);

        // 1. List all objects
        let all_paths = self.list_all().await?;

        if all_paths.is_empty() {
            return Err(MurrError::NoValidPartition(format!(
                "No objects found under prefix '{}'",
                self.prefix
            )));
        }

        // 2. Find date partitions (sorted descending)
        let partitions = find_date_partitions(all_paths.iter().cloned(), &self.prefix);

        if partitions.is_empty() {
            return Err(MurrError::NoValidPartition(
                "No date partitions (YYYY-MM-DD) found".to_string(),
            ));
        }

        log::debug!("Found {} date partitions", partitions.len());

        // 3. Find latest partition with _SUCCESS marker
        let mut valid_partition = None;
        for partition in &partitions {
            if self.has_success_marker(&partition.path).await {
                valid_partition = Some(partition.clone());
                break;
            }
            log::debug!(
                "Partition {} missing _SUCCESS marker, skipping",
                partition.date
            );
        }

        let valid_partition = valid_partition.ok_or_else(|| {
            MurrError::NoValidPartition("No partition found with _SUCCESS marker".to_string())
        })?;

        log::info!(
            "Selected partition: {} ({})",
            valid_partition.date,
            valid_partition.path
        );

        // 4. List parquet files in the valid partition
        let mut partition_paths = Vec::new();
        let mut stream = self.store.list(Some(&valid_partition.path));

        while let Some(result) = stream.next().await {
            let meta = result?;
            partition_paths.push(meta.location);
        }

        let parquet_paths = filter_parquet_files(partition_paths, &valid_partition.path);

        if parquet_paths.is_empty() {
            return Err(MurrError::NoValidPartition(format!(
                "No .parquet files found in partition {}",
                valid_partition.date
            )));
        }

        log::info!(
            "Discovered {} parquet files in partition {}",
            parquet_paths.len(),
            valid_partition.date
        );

        Ok(DiscoveryResult {
            store: self.store.clone(),
            parquet_paths,
            partition_date: valid_partition.date.to_string(),
        })
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::fs::{self, File};
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_structure(dir: &Path) {
        // 2024-01-13 - older complete partition
        let p1 = dir.join("2024-01-13");
        fs::create_dir_all(&p1).unwrap();
        File::create(p1.join("part_0000.parquet")).unwrap();
        File::create(p1.join("_SUCCESS")).unwrap();

        // 2024-01-14 - latest complete partition (should be selected)
        let p2 = dir.join("2024-01-14");
        fs::create_dir_all(&p2).unwrap();
        File::create(p2.join("part_0000.parquet")).unwrap();
        File::create(p2.join("part_0001.parquet")).unwrap();
        File::create(p2.join("_SUCCESS")).unwrap();

        // 2024-01-15 - incomplete partition (no _SUCCESS)
        let p3 = dir.join("2024-01-15");
        fs::create_dir_all(&p3).unwrap();
        File::create(p3.join("part_0000.parquet")).unwrap();
    }

    #[tokio::test]
    async fn test_discover_selects_latest_with_success() {
        let dir = TempDir::new().unwrap();
        create_test_structure(dir.path());

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        // LocalFileSystem uses paths without leading slash
        let prefix = dir
            .path()
            .to_string_lossy()
            .strip_prefix('/')
            .unwrap_or(&dir.path().to_string_lossy())
            .to_string();
        let discovery = ObjectStoreDiscovery::with_store(store, prefix);

        let result = discovery.discover().await.unwrap();

        assert_eq!(result.partition_date, "2024-01-14");
        assert_eq!(result.parquet_paths.len(), 2);
    }

    #[tokio::test]
    async fn test_discover_skips_incomplete_partition() {
        let dir = TempDir::new().unwrap();

        // Only create incomplete partition
        let p = dir.path().join("2024-01-15");
        fs::create_dir_all(&p).unwrap();
        File::create(p.join("part_0000.parquet")).unwrap();
        // No _SUCCESS

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = dir
            .path()
            .to_string_lossy()
            .strip_prefix('/')
            .unwrap_or(&dir.path().to_string_lossy())
            .to_string();
        let discovery = ObjectStoreDiscovery::with_store(store, prefix);

        let result = discovery.discover().await;

        assert!(matches!(result, Err(MurrError::NoValidPartition(_))));
    }

    #[tokio::test]
    async fn test_discover_empty_directory() {
        let dir = TempDir::new().unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = dir
            .path()
            .to_string_lossy()
            .strip_prefix('/')
            .unwrap_or(&dir.path().to_string_lossy())
            .to_string();
        let discovery = ObjectStoreDiscovery::with_store(store, prefix);

        let result = discovery.discover().await;

        assert!(matches!(result, Err(MurrError::NoValidPartition(_))));
    }

    #[tokio::test]
    async fn test_discover_no_date_partitions() {
        let dir = TempDir::new().unwrap();

        // Create non-date directories
        let p = dir.path().join("random");
        fs::create_dir_all(&p).unwrap();
        File::create(p.join("file.parquet")).unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = dir
            .path()
            .to_string_lossy()
            .strip_prefix('/')
            .unwrap_or(&dir.path().to_string_lossy())
            .to_string();
        let discovery = ObjectStoreDiscovery::with_store(store, prefix);

        let result = discovery.discover().await;

        assert!(matches!(result, Err(MurrError::NoValidPartition(_))));
    }

    #[tokio::test]
    async fn test_discover_success_but_no_parquet_files() {
        let dir = TempDir::new().unwrap();

        let p = dir.path().join("2024-01-14");
        fs::create_dir_all(&p).unwrap();
        File::create(p.join("_SUCCESS")).unwrap();
        // No parquet files

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = dir
            .path()
            .to_string_lossy()
            .strip_prefix('/')
            .unwrap_or(&dir.path().to_string_lossy())
            .to_string();
        let discovery = ObjectStoreDiscovery::with_store(store, prefix);

        let result = discovery.discover().await;

        assert!(matches!(result, Err(MurrError::NoValidPartition(_))));
    }

    #[tokio::test]
    async fn test_discovery_kind_new() {
        let dir = TempDir::new().unwrap();
        create_test_structure(dir.path());

        let config = crate::conf::LocalSourceConfig {
            path: dir.path().to_string_lossy().to_string(),
        };
        let source = crate::conf::SourceConfig::Local(config);

        let discovery = DiscoveryKind::new(&source).unwrap();
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.partition_date, "2024-01-14");
    }
}
