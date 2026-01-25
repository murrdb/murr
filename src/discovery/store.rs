use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;

use crate::conf::{LocalSourceConfig, S3SourceConfig};
use crate::core::MurrError;

/// Creates a LocalFileSystem ObjectStore from LocalSourceConfig.
pub fn create_local_store(_config: &LocalSourceConfig) -> Result<Arc<dyn ObjectStore>, MurrError> {
    let store = LocalFileSystem::new();
    Ok(Arc::new(store))
}

/// Creates an S3 ObjectStore from S3SourceConfig.
pub fn create_s3_store(config: &S3SourceConfig) -> Result<Arc<dyn ObjectStore>, MurrError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region);

    // Optional custom endpoint (for MinIO, LocalStack, etc.)
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
        // For custom endpoints, often need to allow HTTP
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }

    let store = builder.build().map_err(|e| {
        MurrError::DiscoveryError(format!(
            "Failed to create S3 store for bucket '{}': {}",
            config.bucket, e
        ))
    })?;

    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_local_store() {
        let config = LocalSourceConfig {
            path: "/tmp".to_string(),
        };

        let result = create_local_store(&config);
        assert!(result.is_ok());
    }
}
