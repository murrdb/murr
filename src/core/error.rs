use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum MurrError {
    #[error("Cannot parse config: {0}")]
    ConfigParsingError(String),
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Arrow error: {0}")]
    ArrowError(String),
    #[error("Table error: {0}")]
    TableError(String),
    #[error("Parquet error: {0}")]
    ParquetError(String),
    #[error("Object store error: {0}")]
    ObjectStoreError(String),
    #[error("Discovery error: {0}")]
    DiscoveryError(String),
    #[error("No valid partition found: {0}")]
    NoValidPartition(String),
}

impl From<std::io::Error> for MurrError {
    fn from(err: std::io::Error) -> Self {
        MurrError::IoError(err.to_string())
    }
}

impl From<arrow::error::ArrowError> for MurrError {
    fn from(err: arrow::error::ArrowError) -> Self {
        MurrError::ArrowError(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for MurrError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        MurrError::ParquetError(err.to_string())
    }
}

impl From<object_store::Error> for MurrError {
    fn from(err: object_store::Error) -> Self {
        MurrError::ObjectStoreError(err.to_string())
    }
}
