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
    #[error("Segment error: {0}")]
    SegmentError(String),
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
