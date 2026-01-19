use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum MurrError {
    #[error("Cannot parse config: {0}")]
    ConfigParsingError(String),
    #[error("IO error: {0}")]
    IoError(String),
}

impl From<std::io::Error> for MurrError {
    fn from(err: std::io::Error) -> Self {
        MurrError::IoError(err.to_string())
    }
}
