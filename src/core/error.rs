use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum MurrError {
    #[error("Cannot parse config:{0}")]
    ConfigParsingError(String),
}
