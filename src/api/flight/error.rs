use tonic::Status;

use crate::core::MurrError;

impl From<MurrError> for Status {
    fn from(err: MurrError) -> Status {
        match err {
            MurrError::TableNotFound(msg) => Status::not_found(msg),
            MurrError::TableAlreadyExists(msg) => Status::already_exists(msg),
            MurrError::TableError(msg) | MurrError::SegmentError(msg) => {
                Status::invalid_argument(msg)
            }
            MurrError::IoError(msg)
            | MurrError::ArrowError(msg)
            | MurrError::ConfigParsingError(msg) => Status::internal(msg),
        }
    }
}
