use murr::core::MurrError;
use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyRuntimeError, PyValueError};
use pyo3::{create_exception, PyErr};

create_exception!(murr, MurrTableError, PyRuntimeError);
create_exception!(murr, MurrSegmentError, PyRuntimeError);

pub fn into_py_err(err: MurrError) -> PyErr {
    match err {
        MurrError::TableNotFound(name) => {
            PyFileNotFoundError::new_err(format!("table not found: {name}"))
        }
        MurrError::TableAlreadyExists(name) => {
            PyValueError::new_err(format!("table already exists: {name}"))
        }
        MurrError::ConfigParsingError(msg) => PyValueError::new_err(msg),
        MurrError::IoError(msg) => PyIOError::new_err(msg),
        MurrError::ArrowError(msg) => PyRuntimeError::new_err(format!("arrow error: {msg}")),
        MurrError::TableError(msg) => MurrTableError::new_err(format!("table error: {msg}")),
        MurrError::SegmentError(msg) => {
            MurrSegmentError::new_err(format!("segment error: {msg}"))
        }
    }
}
