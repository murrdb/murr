mod async_client;
mod error;
mod init;
mod schema;
mod sync_client;

use pyo3::prelude::*;

use async_client::PyMurrLocalAsync;
use error::{MurrSegmentError, MurrTableError};
use sync_client::PyMurrLocalSync;

#[pymodule]
fn libmurr(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMurrLocalSync>()?;
    m.add_class::<PyMurrLocalAsync>()?;
    m.add("MurrTableError", m.py().get_type::<MurrTableError>())?;
    m.add(
        "MurrSegmentError",
        m.py().get_type::<MurrSegmentError>(),
    )?;
    Ok(())
}
