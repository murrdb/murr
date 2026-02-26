mod error;
mod schema;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use murr::conf::Config;
use murr::conf::StorageConfig;
use murr::service::MurrService;

use error::{into_py_err, MurrSegmentError, MurrTableError};
use schema::PyTableSchema;

#[pyclass(name = "LocalMurr")]
struct PyLocalMurr {
    service: Arc<MurrService>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl PyLocalMurr {
    #[new]
    fn new(cache_dir: String) -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let config = Config {
            storage: StorageConfig {
                cache_dir: PathBuf::from(cache_dir),
            },
            ..Config::default()
        };

        let service = runtime
            .block_on(MurrService::new(config))
            .map_err(into_py_err)?;

        Ok(Self {
            service: Arc::new(service),
            runtime,
        })
    }

    fn create_table(&self, py: Python<'_>, name: String, schema: PyTableSchema) -> PyResult<()> {
        let service = self.service.clone();
        let schema = schema.0;
        py.detach(|| self.runtime.block_on(service.create(&name, schema)))
            .map_err(into_py_err)
    }

    fn write(&self, py: Python<'_>, table_name: String, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let service = self.service.clone();
        py.detach(|| self.runtime.block_on(service.write(&table_name, &batch)))
            .map_err(into_py_err)
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        keys: Vec<String>,
        columns: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

        let service = self.service.clone();
        let batch = py
            .detach(|| {
                self.runtime
                    .block_on(service.read(&table_name, &key_refs, &col_refs))
            })
            .map_err(into_py_err)?;

        batch.to_pyarrow(py)
    }

    fn list_tables(&self, py: Python<'_>) -> PyResult<HashMap<String, PyTableSchema>> {
        let service = self.service.clone();
        let tables = py.detach(|| self.runtime.block_on(service.list_tables()));

        Ok(tables
            .into_iter()
            .map(|(name, schema)| (name, PyTableSchema(schema)))
            .collect())
    }

    fn get_schema(&self, py: Python<'_>, table_name: String) -> PyResult<PyTableSchema> {
        let service = self.service.clone();
        let schema = py
            .detach(|| self.runtime.block_on(service.get_schema(&table_name)))
            .map_err(into_py_err)?;

        Ok(PyTableSchema(schema))
    }
}

#[pymodule]
fn libmurr(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLocalMurr>()?;
    m.add("MurrTableError", m.py().get_type::<MurrTableError>())?;
    m.add(
        "MurrSegmentError",
        m.py().get_type::<MurrSegmentError>(),
    )?;
    Ok(())
}
