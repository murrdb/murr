use std::collections::HashMap;
use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use murr::service::MurrService;

use crate::error::into_py_err;
use crate::init::{build_config, spawn_http_server};
use crate::schema::PyTableSchema;

#[pyclass(name = "MurrLocalSync")]
pub struct PyMurrLocalSync {
    service: Arc<MurrService>,
    // Present only when an HTTP server is running — it needs a tokio runtime to live on.
    _runtime: Option<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyMurrLocalSync {
    #[new]
    #[pyo3(signature = (cache_dir, http_port=None))]
    fn new(cache_dir: String, http_port: Option<u16>) -> PyResult<Self> {
        let config = build_config(cache_dir, http_port);
        let service = Arc::new(MurrService::new(config).map_err(into_py_err)?);

        let runtime = if http_port.is_some() {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            spawn_http_server(&service, rt.handle());
            Some(rt)
        } else {
            None
        };

        Ok(Self { service, _runtime: runtime })
    }

    fn create_table(&self, py: Python<'_>, name: String, schema: PyTableSchema) -> PyResult<()> {
        let service = self.service.clone();
        let schema = schema.0;
        py.detach(|| service.create(&name, schema)).map_err(into_py_err)
    }

    fn write(&self, py: Python<'_>, table_name: String, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let service = self.service.clone();
        py.detach(|| service.write(&table_name, &batch)).map_err(into_py_err)
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        keys: Vec<String>,
        columns: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.service.clone();
        let batch = py
            .detach(|| {
                let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
                let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                service.read(&table_name, &key_refs, &col_refs)
            })
            .map_err(into_py_err)?;

        batch.to_pyarrow(py)
    }

    fn list_tables(&self, py: Python<'_>) -> PyResult<HashMap<String, PyTableSchema>> {
        let service = self.service.clone();
        let tables = py.detach(|| service.list_tables());

        Ok(tables
            .into_iter()
            .map(|(name, schema)| (name, PyTableSchema(schema)))
            .collect())
    }

    fn get_schema(&self, py: Python<'_>, table_name: String) -> PyResult<PyTableSchema> {
        let service = self.service.clone();
        let schema = py
            .detach(|| service.get_schema(&table_name))
            .map_err(into_py_err)?;

        Ok(PyTableSchema(schema))
    }
}
