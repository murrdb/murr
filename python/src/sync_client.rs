use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use murr::api::MurrHttpService;
use murr::conf::Config;
use murr::conf::StorageConfig;
use murr::service::MurrService;

use crate::error::into_py_err;
use crate::schema::PyTableSchema;

#[pyclass(name = "MurrLocalSync")]
pub struct PyMurrLocalSync {
    service: Arc<MurrService>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl PyMurrLocalSync {
    #[new]
    #[pyo3(signature = (cache_dir, http_port=None))]
    fn new(cache_dir: String, http_port: Option<u16>) -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let mut config = Config {
            storage: StorageConfig {
                cache_dir: PathBuf::from(cache_dir),
            },
            ..Config::default()
        };

        if let Some(port) = http_port {
            config.server.http.host = "127.0.0.1".to_string();
            config.server.http.port = port;
        }

        let service = runtime
            .block_on(MurrService::new(config))
            .map_err(into_py_err)?;

        let service = Arc::new(service);

        if http_port.is_some() {
            let http = MurrHttpService::new(service.clone());
            runtime.spawn(async move {
                if let Err(e) = http.serve().await {
                    eprintln!("HTTP server error: {e}");
                }
            });
        }

        Ok(Self { service, runtime })
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
