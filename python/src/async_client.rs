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

#[pyclass(name = "MurrLocalAsync")]
pub struct PyMurrLocalAsync {
    service: Arc<MurrService>,
}

#[pymethods]
impl PyMurrLocalAsync {
    #[staticmethod]
    #[pyo3(signature = (cache_dir, http_port=None))]
    fn create(py: Python<'_>, cache_dir: String, http_port: Option<u16>) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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

            let service = MurrService::new(config).await.map_err(into_py_err)?;
            let service = Arc::new(service);

            if http_port.is_some() {
                let http = MurrHttpService::new(service.clone());
                tokio::spawn(async move {
                    if let Err(e) = http.serve().await {
                        eprintln!("HTTP server error: {e}");
                    }
                });
            }

            Ok(PyMurrLocalAsync {
                service,
            })
        })
    }

    fn create_table<'py>(
        &self,
        py: Python<'py>,
        name: String,
        schema: PyTableSchema,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.service.clone();
        let schema = schema.0;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service.create(&name, schema).await.map_err(into_py_err)
        })
    }

    fn write<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        batch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let service = self.service.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service
                .write(&table_name, &batch)
                .await
                .map_err(into_py_err)
        })
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        keys: Vec<String>,
        columns: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.service.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

            let batch = service
                .read(&table_name, &key_refs, &col_refs)
                .await
                .map_err(into_py_err)?;

            Python::try_attach(|py| batch.to_pyarrow(py).map(|b| b.unbind()))
                .expect("GIL should be available")
        })
    }

    fn list_tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.service.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tables = service.list_tables().await;
            let result: HashMap<String, PyTableSchema> = tables
                .into_iter()
                .map(|(name, schema)| (name, PyTableSchema(schema)))
                .collect();
            Ok(result)
        })
    }

    fn get_schema<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.service.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let schema = service
                .get_schema(&table_name)
                .await
                .map_err(into_py_err)?;
            Ok(PyTableSchema(schema))
        })
    }
}
