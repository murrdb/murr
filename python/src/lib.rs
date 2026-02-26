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

use error::into_py_err;
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

    fn create_table(&self, name: &str, schema: PyTableSchema) -> PyResult<()> {
        self.runtime
            .block_on(self.service.create(name, schema.0))
            .map_err(into_py_err)
    }

    fn write(&self, table_name: &str, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;

        self.runtime
            .block_on(self.service.write(table_name, &batch))
            .map_err(into_py_err)
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        table_name: &str,
        keys: Vec<String>,
        columns: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

        let batch = self
            .runtime
            .block_on(self.service.read(table_name, &key_refs, &col_refs))
            .map_err(into_py_err)?;

        batch.to_pyarrow(py)
    }

    fn list_tables(&self) -> PyResult<HashMap<String, PyTableSchema>> {
        let tables = self.runtime.block_on(self.service.list_tables());

        Ok(tables
            .into_iter()
            .map(|(name, schema)| (name, PyTableSchema(schema)))
            .collect())
    }

    fn get_schema(&self, table_name: &str) -> PyResult<PyTableSchema> {
        let schema = self
            .runtime
            .block_on(self.service.get_schema(table_name))
            .map_err(into_py_err)?;

        Ok(PyTableSchema(schema))
    }
}

#[pymodule]
fn libmurr(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLocalMurr>()?;
    Ok(())
}
