use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use murr::core::{ColumnSchema, DType, TableSchema};

pub struct PyDType(pub DType);

impl<'a, 'py> FromPyObject<'a, 'py> for PyDType {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        // Handle Pydantic str enums (.value) and plain strings
        let s: String = if let Ok(val) = ob.getattr("value") {
            val.extract()?
        } else {
            ob.extract()?
        };
        match s.as_str() {
            "utf8" => Ok(PyDType(DType::Utf8)),
            "float32" => Ok(PyDType(DType::Float32)),
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid dtype: expected 'utf8' or 'float32', got '{other}'"
            ))),
        }
    }
}

impl<'py> IntoPyObject<'py> for PyDType {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let s = match self.0 {
            DType::Utf8 => "utf8",
            DType::Float32 => "float32",
        };
        Ok(s.into_pyobject(py)?.into_any())
    }
}

pub struct PyColumnSchema(pub ColumnSchema);

impl<'a, 'py> FromPyObject<'a, 'py> for PyColumnSchema {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let dtype_obj = ob
            .getattr("dtype")
            .or_else(|_| ob.get_item("dtype"))?;
        let dtype: PyDType = dtype_obj.extract()?;

        let nullable = if let Ok(val) = ob.getattr("nullable") {
            val.extract::<bool>()?
        } else if let Ok(val) = ob.get_item("nullable") {
            val.extract::<bool>()?
        } else {
            true
        };

        Ok(PyColumnSchema(ColumnSchema {
            dtype: dtype.0,
            nullable,
        }))
    }
}

impl<'py> IntoPyObject<'py> for PyColumnSchema {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let dict = PyDict::new(py);
        dict.set_item("dtype", PyDType(self.0.dtype).into_pyobject(py)?)?;
        dict.set_item("nullable", self.0.nullable)?;
        Ok(dict)
    }
}

pub struct PyTableSchema(pub TableSchema);

impl<'a, 'py> FromPyObject<'a, 'py> for PyTableSchema {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let key: String = ob
            .getattr("key")
            .or_else(|_| ob.get_item("key"))?
            .extract()?;

        let columns_obj = ob
            .getattr("columns")
            .or_else(|_| ob.get_item("columns"))?;

        let columns_dict: &Bound<'_, PyDict> = columns_obj.cast()?;
        let mut columns = HashMap::new();
        for (k, v) in columns_dict.iter() {
            let name: String = k.extract()?;
            let col: PyColumnSchema = v.extract()?;
            columns.insert(name, col.0);
        }

        Ok(PyTableSchema(TableSchema { key, columns }))
    }
}

impl<'py> IntoPyObject<'py> for PyTableSchema {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let dict = PyDict::new(py);
        dict.set_item("key", &self.0.key)?;
        let cols = PyDict::new(py);
        for (name, col) in self.0.columns {
            cols.set_item(&name, PyColumnSchema(col).into_pyobject(py)?)?;
        }
        dict.set_item("columns", cols)?;
        Ok(dict)
    }
}
