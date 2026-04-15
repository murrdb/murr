//! PyO3 bridge for `murr::conf::Config`.
//!
//! Mirrors the `schema.rs` pattern: newtype wrappers that implement
//! `FromPyObject` by pulling fields off a Pydantic `BaseModel` via
//! `getattr`. A `None` anywhere on the Python side falls back to the
//! matching Rust default.

use std::path::PathBuf;

use pyo3::prelude::*;

use murr::conf::{Config, GrpcConfig, HttpConfig, ServerConfig, StorageConfig};

pub struct PyConfig(pub Config);

impl<'a, 'py> FromPyObject<'a, 'py> for PyConfig {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let server: PyServerConfig = ob.getattr("server")?.extract()?;
        let storage: PyStorageConfig = ob.getattr("storage")?.extract()?;
        Ok(PyConfig(Config {
            server: server.0,
            storage: storage.0,
        }))
    }
}

pub struct PyServerConfig(pub ServerConfig);

impl<'a, 'py> FromPyObject<'a, 'py> for PyServerConfig {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let http: PyHttpConfig = ob.getattr("http")?.extract()?;
        let grpc: PyGrpcConfig = ob.getattr("grpc")?.extract()?;
        Ok(PyServerConfig(ServerConfig {
            http: http.0,
            grpc: grpc.0,
        }))
    }
}

pub struct PyHttpConfig(pub HttpConfig);

impl<'a, 'py> FromPyObject<'a, 'py> for PyHttpConfig {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(PyHttpConfig(HttpConfig {
            host: ob.getattr("host")?.extract()?,
            port: ob.getattr("port")?.extract()?,
            max_payload_size: ob.getattr("max_payload_size")?.extract()?,
        }))
    }
}

pub struct PyGrpcConfig(pub GrpcConfig);

impl<'a, 'py> FromPyObject<'a, 'py> for PyGrpcConfig {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(PyGrpcConfig(GrpcConfig {
            host: ob.getattr("host")?.extract()?,
            port: ob.getattr("port")?.extract()?,
        }))
    }
}

pub struct PyStorageConfig(pub StorageConfig);

impl<'a, 'py> FromPyObject<'a, 'py> for PyStorageConfig {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        // `cache_dir` is `Optional[str]` in Python. `None` means "fall
        // back to the Rust auto-resolution cascade", matching what a
        // missing `storage.cache_dir` does in the YAML file.
        let cache_dir_obj = ob.getattr("cache_dir")?;
        let cache_dir: Option<String> = cache_dir_obj.extract()?;
        let storage = match cache_dir {
            Some(dir) => StorageConfig {
                cache_dir: PathBuf::from(dir),
            },
            None => StorageConfig::default(),
        };
        Ok(PyStorageConfig(storage))
    }
}
