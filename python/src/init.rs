use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use murr::api::MurrHttpService;
use murr::conf::{Config, StorageConfig};
use murr::service::MurrService;

use crate::config::PyConfig;

/// Resolved configuration for a local Murr instance.
pub struct ResolvedConfig {
    pub config: Config,
    /// When true, spawn the HTTP server alongside the service. In the
    /// legacy path this tracks `http_port.is_some()`; when a full
    /// `Config` is passed, the caller opts in via `serve_http=True`.
    pub serve_http: bool,
}

pub fn build_config(cache_dir: String, http_port: Option<u16>) -> Config {
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

    config
}

/// Resolve the caller's arguments into a single `Config` + HTTP flag.
///
/// Exactly one of `(cache_dir + http_port)` or `config` must be
/// supplied; providing both is rejected with a `ValueError` so there
/// is no ambiguity about which configuration wins.
pub fn resolve_config(
    cache_dir: Option<String>,
    http_port: Option<u16>,
    config: Option<PyConfig>,
    serve_http: Option<bool>,
) -> PyResult<ResolvedConfig> {
    match (cache_dir, config) {
        (Some(_), Some(_)) => Err(PyValueError::new_err(
            "cache_dir and config are mutually exclusive: pass one or the other",
        )),
        (Some(dir), None) => Ok(ResolvedConfig {
            config: build_config(dir, http_port),
            serve_http: http_port.is_some(),
        }),
        (None, Some(cfg)) => {
            if http_port.is_some() {
                return Err(PyValueError::new_err(
                    "http_port cannot be combined with config: set config.server.http.port instead",
                ));
            }
            Ok(ResolvedConfig {
                config: cfg.0,
                // When the caller hands us a full Config, default to NOT
                // spawning the HTTP server — the embedded use case is
                // Python-direct reads. They opt in via `serve_http=True`.
                serve_http: serve_http.unwrap_or(false),
            })
        }
        (None, None) => Err(PyValueError::new_err(
            "either cache_dir or config must be provided",
        )),
    }
}

pub fn spawn_http_server(service: &Arc<MurrService>, handle: &tokio::runtime::Handle) {
    let http = MurrHttpService::new(service.clone());
    handle.spawn(async move {
        if let Err(e) = http.serve().await {
            eprintln!("HTTP server error: {e}");
        }
    });
}
