use std::path::PathBuf;
use std::sync::Arc;

use murr::api::MurrHttpService;
use murr::conf::{Config, StorageConfig};
use murr::service::MurrService;

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

pub fn spawn_http_server(service: &Arc<MurrService>, handle: &tokio::runtime::Handle) {
    let http = MurrHttpService::new(service.clone());
    handle.spawn(async move {
        if let Err(e) = http.serve().await {
            eprintln!("HTTP server error: {e}");
        }
    });
}
