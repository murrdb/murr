use std::sync::Arc;

use murr::api::MurrHttpService;
use murr::service::MurrService;

pub fn spawn_http_server(service: &Arc<MurrService>, handle: &tokio::runtime::Handle) {
    let http = MurrHttpService::new(service.clone());
    handle.spawn(async move {
        if let Err(e) = http.serve().await {
            eprintln!("HTTP server error: {e}");
        }
    });
}
