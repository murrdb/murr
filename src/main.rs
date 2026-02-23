#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod api;
mod conf;
mod core;
mod io;
mod service;

use std::sync::Arc;

use crate::api::{MurrFlightService, MurrHttpService};
use crate::core::setup_logging;
use crate::service::MurrService;
use log::info;

#[tokio::main]
async fn main() {
    setup_logging();
    let data_dir = std::env::current_dir().unwrap().join("data");
    let service = Arc::new(MurrService::new(data_dir));

    let http = MurrHttpService::new(service.clone());
    let flight = MurrFlightService::new(service.clone());

    info!("Murr HTTP listening on 0.0.0.0:8080");
    info!("Murr Flight listening on 0.0.0.0:8081");

    let result = tokio::try_join!(http.serve("0.0.0.0:8080"), flight.serve("0.0.0.0:8081"));
    if let Err(e) = result {
        log::error!("Server error: {e}");
    }
}
