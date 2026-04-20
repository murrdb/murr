#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod api;
mod conf;
mod core;
mod io;
mod io3;
mod proto;
mod service;

use std::sync::Arc;

use clap::Parser;
use murr::util::logo::ASCII_LOGO;

use crate::api::{MurrFlightService, MurrHttpService};
use crate::conf::Config;
use crate::core::{CliArgs, setup_logging};
use crate::service::MurrService;
use log::info;

#[tokio::main]
async fn main() {
    setup_logging();
    let args = CliArgs::parse();
    let config = Config::from_args(&args).expect("failed to load config");
    info!("Starting murr with config: {config:?}");
    info!("{ASCII_LOGO}");

    let service = MurrService::new(config)
        .await
        .expect("failed to load tables");
    let service = Arc::new(service);

    let http = MurrHttpService::new(service.clone());
    let flight = MurrFlightService::new(service.clone());

    let result = tokio::try_join!(http.serve(), flight.serve());
    if let Err(e) = result {
        log::error!("Server error: {e}");
    }
}
