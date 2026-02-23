mod api;
mod conf;
mod core;
mod io;
mod service;

use crate::api::MurrApi;
use crate::core::setup_logging;
use crate::service::MurrService;
use log::info;

#[tokio::main]
async fn main() {
    setup_logging();
    let data_dir = std::env::current_dir().unwrap().join("data");
    let service = MurrService::new(data_dir);
    let api = MurrApi::new(service);
    info!("Murr listening on 0.0.0.0:8080");
    api.serve("0.0.0.0:8080").await.unwrap();
}
