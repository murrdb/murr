#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod api;
mod conf;
mod core;
mod io;
mod service;

use std::sync::Arc;

use clap::Parser;
use murr::util::logo::ASCII_LOGO;

use crate::api::{MurrFlightService, MurrHttpService};
use crate::conf::{BackendConfig, Config};
use crate::core::{CliArgs, setup_logging};
use crate::service::MurrService;
use log::info;

#[tokio::main]
async fn main() {
    setup_logging();
    let args = CliArgs::parse();
    let config = Config::from_args(&args).expect("failed to load config");

    info!("{ASCII_LOGO}");
    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    info!("murr v{} ({} build)", env!("CARGO_PKG_VERSION"), profile);
    info!(
        "HTTP listen: {}, max_payload: {} MiB",
        config.server.http.addr(),
        config.server.http.max_payload_size >> 20
    );
    info!("gRPC listen: {}", config.server.grpc.addr());
    info!("Storage path: {}", config.storage.path.display());
    match &config.storage.backend {
        BackendConfig::Mmap(p) => info!(
            "Storage backend: Mmap (read_method={:?}, write_buffer_size={} MiB, bloom_bits_per_key={})",
            p.read_method,
            p.write_buffer_size >> 20,
            p.bloom_bits_per_key
        ),
        BackendConfig::Block(b) => info!(
            "Storage backend: Block (read_method={:?}, write_buffer_size={} MiB, bloom_filter_bits_per_key={:?}, mmap_reads={}, use_direct_reads={})",
            b.read_method,
            b.write_buffer_size >> 20,
            b.bloom_filter_bits_per_key,
            b.mmap_reads,
            b.use_direct_reads
        ),
    }

    let service = Arc::new(MurrService::new(config).expect("failed to load tables"));
    info!("Service initialized, starting listeners");

    let http = MurrHttpService::new(service.clone());
    let flight = MurrFlightService::new(service.clone());

    let result = tokio::try_join!(http.serve(), flight.serve());
    if let Err(e) = result {
        log::error!("Server error: {e}");
    }
}
