mod conf;
mod core;
mod io;
mod service;

use crate::core::setup_logging;
use log::info;

fn main() {
    setup_logging();
    info!("Murr started.");
}
