mod api;
mod conf;
mod core;

use clap::Parser;
use core::setup_logging;
use log::info;

use core::CliArgs;

use crate::conf::Config;

fn main() {
    setup_logging();
    info!("Murr started.");

    let args = CliArgs::parse();
    info!("Cli args: {args:?}");
    let config = Config::from_args(&args).expect("Cannot parse config");

    println!("Hello, world!");
}
