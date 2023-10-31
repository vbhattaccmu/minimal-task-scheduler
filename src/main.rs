mod config;
mod consumer;
mod error;
mod producer;

use anyhow::{Error, Result};
use clap::Parser;
use env_logger::Env;
use std::sync::Arc;

use crate::{
    config::{load_config, CLIArguments},
    consumer::Workers,
    producer::Server,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    /////////////////////////////////
    // 1. Load system configuration.
    /////////////////////////////////

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let cli_args = CLIArguments::parse();
    let config_path = cli_args.config_path.unwrap_or(String::new());
    let config = load_config(&config_path).expect("Irrecoverable esrror: fail to load config.toml");
    let worker_config = Arc::clone(&config);
    let producer_config = Arc::clone(&config);
    let inital_topic_list = config.inital_topic_list.clone();

  

    Ok(())
}
