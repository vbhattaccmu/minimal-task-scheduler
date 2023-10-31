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

    ////////////////////////
    // 2. Start worker pool.
    ////////////////////////

    tokio::spawn(async move {
        let workers = Workers::init(worker_config);
        let _ = workers.consume(inital_topic_list).await;
    });

    /////////////////////////////////
    // 3. Start producer and its API.
    /////////////////////////////////

    Server::new().start_task_pool(producer_config).await;

    Ok(())
}
