use clap::Parser;
use serde_derive::{Deserialize, Serialize};
use std::{fs, sync::Arc};

#[derive(Parser, Debug)]
pub struct CLIArguments {
    /// Path to configuration file.
    #[clap(long, value_parser)]
    pub config_path: Option<String>,
}

/// [Config] defines configuration for the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // Producer session timeout(ms).
    pub producer_timeout: u64,
    // Consumer session timeout(ms).
    pub consumer_timeout: u64,
    // Query timeout(ms).
    pub query_timeout: u64,
    // Enable auto commit.
    pub enable_auto_commit: bool,
    // List of kafka brokers.
    pub brokers: Vec<String>,
    // List of topics
    pub inital_topic_list: Vec<String>,
    // Number of consumers.
    pub num_workers: usize,
    // Number of consumer groups.
    pub num_groups: usize,
    // producer API port.
    pub listening_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            producer_timeout: 5000,   // default value for producer_timeout(ms)
            consumer_timeout: 6000,   // default value for consumer_timeout(ms)
            query_timeout: 5,         // default value for query_timeout
            enable_auto_commit: true, // default value for enable_auto_commit
            brokers: vec![String::from("localhost:29092")], // default value for brokers
            inital_topic_list: vec![
                String::from("A"),
                String::from("B"),
                String::from("C"),
                String::from("D"),
            ], // default value for inital_topic_list
            num_workers: 4,           // default value for num_workers
            num_groups: 4,            // default value for num_groups
            listening_port: 5000,     // default value for listening_port
        }
    }
}

/// `load_config` loads a config file from a toml file.
pub(crate) fn load_config(config_path: &str) -> std::result::Result<Arc<Config>, String> {
    match fs::read_to_string(config_path) {
        Ok(file_str) => {
            let conf: Config = match toml::from_str(&file_str) {
                Ok(r) => r,
                Err(_) => return Err("config.toml is not a proper toml file.".to_string()),
            };

            Ok(Arc::new(conf))
        }
        Err(_) => Ok(Arc::new(Config::default())),
    }
}
