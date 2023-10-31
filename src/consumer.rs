use log::info;
use rand::Rng;
use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer, ConsumerContext, Rebalance},
    error::KafkaResult,
    message::{Headers, Message},
    topic_partition_list::TopicPartitionList,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{config::Config, error::Error};
