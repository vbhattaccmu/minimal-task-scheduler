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

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

pub(crate) struct Workers {
    timeout: u64,
    group_ids: Vec<usize>,
    consumer_groups: HashMap<usize, Vec<LoggingConsumer>>,
}

impl Workers {
    /// `init` creates a Worker Pool.
    pub(crate) fn init(config: Arc<Config>) -> Self {
        let mut consumer_groups: HashMap<usize, Vec<LoggingConsumer>> = HashMap::new();

        // Custom logic:-
        // Assign random group IDs to consumers such that
        // different consumers belong to same consumer groups.
        let group_ids: Vec<usize> = (0..config.num_workers)
            .map(|_| rand::thread_rng().gen_range(1..=config.num_groups))
            .collect();

        for worker in 0..config.num_workers {
            let context = CustomContext;
            let consumer: LoggingConsumer = ClientConfig::new()
                .set("group.id", group_ids[worker].to_string())
                .set("bootstrap.servers", config.brokers.join(","))
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", &config.consumer_timeout.to_string())
                .set("enable.auto.commit", &config.enable_auto_commit.to_string())
                .set("auto.offset.reset", "earliest")
                .create_with_context(context)
                .expect("Consumer creation failed");

            consumer_groups
                .entry(group_ids[worker])
                .or_insert(Vec::new())
                .push(consumer);
        }

        Workers {
            group_ids,
            consumer_groups,
            timeout: config.consumer_timeout,
        }
    }

    /// `consume` denotes the core algorithm for load balancing on worker pool.
    /// Strategy: 1. To balance CPU server load, each consumer in a group
    ///              is allowed to subscribe to a topic till a timer elapses and offset
    ///              is committed.
    ///           2. Once timer has elapsed, next consumer in the group picks up where the
    ///              previous consumer left off.
    ///           3. Once all consumers are processed, we proceed to the next topic.
    pub(crate) async fn consume(&self, topics: Vec<String>) -> Result<(), Error> {
        for (idx, topic) in topics.iter().enumerate() {
            info!("Processing Topic: {}", topic);

            let group_id = self.group_ids[idx];
            if let Some(consumers) = self.consumer_groups.get(&group_id) {
                for consumer in consumers.iter() {
                    consumer
                        .subscribe(&[topic])
                        .map_err(|_| Error::ResourceBusy)?;

                    // process messages till a timer elapses.
                    self.process(consumer).await;
                }
            }
        }

        Ok(())
    }

    /// `process` allows the worker pool to process a
    /// message received from a topic.
    async fn process(&self, consumer: &StreamConsumer<CustomContext>) {
        let start = Instant::now();
        loop {
            match consumer.recv().await {
                Err(e) => {
                    info!("Kafka error: {}", e);
                    break;
                }
                Ok(msg) => {
                    let payload = match msg.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            info!("Error while deserializing message payload: {:?}", e);
                            break;
                        }
                    };

                    info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        msg.key(), payload, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

                    if let Some(headers) = msg.headers() {
                        for header in headers.iter() {
                            info!("Header {:#?}: {:?}", header.key, header.value);
                        }
                    }

                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        info!("Commit error {:?}", e);
                    };
                }
            };

            if start.elapsed() > Duration::from_millis(self.timeout) {
                info!("Timer elapsed");
                // Let this consumer cool down a bit
                // Let other members in the same group process the topic.
                break;
            }
        }
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;
