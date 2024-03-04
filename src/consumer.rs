use log::info;
use rand::Rng;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use std::env;

use futures::TryStreamExt;
use pulsar::{
    authentication::oauth2::OAuth2Authentication, Authentication, Consumer, DeserializeMessage,
    Payload, Pulsar, SubType, TokioExecutor,
};


use crate::{config::Config, error::Error, producer::Task};


impl DeserializeMessage for Task {
    type Output = Result<Task, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

pub(crate) struct Workers {
    timeout: u64,
    group_ids: Vec<usize>,
    consumer: Consumer<Task, _>,
}

impl Workers {
    /// `init` creates a Worker Pool.
    pub(crate) fn init(config: Arc<Config>) -> Self {
        let addr = env::var("PULSAR_ADDRESS")
        .ok()
        .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());
        let topic = env::var("PULSAR_TOPIC")
            .ok()
            .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

        let mut builder = Pulsar::builder(addr, TokioExecutor);

        if let Ok(token) = env::var("PULSAR_TOKEN") {
            let authentication = Authentication {
                name: "token".to_string(),
                data: token.into_bytes(),
            };

            builder = builder.with_auth(authentication);
        } else if let Ok(oauth2_cfg) = env::var("PULSAR_OAUTH2") {
            builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
                serde_json::from_str(oauth2_cfg.as_str())
                    .unwrap_or_else(|_| panic!("invalid oauth2 config [{}]", oauth2_cfg.as_str())),
            ));
        }

        let pulsar: Pulsar<_> = builder.build().await?;

        let mut consumer: Consumer<Task, _> = pulsar
            .consumer()
            .with_topic(topic)
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .await?;

        Workers {
            group_ids,
            consumer,
            timeout: config.consumer_timeout,
        }
    }

    pub(crate) async fn consume(&self, topics: Vec<String>) -> Result<(), Error> {
        let mut counter = 0usize;
        while let Some(msg) = consumer.try_next().await? {
            consumer.ack(&msg).await?;
            log::info!("metadata: {:?}", msg.metadata());
            log::info!("id: {:?}", msg.message_id());
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };
    
            if data.data.as_str() != "data" {
                log::error!("Unexpected payload: {}", &data.data);
                break;
            }
            counter += 1;
            log::info!("got {} messages", counter);
    
            if counter > 10 {
                consumer.close().await.expect("Unable to close consumer");
                break;
            }
        }

        Ok(())
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;
