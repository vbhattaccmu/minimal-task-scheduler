use log::info;
use pulsar::{
    authentication::oauth2::OAuth2Authentication, message::proto, producer, Authentication,
    Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{signal, task::spawn};
use warp::{Filter, Rejection, Reply};

use crate::{
    config::Config,
    error::{handle_rejection, Error},
};

pub(crate) struct Server {
    producer: Arc<Producer>,
}

impl Server {
    /// `new` creates a new instance of a Server
    /// with predefined set of topics and messages.
    pub(crate) fn new() -> Self {
        Server {
            producer: Arc::new(Producer::init(vec![
                Task {
                    topic: String::from("A"),
                    message: String::from("AA"),
                },
                Task {
                    topic: String::from("B"),
                    message: String::from("BB"),
                },
                Task {
                    topic: String::from("C"),
                    message: String::from("CC"),
                },
                Task {
                    topic: String::from("D"),
                    message: String::from("DD"),
                },
            ])),
        }
    }

    /// `start_task_pool` initiates a task pool and its API.
    pub(crate) async fn start_task_pool(&self, config: Arc<Config>) {
        let listening_port = config.listening_port;
        let warp_serve = warp::serve(
            Self::add_tasks_route(Arc::clone(&self.producer.clone()))
                .recover(handle_rejection)
                .with(
                    warp::cors()
                        .allow_any_origin()
                        .allow_headers(vec![
                            "content-type",
                            "User-Agent",
                            "Sec-Fetch-Mode",
                            "Referer",
                            "Origin",
                            "Access-Control-Request-Method",
                            "Access-Control-Request-Headers",
                        ])
                        .allow_methods(&[warp::http::Method::POST]),
                ),
        );

        spawn(async move {
            let (_, server) = warp_serve.bind_with_graceful_shutdown(
                ([0, 0, 0, 0], listening_port),
                async move {
                    signal::ctrl_c()
                        .await
                        .expect("failed to listen to shutdown signal");
                },
            );

            server.await;
        });

        let _ = self.producer.produce_messages(config).await;
    }

    /// `add_tasks_route` denotes the task pool API handler.
    fn add_tasks_route(
        task_pool: Arc<Producer>,
    ) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
        async fn add_task(task: Task, task_pool: Arc<Producer>) -> Result<impl Reply, Rejection> {
            task_pool.add_task(task);
            Ok(warp::reply::json(&String::from("Success")))
        }

        async fn get_tasks(task_pool: Arc<Producer>) -> Result<impl Reply, Rejection> {
            Ok(warp::reply::json(&task_pool.list_tasks()))
        }

        let post_tasks_route = |task_pool: Arc<Producer>| {
            warp::path!("tasks" / "generate")
                .and(warp::post())
                .and(warp::body::content_length_limit(10 * 1024 * 1024))
                .and(warp::body::json())
                .and(warp::path::end())
                .and_then(move |task| add_task(task, Arc::clone(&task_pool)))
        };

        let get_tasks_route = |task_pool: Arc<Producer>| {
            warp::get()
                .and(warp::path("tasks"))
                .and(warp::path::end())
                .and_then(move || get_tasks(Arc::clone(&task_pool)))
        };

        post_tasks_route(task_pool.clone()).or(get_tasks_route(task_pool))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Task {
    topic: String,
    message: String,
}

#[derive(Debug)]
pub(crate) struct Producer {
    task_pool: Arc<Mutex<Vec<Task>>>,
}

impl Producer {
    /// `init` sets up a new producer.
    pub(crate) fn init(tasks: Vec<Task>) -> Self {
        Producer {
            task_pool: Arc::new(Mutex::new(tasks)),
        }
    }

    /// `add_task` lets you add a new task to the
    /// producer task pool.
    fn add_task(&self, task: Task) {
        let mut tasks = self.task_pool.lock().unwrap();
        tasks.push(task);
        drop(tasks);
    }

    /// `list_tasks` lists current tasks in worker pool.
    fn list_tasks(&self) -> Vec<Task> {
        let tasks = self.task_pool.lock().unwrap();
        let task_list: Vec<Task> = tasks.iter().map(|task| task.clone()).collect::<Vec<_>>();
        drop(tasks);

        task_list
    }

    /// `produce_messages` produces messages to a
    /// predefined set of topics.
    async fn produce_messages(&self, config: Arc<Config>) -> Result<(), Error> {
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
        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        let mut counter = 0usize;
        let query_timeout = config.clone().query_timeout;

        // keep producing messages to topic
        loop {
            producer
                .send(Task {
                    topic,
                    message: "a".to_string(),
                })
                .await?
                .await
                .unwrap();

            counter += 1;
            log::info!("{counter} messages");
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

            if counter > 10 {
                producer.close().await.expect("Unable to close connection");
                break;
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl SerializeMessage for Task {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}
