use log::info;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use std::{
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
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("message.timeout.ms", config.producer_timeout.to_string())
            .create()
            .map_err(|_| Error::ProducerTimeout)?;
        let query_timeout = config.clone().query_timeout;

        // keep producing messages to topic
        loop {
            let tasks = self.task_pool.lock().unwrap();
            let futures = tasks
                .iter()
                .enumerate()
                .map(|task| async move {
                    let delivery_status = producer
                        .send(
                            FutureRecord::to(&task.1.topic)
                                .payload(&format!("Message {}", task.1.message))
                                .key(&format!("Key {}", task.0))
                                .headers(OwnedHeaders::new().insert(Header {
                                    key: "header_key",
                                    value: Some("header_value"),
                                })),
                            Duration::from_secs(query_timeout),
                        )
                        .await;

                    delivery_status
                })
                .collect::<Vec<_>>();

            for future in futures {
                info!("Future completed. Result: {:?}", future.await);
            }

            drop(tasks);

            tokio::time::sleep(Duration::from_secs(query_timeout)).await;
        }
    }
}
