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
