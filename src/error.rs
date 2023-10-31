use std::convert::Infallible;
use warp::{self, http, hyper::StatusCode};

#[derive(Debug)]
pub enum Error {
    ResourceBusy,
    ProducerTimeout,
}

impl warp::reject::Reject for Error {}

pub(crate) async fn handle_rejection(
    err: warp::reject::Rejection,
) -> Result<impl warp::Reply, Infallible> {
    let (code, message): (StatusCode, &str) = match err.find() {
        Some(Error::ResourceBusy) => (StatusCode::INTERNAL_SERVER_ERROR, RESOURCE_BUSY),
        Some(Error::ProducerTimeout) => (StatusCode::INTERNAL_SERVER_ERROR, PRODUCER_TIMEOUT),
        None => (StatusCode::INTERNAL_SERVER_ERROR, RESOURCE_BUSY),
    };

    Ok(http::Response::builder()
        .status(code)
        .body(message.to_string()))
}

const RESOURCE_BUSY: &str = "Resource is busy.";
const PRODUCER_TIMEOUT: &str = "Producer timed out.";
