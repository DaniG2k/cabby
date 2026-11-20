//! Adapters mounting the transport-independent endpoints of other components
//! (API, Prometheus metrics) on an [`HttpServer`](super::HttpServer).

use std::sync::Arc;

use super::{BodyError, Handler, Request, full, read_body, status_response};
use crate::{api, metrics};

/// The Prometheus endpoint (mount it at `metrics.http_path` when the metrics
/// port is the main server's; [`metrics::Metrics::http_endpoint`] tells).
pub fn metrics_handler(metrics: Arc<metrics::Metrics>) -> impl Handler {
    move |req: Request| {
        let metrics = Arc::clone(&metrics);
        async move {
            let (parts, _body) = req.into_parts();
            let resp = metrics.prometheus_handler(&http::Request::from_parts(parts, ()));
            let (parts, body) = resp.into_parts();
            http::Response::from_parts(parts, full(body))
        }
    }
}

/// The API server's routes: mount it at `<api.path>/*`. As upstream, the
/// routing, method and authorization checks come first, and only a publish
/// request that passes them has its body read: at most `max_body_size` bytes
/// (0: unlimited), counted while reading, and a larger body is answered 413.
pub fn api_handler(server: Arc<api::Server>, max_body_size: i64) -> impl Handler {
    let limit = u64::try_from(max_body_size).ok().filter(|m| *m > 0);
    move |req: Request| {
        let server = Arc::clone(&server);
        async move {
            let (parts, body) = req.into_parts();
            let request = api::HttpRequest {
                method: parts.method.as_str().to_owned(),
                path: parts
                    .uri
                    .path_and_query()
                    .map_or_else(|| parts.uri.path().to_owned(), |pq| pq.as_str().to_owned()),
                headers: parts
                    .headers
                    .iter()
                    .map(|(k, v)| (k.as_str().to_owned(), String::from_utf8_lossy(v.as_bytes()).into_owned()))
                    .collect(),
                body: Vec::new(),
            };
            let mut failed_read = false;
            let resp = match server.check(&request) {
                api::Check::Done(resp) => resp,
                api::Check::ReadBody(resp) => match read_body(body, limit).await {
                    Ok(body) => server.finish_publish(resp, api::PublishBody::Read(&body)),
                    Err(BodyError::TooLarge(_)) => server.finish_publish(resp, api::PublishBody::TooLarge),
                    Err(BodyError::Read(_)) => {
                        failed_read = true;
                        server.finish_publish(resp, api::PublishBody::Failed)
                    }
                },
            };
            let mut out = status_response(resp.status);
            if failed_read {
                // Go closes a connection whose request body failed.
                out.headers_mut().insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
            }
            for (name, value) in resp.headers {
                if let (Ok(name), Ok(value)) =
                    (http::HeaderName::from_bytes(name.as_bytes()), http::HeaderValue::from_str(&value))
                {
                    out.headers_mut().append(name, value);
                }
            }
            *out.body_mut() = full(resp.body);
            out
        }
    }
}
