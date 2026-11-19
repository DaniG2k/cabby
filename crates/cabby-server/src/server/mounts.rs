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

/// The API server's routes: mount it at `<api.path>/*`. The body is read up to
/// `max_body_size` (0: unlimited); a larger body is answered 413 by the API
/// after its method and authorization checks, as upstream.
pub fn api_handler(server: Arc<api::Server>, max_body_size: i64) -> impl Handler {
    let limit = u64::try_from(max_body_size).ok().filter(|m| *m > 0);
    move |req: Request| {
        let server = Arc::clone(&server);
        async move {
            let (parts, body) = req.into_parts();
            let body = match read_body(body, limit).await {
                Ok(body) => body.to_vec(),
                // Any body over the limit makes the API answer 413.
                Err(BodyError::TooLarge(limit)) => vec![0; usize::try_from(limit).unwrap_or(usize::MAX - 1) + 1],
                Err(BodyError::Read(_)) => return status_response(422),
            };
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
                body,
            };
            let resp = server.serve(&request);
            let mut out = status_response(resp.status);
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
