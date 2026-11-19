//! Prometheus text exposition and the dedicated metrics HTTP server (upstream
//! `metrics/prometheus.go`, `server.NewServer`).

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use cabby_core::common::StringMap;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::{Counter, Gauge, PROMETHEUS_NAMESPACE, Shared, sorted};

fn prom_tags(tags: Option<&StringMap>) -> String {
    let Some(tags) = tags else {
        return String::new();
    };
    // Upstream iterates a Go map (random order); sorted here.
    let mut sorted: Vec<_> = tags.iter().collect();
    sorted.sort();
    let parts: Vec<String> = sorted.into_iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    format!("{{{}}}", parts.join(", "))
}

/// Upstream `Metrics.Prometheus`: counters, then gauges (each in name order).
pub(super) fn render(
    counters: &HashMap<String, Arc<Counter>>,
    gauges: &HashMap<String, Arc<Gauge>>,
    tags: Option<&StringMap>,
) -> String {
    let tags = prom_tags(tags);
    let mut buf = String::with_capacity(128 * (counters.len() + gauges.len()));
    let mut metric = |name: &str, desc: &str, kind: &str, value: u64| {
        let _ = write!(
            buf,
            "\n# HELP {PROMETHEUS_NAMESPACE}_{name} {desc}\n# TYPE {PROMETHEUS_NAMESPACE}_{name} {kind}\n\
             {PROMETHEUS_NAMESPACE}_{name}{tags} {value}\n"
        );
    };
    for c in sorted(counters) {
        metric(c.name(), c.desc(), "counter", c.value());
    }
    for g in sorted(gauges) {
        metric(g.name(), g.desc(), "gauge", g.value());
    }
    buf
}

/// The handler's response: 200 with the exposition text (Go `fmt.Fprint`, whose
/// content type is sniffed as plain text).
pub(super) fn response(body: String) -> http::Response<String> {
    http::Response::builder()
        .status(200)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .expect("a valid response")
}

pub(super) async fn bind(host: &str, port: i64) -> std::io::Result<TcpListener> {
    let port = u16::try_from(port)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid port {port}")))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    TcpListener::bind((host, port)).await
}

/// Serves `path` from `shared` until `shutdown`; other paths get Go's 404.
pub(super) fn serve(listener: TcpListener, path: String, shared: Arc<Shared>, shutdown: CancellationToken) {
    let path: Arc<str> = path.into();
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => return,
                a = listener.accept() => a,
            };
            let Ok((stream, _)) = accepted else { continue };
            let (path, shared, shutdown) = (Arc::clone(&path), Arc::clone(&shared), shutdown.clone());
            tokio::spawn(async move {
                let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let response = if req.uri().path() == &*path {
                        response(shared.prometheus())
                    } else {
                        http::Response::builder()
                            .status(404)
                            .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                            .header("X-Content-Type-Options", "nosniff")
                            .body("404 page not found\n".to_owned())
                            .expect("a valid response")
                    };
                    async move { Ok::<_, Infallible>(response.map(|b| Full::new(Bytes::from(b)))) }
                });
                let conn = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
                // Shutting down drops open connections.
                tokio::select! {
                    _ = conn => {}
                    () = shutdown.cancelled() => {}
                }
            });
        }
    });
}
