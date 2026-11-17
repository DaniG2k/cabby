//! Minimal HTTP/1.1 test servers on loopback.
//!
//! [`serve`] is the shared plumbing (keep-alive connections, accepted-connection
//! count, shutdown on drop); [`HttpRecorder`] records every request and answers
//! with a scripted status, e.g. as the telemetry endpoint in tests.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio_util::sync::CancellationToken;

use crate::runtime::runtime;
use crate::sync::Watched;

pub type HttpResponse = hyper::Response<Full<Bytes>>;
pub type Handler = Arc<dyn Fn(RecordedRequest) -> Pin<Box<dyn Future<Output = HttpResponse> + Send>> + Send + Sync>;

/// One received HTTP request with its full body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    /// Path without the query string.
    pub path: String,
    pub query: Option<String>,
    /// Header names lower-cased, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Index of the TCP connection that carried the request (0-based, accept order).
    pub connection: u64,
}

impl RecordedRequest {
    /// First value of a header (case-insensitive name).
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == name).map(|(_, v)| v.as_str())
    }

    pub fn body_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("request body is not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }
}

/// A running server; stops accepting and drops its connections when dropped.
pub struct Served {
    pub addr: SocketAddr,
    pub connections: Arc<AtomicU64>,
    shutdown: CancellationToken,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub fn response(status: u16, body: impl Into<Bytes>) -> HttpResponse {
    let mut r = hyper::Response::new(Full::new(body.into()));
    *r.status_mut() = hyper::StatusCode::from_u16(status).expect("valid HTTP status");
    r
}

/// Serves HTTP/1.1 on a fresh loopback port. The listener is bound before this
/// returns, so the server is ready to accept.
pub fn serve(handler: Handler, shutdown: CancellationToken) -> Served {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binding a loopback port");
    std_listener.set_nonblocking(true).expect("non-blocking listener");
    let addr = std_listener.local_addr().expect("local addr");
    let connections = Arc::new(AtomicU64::new(0));
    let conns = Arc::clone(&connections);
    let token = shutdown.clone();
    let rt = runtime();
    let listener = {
        let _guard = rt.enter();
        tokio::net::TcpListener::from_std(std_listener).expect("tokio listener")
    };
    rt.spawn(async move {
        loop {
            let accepted = tokio::select! {
                r = listener.accept() => r,
                () = token.cancelled() => return,
            };
            let Ok((stream, _)) = accepted else { continue };
            let index = conns.fetch_add(1, Ordering::SeqCst);
            let handler = Arc::clone(&handler);
            let token = token.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: hyper::Request<Incoming>| {
                    let handler = Arc::clone(&handler);
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = match body.collect().await {
                            Ok(b) => b.to_bytes().to_vec(),
                            Err(_) => Vec::new(),
                        };
                        let recorded = RecordedRequest {
                            method: parts.method.as_str().to_owned(),
                            path: parts.uri.path().to_owned(),
                            query: parts.uri.query().map(str::to_owned),
                            headers: parts
                                .headers
                                .iter()
                                .map(|(k, v)| {
                                    (k.as_str().to_owned(), String::from_utf8_lossy(v.as_bytes()).into_owned())
                                })
                                .collect(),
                            body,
                            connection: index,
                        };
                        Ok::<_, Infallible>(handler(recorded).await)
                    }
                });
                let conn = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), svc);
                tokio::select! {
                    _ = conn => {}
                    () = token.cancelled() => {}
                }
            });
        }
    });
    Served { addr, connections, shutdown }
}

/// Records every request and answers `status` with an empty body.
pub struct HttpRecorder {
    served: Served,
    log: Arc<Watched<Vec<RecordedRequest>>>,
    status: Arc<AtomicU16>,
}

impl HttpRecorder {
    pub fn start() -> Self {
        let log: Arc<Watched<Vec<RecordedRequest>>> = Arc::new(Watched::new(Vec::new()));
        let status = Arc::new(AtomicU16::new(200));
        let (l, s) = (Arc::clone(&log), Arc::clone(&status));
        let handler: Handler = Arc::new(move |req| {
            let (l, s) = (Arc::clone(&l), Arc::clone(&s));
            Box::pin(async move {
                l.update(|v| v.push(req));
                response(s.load(Ordering::SeqCst), Bytes::new())
            })
        });
        Self { served: serve(handler, CancellationToken::new()), log, status }
    }

    /// `http://127.0.0.1:<port>`
    pub fn url(&self) -> String {
        format!("http://{}", self.served.addr)
    }

    pub fn addr(&self) -> SocketAddr {
        self.served.addr
    }

    /// Status for subsequent responses.
    pub fn respond_with(&self, status: u16) {
        self.status.store(status, Ordering::SeqCst);
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.log.read(Clone::clone)
    }

    /// Blocks until at least `n` requests arrived; panics after `deadline`.
    pub fn await_requests(&self, n: usize, deadline: Duration) -> Vec<RecordedRequest> {
        assert!(
            self.log.wait_until(deadline, |v| v.len() >= n),
            "expected {n} HTTP request(s) within {deadline:?}, got {}",
            self.log.read(Vec::len)
        );
        self.requests()
    }

    pub fn connections(&self) -> u64 {
        self.served.connections.load(Ordering::SeqCst)
    }
}
