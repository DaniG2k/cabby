//! A small pooling HTTP/1.1 client for one origin (HTTP RPC, telemetry).
//!
//! Behaves like the Go `http.Transport` the upstream services use: keep-alive
//! connections are reused (LIFO) and kept idle up to `max_idle` and
//! `idle_timeout`, a request that a reused connection could not send (the server
//! closed it while idle) is retried once on a new connection, and an optional
//! client-wide timeout (Go `Client.Timeout`) bounds the whole exchange. The pool
//! lock is held only to push or pop, never across I/O.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, HOST, HeaderName, HeaderValue};
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::time::Instant;

use crate::client_tls::ClientStream;

/// A parsed base URL.
#[derive(Debug, Clone)]
pub(crate) struct Origin {
    pub https: bool,
    pub host: String,
    pub port: u16,
    /// `host[:port]` as written (the `Host` header).
    authority: String,
    /// The base path without a trailing slash (may be empty).
    base_path: String,
    query: Option<String>,
    /// `Basic ...` from URL credentials (Go sends them when no Authorization is set).
    basic_auth: Option<String>,
    /// The URL as given (for error texts).
    raw: String,
}

impl Origin {
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let uri: Uri = raw.parse().map_err(|e| format!("parse {raw:?}: {e}"))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            Some(other) => return Err(format!("unsupported protocol scheme {other:?}")),
            None => return Err(format!("parse {raw:?}: missing protocol scheme")),
        };
        let authority = uri.authority().ok_or_else(|| format!("parse {raw:?}: missing host"))?;
        let (credentials, host_port) = match authority.as_str().rsplit_once('@') {
            Some((c, h)) => (Some(c), h),
            None => (None, authority.as_str()),
        };
        let host = authority.host().to_owned();
        let port = authority.port_u16().unwrap_or(if https { 443 } else { 80 });
        let basic_auth = credentials.map(|c| {
            let decoded = percent_decode(c);
            format!("Basic {}", base64_encode(decoded.as_bytes()))
        });
        let base_path = uri.path().trim_end_matches('/').to_owned();
        Ok(Self {
            https,
            host,
            port,
            authority: host_port.to_owned(),
            base_path,
            query: uri.query().map(str::to_owned),
            basic_auth,
            raw: raw.to_owned(),
        })
    }

    /// The request target for `path` under the base path (Go `URL.JoinPath`).
    pub(crate) fn target(&self, path: &str) -> String {
        let mut target = String::with_capacity(self.base_path.len() + path.len() + 2);
        // `base_path` is empty or starts with a slash, without a trailing one.
        if !self.base_path.is_empty() && !self.base_path.starts_with('/') {
            target.push('/');
        }
        target.push_str(&self.base_path);
        if !path.is_empty() || target.is_empty() {
            target.push('/');
            target.push_str(path.trim_start_matches('/'));
        }
        if let Some(q) = &self.query {
            target.push('?');
            target.push_str(q);
        }
        target
    }

    /// The full URL of `path` (for error texts).
    pub(crate) fn url(&self, path: &str) -> String {
        let scheme = if self.https { "https" } else { "http" };
        format!("{scheme}://{}{}", self.authority, self.target(path))
    }

    pub(crate) fn raw(&self) -> &str {
        &self.raw
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
            && let Ok(b) = u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("zz"), 16)
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn base64_encode(input: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(input)
}

/// A failed exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpError {
    pub message: String,
    /// The TLS handshake failed.
    pub tls: bool,
}

/// A response with its whole body.
#[derive(Debug, Clone)]
pub(crate) struct HttpResponse {
    pub status: u16,
    pub body: Bytes,
}

struct Idle {
    sender: SendRequest<Full<Bytes>>,
    since: Instant,
}

/// Pooling client for one [`Origin`].
pub(crate) struct HttpClient {
    origin: Origin,
    tls: Option<Arc<ClientConfig>>,
    idle: Mutex<Vec<Idle>>,
    max_idle: usize,
    idle_timeout: Duration,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").field("origin", &self.origin.raw).finish_non_exhaustive()
    }
}

impl HttpClient {
    pub(crate) fn new(
        origin: Origin,
        tls: Option<Arc<ClientConfig>>,
        max_idle: usize,
        idle_timeout: Duration,
        timeout: Option<Duration>,
    ) -> Self {
        Self { origin, tls, idle: Mutex::new(Vec::new()), max_idle, idle_timeout, timeout }
    }

    pub(crate) fn origin(&self) -> &Origin {
        &self.origin
    }

    /// Drops every idle connection (Go `CloseIdleConnections`).
    pub(crate) fn close_idle(&self) {
        let idle = std::mem::take(&mut *self.idle.lock().unwrap_or_else(|e| e.into_inner()));
        drop(idle);
    }

    /// `POST <base>/<path>` with `headers` and `body`.
    pub(crate) async fn post(
        &self,
        path: &str,
        headers: &[(HeaderName, HeaderValue)],
        body: Bytes,
    ) -> Result<HttpResponse, HttpError> {
        let exchange = self.exchange(Method::POST, path, headers, body);
        match self.timeout {
            None => exchange.await,
            Some(t) => tokio::time::timeout(t, exchange).await.unwrap_or_else(|_| {
                Err(HttpError {
                    message: format!(
                        "Post {:?}: context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
                        self.origin.url(path)
                    ),
                    tls: false,
                })
            }),
        }
    }

    fn request(
        &self,
        method: &Method,
        path: &str,
        headers: &[(HeaderName, HeaderValue)],
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, HttpError> {
        let mut builder = Request::builder().method(method.clone()).uri(self.origin.target(path));
        let h = builder.headers_mut().expect("a valid request builder");
        if let Ok(v) = HeaderValue::from_str(&self.origin.authority) {
            h.insert(HOST, v);
        }
        for (k, v) in headers {
            h.insert(k.clone(), v.clone());
        }
        if !h.contains_key(AUTHORIZATION)
            && let Some(auth) = self.origin.basic_auth.as_deref().and_then(|a| HeaderValue::from_str(a).ok())
        {
            h.insert(AUTHORIZATION, auth);
        }
        builder.body(Full::new(body)).map_err(|e| self.error(path, &e.to_string(), false))
    }

    fn error(&self, path: &str, cause: &str, tls: bool) -> HttpError {
        HttpError { message: format!("Post {:?}: {cause}", self.origin.url(path)), tls }
    }

    async fn exchange(
        &self,
        method: Method,
        path: &str,
        headers: &[(HeaderName, HeaderValue)],
        body: Bytes,
    ) -> Result<HttpResponse, HttpError> {
        let mut request = self.request(&method, path, headers, body)?;
        // A reused connection may have been closed by the server while idle; the
        // request then comes back unsent and is retried on a fresh connection.
        while let Some(mut sender) = self.checkout_idle().await {
            match sender.try_send_request(request).await {
                Ok(response) => return self.finish(sender, path, response).await,
                Err(mut e) => match e.take_message() {
                    Some(unsent) => request = unsent,
                    None => return Err(self.error(path, &e.into_error().to_string(), false)),
                },
            }
        }
        let mut sender = self.connect(path).await?;
        match sender.send_request(request).await {
            Ok(response) => self.finish(sender, path, response).await,
            Err(e) => Err(self.error(path, &e.to_string(), false)),
        }
    }

    async fn finish(
        &self,
        sender: SendRequest<Full<Bytes>>,
        path: &str,
        response: http::Response<hyper::body::Incoming>,
    ) -> Result<HttpResponse, HttpError> {
        let status = response.status().as_u16();
        let keep_alive = !response
            .headers()
            .get(http::header::CONNECTION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let body =
            response.into_body().collect().await.map_err(|e| self.error(path, &e.to_string(), false))?.to_bytes();
        if keep_alive {
            self.checkin(sender);
        }
        Ok(HttpResponse { status, body })
    }

    async fn checkout_idle(&self) -> Option<SendRequest<Full<Bytes>>> {
        loop {
            let idle = {
                let mut pool = self.idle.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                pool.retain(|i| now.duration_since(i.since) < self.idle_timeout && !i.sender.is_closed());
                pool.pop()
            }?;
            let mut sender = idle.sender;
            if sender.ready().await.is_ok() {
                return Some(sender);
            }
        }
    }

    fn checkin(&self, sender: SendRequest<Full<Bytes>>) {
        if sender.is_closed() || self.max_idle == 0 {
            return;
        }
        let mut pool = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < self.max_idle {
            pool.push(Idle { sender, since: Instant::now() });
        }
    }

    async fn connect(&self, path: &str) -> Result<SendRequest<Full<Bytes>>, HttpError> {
        let o = &self.origin;
        let stream = ClientStream::connect(&o.host, o.port, self.tls.as_ref()).await.map_err(|e| {
            let text = e.to_string();
            let tls = text.starts_with("tls:");
            let cause = if tls { text } else { format!("dial tcp {}:{}: {text}", o.host, o.port) };
            self.error(path, &cause, tls)
        })?;
        let (sender, conn) = http1::Builder::new()
            .title_case_headers(true)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|e| self.error(path, &e.to_string(), false))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(sender)
    }
}
