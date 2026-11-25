//! A small pooling HTTP/1.1 client for one origin (HTTP RPC).
//!
//! Behaves like the Go `http.Transport` the upstream services use: keep-alive
//! connections are reused (LIFO) and kept idle up to `max_idle` and
//! `idle_timeout` (closed when it passes, also when no further request comes),
//! a request that a reused connection could not send (the server
//! closed it while idle) is retried once on a new connection, and an optional
//! client-wide timeout (Go `Client.Timeout`) bounds the whole exchange. The pool
//! lock is held only to push or pop, never across I/O.

use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use bytes::Bytes;
use cabby_core::utils::redact_refused_url;
use http::header::{AUTHORIZATION, HOST, HeaderName, HeaderValue};
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::time::Instant;

use crate::client_tls::ClientStream;

/// Default maximum HTTP RPC response body size (16 MiB, D-068).
pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Removes `userinfo@` from a URL string so credentials do not appear in errors
/// or debug output. The authority starts after `scheme://` (or, without `://`,
/// after an optional `scheme:` and any slashes) and ends at the first `/`, `?`
/// or `#`; everything in it up to its last `@` is removed.
fn redact_url(raw: &str) -> String {
    let (prefix, rest) = raw.split_at(authority_start(raw));
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{prefix}{}", &rest[at + 1..]),
        None => raw.to_owned(),
    }
}

/// Where the authority starts: after `scheme://`, or else after an optional
/// leading `scheme:` (RFC 3986 scheme characters, followed by a slash) and
/// any slashes.
fn authority_start(raw: &str) -> usize {
    if let Some(scheme_end) = raw.find("://") {
        return scheme_end + 3;
    }
    let scheme_len = raw
        .find(':')
        .filter(|&colon| {
            let scheme = &raw[..colon];
            scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
                && raw[colon + 1..].starts_with('/')
        })
        .map_or(0, |colon| colon + 1);
    let rest = &raw[scheme_len..];
    scheme_len + (rest.len() - rest.trim_start_matches('/').len())
}

/// A parsed base URL. Its `Debug` leaves out the credentials.
#[derive(Clone)]
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

impl std::fmt::Debug for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Origin")
            .field("https", &self.https)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("authority", &self.authority)
            .field("base_path", &self.base_path)
            .field("query", &self.query)
            .field("raw", &self.raw)
            .finish_non_exhaustive()
    }
}

impl Origin {
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let safe = redact_url(raw);
        let refused = redact_refused_url(raw);
        let uri: Uri = raw.parse().map_err(|e| format!("parse {refused:?}: {e}"))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            Some(other) => return Err(format!("unsupported protocol scheme {other:?}")),
            None => return Err(format!("parse {refused:?}: missing protocol scheme")),
        };
        let authority = uri.authority().ok_or_else(|| format!("parse {refused:?}: missing host"))?;
        let (credentials, host_port) = match authority.as_str().rsplit_once('@') {
            Some((c, h)) => (Some(c), h),
            None => (None, authority.as_str()),
        };
        // Go rejects a port that is not a number; it may be a part of a
        // password with an unescaped `/`, so the error does not show it.
        let port_text = match host_port.rfind(']') {
            Some(bracket) => host_port[bracket + 1..].strip_prefix(':'),
            None => host_port.rsplit_once(':').map(|(_, p)| p),
        };
        if port_text.is_some_and(|p| !p.bytes().all(|b| b.is_ascii_digit())) {
            return Err(format!("parse {refused:?}: invalid port after host"));
        }
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
            raw: safe,
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
    /// The server answered with a body over the response size limit.
    pub too_large: bool,
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

/// The idle connections, and whether a task is closing expired ones.
#[derive(Default)]
struct Pool {
    idle: Vec<Idle>,
    reaping: bool,
}

/// Pooling client for one [`Origin`].
pub(crate) struct HttpClient {
    origin: Origin,
    tls: Option<Arc<ClientConfig>>,
    pool: Arc<Mutex<Pool>>,
    max_idle: usize,
    idle_timeout: Duration,
    timeout: Option<Duration>,
    max_response_bytes: usize,
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
        Self {
            origin,
            tls,
            pool: Arc::default(),
            max_idle,
            idle_timeout,
            timeout,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }

    pub(crate) fn with_max_response_bytes(mut self, limit: usize) -> Self {
        if limit > 0 {
            self.max_response_bytes = limit;
        }
        self
    }

    pub(crate) fn origin(&self) -> &Origin {
        &self.origin
    }

    /// Drops every idle connection (Go `CloseIdleConnections`).
    pub(crate) fn close_idle(&self) {
        let idle = std::mem::take(&mut lock(&self.pool).idle);
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
                    too_large: false,
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
        HttpError { message: format!("Post {:?}: {cause}", self.origin.url(path)), tls, too_large: false }
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
        let body = match Limited::new(response.into_body(), self.max_response_bytes).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) if e.is::<LengthLimitError>() => {
                let cause = format!("response body exceeds limit of {} bytes", self.max_response_bytes);
                return Err(HttpError { too_large: true, ..self.error(path, &cause, false) });
            }
            Err(e) => return Err(self.error(path, &e.to_string(), false)),
        };
        if keep_alive {
            self.checkin(sender);
        }
        Ok(HttpResponse { status, body })
    }

    async fn checkout_idle(&self) -> Option<SendRequest<Full<Bytes>>> {
        loop {
            let idle = {
                let mut pool = lock(&self.pool);
                expire(&mut pool.idle, self.idle_timeout);
                pool.idle.pop()
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
        let mut pool = lock(&self.pool);
        if pool.idle.len() < self.max_idle {
            pool.idle.push(Idle { sender, since: Instant::now() });
            if !pool.reaping {
                pool.reaping = true;
                tokio::spawn(reap(Arc::downgrade(&self.pool), self.idle_timeout));
            }
        }
    }

    async fn connect(&self, path: &str) -> Result<SendRequest<Full<Bytes>>, HttpError> {
        let o = &self.origin;
        let stream = ClientStream::connect(&o.host, o.port, self.tls.as_ref()).await.map_err(|e| {
            let text = e.to_string();
            let tls = text.starts_with("tls:");
            let cause = if tls { text } else { format!("dial tcp {}:{}: {}", o.host, o.port, go_dial_error(&e)) };
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

fn lock(pool: &Mutex<Pool>) -> MutexGuard<'_, Pool> {
    pool.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Drops the connections idle for `timeout` or closed by the server.
fn expire(idle: &mut Vec<Idle>, timeout: Duration) {
    let now = Instant::now();
    idle.retain(|i| now.duration_since(i.since) < timeout && !i.sender.is_closed());
}

/// Closes idle connections as they reach `timeout`, like Go's per-connection
/// idle timer, so a pool that stops being used does not keep its connections
/// open. Ends when the pool is empty (the next checkin starts it again) or the
/// client is gone.
async fn reap(pool: Weak<Mutex<Pool>>, timeout: Duration) {
    loop {
        let next = {
            let Some(pool) = pool.upgrade() else { return };
            let mut pool = lock(&pool);
            expire(&mut pool.idle, timeout);
            match pool.idle.iter().map(|i| i.since).min() {
                Some(oldest) => oldest + timeout,
                None => {
                    pool.reaping = false;
                    return;
                }
            }
        };
        tokio::time::sleep_until(next).await;
    }
}

/// Go's text of a failed TCP connect (`connect: connection refused`): the
/// syscall name and the lowercase errno string, without Rust's `(os error N)`.
pub(crate) fn go_dial_error(e: &std::io::Error) -> String {
    use std::io::ErrorKind;
    let text = e.to_string();
    let connect_errno = matches!(
        e.kind(),
        ErrorKind::ConnectionRefused | ErrorKind::HostUnreachable | ErrorKind::NetworkUnreachable | ErrorKind::TimedOut
    );
    match (connect_errno, e.raw_os_error(), text.find(" (os error ")) {
        (true, Some(_), Some(at)) => {
            let mut chars = text[..at].chars();
            let first = chars.next().map(|c| c.to_ascii_lowercase()).into_iter();
            format!("connect: {}", first.chain(chars).collect::<String>())
        }
        _ => text,
    }
}

#[cfg(test)]
mod idle_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// An idle keep-alive connection is closed once the idle timeout passes, with
    /// no further request (Go's `IdleConnTimeout`); the pool is usable after.
    #[tokio::test]
    async fn idle_connections_close_without_further_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (closed_tx, mut closed_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let closed_tx = closed_tx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let mut seen = Vec::new();
                    loop {
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            let _ = closed_tx.send(());
                            return;
                        }
                        seen.extend_from_slice(&buf[..n]);
                        while let Some(end) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
                            seen.drain(..end + 4);
                            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.unwrap();
                        }
                    }
                });
            }
        });

        let origin = Origin::parse(&format!("http://{addr}")).unwrap();
        let client = HttpClient::new(origin, None, 2, Duration::from_millis(200), None);
        let response = client.post("x", &[], Bytes::new()).await.unwrap();
        assert_eq!((200, &b"ok"[..]), (response.status, &response.body[..]));
        assert_eq!(1, lock(&client.pool).idle.len());

        tokio::time::timeout(Duration::from_secs(5), closed_rx.recv()).await.expect("idle connection closed").unwrap();
        assert!(lock(&client.pool).idle.is_empty());
        assert!(!lock(&client.pool).reaping);

        let response = client.post("x", &[], Bytes::new()).await.unwrap();
        assert_eq!(200, response.status);
        assert!(lock(&client.pool).reaping);
    }
}

#[cfg(test)]
pub(crate) mod dial_error_tests {
    #[test]
    fn refused_connects_read_like_go() {
        if cfg!(target_os = "linux") {
            let e = std::io::Error::from_raw_os_error(111);
            assert_eq!("connect: connection refused", super::go_dial_error(&e));
        }
        let other = std::io::Error::other("lookup failed");
        assert_eq!("lookup failed", super::go_dial_error(&other));
    }

    /// Fails when `text` holds the password `s3cr3t` or the `Basic` value of `user:s3cr3t`.
    fn assert_no_credentials(what: &str, text: &str) {
        let basic = super::base64_encode(b"user:s3cr3t");
        assert!(!text.contains("s3cr3t"), "password in {what}: {text}");
        assert!(!text.contains(&basic), "Basic credentials in {what}: {text}");
    }

    #[test]
    fn origin_redacts_credentials_in_errors_and_debug() {
        for raw in [
            "http://user:s3cr3t@bad host",
            "user:s3cr3t@localhost:3000",
            "http:/user:s3cr3t@host/x",
            "ftp://user:s3cr3t@host/x",
            "user:s3cr3t@host/x?q",
            "//user:s3cr3t@host/x",
        ] {
            let err = super::Origin::parse(raw).unwrap_err();
            assert_no_credentials(&format!("the error for {raw:?}"), &err);
        }
        let origin = super::Origin::parse("http://user:s3cr3t@127.0.0.1:8080/rpc").unwrap();
        assert_eq!(Some(format!("Basic {}", super::base64_encode(b"user:s3cr3t"))), origin.basic_auth);
        assert_no_credentials("Debug", &format!("{origin:?}"));
        assert_no_credentials("Debug", &format!("{origin:#?}"));
        assert_eq!("http://127.0.0.1:8080/rpc", origin.raw());
        assert_no_credentials("url", &origin.url("connect"));
        let client = super::HttpClient::new(origin, None, 1, std::time::Duration::from_secs(5), None);
        assert_no_credentials("the client's Debug", &format!("{client:?}"));
    }

    #[test]
    fn redact_url_strips_userinfo_with_or_without_a_scheme() {
        for (raw, want) in [
            ("http://user:s3cr3t@host:3000/rpc", "http://host:3000/rpc"),
            ("http://host/rpc/@x", "http://host/rpc/@x"),
            ("user:s3cr3t@localhost:3000", "localhost:3000"),
            ("user:s3cr3t@host/x@y", "host/x@y"),
            ("a@b:s3cr3t@host?q=@", "host?q=@"),
            ("http:/user:s3cr3t@host/x", "http:/host/x"),
            ("localhost:3000/rpc", "localhost:3000/rpc"),
            ("//user:s3cr3t@host/x", "//host/x"),
            ("///user:s3cr3t@host/x", "///host/x"),
            ("//host/x@y", "//host/x@y"),
        ] {
            assert_eq!(want, super::redact_url(raw), "{raw}");
        }
    }

    /// A `/`, `?` or `#` in the password ends the authority early, so the URL does not
    /// parse (its port is not a number, as Go also reports); the error leaves
    /// out everything up to the last `@`.
    #[test]
    fn a_delimiter_in_the_password_is_refused_without_the_password() {
        for raw in [
            "http://user:s3/cr3t@host/rpc",
            "http://user:s3/cr3t@host:3000/rpc?x=1",
            "user:s3/cr3t@host/rpc",
            "http://user:s3?cr3t@host/rpc",
            "http://user:s3#cr3t@host/rpc",
        ] {
            let err = super::Origin::parse(raw).unwrap_err();
            assert!(!err.contains("cr3t") && !err.contains("user:s3"), "credentials in the error for {raw:?}: {err}");
        }
        let err = super::Origin::parse("http://user:s3/cr3t@host/rpc").unwrap_err();
        assert!(err.starts_with("parse \"http://host/rpc\": "), "{err}");
    }

    #[tokio::test]
    async fn errors_of_a_failed_request_omit_credentials() {
        // A server that closes every connection without an answer.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                drop(sock);
            }
        });
        let origin = super::Origin::parse(&format!("http://user:s3cr3t@{addr}/rpc")).unwrap();
        let client = super::HttpClient::new(origin, None, 1, std::time::Duration::from_secs(5), None);
        let err = client.post("connect", &[], bytes::Bytes::new()).await.unwrap_err();
        assert_no_credentials("the request error", &err.message);
        assert_no_credentials("the request error's Debug", &format!("{err:?}"));
    }

    /// A server answering every request on every connection with `response`.
    pub(crate) async fn serve_forever(response: Vec<u8>) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let response = response.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let mut seen = Vec::new();
                    loop {
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        seen.extend_from_slice(&buf[..n]);
                        while let Some(end) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
                            seen.drain(..end + 4);
                            if sock.write_all(&response).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    pub(crate) fn with_length(body: &[u8]) -> Vec<u8> {
        let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        r.extend_from_slice(body);
        r
    }

    fn chunked(chunks: &[&[u8]]) -> Vec<u8> {
        let mut r = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for c in chunks {
            r.extend_from_slice(format!("{:x}\r\n", c.len()).as_bytes());
            r.extend_from_slice(c);
            r.extend_from_slice(b"\r\n");
        }
        r.extend_from_slice(b"0\r\n\r\n");
        r
    }

    async fn post_to(addr: std::net::SocketAddr, limit: usize) -> Result<super::HttpResponse, super::HttpError> {
        let origin = super::Origin::parse(&format!("http://{addr}")).unwrap();
        let client = super::HttpClient::new(origin, None, 1, std::time::Duration::from_secs(5), None)
            .with_max_response_bytes(limit);
        client.post("connect", &[], bytes::Bytes::new()).await
    }

    /// Pins D-068: HTTP RPC responses exceeding the configured limit fail
    /// instead of being buffered whole.
    #[tokio::test]
    async fn deviation_d068_response_body_exceeding_limit_fails() {
        let at_limit = serve_forever(with_length(&[b'x'; 16])).await;
        let response = post_to(at_limit, 16).await.expect("a body of exactly the limit");
        assert_eq!(&[b'x'; 16][..], &response.body[..]);

        let over = serve_forever(with_length(&[b'x'; 17])).await;
        let err = post_to(over, 16).await.unwrap_err();
        assert!(err.message.contains("response body exceeds limit of 16 bytes"), "{}", err.message);
        assert!(err.too_large && !err.tls, "{err:?}");

        let chunked_at_limit = serve_forever(chunked(&[&[b'x'; 10], &[b'x'; 6]])).await;
        assert_eq!(16, post_to(chunked_at_limit, 16).await.expect("chunked, at the limit").body.len());

        let chunked_over = serve_forever(chunked(&[&[b'x'; 10], &[b'x'; 10]])).await;
        let err = post_to(chunked_over, 16).await.unwrap_err();
        assert!(err.message.contains("response body exceeds limit of 16 bytes"), "{}", err.message);
        assert!(err.too_large, "{err:?}");
    }
}
