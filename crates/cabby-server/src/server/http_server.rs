//! The HTTP server (upstream `server.HTTPServer` and `server.ForPort`).
//!
//! # Design
//!
//! * **One server per port.** Components share a port through [`for_port`], as
//!   upstream: the WebSocket endpoints, health, the broadcast endpoint, the API and
//!   Prometheus register routes on the main server when they are configured with
//!   its port, and get their own server otherwise.
//! * **hyper, HTTP/1.1.** Each accepted connection is one Tokio task running a
//!   hyper `http1` connection with upgrades enabled. Nothing else is allocated per
//!   connection: no per-connection buffers outside hyper's own (released when a
//!   WebSocket upgrade takes the socket over), no per-connection timers except the
//!   5s header read timeout. The timeouts are Go's `http.Server` ones as upstream
//!   sets them (`ReadHeaderTimeout: 5s`, no idle, read or write timeout): the
//!   header timeout covers the first request's head from the accept, and each
//!   later request's head from its first bytes ([`HeaderTimeout`]), so an idle
//!   keep-alive connection stays open.
//! * **Header limits.** net/http's: the request line and headers may take up to
//!   1 MiB + 4 KiB (`DefaultMaxHeaderBytes` plus its slack); hyper also needs a
//!   header count limit (see `MAX_HEADERS`).
//! * **Routing.** Exact paths, plus `prefix/*` patterns (chi's wildcard). The route
//!   table is copy-on-write behind an `ArcSwap`: registering takes a lock,
//!   dispatching a request is a lock-free load. Unknown paths get Go's
//!   `404 page not found`.
//! * **Limits.** `max_conn` bounds concurrently open connections (upstream
//!   `netutil.LimitListener`): accepting waits for a free slot, and the slot is
//!   held by the socket itself, so an upgraded WebSocket keeps its slot until it
//!   closes, as in Go.
//! * **TLS.** rustls with the `ring` provider, TLS 1.2 at least (upstream
//!   `tls.Config{MinVersion: tls.VersionTLS12}`), handshakes bounded by 5s.
//! * **Shutdown.** [`HttpServer::shutdown`] stops accepting and asks idle and
//!   in-flight HTTP connections to close gracefully; [`HttpServer::shutdown_graceful`]
//!   also waits for them, with a deadline. Upgraded connections are not tracked
//!   (as Go's hijacked connections are not): the node closes its sessions.
//!   [`HttpServer::shutdown_token`] is upstream `ShutdownCtx`, for long-running
//!   responses (SSE, long polling).

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use cabby_core::logger::{Attr, Level, LogValue};

use super::SslConfig;

/// Upstream `ReadHeaderTimeout`; also bounds TLS handshakes and, in the SSE
/// endpoint, reading a POST body.
pub(crate) const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// net/http reads at most `DefaultMaxHeaderBytes` (1 MiB) plus 4096 bytes of
/// slack for a request line and its headers.
const MAX_HEADER_BYTES: usize = (1 << 20) + 4096;

/// hyper also limits the number of header fields (net/http does not); this
/// many fit in the byte limit only when they are a few bytes each.
const MAX_HEADERS: usize = 1024;

/// A boxed error carried by response bodies.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// A response body: a full buffer or a stream (SSE).
pub type Body = BoxBody<Bytes, BoxError>;
/// An incoming request.
pub type Request = http::Request<Incoming>;
/// A response to an incoming request.
pub type Response = http::Response<Body>;
/// What a [`Handler`] returns.
pub type ResponseFuture = Pin<Box<dyn Future<Output = Response> + Send>>;

/// An HTTP endpoint mounted on a server (upstream `http.Handler`). Closures
/// `Fn(Request) -> impl Future<Output = Response>` are handlers.
pub trait Handler: Send + Sync + 'static {
    fn call(&self, req: Request) -> ResponseFuture;
}

impl<F, Fut> Handler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request) -> ResponseFuture {
        Box::pin(self(req))
    }
}

/// The connection a request arrived on, available as a request extension
/// (see [`conn_info`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnInfo {
    pub remote_addr: Option<SocketAddr>,
    pub tls: bool,
}

/// The connection a request arrived on (default for requests built elsewhere).
pub fn conn_info<B>(req: &http::Request<B>) -> ConnInfo {
    req.extensions().get::<ConnInfo>().copied().unwrap_or_default()
}

/// A body with the given bytes.
pub fn full(body: impl Into<Bytes>) -> Body {
    Full::new(body.into()).map_err(|never| match never {}).boxed()
}

/// An empty body.
pub fn empty() -> Body {
    Empty::new().map_err(|never| match never {}).boxed()
}

/// A response with a status and no body.
pub fn status_response(status: u16) -> Response {
    let mut resp = http::Response::new(empty());
    *resp.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    resp
}

/// Go `http.Error(w, message, status)`: a plain-text body with a newline.
pub fn error_response(status: u16, message: &str) -> Response {
    let mut resp = http::Response::new(full(format!("{message}\n")));
    *resp.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    let headers = resp.headers_mut();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("text/plain; charset=utf-8"));
    headers.insert("x-content-type-options", http::HeaderValue::from_static("nosniff"));
    resp
}

/// Go `http.NotFound`.
pub fn not_found() -> Response {
    error_response(404, "404 page not found")
}

/// A request body could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    /// The body exceeds the limit (Go `http.MaxBytesError`).
    #[error("http: request body too large")]
    TooLarge(u64),
    #[error("{0}")]
    Read(String),
}

/// Reads a whole request body, at most `limit` bytes (Go `http.MaxBytesReader` + `io.ReadAll`).
pub async fn read_body(body: Incoming, limit: Option<u64>) -> Result<Bytes, BodyError> {
    match limit {
        None => body.collect().await.map(|c| c.to_bytes()).map_err(|e| BodyError::Read(e.to_string())),
        Some(limit) => {
            let max = usize::try_from(limit).unwrap_or(usize::MAX);
            match Limited::new(body, max).collect().await {
                Ok(c) => Ok(c.to_bytes()),
                Err(e) if e.is::<LengthLimitError>() => Err(BodyError::TooLarge(limit)),
                Err(e) => Err(BodyError::Read(e.to_string())),
            }
        }
    }
}

/// A server failure (bind, TLS configuration).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ServerError(pub String);

// --- Runtime -----------------------------------------------------------------

/// The Tokio runtime servers run on: the current one, or a shared background
/// runtime when called outside of any (e.g. from a synchronous `start`).
pub fn runtime_handle() -> tokio::runtime::Handle {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return handle;
    }
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("cabby-http")
                .enable_all()
                .build()
                .expect("a Tokio runtime for HTTP servers")
        })
        .handle()
        .clone()
}

fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("context", LogValue::String("http".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

// --- Registry ----------------------------------------------------------------

/// Settings of servers created by [`for_port`] (upstream package variables
/// `server.Host`, `server.SSL` and `server.MaxConn`, set by the CLI).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDefaults {
    pub host: String,
    pub ssl: Option<SslConfig>,
    /// 0: unlimited.
    pub max_conn: usize,
}

impl Default for ServerDefaults {
    fn default() -> Self {
        Self { host: "localhost".to_owned(), ssl: None, max_conn: 0 }
    }
}

fn defaults() -> &'static RwLock<ServerDefaults> {
    static DEFAULTS: OnceLock<RwLock<ServerDefaults>> = OnceLock::new();
    DEFAULTS.get_or_init(|| RwLock::new(ServerDefaults::default()))
}

fn registry() -> &'static Mutex<HashMap<String, Arc<HttpServer>>> {
    static SERVERS: OnceLock<Mutex<HashMap<String, Arc<HttpServer>>>> = OnceLock::new();
    SERVERS.get_or_init(Mutex::default)
}

/// Sets the settings of servers created later by [`for_port`].
pub fn set_defaults(value: ServerDefaults) {
    *defaults().write().unwrap_or_else(|e| e.into_inner()) = value;
}

/// The current [`for_port`] settings.
pub fn current_defaults() -> ServerDefaults {
    defaults().read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// The server for `port`, created on first use with the current defaults
/// (upstream `ForPort`).
pub fn for_port(port: &str) -> Result<Arc<HttpServer>, ServerError> {
    let mut servers = registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(server) = servers.get(port) {
        return Ok(Arc::clone(server));
    }
    let d = current_defaults();
    let server = HttpServer::new(&d.host, port, d.ssl.as_ref(), d.max_conn)?;
    servers.insert(port.to_owned(), Arc::clone(&server));
    Ok(server)
}

// --- Server ------------------------------------------------------------------

struct Route {
    /// The exact path, or the prefix of a `prefix/*` pattern.
    path: String,
    prefix: bool,
    handler: Arc<dyn Handler>,
}

#[derive(Default)]
struct State {
    started: bool,
    stopped: bool,
    local_addr: Option<SocketAddr>,
    /// How long a keep-alive connection may wait for its next request; `None`
    /// (the default, as upstream) keeps it open.
    idle_timeout: Option<Duration>,
}

/// Open HTTP connections (not upgraded ones), for graceful shutdown.
#[derive(Default)]
struct Active {
    count: AtomicUsize,
    idle: Notify,
}

struct ActiveGuard(Arc<Active>);

impl ActiveGuard {
    fn new(active: &Arc<Active>) -> Self {
        active.count.fetch_add(1, Ordering::AcqRel);
        ActiveGuard(Arc::clone(active))
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// What connection tasks share.
struct Inner {
    routes: ArcSwap<Vec<Route>>,
    shutdown: CancellationToken,
    active: Arc<Active>,
}

impl Inner {
    fn dispatch(&self, req: Request) -> ResponseFuture {
        let routes = self.routes.load();
        let path = req.uri().path();
        let handler = routes.iter().find(|r| !r.prefix && r.path == path).or_else(|| {
            routes.iter().filter(|r| r.prefix && path.starts_with(r.path.as_str())).max_by_key(|r| r.path.len())
        });
        match handler {
            Some(route) => route.handler.call(req),
            None => Box::pin(std::future::ready(not_found())),
        }
    }
}

/// An HTTP server bound to one address (upstream `server.HTTPServer`).
pub struct HttpServer {
    host: String,
    port: String,
    addr: String,
    tls: Option<TlsAcceptor>,
    max_conn: usize,
    inner: Arc<Inner>,
    state: Mutex<State>,
    register: Mutex<()>,
}

impl std::fmt::Debug for HttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpServer").field("addr", &self.addr).field("secured", &self.tls.is_some()).finish()
    }
}

/// Go `net.JoinHostPort`.
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

impl HttpServer {
    /// Upstream `NewServer(host, port, ssl, maxConn)`: TLS when `ssl` has both a
    /// certificate and a key.
    pub fn new(host: &str, port: &str, ssl: Option<&SslConfig>, max_conn: usize) -> Result<Arc<Self>, ServerError> {
        let tls = match ssl {
            Some(ssl) if ssl.available() => Some(load_tls(ssl)?),
            _ => None,
        };
        Ok(Arc::new(HttpServer {
            host: host.to_owned(),
            port: port.to_owned(),
            addr: join_host_port(host, port),
            tls,
            max_conn,
            inner: Arc::new(Inner {
                routes: ArcSwap::from_pointee(Vec::new()),
                shutdown: CancellationToken::new(),
                active: Arc::default(),
            }),
            state: Mutex::default(),
            register: Mutex::new(()),
        }))
    }

    /// Mounts `handler` at `path`: an exact path, or `prefix/*` for everything
    /// under `prefix/` (upstream `SetupHandler`). Registering a path again
    /// replaces its handler.
    pub fn setup_handler(&self, path: &str, handler: impl Handler) {
        self.setup_handler_arc(path, Arc::new(handler));
    }

    /// [`HttpServer::setup_handler`] with a shared handler.
    pub fn setup_handler_arc(&self, path: &str, handler: Arc<dyn Handler>) {
        let _guard = self.register.lock().unwrap_or_else(|e| e.into_inner());
        let (path, prefix) = match path.strip_suffix('*') {
            Some(prefix) => (prefix.to_owned(), true),
            None => (path.to_owned(), false),
        };
        let mut routes: Vec<Route> = self
            .inner
            .routes
            .load()
            .iter()
            .filter(|r| r.path != path || r.prefix != prefix)
            .map(|r| Route { path: r.path.clone(), prefix: r.prefix, handler: Arc::clone(&r.handler) })
            .collect();
        routes.push(Route { path, prefix, handler });
        self.inner.routes.store(Arc::new(routes));
    }

    /// Closes keep-alive connections that wait longer than `timeout` for their
    /// next request (Go `http.Server.IdleTimeout`). Takes effect for servers
    /// started afterwards.
    pub fn set_idle_timeout(&self, timeout: Duration) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).idle_timeout = Some(timeout);
    }

    /// Binds the listener and serves in the background (upstream `Start`, which
    /// blocks instead). Returns once listening; a second call is a no-op.
    pub fn start(&self) -> Result<(), ServerError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.started {
            return Ok(());
        }
        let listener =
            bind(&self.host, &self.port).map_err(|e| ServerError(format!("listen tcp {}: {e}", self.addr)))?;
        state.local_addr = listener.local_addr().ok();
        state.started = true;
        let idle_timeout = state.idle_timeout;
        drop(state);

        let inner = Arc::clone(&self.inner);
        let tls = self.tls.clone();
        let limit = (self.max_conn > 0).then(|| Arc::new(Semaphore::new(self.max_conn)));
        let addr = self.addr.clone();
        runtime_handle().spawn(async move {
            match TcpListener::from_std(listener) {
                Ok(listener) => accept_loop(listener, inner, tls, limit, idle_timeout).await,
                Err(e) => log(
                    Level::Error,
                    "HTTP server failed",
                    &[Attr::new("addr", LogValue::String(addr)), Attr::new("error", LogValue::String(e.to_string()))],
                ),
            }
        });
        Ok(())
    }

    /// Upstream `StartAndAnnounce`.
    pub fn start_and_announce(&self, name: &str) -> Result<(), ServerError> {
        let attrs =
            [Attr::new("name", LogValue::String(name.to_owned())), Attr::new("addr", LogValue::String(self.address()))];
        if self.running() {
            log(Level::Debug, "HTTP server has been already started", &attrs);
            return Ok(());
        }
        log(Level::Debug, "starting HTTP server", &attrs);
        self.start()
    }

    /// Whether the server has been started (upstream `Running`).
    pub fn running(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).started
    }

    /// Whether the server has been shut down (upstream `Stopped`).
    pub fn stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).stopped
    }

    /// `scheme://host:port` as configured (upstream `Address`).
    pub fn address(&self) -> String {
        let scheme = if self.tls.is_some() { "https://" } else { "http://" };
        format!("{scheme}{}", self.addr)
    }

    /// Whether the server uses TLS.
    pub fn secured(&self) -> bool {
        self.tls.is_some()
    }

    /// The connection cap (0: unlimited).
    pub fn max_conn(&self) -> usize {
        self.max_conn
    }

    /// The idle timeout of keep-alive connections, if any.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).idle_timeout
    }

    /// The bound address once started (useful with port 0).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).local_addr
    }

    /// Cancelled when the server shuts down (upstream `ShutdownCtx`): long-running
    /// handlers must finish their responses then.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    /// Stops accepting connections and closes HTTP connections once their
    /// current request completes. Later calls are no-ops.
    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped {
            return;
        }
        state.stopped = true;
        drop(state);
        self.inner.shutdown.cancel();
    }

    /// [`HttpServer::shutdown`], then waits up to `timeout` for open HTTP
    /// connections to finish (upstream `Shutdown(ctx)`).
    pub async fn shutdown_graceful(&self, timeout: Duration) -> Result<(), ServerError> {
        self.shutdown();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let idle = self.inner.active.idle.notified();
            if self.inner.active.count.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, idle).await.is_err() {
                return Err(ServerError("context deadline exceeded".to_owned()));
            }
        }
    }
}

/// Binds like Go's `net.Listen("tcp", host:port)`: an empty host listens on all
/// interfaces; a name listens on one of its addresses, IPv4 first.
fn bind(host: &str, port: &str) -> io::Result<std::net::TcpListener> {
    let port: u16 = port.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let listener = if host.is_empty() {
        std::net::TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))
            .or_else(|_| std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)))?
    } else {
        let mut addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
        addrs.sort_by_key(|a| a.is_ipv6());
        std::net::TcpListener::bind(&addrs[..])?
    };
    listener.set_nonblocking(true)?;
    Ok(listener)
}

fn load_tls(ssl: &SslConfig) -> Result<TlsAcceptor, ServerError> {
    let fail = |e: &dyn std::fmt::Display| ServerError(format!("failed to load SSL certificate: {e}"));
    let certs = CertificateDer::pem_file_iter(&ssl.cert_path)
        .map_err(|e| fail(&e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(&e))?;
    let key = PrivateKeyDer::from_pem_file(&ssl.key_path).map_err(|e| fail(&e))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| fail(&e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| fail(&e))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

async fn accept_loop(
    listener: TcpListener,
    inner: Arc<Inner>,
    tls: Option<TlsAcceptor>,
    limit: Option<Arc<Semaphore>>,
    idle_timeout: Option<Duration>,
) {
    let mut backoff = Duration::ZERO;
    loop {
        let permit = match &limit {
            None => None,
            Some(sem) => tokio::select! {
                () = inner.shutdown.cancelled() => return,
                p = Arc::clone(sem).acquire_owned() => p.ok(),
            },
        };
        let accepted = tokio::select! {
            () = inner.shutdown.cancelled() => return,
            a = listener.accept() => a,
        };
        let (stream, peer) = match accepted {
            Ok(a) => {
                backoff = Duration::ZERO;
                a
            }
            Err(e) => {
                // Out of file descriptors and similar: retry with a backoff (as Go's
                // `Serve` does for temporary errors).
                backoff = (backoff * 2).clamp(Duration::from_millis(5), Duration::from_secs(1));
                log(Level::Debug, "accept failed", &[Attr::new("error", LogValue::String(e.to_string()))]);
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        // Go enables TCP_NODELAY on accepted connections.
        let _ = stream.set_nodelay(true);
        let inner = Arc::clone(&inner);
        let tls = tls.clone();
        tokio::spawn(async move {
            let info = ConnInfo { remote_addr: Some(peer), tls: tls.is_some() };
            match tls {
                None => serve_conn(Slotted { io: stream, _permit: permit }, info, inner, idle_timeout).await,
                Some(acceptor) => match tokio::time::timeout(READ_HEADER_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => {
                        serve_conn(Slotted { io: stream, _permit: permit }, info, inner, idle_timeout).await;
                    }
                    Ok(Err(e)) => {
                        log(
                            Level::Debug,
                            "TLS handshake failed",
                            &[Attr::new("error", LogValue::String(e.to_string()))],
                        );
                    }
                    Err(_) => log(Level::Debug, "TLS handshake timed out", &[]),
                },
            }
        });
    }
}

async fn serve_conn<I>(io: I, info: ConnInfo, inner: Arc<Inner>, idle_timeout: Option<Duration>)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _active = ActiveGuard::new(&inner.active);
    let shutdown = inner.shutdown.clone();
    let state = Arc::new(ConnState::default());
    let io = HeaderTimeout::new(io, Arc::clone(&state), idle_timeout);
    let service = service_fn(move |mut req: Request| {
        state.phase.store(PHASE_REQUEST, Ordering::Release);
        req.extensions_mut().insert(info);
        let bodyless = hyper::body::Body::is_end_stream(req.body());
        req.extensions_mut().insert(ClientClosed(state.next_request(bodyless)));
        let http10_keep_alive = (req.version() == http::Version::HTTP_10)
            .then(|| has_token(req.headers().get(http::header::CONNECTION), "keep-alive"));
        let response = match go_request_check(&req) {
            Some(resp) => Box::pin(std::future::ready(resp)),
            None => inner.dispatch(req),
        };
        let conn_state = Arc::clone(&state);
        async move {
            let mut response = response.await;
            if response.status() == http::StatusCode::SWITCHING_PROTOCOLS {
                // The socket is taken over (WebSocket): no more HTTP heads.
                conn_state.phase.store(PHASE_UPGRADED, Ordering::Release);
                return Ok::<_, Infallible>(response);
            }
            let chunked = go_framing(&mut response, http10_keep_alive);
            Ok(response.map(|body| ResponseBody { inner: body, conn: conn_state, chunked }.boxed()))
        }
    });
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        // Go's timeout semantics are implemented by `HeaderTimeout`.
        .header_read_timeout(None)
        .max_header_size(MAX_HEADER_BYTES)
        .max_buf_size(MAX_HEADER_BYTES + 8192)
        .max_headers(MAX_HEADERS)
        .title_case_headers(true)
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades();
    tokio::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => return,
        () = shutdown.cancelled() => conn.as_mut().graceful_shutdown(),
    }
    let _ = conn.await;
}

// --- Header read timeout ------------------------------------------------------

/// Reading a request head (the header timeout runs).
const PHASE_HEAD: u8 = 0;
/// Serving a request: from its parsed head until its response body is done.
const PHASE_REQUEST: u8 = 1;
/// Between requests on a keep-alive connection: no timeout.
const PHASE_IDLE: u8 = 2;
/// Upgraded (WebSocket): no timeout.
const PHASE_UPGRADED: u8 = 3;

/// What a connection's reader shares with its requests.
struct ConnState {
    phase: AtomicU8,
    /// Cancelled when the client closes its side or the connection fails
    /// while the current request is served (Go cancels the request context
    /// when its background read sees EOF).
    closed: Mutex<CancellationToken>,
    /// The current request has no body left to read, so an EOF while it is
    /// served is a half-close: hidden from hyper until the response is done.
    hide_eof: AtomicBool,
    /// The reader waiting for the response to finish after a hidden EOF.
    reader: Mutex<Option<std::task::Waker>>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            phase: AtomicU8::new(PHASE_HEAD),
            closed: Mutex::new(CancellationToken::new()),
            hide_eof: AtomicBool::new(false),
            reader: Mutex::new(None),
        }
    }
}

impl ConnState {
    fn next_request(&self, bodyless: bool) -> CancellationToken {
        self.hide_eof.store(bodyless, Ordering::Release);
        let token = CancellationToken::new();
        *self.closed.lock().unwrap_or_else(|e| e.into_inner()) = token.clone();
        token
    }

    /// The response is done: the connection is between requests.
    fn response_done(&self) {
        let _ = self.phase.compare_exchange(PHASE_REQUEST, PHASE_IDLE, Ordering::AcqRel, Ordering::Acquire);
        if let Some(waker) = self.reader.lock().unwrap_or_else(|e| e.into_inner()).take() {
            waker.wake();
        }
    }

    fn client_closed(&self) {
        self.closed.lock().unwrap_or_else(|e| e.into_inner()).cancel();
    }
}

/// Cancelled when the client closes its side of the connection (or the
/// connection fails) during the request: a request extension (see
/// [`client_closed`]).
#[derive(Debug, Clone)]
pub struct ClientClosed(pub CancellationToken);

/// The token cancelled when the client of `req` goes away (Go
/// `r.Context().Done()`), if the request came from an [`HttpServer`].
pub fn client_closed<B>(req: &http::Request<B>) -> Option<CancellationToken> {
    req.extensions().get::<ClientClosed>().map(|c| c.0.clone())
}

/// Go `http.Server` with `ReadHeaderTimeout` and no idle or read timeout: the
/// first request's head must arrive within the timeout from the accept; between
/// requests the connection may stay idle indefinitely, and the timeout for the
/// next head starts once its first 4 bytes are received (Go `bufr.Peek(4)`).
/// When it expires, reading fails and the connection is closed without a
/// response, as in Go. (hyper's own `header_read_timeout` also runs while a
/// keep-alive connection waits for the next request.) With an idle timeout
/// (Go `IdleTimeout`), waiting between requests is bounded too.
struct HeaderTimeout<I> {
    io: I,
    conn: Arc<ConnState>,
    /// The deadline of the head being read; `None` outside a head, so a
    /// long-lived (keep-alive or upgraded) connection holds no timer.
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    idle_timeout: Option<Duration>,
    /// The idle deadline while between requests, when there is an idle timeout.
    idle_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    /// Bytes received while idle.
    idle_bytes: usize,
    /// The client closed its side while a request was served (see
    /// [`ConnState::hide_eof`]): reads wait for the response, then see EOF.
    eof: bool,
}

impl<I> HeaderTimeout<I> {
    fn new(io: I, conn: Arc<ConnState>, idle_timeout: Option<Duration>) -> Self {
        let deadline = tokio::time::Instant::now() + READ_HEADER_TIMEOUT;
        Self {
            io,
            conn,
            sleep: Some(Box::pin(tokio::time::sleep_until(deadline))),
            idle_timeout,
            idle_sleep: None,
            idle_bytes: 0,
            eof: false,
        }
    }

    fn arm(&mut self) {
        self.sleep = Some(Box::pin(tokio::time::sleep_until(tokio::time::Instant::now() + READ_HEADER_TIMEOUT)));
    }
}

fn header_timeout_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "read header timeout")
}

fn idle_timeout_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "idle timeout")
}

impl<I: AsyncRead + Unpin> AsyncRead for HeaderTimeout<I> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let phase = this.conn.phase.load(Ordering::Acquire);
        if this.eof {
            // A half-close: hyper (which would end the connection on an EOF
            // in the middle of a request) sees it once the response is done,
            // as Go writes the response after its handler saw the client go.
            if phase == PHASE_REQUEST {
                *this.conn.reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
                if this.conn.phase.load(Ordering::Acquire) == PHASE_REQUEST {
                    return Poll::Pending;
                }
            }
            return Poll::Ready(Ok(()));
        }
        if phase == PHASE_HEAD {
            if this.sleep.is_none() {
                this.arm();
            }
            if this.sleep.as_ref().is_some_and(|s| tokio::time::Instant::now() >= s.deadline()) {
                return Poll::Ready(Err(header_timeout_error()));
            }
        } else {
            this.sleep = None;
        }
        if phase == PHASE_IDLE {
            if let Some(timeout) = this.idle_timeout {
                let idle = this.idle_sleep.get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
                if idle.is_elapsed() {
                    return Poll::Ready(Err(idle_timeout_error()));
                }
            }
        } else {
            this.idle_bytes = 0;
            this.idle_sleep = None;
        }
        let before = buf.filled().len();
        let r = Pin::new(&mut this.io).poll_read(cx, buf);
        let eof = matches!(r, Poll::Ready(Ok(()))) && buf.remaining() > 0 && buf.filled().len() == before;
        if eof || matches!(r, Poll::Ready(Err(_))) {
            this.conn.client_closed();
        }
        if eof && phase == PHASE_REQUEST && this.conn.hide_eof.load(Ordering::Acquire) {
            this.eof = true;
            *this.conn.reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
            if this.conn.phase.load(Ordering::Acquire) == PHASE_REQUEST {
                return Poll::Pending;
            }
        }
        match (phase, &r) {
            (PHASE_IDLE, Poll::Ready(Ok(()))) => {
                this.idle_bytes += buf.filled().len() - before;
                if this.idle_bytes >= 4
                    && this
                        .conn
                        .phase
                        .compare_exchange(PHASE_IDLE, PHASE_HEAD, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    this.idle_bytes = 0;
                    this.arm();
                }
            }
            (PHASE_HEAD, Poll::Pending) if this.sleep.as_mut().is_some_and(|s| s.as_mut().poll(cx).is_ready()) => {
                return Poll::Ready(Err(header_timeout_error()));
            }
            (PHASE_IDLE, Poll::Pending) if this.idle_sleep.as_mut().is_some_and(|s| s.as_mut().poll(cx).is_ready()) => {
                return Poll::Ready(Err(idle_timeout_error()));
            }
            (PHASE_REQUEST, Poll::Pending) if this.idle_timeout.is_some() => {
                // hyper may wait on this read through the response: have the
                // end of the response poll again, so the idle timer starts.
                *this.conn.reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
                if this.conn.phase.load(Ordering::Acquire) != PHASE_REQUEST {
                    cx.waker().wake_by_ref();
                }
            }
            _ => {}
        }
        r
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for HeaderTimeout<I> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

// --- Response framing ----------------------------------------------------------

/// Go buffers this many bytes of a response body (`bufferBeforeChunkingSize`):
/// a handler that writes no more and returns gets a `Content-Length`; once
/// more is written, the response is chunked (unless the handler set a length).
pub const BUFFER_BEFORE_CHUNKING: usize = 2048;

/// Go's `http.StatusText` where it differs from the `http` crate's canonical
/// reason phrases.
fn go_reason_phrase(status: http::StatusCode) -> Option<&'static str> {
    match status.as_u16() {
        203 => Some("Non-Authoritative Information"),
        413 => Some("Request Entity Too Large"),
        414 => Some("Request URI Too Long"),
        416 => Some("Requested Range Not Satisfiable"),
        _ => None,
    }
}

/// Applies what Go's `net/http` does to every response head: its reason
/// phrases, the headers it suppresses for statuses without a body
/// (`suppressedHeaders`), and `Connection: close` after a 413 (a body over a
/// `MaxBytesReader` limit closes the connection). Returns whether the body
/// must be chunked: more than [`BUFFER_BEFORE_CHUNKING`] bytes and no
/// `Content-Length` set by the handler.
fn go_framing(response: &mut Response, http10_keep_alive: Option<bool>) -> bool {
    use hyper::body::Body as _;
    let status = response.status();
    if let Some(wants_keep_alive) = http10_keep_alive {
        // An HTTP/1.0 response is kept alive only when the client asked for it
        // and the length is known; otherwise Go drops the handler's
        // `Connection` header (unless it says `close`).
        let has_length = response.headers().contains_key(http::header::CONTENT_LENGTH)
            || response.body().size_hint().exact().is_some_and(|n| n <= BUFFER_BEFORE_CHUNKING as u64);
        let connection = response.headers().get(http::header::CONNECTION);
        let keep = wants_keep_alive && has_length && connection.is_some_and(|c| c.as_bytes() == b"keep-alive");
        if !keep && !has_token(connection, "close") {
            response.headers_mut().remove(http::header::CONNECTION);
        }
    }
    if let Some(reason) = go_reason_phrase(status) {
        response.extensions_mut().insert(hyper::ext::ReasonPhrase::from_static(reason.as_bytes()));
    }
    let headers = response.headers_mut();
    if status == http::StatusCode::NOT_MODIFIED {
        headers.remove(http::header::CONTENT_TYPE);
    }
    if status == http::StatusCode::NOT_MODIFIED || status == http::StatusCode::NO_CONTENT || status.is_informational() {
        headers.remove(http::header::CONTENT_LENGTH);
        headers.remove(http::header::TRANSFER_ENCODING);
        return false;
    }
    if status == http::StatusCode::PAYLOAD_TOO_LARGE {
        headers.insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
    }
    !headers.contains_key(http::header::CONTENT_LENGTH)
        && response.body().size_hint().exact().is_some_and(|n| n > BUFFER_BEFORE_CHUNKING as u64)
}

/// Whether a comma-separated header value lists `token` (case-insensitive).
fn has_token(value: Option<&http::HeaderValue>, token: &str) -> bool {
    value.is_some_and(|v| {
        v.as_bytes().split(|b| *b == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(token.as_bytes()))
    })
}

/// Go `net/http` answers some requests before any handler sees them:
/// `OPTIONS *` (200, `Content-Length: 0`), an HTTP/1.1 request without a
/// `Host` header or with several, and a request path with an invalid
/// percent escape (`url.ParseRequestURI` fails), with a plain 400 and
/// `Connection: close`.
fn go_request_check(req: &Request) -> Option<Response> {
    if req.method() == http::Method::OPTIONS && req.uri() == "*" {
        let mut resp = status_response(200);
        resp.headers_mut().insert(http::header::CONTENT_LENGTH, http::HeaderValue::from_static("0"));
        return Some(resp);
    }
    let hosts = req.headers().get_all(http::header::HOST).iter().count();
    if req.version() == http::Version::HTTP_11 && req.method() != http::Method::CONNECT && hosts == 0 {
        return Some(go_bad_request(Some("missing required Host header")));
    }
    if hosts > 1 {
        return Some(go_bad_request(Some("too many Host headers")));
    }
    let path = req.uri().path().as_bytes();
    let bad_escape = path.iter().enumerate().any(|(i, &b)| {
        b == b'%'
            && !(path.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && path.get(i + 2).is_some_and(u8::is_ascii_hexdigit))
    });
    if bad_escape {
        return Some(go_bad_request(None));
    }
    None
}

/// Go's response to a malformed request: `400 Bad Request[: reason]` as the
/// status line and the body, `Connection: close`.
fn go_bad_request(reason: Option<&'static str>) -> Response {
    let text = reason.map_or_else(|| "400 Bad Request".to_owned(), |r| format!("400 Bad Request: {r}"));
    let mut resp = http::Response::new(full(text));
    *resp.status_mut() = http::StatusCode::BAD_REQUEST;
    if let Some(reason) = reason {
        let phrase = format!("Bad Request: {reason}");
        if let Ok(phrase) = hyper::ext::ReasonPhrase::try_from(phrase) {
            resp.extensions_mut().insert(phrase);
        }
    }
    let headers = resp.headers_mut();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("text/plain; charset=utf-8"));
    headers.insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
    resp
}

/// A response body that marks the connection idle once hyper is done with
/// it (the response is complete, so the next bytes start a new request).
struct ResponseBody {
    inner: Body,
    conn: Arc<ConnState>,
    /// Hide the exact size so hyper chunks the body, as Go does (see
    /// [`go_framing`]).
    chunked: bool,
}

impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.conn.response_done();
    }
}

impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.chunked { hyper::body::SizeHint::default() } else { self.inner.size_hint() }
    }
}

/// A socket holding a `max_conn` slot until it is dropped (also after a
/// WebSocket upgrade, since the upgraded connection owns the socket).
struct Slotted<I> {
    io: I,
    _permit: Option<OwnedSemaphorePermit>,
}

impl<I: AsyncRead + Unpin> AsyncRead for Slotted<I> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for Slotted<I> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_host_and_port() {
        assert_eq!("localhost:80", join_host_port("localhost", "80"));
        assert_eq!("[::1]:80", join_host_port("::1", "80"));
    }

    #[test]
    fn routes_exact_paths_before_wildcards() {
        let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
        server.setup_handler("/a/*", |_req: Request| async { status_response(201) });
        server.setup_handler("/a/b", |_req: Request| async { status_response(202) });
        server.setup_handler("/a/b/*", |_req: Request| async { status_response(204) });
        server.setup_handler("/a/b", |_req: Request| async { status_response(203) });
        // The second `/a/b` replaces the first and moves to the end.
        let routes: Vec<(String, bool)> =
            server.inner.routes.load().iter().map(|r| (r.path.clone(), r.prefix)).collect();
        assert_eq!(vec![("/a/".to_owned(), true), ("/a/b/".to_owned(), true), ("/a/b".to_owned(), false)], routes);

        // An exact path wins over every wildcard, then the longest prefix wins.
        let addr = crate::server::tests::support::start(&server);
        for (path, status) in [
            ("/a/b", 203),
            ("/a/b/", 204),
            ("/a/b/c", 204),
            ("/a/c", 201),
            ("/a/", 201),
            ("/a/bc", 201),
            ("/a", 404),
            ("/b", 404),
        ] {
            assert_eq!(status, crate::server::tests::support::get(addr, path).status, "GET {path}");
        }
        server.shutdown();
    }
}
