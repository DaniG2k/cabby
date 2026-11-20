//! gRPC transport for the RPC controller (upstream `defaultDialer`,
//! `grpcClientHelper`, and the `grpc-list://` multi-host resolver).
//!
//! Like grpc-go with the `round_robin` policy, the client keeps one HTTP/2
//! connection (a "subchannel") per backend address and spreads calls over them
//! in turn, skipping backends whose last connection attempt failed:
//!
//! * `host:port`, `grpc://host:port`, `dns:///host:port`: one subchannel per
//!   address the host name resolves to (resolved once, on first use);
//! * `grpc-list://host1:port,host2:port`: one subchannel per listed host.
//!
//! On first use every subchannel starts connecting in the background, so all
//! backends are ready early (grpc-go connects all subchannels when the channel
//! leaves idle). Connections are counted for `grpc_active_conn_num`.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use cabby_core::logger::Level;
use cabby_core::protos;
use cabby_core::protos::rpc_client::RpcClient as Stub;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

use super::{BoxFuture, ClientError, ClientHelper, Code, Config, Context, Dialed, RpcClient, RpcError, RpcStatus};
use crate::client_tls::{self, ClientStream};

/// URL scheme listing several hosts: `grpc-list://host1:port,host2:port`.
pub const MULTI_HOST_SCHEME: &str = "grpc-list";

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_PORT: u16 = 443;

/// Round-robin over all resolved addresses, keepalive pings every 10s, TLS per
/// config, message size limits per config.
pub fn default_dialer(config: &Config) -> Result<Dialed, RpcError> {
    let client = Arc::new(GrpcClient::new(config)?);
    Ok((Arc::clone(&client) as Arc<dyn RpcClient>, client as Arc<dyn ClientHelper>))
}

/// One configured backend (before resolution).
#[derive(Debug, Clone)]
struct Target {
    host: String,
    port: u16,
    /// Expand into one subchannel per resolved address.
    resolve: bool,
}

fn split_host_port(s: &str) -> (String, u16) {
    // `[v6]:port`, `host:port`, or a bare host.
    if let Some(rest) = s.strip_prefix('[')
        && let Some((h, p)) = rest.split_once(']')
    {
        let port = p.strip_prefix(':').and_then(|p| p.parse().ok()).unwrap_or(DEFAULT_PORT);
        return (format!("[{h}]"), port);
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => (h.to_owned(), p.parse().unwrap_or(DEFAULT_PORT)),
        _ => (s.to_owned(), DEFAULT_PORT),
    }
}

fn parse_targets(host: &str) -> Vec<Target> {
    let target = |s: &str, resolve: bool| {
        let (host, port) = split_host_port(s);
        Target { host, port, resolve }
    };
    if let Some(list) = host.strip_prefix("grpc-list://") {
        return list.split([',', '/']).filter(|s| !s.is_empty()).map(|s| target(s, false)).collect();
    }
    if let Some(rest) = host.strip_prefix("dns:") {
        // `dns:///name:port` or `dns://authority/name:port`.
        let name = rest.strip_prefix("//").map_or(rest, |r| r.split_once('/').map_or(r, |(_, n)| n));
        return vec![target(name, true)];
    }
    let plain = host.strip_prefix("grpc://").unwrap_or(host);
    let plain = plain.split_once('/').map_or(plain, |(h, _)| h);
    vec![target(plain, true)]
}

/// Connection bookkeeping shared with the connectors.
#[derive(Debug, Default)]
struct State {
    active: AtomicI64,
    closed: AtomicBool,
    recovering: Mutex<bool>,
}

/// One backend address with its (lazily created) channel.
struct Subchannel {
    /// `host:port` as configured (the `:authority`).
    authority: String,
    /// The TLS server name.
    host: String,
    port: u16,
    addr: Option<SocketAddr>,
    channel: ArcSwapOption<Channel>,
    /// The last connection attempt failed (gRPC `TRANSIENT_FAILURE`).
    failing: AtomicBool,
}

/// A connection counted in [`State::active`] while open.
struct Counted {
    inner: ClientStream,
    state: Arc<State>,
    /// The peer address (upstream logs it on `stats.ConnBegin`/`ConnEnd`).
    addr: String,
}

impl Drop for Counted {
    fn drop(&mut self) {
        cabby_core::log_debug!(grpc_logger(), "disconnected", "addr" => &self.addr);
        self.state.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AsyncRead for Counted {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// The gRPC client and its helper (upstream `pb.RPCClient` over a
/// `grpc.ClientConn`, and `grpcClientHelper`).
pub struct GrpcClient {
    targets: Vec<Target>,
    subchannels: OnceCell<Vec<Arc<Subchannel>>>,
    next: AtomicUsize,
    state: Arc<State>,
    tls: Option<Arc<ClientConfig>>,
    max_recv: Option<usize>,
    max_send: Option<usize>,
}

impl std::fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClient").field("targets", &self.targets).finish_non_exhaustive()
    }
}

/// Upstream `l.With("impl", "grpc")` on the controller's `context=rpc` logger.
fn grpc_logger() -> cabby_core::logger::Logger {
    cabby_core::logger::Logger::default().with("context", "rpc").with("impl", "grpc")
}

fn log(level: Level, msg: &str) {
    grpc_logger().log(level, msg, &[]);
}

impl GrpcClient {
    pub fn new(config: &Config) -> Result<Self, RpcError> {
        let targets = parse_targets(&config.host);
        if targets.is_empty() {
            return Err(RpcError::Other(format!("invalid RPC host: {}", config.host)));
        }
        let size = |v: i64| usize::try_from(v).ok().filter(|v| *v > 0);
        Ok(Self {
            targets,
            subchannels: OnceCell::new(),
            next: AtomicUsize::new(0),
            state: Arc::new(State::default()),
            tls: config.tls_config(&[b"h2"])?,
            max_recv: size(config.max_recv_size),
            max_send: size(config.max_send_size),
        })
    }

    async fn resolve(&self) -> Vec<Arc<Subchannel>> {
        let mut subs = Vec::new();
        for t in &self.targets {
            let authority = format!("{}:{}", t.host, t.port);
            let bare = t.host.trim_start_matches('[').trim_end_matches(']').to_owned();
            let addrs: Vec<SocketAddr> = if t.resolve {
                match tokio::net::lookup_host((bare.as_str(), t.port)).await {
                    Ok(addrs) => {
                        let mut v: Vec<SocketAddr> = Vec::new();
                        for a in addrs {
                            if !v.contains(&a) {
                                v.push(a);
                            }
                        }
                        v
                    }
                    Err(_) => Vec::new(),
                }
            } else {
                Vec::new()
            };
            let sub = |addr| {
                Arc::new(Subchannel {
                    authority: authority.clone(),
                    host: bare.clone(),
                    port: t.port,
                    addr,
                    channel: ArcSwapOption::empty(),
                    failing: AtomicBool::new(false),
                })
            };
            if addrs.is_empty() {
                // Dialed by name (resolution happens per connection attempt).
                subs.push(sub(None));
            } else {
                subs.extend(addrs.into_iter().map(|a| sub(Some(a))));
            }
        }
        // Connect every subchannel in the background.
        for s in &subs {
            let endpoint = self.endpoint(s);
            let connector = self.connector(s);
            let s = Arc::clone(s);
            tokio::spawn(async move {
                if let Ok(channel) = endpoint.connect_with_connector(connector).await {
                    let channel = Arc::new(channel);
                    s.channel.rcu(|cur| cur.clone().or_else(|| Some(Arc::clone(&channel))));
                }
            });
        }
        subs
    }

    fn endpoint(&self, s: &Subchannel) -> Endpoint {
        Endpoint::from_shared(format!("http://{}", s.authority))
            .unwrap_or_else(|_| Endpoint::from_static("http://localhost"))
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_while_idle(true)
            .tcp_nodelay(true)
    }

    fn connector(
        &self,
        s: &Arc<Subchannel>,
    ) -> impl tower::Service<
        http::Uri,
        Response = TokioIo<Counted>,
        Error = io::Error,
        Future = impl Future<Output = io::Result<TokioIo<Counted>>> + Send,
    > + Send
    + 'static {
        let (s, state, tls) = (Arc::clone(s), Arc::clone(&self.state), self.tls.clone());
        tower::service_fn(move |_uri: http::Uri| {
            let (s, state, tls) = (Arc::clone(&s), Arc::clone(&state), tls.clone());
            async move {
                let tcp = match s.addr {
                    Some(addr) => TcpStream::connect(addr).await,
                    None => TcpStream::connect((s.host.as_str(), s.port)).await,
                };
                let addr =
                    tcp.as_ref().ok().and_then(|t| t.peer_addr().ok()).map(|a| a.to_string()).unwrap_or_default();
                let stream = match (tcp, tls) {
                    // grpc-go's text for a failed dial (what `RPC failed` logs).
                    (Err(e), _) => {
                        let target = s.addr.map_or_else(|| format!("{}:{}", s.host, s.port), |a| a.to_string());
                        let text = crate::http_client::go_dial_error(&e);
                        Err(io::Error::new(
                            e.kind(),
                            format!(
                                "connection error: desc = \"transport: Error while dialing: dial tcp {target}: {text}\""
                            ),
                        ))
                    }
                    (Ok(tcp), None) => {
                        let _ = tcp.set_nodelay(true);
                        Ok(ClientStream::Plain(tcp))
                    }
                    (Ok(tcp), Some(tls)) => {
                        let _ = tcp.set_nodelay(true);
                        client_tls::connect(&tls, &s.host, tcp).await.map(|t| ClientStream::Tls(Box::new(t)))
                    }
                };
                match stream {
                    Ok(inner) => {
                        s.failing.store(false, Ordering::Relaxed);
                        cabby_core::log_debug!(grpc_logger(), "connected", "addr" => &addr);
                        state.active.fetch_add(1, Ordering::Relaxed);
                        Ok(TokioIo::new(Counted { inner, state, addr }))
                    }
                    Err(e) => {
                        s.failing.store(true, Ordering::Relaxed);
                        Err(e)
                    }
                }
            }
        })
    }

    /// The next subchannel's channel, round robin over the healthy ones.
    async fn pick(&self) -> Channel {
        let subs = self.subchannels.get_or_init(|| self.resolve()).await;
        let n = subs.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let chosen = (0..n)
            .map(|i| &subs[(start + i) % n])
            .find(|s| !s.failing.load(Ordering::Relaxed))
            .unwrap_or(&subs[start % n]);
        if let Some(channel) = chosen.channel.load_full() {
            return Channel::clone(&channel);
        }
        let lazy = Arc::new(self.endpoint(chosen).connect_with_connector_lazy(self.connector(chosen)));
        let previous = chosen.channel.rcu(|cur| cur.clone().or_else(|| Some(Arc::clone(&lazy))));
        Channel::clone(previous.as_deref().unwrap_or(&lazy))
    }

    async fn stub(&self) -> Result<Stub<Channel>, ClientError> {
        if self.state.closed.load(Ordering::Relaxed) {
            return Err(ClientError::status(Code::Cancelled, "grpc: the client connection is closing"));
        }
        let mut stub = Stub::new(self.pick().await);
        if let Some(n) = self.max_recv {
            stub = stub.max_decoding_message_size(n);
        }
        if let Some(n) = self.max_send {
            stub = stub.max_encoding_message_size(n);
        }
        Ok(stub)
    }

    fn transient_failure(&self) -> bool {
        self.subchannels.get().is_some_and(|subs| subs.iter().all(|s| s.failing.load(Ordering::Relaxed)))
    }
}

fn request<T>(ctx: &Context, message: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    for (k, v) in &ctx.metadata {
        if let (Ok(k), Ok(v)) = (
            k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
            v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
        ) {
            req.metadata_mut().append(k, v);
        }
    }
    if let Some(deadline) = ctx.deadline {
        req.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()).max(Duration::from_millis(1)));
    }
    req
}

fn status(s: tonic::Status) -> ClientError {
    ClientError::Status(RpcStatus { code: s.code(), message: s.message().to_owned() })
}

macro_rules! unary {
    ($self:ident, $ctx:ident, $message:ident, $method:ident) => {
        Box::pin(async move {
            $ctx.bound(async {
                let mut stub = $self.stub().await?;
                stub.$method(request($ctx, $message)).await.map(tonic::Response::into_inner).map_err(status)
            })
            .await
        })
    };
}

impl RpcClient for GrpcClient {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        unary!(self, ctx, message, connect)
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        unary!(self, ctx, message, command)
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        unary!(self, ctx, message, disconnect)
    }
}

impl ClientHelper for GrpcClient {
    /// Ready unless closed; after a transient failure (every backend's last
    /// connection attempt failed) one immediate attempt is allowed.
    fn ready(&self) -> Result<(), RpcError> {
        if self.state.closed.load(Ordering::Relaxed) {
            return Err(RpcError::Other("grpc connection is closed".into()));
        }
        let mut recovering = self.state.recovering.lock().unwrap_or_else(|e| e.into_inner());
        if self.transient_failure() {
            if *recovering {
                return Err(RpcError::Other("grpc connection is not ready".into()));
            }
            *recovering = true;
            log(Level::Warn, "connection is lost, trying to reconnect immediately");
            return Ok(());
        }
        if *recovering {
            *recovering = false;
            log(Level::Info, "connection is restored");
        }
        Ok(())
    }

    fn supports_active_conns(&self) -> bool {
        true
    }

    fn active_conns(&self) -> usize {
        usize::try_from(self.state.active.load(Ordering::Relaxed)).unwrap_or(0)
    }

    fn close(&self) {
        self.state.closed.store(true, Ordering::Relaxed);
        if let Some(subs) = self.subchannels.get() {
            for s in subs {
                s.channel.store(None);
            }
        }
    }
}
