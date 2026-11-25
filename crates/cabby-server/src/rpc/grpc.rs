//! gRPC transport for the RPC controller (upstream `defaultDialer`,
//! `grpcClientHelper`, and the `grpc-list://` multi-host resolver).
//!
//! Like grpc-go with the `round_robin` policy, the client keeps one HTTP/2
//! connection (a "subchannel") per backend address and spreads calls over them
//! in turn, skipping backends whose last connection attempt failed:
//!
//! * `host:port`, `grpc://host:port`, `dns:///host:port`: one subchannel per
//!   address the host name resolves to. Like grpc-go's DNS resolver, the name
//!   is resolved on first use and again after a connection attempt fails or a
//!   connection is lost (at most once per 30 s), so a backend that moves to
//!   another address is picked up;
//! * `grpc-list://host1:port,host2:port`: one subchannel per listed host.
//!
//! On first use every subchannel starts connecting in the background, so all
//! backends are ready early (grpc-go connects all subchannels when the channel
//! leaves idle). Connections are counted for `grpc_active_conn_num`.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use cabby_core::logger::Level;
use cabby_core::protos;
use cabby_core::protos::rpc_client::RpcClient as Stub;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Notify, OnceCell};
use tokio_util::sync::CancellationToken;
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

/// Resolves a host name to socket addresses (injectable for tests).
pub type Resolver = Arc<dyn Fn(String, u16) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> + Send + Sync>;

/// grpc-go's DNS resolver (`internal/resolver/dns`): after a successful
/// resolution the next one waits for a re-resolution request and for at least
/// this long (`MinResolutionInterval`).
const MIN_RESOLUTION_INTERVAL: Duration = Duration::from_secs(30);

/// The system resolver (`getaddrinfo`), duplicates removed.
fn system_resolver() -> Resolver {
    Arc::new(|host: String, port: u16| {
        Box::pin(async move {
            let mut v: Vec<SocketAddr> = Vec::new();
            for a in tokio::net::lookup_host((host.as_str(), port)).await? {
                if !v.contains(&a) {
                    v.push(a);
                }
            }
            Ok(v)
        })
    })
}

/// grpc-go's `minConnectTimeout`: how long a background connection attempt may
/// take (or its backoff, if longer).
const MIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// grpc-go `backoff.DefaultExponential.Backoff(retries)`: 1 s base, factor
/// 1.6, jitter 0.2, at most 120 s.
fn exponential_backoff(retries: u32) -> Duration {
    let (base, max) = (1.0_f64, 120.0_f64);
    let mut backoff = base;
    let mut n = retries;
    while backoff < max && n > 0 {
        backoff *= 1.6;
        n -= 1;
    }
    let mut bytes = [0u8; 8];
    let _ = getrandom::fill(&mut bytes);
    #[allow(clippy::cast_precision_loss)]
    let unit = (u64::from_le_bytes(bytes) >> 11) as f64 / (1u64 << 53) as f64;
    Duration::from_secs_f64((backoff.min(max) * (1.0 + 0.2 * (unit * 2.0 - 1.0))).max(0.0))
}

/// How the background reconnects (injectable for tests).
#[derive(Clone)]
struct Reconnect {
    /// The wait before retry `n` (from 0).
    backoff: Arc<dyn Fn(u32) -> Duration + Send + Sync>,
    /// How long an attempt may take (or its backoff, if longer).
    connect_timeout: Duration,
}

impl Default for Reconnect {
    fn default() -> Self {
        Self { backoff: Arc::new(exponential_backoff), connect_timeout: MIN_CONNECT_TIMEOUT }
    }
}

/// Connection bookkeeping shared with the connectors.
#[derive(Debug, Default)]
struct State {
    active: AtomicI64,
    closed: AtomicBool,
    recovering: Mutex<bool>,
    /// A connection attempt failed or an open connection was lost: the name
    /// resolver should re-resolve (grpc-go `cc.resolveNow`, a one-slot queue).
    resolve_now: Notify,
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
    /// A connection attempt failed (a one-slot queue): reconnect in the background.
    failed: Notify,
    /// The address is no longer listed, or the client is closed.
    removed: CancellationToken,
}

/// A connection counted in [`State::active`] while open.
struct Counted {
    inner: ClientStream,
    state: Arc<State>,
    /// The peer closed the connection or it failed (as opposed to being
    /// dropped by this side).
    lost: bool,
    /// The peer address (upstream logs it on `stats.ConnBegin`/`ConnEnd`).
    addr: String,
}

impl Counted {
    fn track<T>(&mut self, r: Poll<io::Result<T>>, eof: impl FnOnce(&T) -> bool) -> Poll<io::Result<T>> {
        match &r {
            Poll::Ready(Err(_)) => self.lost = true,
            Poll::Ready(Ok(v)) if eof(v) => self.lost = true,
            _ => {}
        }
        r
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        cabby_core::log_debug!(grpc_logger(), "disconnected", "addr" => &self.addr);
        self.state.active.fetch_sub(1, Ordering::Relaxed);
        if self.lost {
            // grpc-go refreshes the name resolver on any connection loss.
            self.state.resolve_now.notify_one();
        }
    }
}

impl AsyncRead for Counted {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let r = Pin::new(&mut this.inner).poll_read(cx, buf);
        let eof = buf.remaining() > 0 && buf.filled().len() == before;
        this.track(r, |()| eof)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.track(r, |_| false)
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
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.track(r, |_| false)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// The gRPC client and its helper (upstream `pb.RPCClient` over a
/// `grpc.ClientConn`, and `grpcClientHelper`).
pub struct GrpcClient {
    inner: Arc<Inner>,
}

struct Inner {
    targets: Vec<Target>,
    /// The current subchannels, set by the first resolution and replaced
    /// when a re-resolution changes the address list.
    subchannels: OnceCell<ArcSwap<Vec<Arc<Subchannel>>>>,
    next: AtomicUsize,
    state: Arc<State>,
    tls: Option<Arc<ClientConfig>>,
    max_recv: Option<usize>,
    max_send: Option<usize>,
    resolver: Resolver,
    min_resolution_interval: Duration,
    reconnect: Reconnect,
    /// Stops the re-resolution and reconnection tasks.
    stop: CancellationToken,
}

impl std::fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClient").field("targets", &self.inner.targets).finish_non_exhaustive()
    }
}

impl Drop for GrpcClient {
    fn drop(&mut self) {
        self.inner.stop.cancel();
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
        Self::with_resolver(config, system_resolver(), MIN_RESOLUTION_INTERVAL)
    }

    /// A client resolving names with `resolver`, re-resolving at most once per
    /// `min_resolution_interval` (grpc-go: 30 s).
    pub fn with_resolver(
        config: &Config,
        resolver: Resolver,
        min_resolution_interval: Duration,
    ) -> Result<Self, RpcError> {
        let targets = parse_targets(&config.host);
        if targets.is_empty() {
            return Err(RpcError::Other(format!("invalid RPC host: {}", config.host)));
        }
        let size = |v: i64| usize::try_from(v).ok().filter(|v| *v > 0);
        Ok(Self {
            inner: Arc::new(Inner {
                targets,
                subchannels: OnceCell::new(),
                next: AtomicUsize::new(0),
                state: Arc::new(State::default()),
                tls: config.tls_config(&[b"h2"])?,
                max_recv: size(config.max_recv_size),
                max_send: size(config.max_send_size),
                resolver,
                min_resolution_interval,
                reconnect: Reconnect::default(),
                stop: CancellationToken::new(),
            }),
        })
    }

    async fn stub(&self) -> Result<Stub<Channel>, ClientError> {
        if self.inner.state.closed.load(Ordering::Relaxed) {
            return Err(ClientError::status(Code::Cancelled, "grpc: the client connection is closing"));
        }
        let mut stub = Stub::new(self.inner.pick().await);
        if let Some(n) = self.inner.max_recv {
            stub = stub.max_decoding_message_size(n);
        }
        if let Some(n) = self.inner.max_send {
            stub = stub.max_encoding_message_size(n);
        }
        Ok(stub)
    }
}

/// The target re-resolved by the DNS watcher: a single name to resolve
/// (`host:port`, `grpc://`, `dns:///`). `grpc-list://` hosts are dialed by
/// name, so every connection attempt resolves them afresh.
fn dns_target(targets: &[Target]) -> Option<&Target> {
    match targets {
        [t] if t.resolve => Some(t),
        _ => None,
    }
}

fn bare_host(t: &Target) -> String {
    t.host.trim_start_matches('[').trim_end_matches(']').to_owned()
}

impl Inner {
    async fn lookup(&self, t: &Target) -> Option<Vec<SocketAddr>> {
        (self.resolver)(bare_host(t), t.port).await.ok().filter(|a| !a.is_empty())
    }

    fn subchannel(&self, t: &Target, addr: Option<SocketAddr>) -> Arc<Subchannel> {
        Arc::new(Subchannel {
            authority: format!("{}:{}", t.host, t.port),
            host: bare_host(t),
            port: t.port,
            addr,
            channel: ArcSwapOption::empty(),
            failing: AtomicBool::new(false),
            failed: Notify::new(),
            removed: self.stop.child_token(),
        })
    }

    /// Starts connecting `s` in the background (grpc-go connects every
    /// subchannel when the channel leaves idle or the address list changes) and
    /// keeps reconnecting it with backoff when connection attempts fail.
    fn connect_in_background(&self, s: &Arc<Subchannel>) {
        let endpoint = self.endpoint(s);
        let (s, state, tls) = (Arc::clone(s), Arc::clone(&self.state), self.tls.clone());
        let reconnect = self.reconnect.clone();
        tokio::spawn(async move {
            let removed = s.removed.clone();
            let connect = || endpoint.connect_with_connector(Self::connector(&s, &state, &tls));
            let connections = async {
                if let Ok(channel) = connect().await {
                    let channel = Arc::new(channel);
                    s.channel.rcu(|cur| cur.clone().or_else(|| Some(Arc::clone(&channel))));
                }
                loop {
                    s.failed.notified().await;
                    let mut retries = 0;
                    while s.failing.load(Ordering::Relaxed) {
                        let backoff = (reconnect.backoff)(retries);
                        retries += 1;
                        tokio::time::sleep(backoff).await;
                        // A call may have reconnected it meanwhile.
                        if !s.failing.load(Ordering::Relaxed) {
                            break;
                        }
                        let limit = reconnect.connect_timeout.max(backoff);
                        if let Ok(Ok(channel)) = tokio::time::timeout(limit, connect()).await {
                            s.channel.store(Some(Arc::new(channel)));
                        }
                    }
                }
            };
            tokio::select! {
                () = removed.cancelled() => {}
                () = connections => {}
            }
        });
    }

    /// First use: resolve, connect every subchannel, and start the DNS
    /// watcher.
    async fn init(self: &Arc<Self>) -> ArcSwap<Vec<Arc<Subchannel>>> {
        let mut subs = Vec::new();
        let mut resolved = true;
        for t in &self.targets {
            let addrs = if t.resolve { self.lookup(t).await } else { None };
            match addrs {
                Some(addrs) => subs.extend(addrs.into_iter().map(|a| self.subchannel(t, Some(a)))),
                None => {
                    resolved &= !t.resolve;
                    // Dialed by name (resolution happens per connection attempt).
                    subs.push(self.subchannel(t, None));
                }
            }
        }
        for s in &subs {
            self.connect_in_background(s);
        }
        if dns_target(&self.targets).is_some() {
            tokio::spawn(watch_dns(Arc::downgrade(self), self.stop.clone(), resolved));
        }
        ArcSwap::from_pointee(subs)
    }

    /// Applies a new address list: subchannels of addresses still listed are
    /// kept with their connections; new addresses get new subchannels, and
    /// subchannels of addresses no longer listed are shut down (grpc-go
    /// `round_robin` on a resolver update).
    fn update_addresses(&self, t: &Target, addrs: Vec<SocketAddr>) {
        let Some(current) = self.subchannels.get() else { return };
        let old = current.load_full();
        let same = old.len() == addrs.len() && old.iter().all(|s| s.addr.is_some_and(|a| addrs.contains(&a)));
        if same {
            return;
        }
        let mut fresh = Vec::with_capacity(addrs.len());
        for a in addrs {
            match old.iter().find(|s| s.addr == Some(a)) {
                Some(s) => fresh.push(Arc::clone(s)),
                None => {
                    let s = self.subchannel(t, Some(a));
                    self.connect_in_background(&s);
                    fresh.push(s);
                }
            }
        }
        current.store(Arc::new(fresh.clone()));
        for s in old.iter() {
            if !fresh.iter().any(|f| Arc::ptr_eq(f, s)) {
                s.removed.cancel();
                s.channel.store(None);
            }
        }
    }

    fn endpoint(&self, s: &Subchannel) -> Endpoint {
        Endpoint::from_shared(format!("http://{}", s.authority))
            .unwrap_or_else(|_| Endpoint::from_static("http://localhost"))
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_while_idle(true)
            .tcp_nodelay(true)
    }

    /// Opens connections to `s`, and keeps track of their state.
    fn connector(
        s: &Arc<Subchannel>,
        state: &Arc<State>,
        tls: &Option<Arc<ClientConfig>>,
    ) -> impl tower::Service<
        http::Uri,
        Response = TokioIo<Counted>,
        Error = io::Error,
        Future = impl Future<Output = io::Result<TokioIo<Counted>>> + Send,
    > + Send
    + 'static {
        let (s, state, tls) = (Arc::clone(s), Arc::clone(state), tls.clone());
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
                        Ok(TokioIo::new(Counted { inner, state, lost: false, addr }))
                    }
                    Err(e) => {
                        s.failing.store(true, Ordering::Relaxed);
                        s.failed.notify_one();
                        state.resolve_now.notify_one();
                        Err(e)
                    }
                }
            }
        })
    }

    /// The next subchannel's channel, round robin over the healthy ones.
    async fn pick(self: &Arc<Self>) -> Channel {
        let subs = self.subchannels.get_or_init(|| self.init()).await.load_full();
        let n = subs.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let chosen = (0..n)
            .map(|i| &subs[(start + i) % n])
            .find(|s| !s.failing.load(Ordering::Relaxed))
            .unwrap_or(&subs[start % n]);
        if let Some(channel) = chosen.channel.load_full() {
            return Channel::clone(&channel);
        }
        let lazy = Arc::new(self.endpoint(chosen).connect_with_connector_lazy(Self::connector(
            chosen,
            &self.state,
            &self.tls,
        )));
        let previous = chosen.channel.rcu(|cur| cur.clone().or_else(|| Some(Arc::clone(&lazy))));
        Channel::clone(previous.as_deref().unwrap_or(&lazy))
    }

    fn transient_failure(&self) -> bool {
        self.subchannels.get().is_some_and(|subs| subs.load().iter().all(|s| s.failing.load(Ordering::Relaxed)))
    }
}

/// grpc-go's DNS resolver loop (`dnsResolver.watcher`): after a successful
/// resolution, wait for a re-resolution request (a failed connection attempt
/// or a lost connection) and at least the minimum resolution interval; after
/// a failed one (an error or no addresses), retry with exponential backoff.
/// There is no periodic re-resolution.
async fn watch_dns(inner: Weak<Inner>, stop: CancellationToken, mut resolved: bool) {
    let mut backoff_index = 1;
    loop {
        let Some(this) = inner.upgrade() else { return };
        let next = if resolved {
            backoff_index = 1;
            let next = tokio::time::Instant::now() + this.min_resolution_interval;
            let state = Arc::clone(&this.state);
            drop(this);
            tokio::select! {
                () = stop.cancelled() => return,
                () = state.resolve_now.notified() => {}
            }
            next
        } else {
            let next = tokio::time::Instant::now() + exponential_backoff(backoff_index);
            backoff_index += 1;
            drop(this);
            next
        };
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep_until(next) => {}
        }
        let Some(this) = inner.upgrade() else { return };
        let Some(t) = dns_target(&this.targets) else { return };
        match this.lookup(t).await {
            Some(addrs) => {
                resolved = true;
                this.update_addresses(t, addrs);
            }
            None => resolved = false,
        }
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
    /// connection attempt failed) one immediate attempt is allowed. Further
    /// attempts are made in the background, until one succeeds.
    fn ready(&self) -> Result<(), RpcError> {
        if self.inner.state.closed.load(Ordering::Relaxed) {
            return Err(RpcError::Other("grpc connection is closed".into()));
        }
        let mut recovering = self.inner.state.recovering.lock().unwrap_or_else(|e| e.into_inner());
        if self.inner.transient_failure() {
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
        usize::try_from(self.inner.state.active.load(Ordering::Relaxed)).unwrap_or(0)
    }

    fn close(&self) {
        self.inner.state.closed.store(true, Ordering::Relaxed);
        self.inner.stop.cancel();
        if let Some(subs) = self.inner.subchannels.get() {
            for s in subs.load().iter() {
                s.channel.store(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A TCP backend that counts accepted connections; aborting `task` stops
    /// it and closes its connections (like a pod going away).
    struct Backend {
        addr: SocketAddr,
        accepts: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    async fn backend() -> Backend {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        let task = tokio::spawn(async move {
            let mut conns = Vec::new();
            while let Ok((c, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                conns.push(c);
            }
        });
        Backend { addr, accepts, task }
    }

    fn call_context() -> Context {
        Context { deadline: Some(tokio::time::Instant::now() + Duration::from_millis(100)), ..Context::default() }
    }

    fn counting_resolver(current: &Arc<Mutex<SocketAddr>>, lookups: &Arc<AtomicUsize>) -> Resolver {
        let (current, lookups) = (Arc::clone(current), Arc::clone(lookups));
        Arc::new(move |host: String, _port: u16| {
            assert_eq!("rpc.test", host);
            lookups.fetch_add(1, Ordering::SeqCst);
            let addr = *current.lock().expect("lock");
            Box::pin(async move { Ok(vec![addr]) })
        })
    }

    /// H1 (independent review): when the backend
    /// behind the RPC host name goes away and the name moves to a new
    /// address, the client re-resolves after the connection loss (like
    /// grpc-go's DNS resolver) and connects to the new address, without a
    /// restart. Before, the name was resolved once and never again.
    #[tokio::test]
    async fn re_resolves_the_host_after_the_backend_moves() {
        let (old, new) = (backend().await, backend().await);
        let current = Arc::new(Mutex::new(old.addr));
        let lookups = Arc::new(AtomicUsize::new(0));
        let config = Config { host: "rpc.test:50051".into(), ..Config::default() };
        let client =
            GrpcClient::with_resolver(&config, counting_resolver(&current, &lookups), Duration::from_millis(200))
                .expect("client");

        let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while old.accepts.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(old.accepts.load(Ordering::SeqCst) > 0, "connected to the first address");

        // The backend dies and DNS now points at the new one.
        old.task.abort();
        *current.lock().expect("lock") = new.addr;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while new.accepts.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(new.accepts.load(Ordering::SeqCst) > 0, "connected to the new address after re-resolution");
        assert!(lookups.load(Ordering::SeqCst) >= 2);
        client.close();
        new.task.abort();
    }

    /// Re-resolution happens on demand only (grpc-go has no periodic DNS
    /// re-resolution): while connections stay healthy the name is not
    /// resolved again.
    #[tokio::test]
    async fn does_not_re_resolve_while_connections_are_healthy() {
        let b = backend().await;
        let current = Arc::new(Mutex::new(b.addr));
        let lookups = Arc::new(AtomicUsize::new(0));
        let config = Config { host: "dns:///rpc.test:50051".into(), ..Config::default() };
        let client =
            GrpcClient::with_resolver(&config, counting_resolver(&current, &lookups), Duration::from_millis(50))
                .expect("client");
        for _ in 0..5 {
            let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(1, lookups.load(Ordering::SeqCst));
        client.close();
        b.task.abort();
    }

    /// After every connection attempt failed, the client reconnects in the
    /// background (grpc-go's backoff, from 1 s) and is ready again once the
    /// backend is back on its address.
    #[tokio::test]
    async fn reconnects_when_the_backend_returns_on_the_same_address() {
        let b = backend().await;
        let config = Config { host: b.addr.to_string(), ..Config::default() };
        let client = GrpcClient::new(&config).expect("client");
        let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
        assert_eq!(Ok(()), client.ready());

        b.task.abort();
        let _ = b.task.await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while client.ready().is_ok() {
            assert!(tokio::time::Instant::now() < deadline, "the client notices that the backend is gone");
            let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
        }
        let not_ready = RpcError::Other("grpc connection is not ready".into());
        assert_eq!(Err(not_ready), client.ready(), "one immediate attempt is allowed");

        let listener = TcpListener::bind(b.addr).await.expect("bind the same address");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while client.ready().is_err() {
            assert!(tokio::time::Instant::now() < deadline, "the client reconnects");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = listener.accept().await.expect("a connection");
        client.close();
    }

    /// Reconnecting ends when the client is closed.
    #[tokio::test]
    async fn does_not_reconnect_once_closed() {
        let refusing = Refusing::new().await;
        let config = Config { host: refusing.addr.to_string(), ..Config::default() };
        let client = GrpcClient::new(&config).expect("client");
        call(&client).await;
        client.close();

        let listener = refusing.listen().await;
        assert!(tokio::time::timeout(Duration::from_secs(2), listener.accept()).await.is_err());
    }

    /// An address that refuses connections until it listens. A connection holds
    /// its port meanwhile, so that no other test gets it.
    struct Refusing {
        addr: SocketAddr,
        holder: TcpStream,
        _peer: TcpStream,
    }

    impl Refusing {
        async fn new() -> Self {
            let helper = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let holder = TcpStream::connect(helper.local_addr().expect("addr")).await.expect("connect");
            let (peer, addr) = helper.accept().await.expect("accept");
            Self { addr, holder, _peer: peer }
        }

        async fn listen(self) -> TcpListener {
            self.holder.set_zero_linger().expect("no TIME_WAIT");
            drop(self.holder);
            TcpListener::bind(self.addr).await.expect("bind the same address")
        }
    }

    /// A client for `config` that re-resolves at most every 20 ms.
    fn client_with(config: &Config, resolver: Resolver, reconnect: Reconnect) -> GrpcClient {
        let mut client = GrpcClient::with_resolver(config, resolver, Duration::from_millis(20)).expect("client");
        Arc::get_mut(&mut client.inner).expect("not shared yet").reconnect = reconnect;
        client
    }

    fn fast_reconnect(backoff: impl Fn(u32) -> Duration + Send + Sync + 'static) -> Reconnect {
        Reconnect { backoff: Arc::new(backoff), ..Reconnect::default() }
    }

    async fn call(client: &GrpcClient) {
        let _ = client.connect(&call_context(), protos::ConnectionRequest::default()).await;
    }

    /// Waits (up to 10 s) until `done`.
    async fn until(what: &str, done: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(tokio::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn only_subchannel(client: &GrpcClient) -> Arc<Subchannel> {
        Arc::clone(&client.inner.subchannels.get().expect("initialized").load()[0])
    }

    /// The background goes on until an attempt succeeds, waiting longer each time.
    #[tokio::test]
    async fn keeps_reconnecting_with_growing_backoff() {
        let refusing = Refusing::new().await;
        let waits = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&waits);
        let reconnect = fast_reconnect(move |n| {
            seen.lock().expect("lock").push(n);
            Duration::from_millis(10)
        });
        let config = Config { host: refusing.addr.to_string(), ..Config::default() };
        let client = client_with(&config, system_resolver(), reconnect);
        call(&client).await;

        until("the background keeps retrying", || waits.lock().expect("lock").len() >= 5).await;
        assert_eq!([0, 1, 2, 3, 4], waits.lock().expect("lock")[..5]);
        client.close();
    }

    /// Calls use the connection the background opened.
    #[tokio::test]
    async fn calls_use_the_connection_opened_in_the_background() {
        let refusing = Refusing::new().await;
        let config = Config { host: refusing.addr.to_string(), ..Config::default() };
        let client = client_with(&config, system_resolver(), fast_reconnect(|_| Duration::from_millis(10)));
        call(&client).await;
        let subchannel = only_subchannel(&client);
        let refused = subchannel.channel.load_full();

        let listener = refusing.listen().await;
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            let mut conns = Vec::new();
            while let Ok((c, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                conns.push(c);
            }
        });
        until("the new connection is stored", || match (&refused, subchannel.channel.load_full()) {
            (Some(old), Some(new)) => !Arc::ptr_eq(old, &new),
            (old, new) => old.is_some() != new.is_some(),
        })
        .await;
        assert_eq!(Ok(()), client.ready());
        for _ in 0..3 {
            call(&client).await;
        }
        assert_eq!(1, accepts.load(Ordering::SeqCst), "every call uses the same connection");
        client.close();
        server.abort();
    }

    /// Reconnecting to an address ends when it is no longer listed.
    #[tokio::test]
    async fn does_not_reconnect_to_a_removed_address() {
        let refusing = Refusing::new().await;
        let (old, new) = (refusing.addr, backend().await);
        let current = Arc::new(Mutex::new(old));
        let lookups = Arc::new(AtomicUsize::new(0));
        let config = Config { host: "rpc.test:50051".into(), ..Config::default() };
        let resolver = counting_resolver(&current, &lookups);
        let client = client_with(&config, resolver, fast_reconnect(|_| Duration::from_millis(50)));
        call(&client).await;
        let removed = only_subchannel(&client);
        assert_eq!(Some(old), removed.addr);

        *current.lock().expect("lock") = new.addr;
        until("connected to the new address", || new.accepts.load(Ordering::SeqCst) > 0).await;
        let listener = refusing.listen().await;
        assert!(tokio::time::timeout(Duration::from_millis(500), listener.accept()).await.is_err());
        client.close();
        new.task.abort();
    }

    #[test]
    fn backoff_matches_grpc_defaults() {
        for (retries, expected) in [(0, 1.0), (1, 1.6), (2, 2.56), (20, 120.0)] {
            let d = exponential_backoff(retries).as_secs_f64();
            assert!(d >= expected * 0.8 - 1e-9 && d <= expected * 1.2 + 1e-9, "{retries}: {d}");
        }
    }
}
