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
}

/// A connection counted in [`State::active`] while open.
struct Counted {
    inner: ClientStream,
    state: Arc<State>,
    /// The peer closed the connection or it failed (as opposed to being
    /// dropped by this side).
    lost: bool,
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
    /// Stops the re-resolution task.
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

fn log(level: Level, msg: &str) {
    use cabby_core::logger::{Attr, LogValue};
    crate::metrics::log(
        level,
        msg,
        &[Attr::new("context", LogValue::String("rpc".into())), Attr::new("impl", LogValue::String("grpc".into()))],
    );
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

    fn subchannel(t: &Target, addr: Option<SocketAddr>) -> Arc<Subchannel> {
        Arc::new(Subchannel {
            authority: format!("{}:{}", t.host, t.port),
            host: bare_host(t),
            port: t.port,
            addr,
            channel: ArcSwapOption::empty(),
            failing: AtomicBool::new(false),
        })
    }

    /// Starts connecting `s` in the background (grpc-go connects every
    /// subchannel when the channel leaves idle or the address list changes).
    fn connect_in_background(&self, s: &Arc<Subchannel>) {
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

    /// First use: resolve, connect every subchannel, and start the DNS
    /// watcher.
    async fn init(self: &Arc<Self>) -> ArcSwap<Vec<Arc<Subchannel>>> {
        let mut subs = Vec::new();
        let mut resolved = true;
        for t in &self.targets {
            let addrs = if t.resolve { self.lookup(t).await } else { None };
            match addrs {
                Some(addrs) => subs.extend(addrs.into_iter().map(|a| Self::subchannel(t, Some(a)))),
                None => {
                    resolved &= !t.resolve;
                    // Dialed by name (resolution happens per connection attempt).
                    subs.push(Self::subchannel(t, None));
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
                    let s = Self::subchannel(t, Some(a));
                    self.connect_in_background(&s);
                    fresh.push(s);
                }
            }
        }
        current.store(Arc::new(fresh.clone()));
        for s in old.iter() {
            if !fresh.iter().any(|f| Arc::ptr_eq(f, s)) {
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
                let stream = match (tcp, tls) {
                    (Err(e), _) => Err(e),
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
                        state.active.fetch_add(1, Ordering::Relaxed);
                        Ok(TokioIo::new(Counted { inner, state, lost: false }))
                    }
                    Err(e) => {
                        s.failing.store(true, Ordering::Relaxed);
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
        let lazy = Arc::new(self.endpoint(chosen).connect_with_connector_lazy(self.connector(chosen)));
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
    /// connection attempt failed) one immediate attempt is allowed.
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

    #[test]
    fn backoff_matches_grpc_defaults() {
        for (retries, expected) in [(0, 1.0), (1, 1.6), (2, 2.56), (20, 120.0)] {
            let d = exponential_backoff(retries).as_secs_f64();
            assert!(d >= expected * 0.8 - 1e-9 && d <= expected * 1.2 + 1e-9, "{retries}: {d}");
        }
    }
}
