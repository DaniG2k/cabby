//! Facade for Benchi (upstream `etc/benchi`), ported as the `cabby-bench` crate.
//!
//! * `Backend::Rust`: the Rust harness (`cabby_bench`), which drives the server under
//!   test ([`crate::sut::sut_binary`]) as an external process.
//! * `Backend::Go`: the upstream Go harness through `benchi-bridge` (one bridge
//!   process per test, see [`crate::benchi_bridge`]); Go objects are handles. Scenario
//!   entry points call the unexported upstream `run`/`parseFlags`/`runWithConfig`/
//!   `commandStall` through the scenario shims ([`benchi_bridge::shim_binary`]).
//!
//! Facades only marshal. Where Go cannot observe what a test asserts, the Go arm is
//! `unimplemented!("not_observable: ...")`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cabby_bench as rs;
use serde::Deserialize;
use serde_json::{Value, json};

pub use rs::client::{Message, Received};
pub use rs::reconnecting_client::{Backoff, ClientState, CloseReason};
pub use rs::rpc_server::{RpcMethod, RpcMethodStats};
pub use rs::server::ServerTuning;
pub use rs::throughput::{ReceiveSample, WindowStats};

use crate::benchi_bridge::Session;
use crate::{Backend, backend};

/// A Go object: its session and handle.
#[derive(Clone)]
pub struct GoObj {
    session: Arc<Session>,
    h: u64,
}

impl GoObj {
    fn new(session: Arc<Session>, h: u64) -> Self {
        Self { session, h }
    }

    /// Calls method `op` with `args` plus the receiver handle.
    fn call<R: serde::de::DeserializeOwned>(&self, op: &str, mut args: Value) -> R {
        args["H"] = json!(self.h);
        self.session.call(op, args)
    }
}

fn session() -> Arc<Session> {
    Session::current()
}

fn ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).expect("duration fits in int64 nanoseconds")
}

fn from_ns(n: i64) -> Duration {
    Duration::from_nanos(u64::try_from(n).expect("non-negative duration"))
}

fn index_of<T: PartialEq>(all: &[T], x: &T) -> usize {
    all.iter().position(|a| a == x).expect("enum value listed in ALL")
}

// ------------------------------------------------------------------ linkage

/// Upstream `TestModuleLinks`: the harness links the server and the client. Rust: the
/// external harness can execute the server binary (`--version`). Go: the bridge links
/// `cli.Embedded` and the forked client.
pub fn module_links() -> bool {
    match backend() {
        Backend::Rust => rs::server::Server::probe_binary(&crate::sut::sut_binary()).is_ok(),
        Backend::Go => session().call("ModuleLinks", json!({})),
    }
}

/// An unconnected forked-client instance (upstream `anycable.NewClient`).
pub enum CableClient {
    Rust(rs::cable::Client),
    Go(GoObj),
}

pub fn new_cable_client(url: &str) -> CableClient {
    match backend() {
        Backend::Rust => CableClient::Rust(rs::cable::Client::new(url)),
        Backend::Go => {
            let s = session();
            let h = s.call("cable.NewClient", json!({"URL": url}));
            CableClient::Go(GoObj::new(s, h))
        }
    }
}

// ------------------------------------------------------------------ counter.go

pub enum Accumulator {
    Rust(Arc<rs::counter::Accumulator>),
    Go(GoObj),
}

impl Accumulator {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Accumulator::Rust(Arc::new(rs::counter::Accumulator::new())),
            Backend::Go => {
                let s = session();
                let h = s.call("NewAccumulator", json!({}));
                Accumulator::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn bump(&self, client_id: &str) {
        match self {
            Accumulator::Rust(a) => a.bump(client_id),
            Accumulator::Go(g) => g.call("Accumulator.Bump", json!({"ID": client_id})),
        }
    }

    pub fn snapshot(&self) -> HashMap<String, i64> {
        match self {
            Accumulator::Rust(a) => a.snapshot(),
            Accumulator::Go(g) => g.call("Accumulator.Snapshot", json!({})),
        }
    }

    /// A snapshot the caller is going to mutate, to check it does not alias the
    /// accumulator's storage. Go: the map `Snapshot()` returned stays on the Go side
    /// behind a handle, so writes really land in it.
    pub fn snapshot_to_mutate(&self) -> MutableSnapshot {
        match self {
            Accumulator::Rust(a) => MutableSnapshot::Rust(a.snapshot()),
            Accumulator::Go(g) => {
                let h = g.call("Accumulator.SnapshotHandle", json!({}));
                MutableSnapshot::Go(GoObj::new(Arc::clone(&g.session), h))
            }
        }
    }
}

/// A snapshot map that stays where it was produced (upstream: the `map[string]int`
/// returned by `Snapshot`).
pub enum MutableSnapshot {
    Rust(HashMap<String, i64>),
    Go(GoObj),
}

impl MutableSnapshot {
    /// `snap[k] = v`.
    pub fn insert(&mut self, k: &str, v: i64) {
        match self {
            MutableSnapshot::Rust(m) => {
                m.insert(k.to_owned(), v);
            }
            MutableSnapshot::Go(g) => g.call("map.Set", json!({"K": k, "V": v})),
        }
    }
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------ throughput.go

/// A monotonic counter a [`ReceiveSampler`] polls (upstream: an `atomic.Int64`'s `Load`).
pub enum Counter {
    Rust(Arc<AtomicI64>),
    Go(GoObj),
}

impl Counter {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Counter::Rust(Arc::new(AtomicI64::new(0))),
            Backend::Go => {
                let s = session();
                let h = s.call("atomic.NewInt64", json!({}));
                Counter::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn store(&self, v: i64) {
        match self {
            Counter::Rust(n) => n.store(v, Ordering::SeqCst),
            Counter::Go(g) => g.call("atomic.Store", json!({"V": v})),
        }
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReceiveSampler {
    Rust(rs::throughput::ReceiveSampler),
    Go(GoObj),
}

#[derive(Deserialize)]
struct GoSample {
    t_nanos: i64,
    n: i64,
}

fn sample_json(s: &ReceiveSample) -> Value {
    let t = s.t.duration_since(UNIX_EPOCH).expect("sample after the Unix epoch");
    json!({"t_nanos": ns(t), "n": s.n})
}

impl ReceiveSampler {
    pub fn new(interval: Duration, source: &Counter) -> Self {
        match source {
            Counter::Rust(n) => {
                let n = Arc::clone(n);
                ReceiveSampler::Rust(rs::throughput::ReceiveSampler::new(
                    interval,
                    Arc::new(move || n.load(Ordering::SeqCst)),
                ))
            }
            Counter::Go(c) => {
                let h = c.session.call("NewReceiveSampler", json!({"IntervalNanos": ns(interval), "Source": c.h}));
                ReceiveSampler::Go(GoObj::new(Arc::clone(&c.session), h))
            }
        }
    }

    pub fn start(&self) {
        match self {
            ReceiveSampler::Rust(s) => s.start(),
            ReceiveSampler::Go(g) => g.call("ReceiveSampler.Start", json!({})),
        }
    }

    pub fn stop(&self) {
        match self {
            ReceiveSampler::Rust(s) => s.stop(),
            ReceiveSampler::Go(g) => g.call("ReceiveSampler.Stop", json!({})),
        }
    }

    pub fn samples(&self) -> Vec<ReceiveSample> {
        match self {
            ReceiveSampler::Rust(s) => s.samples(),
            ReceiveSampler::Go(g) => {
                let v: Vec<GoSample> = g.call("ReceiveSampler.Samples", json!({}));
                v.into_iter().map(|s| ReceiveSample { t: UNIX_EPOCH + from_ns(s.t_nanos), n: s.n }).collect()
            }
        }
    }
}

/// Upstream `Overall`. `None` stands for a nil slice.
pub fn overall(samples: Option<&[ReceiveSample]>) -> f64 {
    match backend() {
        Backend::Rust => rs::throughput::overall(samples.unwrap_or_default()),
        Backend::Go => session()
            .call("Overall", json!({"Samples": samples.map(|s| s.iter().map(sample_json).collect::<Vec<_>>())})),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoWindowStats {
    max: f64,
    min: f64,
    p50: f64,
    p95: f64,
    valid: bool,
}

/// Upstream `ComputeWindowStats`. `None` stands for a nil slice.
pub fn compute_window_stats(samples: Option<&[ReceiveSample]>, window: Duration) -> WindowStats {
    match backend() {
        Backend::Rust => rs::throughput::compute_window_stats(samples.unwrap_or_default(), window),
        Backend::Go => {
            let g: GoWindowStats = session().call(
                "ComputeWindowStats",
                json!({"Samples": samples.map(|s| s.iter().map(sample_json).collect::<Vec<_>>()),
                       "WindowNanos": ns(window)}),
            );
            WindowStats { max: g.max, min: g.min, p50: g.p50, p95: g.p95, valid: g.valid }
        }
    }
}

/// `time.Unix(0, 0)`.
pub fn unix_epoch() -> SystemTime {
    UNIX_EPOCH
}

// ------------------------------------------------------------------ histogram.go

pub enum Histogram {
    Rust(rs::histogram::Histogram),
    Go(GoObj),
}

#[derive(Deserialize)]
struct GoSnapshot {
    h: u64,
    count: i64,
    max_nanos: i64,
}

pub enum HistogramSnapshot {
    Rust(rs::histogram::HistogramSnapshot),
    Go { obj: GoObj, count: i64, max: Duration },
}

impl HistogramSnapshot {
    fn go(session: &Arc<Session>, s: GoSnapshot) -> Self {
        HistogramSnapshot::Go { obj: GoObj::new(Arc::clone(session), s.h), count: s.count, max: from_ns(s.max_nanos) }
    }

    pub fn count(&self) -> i64 {
        match self {
            HistogramSnapshot::Rust(s) => s.count,
            HistogramSnapshot::Go { count, .. } => *count,
        }
    }

    pub fn max(&self) -> Duration {
        match self {
            HistogramSnapshot::Rust(s) => s.max,
            HistogramSnapshot::Go { max, .. } => *max,
        }
    }

    pub fn quantile(&self, q: f64) -> Duration {
        match self {
            HistogramSnapshot::Rust(s) => s.quantile(q),
            HistogramSnapshot::Go { obj, .. } => from_ns(obj.call("HistogramSnapshot.Quantile", json!({"Q": q}))),
        }
    }
}

impl Histogram {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Histogram::Rust(rs::histogram::Histogram::new()),
            Backend::Go => {
                let s = session();
                let h = s.call("NewHistogram", json!({}));
                Histogram::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn record(&self, d: Duration) {
        match self {
            Histogram::Rust(h) => h.record(d),
            Histogram::Go(g) => g.call("Histogram.Record", json!({"Nanos": ns(d)})),
        }
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        match self {
            Histogram::Rust(h) => HistogramSnapshot::Rust(h.snapshot()),
            Histogram::Go(g) => HistogramSnapshot::go(&g.session, g.call("Histogram.Snapshot", json!({}))),
        }
    }

    pub fn swap(&self) -> HistogramSnapshot {
        match self {
            Histogram::Rust(h) => HistogramSnapshot::Rust(h.swap()),
            Histogram::Go(g) => HistogramSnapshot::go(&g.session, g.call("Histogram.Swap", json!({}))),
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------ server.go

/// Facade form of upstream `ServerConfig` (the fields the tests set).
#[derive(Debug, Clone, Default)]
pub struct ServerConfig {
    pub broker_adapter: String,
    pub rpc_host: Option<String>,
    /// Upstream `Configure`.
    pub tuning: ServerTuning,
}

pub enum Server {
    Rust(rs::server::Server),
    Go { obj: GoObj, websocket_url: String, broadcast_url: String },
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoBuilt {
    #[serde(default)]
    h: u64,
    #[serde(default, rename = "WebSocketURL")]
    websocket_url: String,
    #[serde(default, rename = "BroadcastURL")]
    broadcast_url: String,
    #[serde(default)]
    error: Option<String>,
}

fn rust_server_config(cfg: &ServerConfig) -> rs::server::ServerConfig {
    rs::server::ServerConfig {
        broker_adapter: cfg.broker_adapter.clone(),
        rpc_host: cfg.rpc_host.clone(),
        tuning: cfg.tuning.clone(),
        ..rs::server::ServerConfig::new(crate::sut::sut_binary())
    }
}

fn go_tuning(t: &ServerTuning) -> Value {
    let ServerTuning {
        ping_interval,
        rpc_concurrency,
        rpc_wait_timeout_ms,
        disconnect_rate,
        disconnect_backlog,
        stats_refresh_interval,
    } = t;
    assert!(
        rpc_concurrency.is_none()
            && rpc_wait_timeout_ms.is_none()
            && disconnect_rate.is_none()
            && disconnect_backlog.is_none()
            && stats_refresh_interval.is_none(),
        "benchi-bridge maps only ping_interval to ServerConfig.Configure"
    );
    json!({"PingInterval": ping_interval})
}

/// Upstream `BuildServer`.
pub fn build_server(cfg: ServerConfig) -> Result<Server, String> {
    match backend() {
        Backend::Rust => rs::server::Server::build(rust_server_config(&cfg)).map(Server::Rust).map_err(|e| e.0),
        Backend::Go => {
            let s = session();
            let b: GoBuilt = s.call(
                "BuildServer",
                json!({"BrokerAdapter": cfg.broker_adapter, "RPCHost": cfg.rpc_host.clone().unwrap_or_default(),
                       "Tuning": go_tuning(&cfg.tuning)}),
            );
            match b.error {
                Some(e) => Err(e),
                None => Ok(Server::Go {
                    obj: GoObj::new(s, b.h),
                    websocket_url: b.websocket_url,
                    broadcast_url: b.broadcast_url,
                }),
            }
        }
    }
}

/// Builds and shuts down a server with `cfg` and returns what the harness wrote to
/// stderr meanwhile.
///
/// Go: the bridge process's stderr (upstream redirects `os.Stderr`), read after a
/// sentinel has come through the pipe, so nothing written before it is missed.
/// Rust: the server is a separate process whose output is captured, so this checks
/// a harness default instead: that `ServerLog::Discard` forwards none of it to the
/// harness's stderr.
pub fn build_and_shutdown_stderr(cfg: ServerConfig) -> Result<String, String> {
    match backend() {
        Backend::Rust => {
            let srv = rs::server::Server::build(rust_server_config(&cfg)).map_err(|e| e.0)?;
            srv.shutdown().map_err(|e| e.0)?;
            Ok(srv.forwarded_output())
        }
        Backend::Go => {
            let srv = build_server(cfg)?;
            srv.shutdown()?;
            let Server::Go { obj, .. } = &srv else { unreachable!() };
            Ok(obj.session.stderr_flushed())
        }
    }
}

impl Server {
    pub fn websocket_url(&self) -> String {
        match self {
            Server::Rust(s) => s.websocket_url(),
            Server::Go { websocket_url, .. } => websocket_url.clone(),
        }
    }

    pub fn broadcast_url(&self) -> String {
        match self {
            Server::Rust(s) => s.broadcast_url(),
            Server::Go { broadcast_url, .. } => broadcast_url.clone(),
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match self {
            Server::Rust(s) => s.shutdown().map_err(|e| e.0),
            Server::Go { obj, .. } => obj.call::<Option<String>>("Server.Shutdown", json!({})).map_or(Ok(()), Err),
        }
    }
}

// ------------------------------------------------------------------ client.go

pub enum Client {
    Rust(Box<rs::client::Client>),
    Go(GoObj),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoHandleOrError {
    #[serde(default)]
    h: u64,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    is_nil: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoReceived {
    stream: String,
    data: String,
    ok: bool,
}

/// Upstream `BuildClient`.
pub fn build_client(url: &str) -> Result<Client, String> {
    match backend() {
        Backend::Rust => rs::client::Client::build(url).map(|c| Client::Rust(Box::new(c))).map_err(|e| e.0),
        Backend::Go => {
            let s = session();
            let r: GoHandleOrError = s.call("BuildClient", json!({"URL": url}));
            match r.error {
                Some(e) => Err(e),
                None => Ok(Client::Go(GoObj::new(s, r.h))),
            }
        }
    }
}

impl Client {
    /// `timeout`: upstream's context deadline (`None`: `context.Background()`).
    pub fn connect(&self, timeout: Option<Duration>) -> Result<(), String> {
        match self {
            Client::Rust(c) => c.connect(timeout).map_err(|e| e.0),
            Client::Go(g) => g
                .call::<Option<String>>("Client.Connect", json!({"TimeoutNanos": timeout.map_or(0, ns)}))
                .map_or(Ok(()), Err),
        }
    }

    pub fn subscribe(&self, streams: &[&str]) -> Result<(), String> {
        match self {
            Client::Rust(c) => {
                let streams: Vec<String> = streams.iter().map(|s| (*s).to_owned()).collect();
                c.subscribe(&streams).map_err(|e| e.0)
            }
            Client::Go(g) => {
                g.call::<Option<String>>("Client.Subscribe", json!({"Streams": streams})).map_or(Ok(()), Err)
            }
        }
    }

    /// Upstream `Receive` bounded by `timeout` (the tests' `receiveWithin`).
    pub fn receive_within(&self, timeout: Duration) -> Received {
        match self {
            Client::Rust(c) => c.receive_timeout(timeout),
            Client::Go(g) => {
                let v = g
                    .session
                    .call_within("Client.Receive", json!({"H": g.h}), timeout)
                    .unwrap_or_else(|e| panic!("{e:#}"));
                match v {
                    None => Received::TimedOut,
                    Some(v) => {
                        let r: GoReceived = serde_json::from_value(v).expect("Client.Receive result");
                        if r.ok {
                            Received::Message(Message { stream: r.stream, data: r.data })
                        } else {
                            assert!(r.stream.is_empty() && r.data.is_empty(), "closed receive returned a message");
                            Received::Closed
                        }
                    }
                }
            }
        }
    }

    pub fn close(&self) {
        match self {
            Client::Rust(c) => c.close(),
            Client::Go(g) => g.call("Client.Close", json!({})),
        }
    }
}

// ------------------------------------------------------------------ pool.go

#[derive(Default)]
pub struct PoolConfig<'a> {
    pub server_url: String,
    pub c: usize,
    pub s: usize,
    pub big_s: usize,
    pub tolerance: usize,
    pub seed: u64,
    pub accumulator: Option<&'a Accumulator>,
    pub streams: Option<Vec<Vec<String>>>,
}

pub enum ClientPool {
    Rust(rs::pool::ClientPool),
    Go(GoObj),
}

/// Upstream `BuildPool` returns `(*ClientPool, error)`; both are kept.
pub struct PoolBuild {
    pub pool: Option<ClientPool>,
    pub error: Option<String>,
}

pub fn build_pool(cfg: PoolConfig<'_>) -> PoolBuild {
    match backend() {
        Backend::Rust => {
            let accumulator = cfg.accumulator.map(|a| match a {
                Accumulator::Rust(a) => Arc::clone(a),
                Accumulator::Go(_) => panic!("Go accumulator with the Rust backend"),
            });
            let r = rs::pool::ClientPool::build(rs::pool::PoolConfig {
                server_url: cfg.server_url,
                c: cfg.c,
                s: cfg.s,
                big_s: cfg.big_s,
                tolerance: cfg.tolerance,
                seed: cfg.seed,
                accumulator,
                ledger: None,
                streams: cfg.streams,
                on_connect: None,
                on_subscribe: None,
                wire: Default::default(),
            });
            match r {
                Ok(p) => PoolBuild { pool: Some(ClientPool::Rust(p)), error: None },
                Err(e) => PoolBuild { pool: None, error: Some(e.0) },
            }
        }
        Backend::Go => {
            let s = session();
            let acc = cfg.accumulator.map(|a| match a {
                Accumulator::Go(g) => g.h,
                Accumulator::Rust(_) => panic!("Rust accumulator with the Go backend"),
            });
            let r: GoHandleOrError = s.call(
                "BuildPool",
                json!({"ServerURL": cfg.server_url, "C": cfg.c, "S": cfg.s, "BigS": cfg.big_s,
                       "Tolerance": cfg.tolerance, "Seed": cfg.seed, "Accumulator": acc, "Streams": cfg.streams}),
            );
            match r.error {
                Some(e) => PoolBuild {
                    pool: if r.is_nil { None } else { Some(ClientPool::Go(GoObj::new(s, 0))) },
                    error: Some(e),
                },
                None => PoolBuild { pool: Some(ClientPool::Go(GoObj::new(s, r.h))), error: None },
            }
        }
    }
}

impl ClientPool {
    pub fn size(&self) -> usize {
        match self {
            ClientPool::Rust(p) => p.size(),
            ClientPool::Go(g) => g.call("ClientPool.Size", json!({})),
        }
    }

    pub fn streams(&self) -> Vec<Vec<String>> {
        match self {
            ClientPool::Rust(p) => p.streams(),
            ClientPool::Go(g) => g.call("ClientPool.Streams", json!({})),
        }
    }

    pub fn close(&self) {
        match self {
            ClientPool::Rust(p) => p.close(),
            ClientPool::Go(g) => g.call("ClientPool.Close", json!({})),
        }
    }
}

/// Upstream `BuildStreamSubsets`.
pub fn build_stream_subsets(seed: u64, c: usize, s: usize, big_s: usize) -> Vec<Vec<String>> {
    match backend() {
        Backend::Rust => rs::pool::build_stream_subsets(seed, c, s, big_s),
        Backend::Go => session().call("BuildStreamSubsets", json!({"Seed": seed, "C": c, "S": s, "BigS": big_s})),
    }
}

// ------------------------------------------------------------------ publisher.go

/// Upstream `Option`s: `None` leaves the default (the option is not passed).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublisherOptions {
    pub workers: Option<i64>,
    pub batch_size: Option<i64>,
}

pub enum Publisher {
    Rust(rs::publisher::Publisher),
    Go(GoObj),
}

impl Publisher {
    /// Upstream `NewPublisher(url, maxInflight, opts...)`.
    pub fn new(url: &str, max_inflight: i64, opts: PublisherOptions) -> Self {
        match backend() {
            Backend::Rust => {
                let mut o = rs::publisher::PublisherOptions::default();
                if let Some(w) = opts.workers {
                    o = o.with_workers(w);
                }
                if let Some(b) = opts.batch_size {
                    o = o.with_batch_size(b);
                }
                Publisher::Rust(rs::publisher::Publisher::new(url, max_inflight, o))
            }
            Backend::Go => {
                let s = session();
                let h = s.call(
                    "NewPublisher",
                    json!({"URL": url, "MaxInflight": max_inflight, "Workers": opts.workers, "BatchSize": opts.batch_size}),
                );
                Publisher::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn publish(&self, stream: &str, data: &str) {
        match self {
            Publisher::Rust(p) => p.publish(stream, data),
            Publisher::Go(g) => g.call("Publisher.Publish", json!({"Stream": stream, "Data": data})),
        }
    }

    pub fn issued_count(&self) -> i64 {
        match self {
            Publisher::Rust(p) => p.issued_count(),
            Publisher::Go(g) => g.call("Publisher.IssuedCount", json!({})),
        }
    }

    pub fn dropped_count(&self) -> i64 {
        match self {
            Publisher::Rust(p) => p.dropped_count(),
            Publisher::Go(g) => g.call("Publisher.DroppedCount", json!({})),
        }
    }

    pub fn completed_count(&self) -> i64 {
        match self {
            Publisher::Rust(p) => p.completed_count(),
            Publisher::Go(g) => g.call("Publisher.CompletedCount", json!({})),
        }
    }

    pub fn target_counts(&self) -> HashMap<String, i64> {
        match self {
            Publisher::Rust(p) => p.target_counts(),
            Publisher::Go(g) => g.call("Publisher.TargetCounts", json!({})),
        }
    }

    pub fn close(&self) {
        match self {
            Publisher::Rust(p) => p.close(),
            Publisher::Go(g) => g.call("Publisher.Close", json!({})),
        }
    }
}

// ------------------------------------------------------------------ rpc_server.go

pub type SessionClosedFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct RpcServerConfig {
    pub latency: Duration,
    pub connect_latency: Duration,
    pub command_latency: Duration,
    pub disconnect_latency: Duration,
    pub jitter: f64,
    pub session_closed: Option<SessionClosedFn>,
}

pub enum RpcServer {
    Rust(rs::rpc_server::RpcServer),
    Go { obj: GoObj, url: String, callback: Option<u64> },
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoRpcServer {
    h: u64,
    #[serde(rename = "URL")]
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoRpcStats {
    started: i64,
    completed: i64,
    in_flight: i64,
    after_close: i64,
}

impl RpcServer {
    /// Upstream `NewRPCServer`. Go: `session_closed` becomes a bridge callback.
    pub fn new(cfg: RpcServerConfig) -> Self {
        match backend() {
            Backend::Rust => RpcServer::Rust(rs::rpc_server::RpcServer::new(rs::rpc_server::RpcServerConfig {
                latency: cfg.latency,
                connect_latency: cfg.connect_latency,
                command_latency: cfg.command_latency,
                disconnect_latency: cfg.disconnect_latency,
                jitter: cfg.jitter,
                session_closed: cfg.session_closed,
            })),
            Backend::Go => {
                let s = session();
                let callback = cfg.session_closed.map(|f| {
                    s.register_callback(move |args| {
                        let sid = args.as_str().expect("SessionClosed argument is a string");
                        json!(f(sid))
                    })
                });
                let r: GoRpcServer = s.call(
                    "NewRPCServer",
                    json!({"LatencyNanos": ns(cfg.latency), "ConnectLatencyNanos": ns(cfg.connect_latency),
                           "CommandLatencyNanos": ns(cfg.command_latency),
                           "DisconnectLatencyNanos": ns(cfg.disconnect_latency),
                           "Jitter": cfg.jitter, "SessionClosed": callback}),
                );
                RpcServer::Go { obj: GoObj::new(s, r.h), url: r.url, callback }
            }
        }
    }

    pub fn url(&self) -> String {
        match self {
            RpcServer::Rust(s) => s.url(),
            RpcServer::Go { url, .. } => url.clone(),
        }
    }

    pub fn close(&self) {
        match self {
            RpcServer::Rust(s) => s.close(),
            RpcServer::Go { obj, .. } => obj.call("RPCServer.Close", json!({})),
        }
    }

    pub fn stats(&self, m: RpcMethod) -> RpcMethodStats {
        match self {
            RpcServer::Rust(s) => s.stats(m),
            RpcServer::Go { obj, .. } => {
                let g: GoRpcStats = obj.call("RPCServer.Stats", json!({"Method": index_of(&RpcMethod::ALL, &m)}));
                RpcMethodStats {
                    started: g.started,
                    completed: g.completed,
                    in_flight: g.in_flight,
                    after_close: g.after_close,
                }
            }
        }
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        // The callback may capture objects of this session; unregistering breaks the
        // session -> callback -> object -> session cycle.
        if let RpcServer::Go { obj, callback: Some(cb), .. } = self {
            obj.session.unregister_callback(*cb);
        }
    }
}

// ------------------------------------------------------------------ reconnecting_client.go

pub enum ClientStats {
    Rust(Arc<rs::reconnecting_client::ClientStats>),
    Go(GoObj),
}

impl ClientStats {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => ClientStats::Rust(Arc::new(rs::reconnecting_client::ClientStats::new())),
            Backend::Go => {
                let s = session();
                let h = s.call("NewClientStats", json!({}));
                ClientStats::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn state(&self, st: ClientState) -> i64 {
        match self {
            ClientStats::Rust(s) => s.state(st),
            ClientStats::Go(g) => g.call("ClientStats.State", json!({"Arg": index_of(&ClientState::ALL, &st)})),
        }
    }

    pub fn reason(&self, r: CloseReason) -> i64 {
        match self {
            ClientStats::Rust(s) => s.reason(r),
            ClientStats::Go(g) => g.call("ClientStats.Reason", json!({"Arg": index_of(&CloseReason::ALL, &r)})),
        }
    }

    pub fn attempts(&self) -> i64 {
        match self {
            ClientStats::Rust(s) => s.attempts(),
            ClientStats::Go(g) => g.call("ClientStats.Attempts", json!({})),
        }
    }

    pub fn subscribe_retries(&self) -> i64 {
        match self {
            ClientStats::Rust(s) => s.subscribe_retries(),
            ClientStats::Go(g) => g.call("ClientStats.SubscribeRetries", json!({})),
        }
    }

    pub fn session_closed(&self, sid: &str) -> bool {
        match self {
            ClientStats::Rust(s) => s.session_closed(sid),
            ClientStats::Go(g) => g.call("ClientStats.SessionClosed", json!({"Sid": sid})),
        }
    }

    fn histogram(
        &self,
        name: &str,
        pick: fn(&rs::reconnecting_client::ClientStats) -> &rs::histogram::Histogram,
    ) -> HistogramSnapshot {
        match self {
            ClientStats::Rust(s) => HistogramSnapshot::Rust(pick(s).snapshot()),
            ClientStats::Go(g) => {
                HistogramSnapshot::go(&g.session, g.call("ClientStats.Histogram", json!({"Name": name})))
            }
        }
    }

    /// `stats.Welcome.Snapshot()`.
    pub fn welcome(&self) -> HistogramSnapshot {
        self.histogram("Welcome", |s| &s.welcome)
    }

    /// `stats.Confirm.Snapshot()`.
    pub fn confirm(&self) -> HistogramSnapshot {
        self.histogram("Confirm", |s| &s.confirm)
    }

    /// `stats.Probe.Snapshot()`.
    pub fn probe(&self) -> HistogramSnapshot {
        self.histogram("Probe", |s| &s.probe)
    }
}

impl Default for ClientStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Cancellation for [`ReconnectingClient::run`] (upstream `context.WithCancel`).
pub enum RunContext {
    Rust(rs::cancel::CancelToken),
    Go(GoObj),
}

impl RunContext {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => RunContext::Rust(rs::cancel::CancelToken::new()),
            Backend::Go => {
                let s = session();
                let h = s.call("context.WithCancel", json!({}));
                RunContext::Go(GoObj::new(s, h))
            }
        }
    }

    pub fn cancel(&self) {
        match self {
            RunContext::Rust(t) => t.cancel(),
            RunContext::Go(g) => g.call("context.Cancel", json!({})),
        }
    }
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ReconnectingClientConfig<'a> {
    pub url: String,
    pub channels: Vec<String>,
    pub sequential_subscribe: bool,
    pub subscribe_retry_interval: Duration,
    pub subscribe_retries_max: u32,
    pub ping_timeout: Duration,
    pub probe_interval: Duration,
    pub backoff: Backoff,
    pub stats: &'a ClientStats,
    pub seed: u64,
}

pub enum ReconnectingClient {
    Rust(rs::reconnecting_client::ReconnectingClient),
    Go(GoObj),
}

/// Deadline of a `Run` call: it returns only when its context is cancelled.
const RUN_DEADLINE: Duration = Duration::from_secs(3600);

impl ReconnectingClient {
    pub fn new(cfg: ReconnectingClientConfig<'_>) -> Self {
        match cfg.stats {
            ClientStats::Rust(stats) => ReconnectingClient::Rust(rs::reconnecting_client::ReconnectingClient::new(
                rs::reconnecting_client::ReconnectingClientConfig {
                    url: cfg.url,
                    channels: cfg.channels,
                    sequential_subscribe: cfg.sequential_subscribe,
                    subscribe_retry_interval: cfg.subscribe_retry_interval,
                    subscribe_retries_max: cfg.subscribe_retries_max,
                    ping_timeout: cfg.ping_timeout,
                    probe_interval: cfg.probe_interval,
                    backoff: cfg.backoff,
                    stats: Arc::clone(stats),
                    seed: cfg.seed,
                    wire: Default::default(),
                    keepalive: Duration::ZERO,
                },
            )),
            ClientStats::Go(stats) => {
                let h = stats.session.call(
                    "NewReconnectingClient",
                    json!({"URL": cfg.url, "Channels": cfg.channels, "SequentialSubscribe": cfg.sequential_subscribe,
                           "SubscribeRetryIntervalNanos": ns(cfg.subscribe_retry_interval),
                           "SubscribeRetriesMax": cfg.subscribe_retries_max,
                           "PingTimeoutNanos": ns(cfg.ping_timeout), "ProbeIntervalNanos": ns(cfg.probe_interval),
                           "BackoffMinNanos": ns(cfg.backoff.min), "BackoffMaxNanos": ns(cfg.backoff.max),
                           "Stats": stats.h, "Seed": cfg.seed}),
                );
                ReconnectingClient::Go(GoObj::new(Arc::clone(&stats.session), h))
            }
        }
    }

    /// Blocks until `ctx` is cancelled.
    pub fn run(&self, ctx: &RunContext, initial_delay: Duration) {
        match (self, ctx) {
            (ReconnectingClient::Rust(c), RunContext::Rust(t)) => c.run(t, initial_delay),
            (ReconnectingClient::Go(g), RunContext::Go(ctx)) => {
                g.session
                    .call_within(
                        "ReconnectingClient.Run",
                        json!({"H": g.h, "Ctx": ctx.h, "InitialNanos": ns(initial_delay)}),
                        RUN_DEADLINE,
                    )
                    .unwrap_or_else(|e| panic!("{e:#}"))
                    .expect("ReconnectingClient.Run returned after its context was cancelled");
            }
            _ => panic!("client and context from different backends"),
        }
    }

    /// Upstream `Drop`.
    pub fn drop_connection(&self) -> bool {
        match self {
            ReconnectingClient::Rust(c) => c.drop_connection(),
            ReconnectingClient::Go(g) => g.call("ReconnectingClient.Drop", json!({})),
        }
    }
}

// ------------------------------------------------------------------ scenarios

/// Exit code and output of a scenario run.
#[derive(Debug, Clone)]
pub struct ScenarioOutput {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Bounds a whole scenario run.
const SCENARIO_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoScenarioOutput {
    code: i32,
    stdout: String,
    stderr: String,
}

/// Calls a scenario shim op that returns `{Code, Stdout, Stderr}`.
fn run_shim(session: &Session, op: &str, args: Value) -> ScenarioOutput {
    let v = session
        .call_within(op, args, SCENARIO_DEADLINE)
        .unwrap_or_else(|e| panic!("{e:#}"))
        .unwrap_or_else(|| panic!("scenario {op} still running after {SCENARIO_DEADLINE:?}"));
    let o: GoScenarioOutput = serde_json::from_value(v).expect("scenario output");
    ScenarioOutput { code: o.code, stdout: o.stdout, stderr: o.stderr }
}

fn owned(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| (*a).to_owned()).collect()
}

fn run_rust_scenario(f: impl FnOnce(&mut Vec<u8>, &mut Vec<u8>) -> i32) -> ScenarioOutput {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = f(&mut out, &mut err);
    ScenarioOutput {
        code,
        stdout: String::from_utf8_lossy(&out).into_owned(),
        stderr: String::from_utf8_lossy(&err).into_owned(),
    }
}

pub mod stress_publications {
    use super::*;

    /// A pool client as seen by a setup hook.
    pub enum HookClient<'a> {
        Rust(&'a rs::client::Client),
        Go(GoObj),
    }

    impl HookClient<'_> {
        pub fn close(&self) {
            match self {
                HookClient::Rust(c) => c.close(),
                HookClient::Go(g) => g.call("Client.Close", json!({})),
            }
        }
    }

    /// The pool as seen by a setup hook (upstream `setupHook`'s `*lib.ClientPool`).
    pub enum HookPool<'a> {
        Rust(&'a rs::pool::ClientPool),
        Go(GoObj),
    }

    impl HookPool<'_> {
        /// Upstream `pool.Each`, in pool order.
        pub fn each(&self, f: &mut dyn FnMut(&HookClient<'_>)) {
            match self {
                HookPool::Rust(p) => p.each(&mut |c| f(&HookClient::Rust(c))),
                HookPool::Go(g) => {
                    let clients: Vec<u64> = g.call("ClientPool.Clients", json!({}));
                    for h in clients {
                        f(&HookClient::Go(GoObj::new(Arc::clone(&g.session), h)));
                    }
                }
            }
        }
    }

    /// `Send`: on Go the hook runs as a bridge callback on another thread.
    pub type SetupHook = Box<dyn FnOnce(&HookPool<'_>) + Send>;

    const NAME: &str = "stress_publications";

    /// Builds what a run needs ahead of time (Go: the scenario shim), so tests that
    /// time `run` measure the scenario, not a one-time `go test -c`.
    pub fn prepare() {
        if backend() == Backend::Go {
            crate::benchi_bridge::shim_binary(NAME);
        }
    }

    /// Upstream `runOpts`.
    #[derive(Default)]
    pub struct RunOpts {
        pub stream_subsets: Option<Vec<Vec<String>>>,
        pub broadcast_url: Option<String>,
        pub setup_hook: Option<SetupHook>,
    }

    /// Upstream `run(args, stdout, stderr, opts)`.
    pub fn run(args: &[&str], opts: RunOpts) -> ScenarioOutput {
        match backend() {
            Backend::Rust => {
                let hook = opts.setup_hook.map(|h| -> rs::scenarios::stress_publications::SetupHook {
                    Box::new(move |_server, pool, _publisher| h(&HookPool::Rust(pool)))
                });
                let opts = rs::scenarios::stress_publications::RunOpts {
                    stream_subsets: opts.stream_subsets,
                    broadcast_url: opts.broadcast_url,
                    setup_hook: hook,
                    sut: Some(crate::sut::sut_binary()),
                };
                run_rust_scenario(|out, err| rs::scenarios::stress_publications::run(&owned(args), out, err, opts))
            }
            Backend::Go => {
                let session = Session::shim(NAME);
                let hook = opts.setup_hook.map(|h| {
                    let h = Mutex::new(Some(h));
                    let weak = Arc::downgrade(&session);
                    session.register_callback(move |args| {
                        let session = weak.upgrade().expect("shim session alive during run");
                        let pool = args["Pool"].as_u64().expect("setup hook pool handle");
                        let hook = h.lock().unwrap_or_else(|e| e.into_inner()).take().expect("setup hook runs once");
                        hook(&HookPool::Go(GoObj::new(session, pool)));
                        json!(true)
                    })
                });
                run_shim(
                    &session,
                    "Run",
                    json!({"Args": args, "StreamSubsets": opts.stream_subsets,
                           "BroadcastURL": opts.broadcast_url.unwrap_or_default(), "SetupHook": hook}),
                )
            }
        }
    }
}

pub mod reconnect_floods {
    use super::*;

    pub use rs::scenarios::reconnect_floods::Sample;

    const NAME: &str = "reconnect_floods";

    /// Builds what a run needs ahead of time (Go: the scenario shim), so tests that
    /// time `run` measure the scenario, not a one-time `go test -c`.
    pub fn prepare() {
        if backend() == Backend::Go {
            crate::benchi_bridge::shim_binary(NAME);
        }
    }

    /// Upstream `run(args, stdout, stderr)`.
    pub fn run(args: &[&str]) -> ScenarioOutput {
        match backend() {
            Backend::Rust => {
                let mut cfg_args = owned(args);
                cfg_args.push(format!("--sut={}", crate::sut::sut_binary().display()));
                run_rust_scenario(|out, err| rs::scenarios::reconnect_floods::run(&cfg_args, out, err))
            }
            Backend::Go => run_shim(&Session::shim(NAME), "Run", json!({ "Args": args })),
        }
    }

    /// Upstream `parseFlags(args)` + `runWithConfig(ctx, ...)` with a context that is
    /// done after `after`.
    pub fn run_interrupted_after(args: &[&str], after: Duration) -> ScenarioOutput {
        match backend() {
            Backend::Rust => {
                let mut cfg_args = owned(args);
                cfg_args.push(format!("--sut={}", crate::sut::sut_binary().display()));
                run_rust_scenario(|out, err| {
                    let cfg = rs::scenarios::reconnect_floods::parse_flags(&cfg_args, err).expect("flags parse");
                    let interrupt = rs::cancel::CancelToken::with_timeout(after);
                    rs::scenarios::reconnect_floods::run_with_config(&interrupt, cfg, out, err)
                })
            }
            Backend::Go => {
                run_shim(&Session::shim(NAME), "RunWithTimeout", json!({"Args": args, "TimeoutNanos": ns(after)}))
            }
        }
    }

    /// The parsed fields upstream `TestParseFlags_Waves` checks.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ParsedFlags {
        pub waves: Vec<usize>,
        pub s: usize,
        pub rpc_latency: Duration,
    }

    /// Upstream `parseFlags(args, stderr)`.
    pub fn parse_flags(args: &[&str]) -> Result<ParsedFlags, String> {
        match backend() {
            Backend::Rust => {
                let mut sink = Vec::new();
                rs::scenarios::reconnect_floods::parse_flags(&owned(args), &mut sink).map(|c| ParsedFlags {
                    waves: c.waves,
                    s: c.s,
                    rpc_latency: c.rpc_latency,
                })
            }
            Backend::Go => {
                #[derive(Deserialize)]
                #[serde(rename_all = "PascalCase")]
                struct GoFlags {
                    #[serde(default)]
                    waves: Vec<usize>,
                    #[serde(default)]
                    s: usize,
                    #[serde(default, rename = "RPCLatencyNanos")]
                    rpc_latency_nanos: i64,
                    #[serde(default)]
                    error: Option<String>,
                }
                let f: GoFlags = Session::shim(NAME).call("ParseFlags", json!({ "Args": args }));
                match f.error {
                    Some(e) => Err(e),
                    None => Ok(ParsedFlags { waves: f.waves, s: f.s, rpc_latency: from_ns(f.rpc_latency_nanos) }),
                }
            }
        }
    }

    /// Upstream `commandStall(samples)`.
    pub fn command_stall(samples: &[Sample]) -> (Duration, Duration) {
        match backend() {
            Backend::Rust => rs::scenarios::reconnect_floods::command_stall(samples),
            Backend::Go => {
                #[derive(Deserialize)]
                #[serde(rename_all = "PascalCase")]
                struct GoStall {
                    current_nanos: i64,
                    longest_nanos: i64,
                }
                // Only the fields commandStall reads: at, states, rpcDone.
                let samples: Vec<Value> = samples
                    .iter()
                    .map(|s| json!({"AtNanos": ns(s.at), "States": s.states, "RPCDone": s.rpc_done}))
                    .collect();
                let r: GoStall = Session::shim(NAME).call("CommandStall", json!({ "Samples": samples }));
                (from_ns(r.current_nanos), from_ns(r.longest_nanos))
            }
        }
    }
}

/// Polls `cond` every `tick` until it holds or `timeout` passes (testify's
/// `require.Eventually`). Returns whether it held.
pub fn eventually(timeout: Duration, tick: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(tick);
    }
}
