//! Server metrics (upstream package `metrics`): counters and gauges, interval
//! rotation, log/StatsD writers and the Prometheus endpoint.
//!
//! The registry is copy-on-write: registering a metric publishes a new name map
//! (rare, at startup), while lookups by name on the hot path read the current
//! map without taking a lock ([`arc_swap`]). Counters and gauges are plain
//! atomics.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use cabby_core::common::StringMap;
use cabby_core::logger::{Attr, Level, LogValue};
use tokio_util::sync::CancellationToken;

mod config;
mod counter;
mod custom_printer;
mod gauge;
mod printer;
mod prometheus;
mod statsd;

pub use config::{Config, StatsdConfig};
pub use counter::Counter;
pub use custom_printer::RubyPrinter;
pub use gauge::Gauge;
pub use printer::BasePrinter;
pub(crate) use printer::log;
pub use printer::{LogFormat, discard_logs, format_tint, init_logger, install_logger, log_enabled, set_log_attrs};
pub use statsd::StatsdWriter;

/// Default rotation interval (seconds).
pub const DEFAULT_ROTATE_INTERVAL: u64 = 15;
/// The connection cap of a dedicated metrics server when `server.max_conn` is
/// 0 (D-067).
pub const DEDICATED_MAX_CONN: usize = 1024;
/// How long a dedicated metrics server keeps an idle keep-alive connection
/// open (D-067).
pub const DEDICATED_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Prefix of every Prometheus metric name.
pub const PROMETHEUS_NAMESPACE: &str = "anycable_go";
/// The main HTTP server's default bind host (upstream `server.Host`).
pub const DEFAULT_SERVER_HOST: &str = "localhost";

/// Error reported by metrics writers and the metrics server.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MetricsError(pub String);

/// A periodic metrics writer (upstream `IntervalWriter`). [`Metrics::run`]
/// calls `run` once on a thread of its own, then `write` every interval on the
/// metrics task, which runs on the server's runtime: `write` must not block.
pub trait IntervalWriter: Send + Sync {
    /// Called once before the rotation loop with the interval in seconds.
    fn run(&self, interval: u64) -> Result<(), MetricsError>;
    fn stop(&self);
    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError>;
}

/// What the rest of the server reports to (upstream `Instrumenter`).
pub trait Instrumenter: Send + Sync {
    fn counter_increment(&self, name: &str);
    fn counter_add(&self, name: &str, value: u64);
    fn gauge_increment(&self, name: &str);
    fn gauge_decrement(&self, name: &str);
    fn gauge_set(&self, name: &str, value: u64);
    fn register_counter(&self, name: &str, desc: &str);
    fn register_gauge(&self, name: &str, desc: &str);
    /// Direct handle to a registered counter for lookup-free hot-path updates.
    fn counter_handle(&self, _name: &str) -> Option<Arc<Counter>> {
        None
    }
}

/// Discards everything (upstream `NoopMetrics`).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Instrumenter for NoopMetrics {
    fn counter_increment(&self, _name: &str) {}
    fn counter_add(&self, _name: &str, _value: u64) {}
    fn gauge_increment(&self, _name: &str) {}
    fn gauge_decrement(&self, _name: &str) {}
    fn gauge_set(&self, _name: &str, _value: u64) {}
    fn register_counter(&self, _name: &str, _desc: &str) {}
    fn register_gauge(&self, _name: &str, _desc: &str) {}
}

/// Upstream `NewCustomPrinter`: the mruby log formatter.
fn new_custom_printer(path: &str) -> Result<Arc<dyn IntervalWriter>, MetricsError> {
    Ok(Arc::new(RubyPrinter::new(path)?))
}

/// Where the Prometheus endpoint is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpEndpoint {
    /// Request path (`metrics.http_path`).
    pub path: String,
    pub port: i64,
    /// `Some(host)`: a dedicated server bound to `host:port`, run by
    /// [`Metrics::run`]. `None`: mounted on the main server for `port`
    /// (upstream `server.ForPort`), which the server owner does with
    /// [`Metrics::prometheus_handler`].
    pub dedicated_host: Option<String>,
}

type Registry<T> = ArcSwap<HashMap<String, Arc<T>>>;

/// What the Prometheus endpoint reads (shared with the dedicated server task).
pub(crate) struct Shared {
    pub(crate) counters: Registry<Counter>,
    pub(crate) gauges: Registry<Gauge>,
    pub(crate) tags: RwLock<Option<StringMap>>,
}

impl Shared {
    pub(crate) fn prometheus(&self) -> String {
        let tags = self.tags.read().unwrap_or_else(|e| e.into_inner());
        prometheus::render(&self.counters.load(), &self.gauges.load(), tags.as_ref())
    }
}

/// The metrics registry (upstream `Metrics`).
pub struct Metrics {
    shared: Arc<Shared>,
    /// Serializes registrations (copy-on-write of the maps) and rotation.
    registering: Mutex<()>,
    writers: Mutex<Vec<Arc<dyn IntervalWriter>>>,
    rotate_interval: Duration,
    http: Option<HttpEndpoint>,
    shutdown: CancellationToken,
    closed: Mutex<bool>,
    /// The dedicated server's bound address, once listening.
    server_addr: Mutex<Option<SocketAddr>>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

fn sorted<T>(map: &HashMap<String, Arc<T>>) -> Vec<&Arc<T>> {
    let mut items: Vec<(&String, &Arc<T>)> = map.iter().collect();
    items.sort_unstable_by(|a, b| a.0.cmp(b.0));
    items.into_iter().map(|(_, v)| v).collect()
}

impl Metrics {
    /// Upstream `NewMetrics(writers, rotateIntervalSeconds)`.
    pub fn new(writers: Vec<Arc<dyn IntervalWriter>>, rotate_interval_seconds: u64) -> Self {
        Self {
            shared: Arc::new(Shared {
                counters: ArcSwap::from_pointee(HashMap::new()),
                gauges: ArcSwap::from_pointee(HashMap::new()),
                tags: RwLock::new(None),
            }),
            registering: Mutex::new(()),
            writers: Mutex::new(writers),
            rotate_interval: Duration::from_secs(rotate_interval_seconds),
            http: None,
            shutdown: CancellationToken::new(),
            closed: Mutex::new(false),
            server_addr: Mutex::new(None),
        }
    }

    /// Upstream `NewFromConfig`: log printer, tags and the Prometheus HTTP server
    /// (dedicated when `config.host` differs from the main server host).
    pub fn from_config(config: &Config) -> Result<Self, MetricsError> {
        Self::from_config_with_server_host(config, DEFAULT_SERVER_HOST)
    }

    /// [`Metrics::from_config`] with the main server's bind host (upstream
    /// `server.Host`, set from `server.host` by the CLI).
    pub fn from_config_with_server_host(config: &Config, server_host: &str) -> Result<Self, MetricsError> {
        let mut writers: Vec<Arc<dyn IntervalWriter>> = Vec::new();
        if config.log_enabled() {
            if config.log_formatter_enabled() {
                writers.push(new_custom_printer(&config.log_formatter)?);
            } else {
                writers.push(Arc::new(BasePrinter::new(config.log_filter.clone())));
            }
        }

        let rotate = u64::try_from(config.rotate_interval).unwrap_or(0);
        let mut instance = Self::new(writers, rotate);
        if let Some(tags) = &config.tags {
            instance.default_tags(tags.clone());
        }
        if config.http_enabled() {
            let dedicated = !config.host.is_empty() && config.host != server_host;
            instance.http = Some(HttpEndpoint {
                path: config.http.clone(),
                port: config.port,
                dedicated_host: dedicated.then(|| config.host.clone()),
            });
        }
        Ok(instance)
    }

    /// The Prometheus endpoint, when enabled.
    pub fn http_endpoint(&self) -> Option<&HttpEndpoint> {
        self.http.as_ref()
    }

    /// The dedicated metrics server's address once it listens.
    pub fn server_addr(&self) -> Option<SocketAddr> {
        *self.server_addr.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn default_tags(&self, tags: StringMap) {
        *self.shared.tags.write().unwrap_or_else(|e| e.into_inner()) = Some(tags);
    }

    pub fn register_writer(&self, writer: Arc<dyn IntervalWriter>) {
        self.writers.lock().unwrap_or_else(|e| e.into_inner()).push(writer);
    }

    /// The dedicated metrics server, not started yet: an `HttpServer` (its
    /// header timeout, header limits and accept backoff) with the server's TLS
    /// settings, as upstream (`server.SSL`), `server.max_conn` connections or
    /// [`DEDICATED_MAX_CONN`] when that is 0, and a [`DEDICATED_IDLE_TIMEOUT`]
    /// for keep-alive connections (D-067).
    fn dedicated_server(
        &self,
        host: &str,
        port: i64,
        path: &str,
        defaults: &crate::server::ServerDefaults,
    ) -> Result<Arc<crate::server::HttpServer>, MetricsError> {
        let max_conn = if defaults.max_conn == 0 { DEDICATED_MAX_CONN } else { defaults.max_conn };
        let server = crate::server::HttpServer::new(host, &port.to_string(), defaults.ssl.as_ref(), max_conn)
            .map_err(|e| MetricsError(format!("metrics HTTP server at {host}:{port} stopped: {e}")))?;
        server.set_idle_timeout(DEDICATED_IDLE_TIMEOUT);
        let shared = Arc::clone(&self.shared);
        server.setup_handler(path, move |_req: crate::server::Request| {
            let shared = Arc::clone(&shared);
            async move {
                let resp = prometheus::response(shared.prometheus());
                let (parts, body) = resp.into_parts();
                http::Response::from_parts(parts, crate::server::full(body))
            }
        });
        Ok(server)
    }

    /// Starts the metrics server (if any) and the writers, then rotates every
    /// interval until [`Metrics::shutdown`]. Returns when shut down, when the
    /// dedicated server fails, or with the first writer's `run` error. Each
    /// writer's `run` is called off the runtime (`start_writer`), so a slow
    /// start holds no runtime worker; shutting down meanwhile returns at once.
    pub async fn run(&self) -> Result<(), MetricsError> {
        let mut has_server = false;
        if let Some(HttpEndpoint { path, port, dedicated_host: Some(host) }) = &self.http {
            has_server = true;
            let server = self.dedicated_server(host, *port, path, &crate::server::current_defaults())?;
            server.start().map_err(|e| MetricsError(format!("metrics HTTP server at {host}:{port} stopped: {e}")))?;
            let addr = server.local_addr();
            *self.server_addr.lock().unwrap_or_else(|e| e.into_inner()) = addr;
            let shown = addr.map_or_else(|| format!("{host}:{port}"), |a| a.to_string());
            printer::log(
                Level::Info,
                &format!("Serve metrics at {shown}{path}"),
                &[Attr::new("context", LogValue::String("metrics".into()))],
            );
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                shutdown.cancelled().await;
                server.shutdown();
            });
        }

        let writers: Vec<Arc<dyn IntervalWriter>> = self.writers.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if writers.is_empty() {
            printer::log(
                Level::Debug,
                "no metrics writers, disabling metrics rotation",
                &[Attr::new("context", LogValue::String("metrics".into()))],
            );
            if has_server {
                // No rotation to drive; keep running until shut down.
                self.shutdown.cancelled().await;
            }
            return Ok(());
        }

        let interval = if self.rotate_interval.is_zero() {
            Duration::from_secs(DEFAULT_ROTATE_INTERVAL)
        } else {
            self.rotate_interval
        };

        for writer in &writers {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                started = start_writer(Arc::clone(writer), interval.as_secs()) => started?,
            }
        }

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(interval) => {
                    if cabby_core::logger::enabled(Level::Debug) {
                        printer::log(
                            Level::Debug,
                            "rotate metrics",
                            &[
                                Attr::new("context", LogValue::String("metrics".into())),
                                Attr::new("interval", LogValue::String(go_duration_secs(interval.as_secs()))),
                            ],
                        );
                    }
                    self.rotate();
                    for writer in &writers {
                        if let Err(e) = writer.write(self) {
                            printer::log(
                                Level::Error,
                                "metrics writer failed to write",
                                &[
                                    Attr::new("context", LogValue::String("metrics".into())),
                                    Attr::new("error", LogValue::String(e.0)),
                                ],
                            );
                        }
                    }
                }
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), MetricsError> {
        {
            let mut closed = self.closed.lock().unwrap_or_else(|e| e.into_inner());
            if *closed {
                return Ok(());
            }
            *closed = true;
        }
        self.shutdown.cancel();
        let writers: Vec<Arc<dyn IntervalWriter>> = self.writers.lock().unwrap_or_else(|e| e.into_inner()).clone();
        for writer in writers {
            writer.stop();
        }
        Ok(())
    }

    pub fn counter(&self, name: &str) -> Option<Arc<Counter>> {
        self.shared.counters.load().get(name).cloned()
    }

    pub fn gauge(&self, name: &str) -> Option<Arc<Gauge>> {
        self.shared.gauges.load().get(name).cloned()
    }

    /// Visits the counters in name order.
    pub fn each_counter(&self, f: &mut dyn FnMut(&Counter)) {
        let counters = self.shared.counters.load();
        for c in sorted(&counters) {
            f(c);
        }
    }

    /// Visits the gauges in name order.
    pub fn each_gauge(&self, f: &mut dyn FnMut(&Gauge)) {
        let gauges = self.shared.gauges.load();
        for g in sorted(&gauges) {
            f(g);
        }
    }

    /// Counter interval values and gauge values by name.
    pub fn interval_snapshot(&self) -> HashMap<String, u64> {
        let counters = self.shared.counters.load();
        let gauges = self.shared.gauges.load();
        let mut snapshot = HashMap::with_capacity(counters.len() + gauges.len());
        for (name, c) in counters.iter() {
            snapshot.insert(name.clone(), c.interval_value());
        }
        for (name, g) in gauges.iter() {
            snapshot.insert(name.clone(), g.value());
        }
        snapshot
    }

    /// Updates every counter's interval delta (upstream unexported `rotate`).
    pub fn rotate(&self) {
        // Rotation and registration are serialized, as upstream (one lock).
        let _guard = self.registering.lock().unwrap_or_else(|e| e.into_inner());
        for c in self.shared.counters.load().values() {
            c.update_delta();
        }
    }

    /// Prometheus text exposition of all counters and gauges.
    pub fn prometheus(&self) -> String {
        self.shared.prometheus()
    }

    /// The Prometheus HTTP endpoint handler.
    pub fn prometheus_handler(&self, _request: &http::Request<()>) -> http::Response<String> {
        prometheus::response(self.prometheus())
    }

    fn register<T>(&self, registry: &Registry<T>, name: &str, metric: T) {
        let _guard = self.registering.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = HashMap::clone(&registry.load());
        // Re-registering replaces the metric, as upstream.
        next.insert(name.to_owned(), Arc::new(metric));
        registry.store(Arc::new(next));
    }
}

impl Instrumenter for Metrics {
    #[inline]
    fn counter_increment(&self, name: &str) {
        if let Some(c) = self.shared.counters.load().get(name) {
            c.inc();
        }
    }
    #[inline]
    fn counter_add(&self, name: &str, value: u64) {
        if let Some(c) = self.shared.counters.load().get(name) {
            c.add(value);
        }
    }
    #[inline]
    fn gauge_increment(&self, name: &str) {
        if let Some(g) = self.shared.gauges.load().get(name) {
            g.inc();
        }
    }
    #[inline]
    fn gauge_decrement(&self, name: &str) {
        if let Some(g) = self.shared.gauges.load().get(name) {
            g.dec();
        }
    }
    #[inline]
    fn gauge_set(&self, name: &str, value: u64) {
        if let Some(g) = self.shared.gauges.load().get(name) {
            g.set64(value);
        }
    }
    fn register_counter(&self, name: &str, desc: &str) {
        self.register(&self.shared.counters, name, Counter::new(name, desc));
    }
    fn register_gauge(&self, name: &str, desc: &str) {
        self.register(&self.shared.gauges, name, Gauge::new(name, desc));
    }
    fn counter_handle(&self, name: &str) -> Option<Arc<Counter>> {
        self.counter(name)
    }
}

/// Calls the writer's `run` on a thread of its own and waits for it without
/// holding a runtime worker: starting a writer may take long (the mruby
/// formatter runs its script's top-level code), as upstream, where only the
/// metrics goroutine waits. The thread has the runtime's context. A `run` that
/// never returns keeps its thread, but not the server or its shutdown: unlike
/// `spawn_blocking`, nothing waits for the thread when the runtime is dropped.
async fn start_writer(writer: Arc<dyn IntervalWriter>, interval: u64) -> Result<(), MetricsError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("metrics-writer".into())
        .spawn(move || {
            let _guard = runtime.enter();
            let _ = tx.send(writer.run(interval));
        })
        .map_err(|e| MetricsError(format!("starting a metrics writer: {e}")))?;
    rx.await.unwrap_or_else(|_| Err(MetricsError("a metrics writer stopped while starting".into())))
}

/// Go's `time.Duration.String()` for a whole number of seconds (`15s`, `1m0s`, `1h0m0s`).
fn go_duration_secs(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

/// A runtime like the server's with one worker, and a heartbeat on it: tests
/// that a metrics writer never blocks a runtime worker.
#[cfg(test)]
pub(crate) mod worker_tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    /// The longest pause between two heartbeats that counts as a live worker.
    pub(crate) const MAX_GAP: Duration = Duration::from_millis(250);

    /// A multi-thread runtime with a single worker (the server's on a one-CPU host).
    pub(crate) fn one_worker() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().expect("runtime")
    }

    /// A task on `rt` that reports a beat every 10 ms until the receiver is dropped.
    pub(crate) fn heartbeat(rt: &tokio::runtime::Runtime) -> mpsc::Receiver<Instant> {
        let (tx, rx) = mpsc::channel();
        rt.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if tx.send(Instant::now()).is_err() {
                    return;
                }
            }
        });
        rx
    }

    /// Discards the beats so far, then requires `n` more, each within
    /// [`MAX_GAP`] of the previous one.
    pub(crate) fn assert_beats(beats: &mpsc::Receiver<Instant>, n: usize) {
        while beats.try_recv().is_ok() {}
        let mut last = Instant::now();
        for i in 0..n {
            let beat = beats.recv_timeout(MAX_GAP).unwrap_or_else(|_| {
                panic!("heartbeat {i} of {n} missing: the runtime worker is blocked for over {MAX_GAP:?}")
            });
            assert!(beat.duration_since(last) < MAX_GAP, "heartbeat {i} came {:?} late", beat.duration_since(last));
            last = beat;
        }
    }
}

#[cfg(test)]
mod duration_tests {
    #[test]
    fn formats_like_go_durations() {
        assert_eq!("15s", super::go_duration_secs(15));
        assert_eq!("1m0s", super::go_duration_secs(60));
        assert_eq!("1h1m5s", super::go_duration_secs(3665));
        assert_eq!("0s", super::go_duration_secs(0));
    }
}

#[cfg(test)]
mod dedicated_server_tests {
    use std::io::Write as _;

    use super::*;
    use crate::server::tests::support::{connect, read_response, start};
    use crate::server::{ServerDefaults, SslConfig};

    fn get(addr: SocketAddr, path: &str) -> crate::server::tests::support::RawResponse {
        let mut stream = connect(addr);
        write!(stream, "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").unwrap();
        read_response(&mut stream)
    }

    /// Pins D-067: the dedicated listener serves the exposition at its path and
    /// 404 elsewhere, and holds at most 1024 connections when `server.max_conn`
    /// is 0, `server.max_conn` otherwise, closing idle keep-alive connections.
    #[test]
    fn deviation_d067_the_dedicated_metrics_server() {
        let metrics = Metrics::new(Vec::new(), 0);
        metrics.register_counter("clients_total", "Total clients");
        metrics.counter_add("clients_total", 3);

        let server = metrics.dedicated_server("127.0.0.1", 0, "/metrics", &ServerDefaults::default()).unwrap();
        assert_eq!(DEDICATED_MAX_CONN, server.max_conn());
        assert_eq!(1024, server.max_conn());
        assert_eq!(Some(DEDICATED_IDLE_TIMEOUT), server.idle_timeout());
        assert_eq!("http://127.0.0.1:0", server.address());
        let addr = start(&server);

        let ok = get(addr, "/metrics");
        assert_eq!(200, ok.status);
        assert_eq!(Some("text/plain; charset=utf-8"), ok.header("content-type"));
        let body = String::from_utf8(ok.body).unwrap();
        assert!(body.contains("anycable_go_clients_total"), "{body}");
        for path in ["/", "/metrics/x", "/health", "/other"] {
            assert_eq!(404, get(addr, path).status, "{path}");
        }
        server.shutdown();

        let limited = ServerDefaults { max_conn: 7, ..ServerDefaults::default() };
        let server = metrics.dedicated_server("127.0.0.1", 0, "/metrics", &limited).unwrap();
        assert_eq!(7, server.max_conn());
    }

    /// The dedicated listener uses the server's TLS settings, as upstream.
    #[test]
    fn the_dedicated_metrics_server_uses_the_server_tls_settings() {
        let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
        let ssl = SslConfig { cert_path: format!("{fixtures}/server.crt"), key_path: format!("{fixtures}/server.key") };
        let defaults = ServerDefaults { ssl: Some(ssl), ..ServerDefaults::default() };
        let metrics = Metrics::new(Vec::new(), 0);

        let server = metrics.dedicated_server("127.0.0.1", 0, "/metrics", &defaults).unwrap();

        assert_eq!("https://127.0.0.1:0", server.address());
    }
}
