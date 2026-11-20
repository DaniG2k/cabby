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
/// Prefix of every Prometheus metric name.
pub const PROMETHEUS_NAMESPACE: &str = "anycable_go";
/// The main HTTP server's default bind host (upstream `server.Host`).
pub const DEFAULT_SERVER_HOST: &str = "localhost";

/// Error reported by metrics writers and the metrics server.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MetricsError(pub String);

/// A periodic metrics writer (upstream `IntervalWriter`).
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

    /// Starts the metrics server (if any) and the writers, then rotates every
    /// interval until [`Metrics::shutdown`]. Returns when shut down or the
    /// dedicated server fails.
    pub async fn run(&self) -> Result<(), MetricsError> {
        let mut has_server = false;
        if let Some(HttpEndpoint { path, port, dedicated_host: Some(host) }) = &self.http {
            has_server = true;
            // The server runs in its own task (it never blocks rotation); a bind
            // failure ends `run`, as upstream's server error does.
            let listener = prometheus::bind(host, *port)
                .await
                .map_err(|e| MetricsError(format!("metrics HTTP server at {host}:{port} stopped: {e}")))?;
            let addr = listener.local_addr().ok();
            *self.server_addr.lock().unwrap_or_else(|e| e.into_inner()) = addr;
            let shown = addr.map_or_else(|| format!("{host}:{port}"), |a| a.to_string());
            printer::log(
                Level::Info,
                &format!("Serve metrics at {shown}{path}"),
                &[Attr::new("context", LogValue::String("metrics".into()))],
            );
            prometheus::serve(listener, path.clone(), Arc::clone(&self.shared), self.shutdown.clone());
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
            writer.run(interval.as_secs())?;
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
