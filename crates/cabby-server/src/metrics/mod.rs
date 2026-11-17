//! Server metrics (upstream package `metrics`): counters and gauges, interval
//! rotation, log/StatsD writers and the Prometheus endpoint.

use std::sync::Arc;

use cabby_core::common::StringMap;
use cabby_core::pending;

mod config;
mod counter;
mod gauge;
mod printer;
mod statsd;

pub use config::{Config, StatsdConfig};
pub use counter::Counter;
pub use gauge::Gauge;
pub use printer::BasePrinter;
pub use statsd::StatsdWriter;

/// Default rotation interval (seconds).
pub const DEFAULT_ROTATE_INTERVAL: u64 = 15;
/// Prefix of every Prometheus metric name.
pub const PROMETHEUS_NAMESPACE: &str = "anycable_go";

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

/// The metrics registry (upstream `Metrics`).
pub struct Metrics {
    _private: (),
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Upstream `NewMetrics(writers, rotateIntervalSeconds)`.
    pub fn new(_writers: Vec<Arc<dyn IntervalWriter>>, _rotate_interval_seconds: u64) -> Self {
        pending!("metrics::new_metrics")
    }

    /// Upstream `NewFromConfig`: log printer, tags and the Prometheus HTTP server
    /// (dedicated when `config.host` differs from the main server host).
    pub fn from_config(_config: &Config) -> Result<Self, MetricsError> {
        pending!("metrics::new_from_config")
    }

    pub fn default_tags(&self, _tags: StringMap) {
        pending!("metrics::default_tags")
    }

    pub fn register_writer(&self, _writer: Arc<dyn IntervalWriter>) {
        pending!("metrics::register_writer")
    }

    /// Starts the metrics server (if any) and the writers, then rotates every
    /// interval until [`Metrics::shutdown`]. Returns when shut down or the
    /// dedicated server fails.
    pub async fn run(&self) -> Result<(), MetricsError> {
        pending!("metrics::run")
    }

    pub async fn shutdown(&self) -> Result<(), MetricsError> {
        pending!("metrics::shutdown")
    }

    pub fn counter(&self, _name: &str) -> Option<Arc<Counter>> {
        pending!("metrics::counter")
    }

    pub fn gauge(&self, _name: &str) -> Option<Arc<Gauge>> {
        pending!("metrics::gauge")
    }

    pub fn each_counter(&self, _f: &mut dyn FnMut(&Counter)) {
        pending!("metrics::each_counter")
    }

    pub fn each_gauge(&self, _f: &mut dyn FnMut(&Gauge)) {
        pending!("metrics::each_gauge")
    }

    /// Counter interval values and gauge values by name.
    pub fn interval_snapshot(&self) -> std::collections::HashMap<String, u64> {
        pending!("metrics::interval_snapshot")
    }

    /// Updates every counter's interval delta (upstream unexported `rotate`).
    pub fn rotate(&self) {
        pending!("metrics::rotate")
    }

    /// Prometheus text exposition of all counters and gauges.
    pub fn prometheus(&self) -> String {
        pending!("metrics::prometheus")
    }

    /// The Prometheus HTTP endpoint handler.
    pub fn prometheus_handler(&self, _request: &http::Request<()>) -> http::Response<String> {
        pending!("metrics::prometheus_handler")
    }
}

impl Instrumenter for Metrics {
    fn counter_increment(&self, _name: &str) {
        pending!("metrics::counter_increment")
    }
    fn counter_add(&self, _name: &str, _value: u64) {
        pending!("metrics::counter_add")
    }
    fn gauge_increment(&self, _name: &str) {
        pending!("metrics::gauge_increment")
    }
    fn gauge_decrement(&self, _name: &str) {
        pending!("metrics::gauge_decrement")
    }
    fn gauge_set(&self, _name: &str, _value: u64) {
        pending!("metrics::gauge_set")
    }
    fn register_counter(&self, _name: &str, _desc: &str) {
        pending!("metrics::register_counter")
    }
    fn register_gauge(&self, _name: &str, _desc: &str) {
        pending!("metrics::register_gauge")
    }
}
