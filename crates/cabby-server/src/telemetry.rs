//! Anonymized usage telemetry (upstream package `telemetry`).
//!
//! The tracker posts a `boot` event on [`Tracker::collect`] and a `usage` event
//! (maxima of observed gauges) every 30 minutes. Process environment and clock are
//! explicit inputs so tests never mutate global state or reach a real endpoint.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::StringMap;
use cabby_core::pending;

use crate::metrics::Metrics;

/// Default telemetry endpoint. Tests must always override it.
pub const DEFAULT_ENDPOINT: &str = "https://telemetry.anycable.io";
/// Delay between usage events.
pub const USAGE_MEASUREMENT_DELAY: Duration = Duration::from_secs(30 * 60);

/// Tracker configuration (upstream `telemetry.Config`).
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Empty token disables tracking.
    pub token: String,
    pub endpoint: String,
    pub custom_props: StringMap,
    pub debug: bool,
}

impl Config {
    /// Upstream `NewConfig`: build-time token, default endpoint,
    /// `ANYCABLE_TELEMETRY_DEBUG=1` enables debug logging.
    pub fn new(_env: &dyn EnvSource) -> Self {
        pending!("telemetry::new_config")
    }
}

/// Read access to the process environment (upstream reads `os.Getenv`).
pub trait EnvSource: Send + Sync {
    fn var(&self, name: &str) -> Option<String>;
    /// All variables, for the cluster fingerprint.
    fn vars(&self) -> Vec<(String, String)>;
}

/// A fixed environment.
#[derive(Debug, Clone, Default)]
pub struct StaticEnv(pub HashMap<String, String>);

impl EnvSource for StaticEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
    fn vars(&self) -> Vec<(String, String)> {
        self.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

/// The server settings reported as feature flags (the subset of upstream
/// `config.Config` that `appProperties` reads).
#[derive(Debug, Clone, PartialEq)]
pub struct AppConfig {
    pub secret: String,
    pub skip_auth: bool,
    pub jwt_enabled: bool,
    pub public_streams: bool,
    pub turbo_streams: bool,
    pub cable_ready_streams: bool,
    pub embedded_nats: bool,
    pub broadcast_adapters: Vec<String>,
    pub pubsub_adapter: String,
    pub broker_adapter: String,
    pub ssl_available: bool,
    pub metrics_log_formatter: bool,
    pub statsd_enabled: bool,
    pub prometheus_enabled: bool,
    pub rpc_impl: String,
    /// Path of the TOML configuration file, if any (hashed into the fingerprint).
    pub config_file_path: String,
}

impl AppConfig {
    /// The values derived from the server's default configuration (upstream
    /// `config.NewConfig()`).
    pub fn defaults() -> Self {
        pending!("telemetry::app_config_defaults")
    }
}

/// Upstream `telemetry.Tracker`.
pub struct Tracker {
    _private: (),
}

impl std::fmt::Debug for Tracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracker").finish_non_exhaustive()
    }
}

impl Tracker {
    /// A disabled tracker when `config.token` is empty.
    pub fn new(
        _metrics: Arc<Metrics>,
        _app: &AppConfig,
        _config: &Config,
        _env: Arc<dyn EnvSource>,
        _args: Vec<String>,
    ) -> Self {
        pending!("telemetry::new_tracker")
    }

    pub fn enabled(&self) -> bool {
        pending!("telemetry::enabled")
    }

    pub fn announce(&self) -> String {
        pending!("telemetry::announce")
    }

    /// Sends the `boot` event (synchronously, 1s timeout), starts the usage monitor
    /// (first observation immediately, then every minute) and schedules the
    /// usage event.
    pub async fn collect(&self) {
        pending!("telemetry::collect")
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        pending!("telemetry::shutdown")
    }

    /// Records the current `clients_num` / `mem_sys_bytes` gauges, keeping maxima.
    pub fn observe_usage(&self) {
        pending!("telemetry::observe_usage")
    }

    /// Sends the `usage` event with the observed maxima and resets them.
    pub async fn collect_usage(&self) {
        pending!("telemetry::collect_usage")
    }

    /// Waits until the usage monitor has made at least `n` observations
    /// (readiness hook replacing upstream's sleep; `false` on deadline).
    pub async fn wait_for_observations(&self, _n: u64, _deadline: Duration) -> bool {
        pending!("telemetry::wait_for_observations")
    }
}

/// The version string reported in telemetry events (upstream `version.Version()`).
pub fn version() -> String {
    pending!("telemetry::version")
}
