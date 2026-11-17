//! Metrics configuration (upstream `metrics/config.go`, `StatsdConfig`).

use cabby_core::common::StringMap;
use cabby_core::pending;

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub log: bool,
    /// Deprecated upstream; kept for configuration compatibility.
    pub log_interval: i64,
    pub rotate_interval: i64,
    /// Path to an mruby formatter script.
    pub log_formatter: String,
    /// Print only these metrics (`None`: all).
    pub log_filter: Option<Vec<String>>,
    /// Prometheus endpoint path (empty: disabled).
    pub http: String,
    pub host: String,
    pub port: i64,
    pub tags: Option<StringMap>,
    pub statsd: StatsdConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    /// Upstream `NewConfig` defaults.
    pub fn new() -> Self {
        pending!("metrics::new_config")
    }

    pub fn log_enabled(&self) -> bool {
        pending!("metrics::config_log_enabled")
    }

    pub fn http_enabled(&self) -> bool {
        pending!("metrics::config_http_enabled")
    }

    pub fn log_formatter_enabled(&self) -> bool {
        pending!("metrics::config_log_formatter_enabled")
    }

    /// The commented TOML section written by `anycable-go --print-config`.
    pub fn to_toml(&self) -> String {
        pending!("metrics::config_to_toml")
    }

    /// Decodes a TOML document on top of `self`.
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), String> {
        pending!("metrics::config_decode_toml")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsdConfig {
    /// `host:port` of the StatsD UDP endpoint (empty: disabled).
    pub host: String,
    pub prefix: String,
    /// `datadog`, `influxdb` or `graphite`.
    pub tag_format: String,
    pub max_packet_size: i64,
}

impl Default for StatsdConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsdConfig {
    /// Upstream `NewStatsdConfig` defaults.
    pub fn new() -> Self {
        pending!("metrics::new_statsd_config")
    }

    pub fn enabled(&self) -> bool {
        pending!("metrics::statsd_config_enabled")
    }
}
