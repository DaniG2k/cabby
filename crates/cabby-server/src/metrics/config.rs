//! Metrics configuration (upstream `metrics/config.go`, `StatsdConfig`).

use cabby_core::common::StringMap;

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

cabby_core::toml_section!(Config, "metrics.Config" {
    "log" => log,
    "LogInterval" => log_interval,
    "rotate_interval" => rotate_interval,
    "LogFormatter" => log_formatter,
    "log_filter" => log_filter,
    "http_path" => http,
    "host" => host,
    "port" => port,
    "tags" => tags,
    "statsd" => statsd,
});

impl Config {
    /// Upstream `NewConfig` defaults.
    pub fn new() -> Self {
        Self {
            log: false,
            log_interval: 0,
            rotate_interval: 15,
            log_formatter: String::new(),
            log_filter: None,
            http: String::new(),
            host: String::new(),
            port: 0,
            tags: None,
            statsd: StatsdConfig::new(),
        }
    }

    pub fn log_enabled(&self) -> bool {
        self.log || self.log_formatter_enabled()
    }

    pub fn http_enabled(&self) -> bool {
        !self.http.is_empty()
    }

    pub fn log_formatter_enabled(&self) -> bool {
        !self.log_formatter.is_empty()
    }

    /// The commented TOML section written by `anycable-go --print-config`.
    pub fn to_toml(&self) -> String {
        let Self { http, host, port, log, rotate_interval, statsd, .. } = self;
        let log_filter = self.log_filter.as_deref().unwrap_or_default();
        let mut out = String::new();
        out.push_str("# HTTP endpoint (Prometheus)\n");
        if http.is_empty() {
            out.push_str("# http_path = \"/metrics\"\n");
        } else {
            out.push_str(&format!("http_path = \"{http}\"\n"));
        }
        out.push_str("# Standalone metrics HTTP server host to bind to\n");
        if host.is_empty() {
            out.push_str("# host = \"localhost\"\n");
        } else {
            out.push_str(&format!("host = \"{host}\"\n"));
        }
        out.push_str("# Metrics HTTP server port to listen on\n# (can be the same as the main server's port)\n");
        if *port == 0 {
            out.push_str("# port = 8082\n");
        } else {
            out.push_str(&format!("port = {port}\n"));
        }
        out.push_str("# Enable metrics logging\n");
        out.push_str(if *log { "log = true\n" } else { "# log = true\n" });
        out.push_str("# Log rotation interval (seconds)\n");
        out.push_str(&format!("rotate_interval = {rotate_interval}\n"));
        out.push_str("# Log filter (show only selected metrics)\n");
        if log_filter.is_empty() {
            out.push_str("# log_filter = []\n");
        } else {
            out.push_str(&format!("log_filter = [ \"{}\" ]\n", log_filter.join("\", \"")));
        }
        out.push_str("# Metrics tags\n");
        match self.tags.as_ref().filter(|t| !t.is_empty()) {
            // Upstream iterates a Go map (random order); sorted here.
            Some(tags) => {
                let mut sorted: Vec<_> = tags.iter().collect();
                sorted.sort();
                for (key, value) in sorted {
                    out.push_str(&format!("tags.{key} = \"{value}\"\n"));
                }
            }
            None => out.push_str("# tags.key = \"value\"\n"),
        }
        out.push_str("# StatsD configuration\n");
        let prefix = if statsd.enabled() { "" } else { "# " };
        out.push_str(&format!("{prefix}statsd.host = \"{}\"\n", statsd.host));
        out.push_str(&format!("{prefix}statsd.prefix = \"{}\"\n", statsd.prefix));
        out.push_str(&format!("{prefix}statsd.tags_format = \"{}\"\n", statsd.tag_format));
        out.push_str(&format!("{prefix}statsd.max_packet_size = {}\n", statsd.max_packet_size));
        out.push('\n');
        out
    }

    /// Decodes a TOML document on top of `self`.
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), String> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| e.0)?;
        Ok(())
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

cabby_core::toml_section!(StatsdConfig, "metrics.StatsdConfig" {
    "host" => host,
    "prefix" => prefix,
    "tags_format" => tag_format,
    "max_packet_size" => max_packet_size,
});

impl StatsdConfig {
    /// Upstream `NewStatsdConfig` defaults.
    pub fn new() -> Self {
        Self {
            host: String::new(),
            prefix: "anycable_go.".to_owned(),
            tag_format: "datadog".to_owned(),
            max_packet_size: 1400,
        }
    }

    pub fn enabled(&self) -> bool {
        !self.host.is_empty()
    }
}
