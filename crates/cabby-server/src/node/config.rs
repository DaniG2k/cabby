//! Node and disconnect queue settings (upstream `node/config.go` and the config
//! half of `node/disconnect_queue.go`).

use cabby_core::logger::TomlError;
use cabby_core::toml_config::{self, toml_quote};

/// Invoke the Disconnect callback for every session.
pub const DISCONNECT_MODE_ALWAYS: &str = "always";
/// Invoke it when the application declared interest (upstream default).
pub const DISCONNECT_MODE_AUTO: &str = "auto";
/// Never invoke it.
pub const DISCONNECT_MODE_NEVER: &str = "never";

/// Upstream `DISCONNECT_MODES`.
pub const DISCONNECT_MODES: [&str; 3] = [DISCONNECT_MODE_ALWAYS, DISCONNECT_MODE_AUTO, DISCONNECT_MODE_NEVER];

/// General node settings (upstream `node.Config`, TOML table `[app]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    /// `disconnect_mode`: when to invoke the Disconnect callback.
    pub disconnect_mode: String,
    /// `shutdown_disconnect_gopool_size`: workers for disconnect calls on shutdown.
    pub shutdown_disconnect_pool_size: i64,
    /// `ping_interval` (seconds).
    pub ping_interval: i64,
    /// `stats_refresh_interval` (seconds).
    pub stats_refresh_interval: i64,
    /// `broadcast_gopool_size`: the hub's worker pool size.
    pub hub_gopool_size: i64,
    /// `ping_timestamp_precision`: `s`, `ms` or `ns`.
    pub ping_timestamp_precision: String,
    /// `pong_timeout` (seconds, 0 disables).
    pub pong_timeout: i64,
    /// `enable_native_ping`.
    pub enable_native_ping: bool,
    /// `shutdown_timeout` (seconds).
    pub shutdown_timeout: i64,
    /// `shutdown_delay` (seconds).
    pub shutdown_delay: i64,
}

cabby_core::toml_section!(Config, "node.Config" {
    "disconnect_mode" => disconnect_mode,
    "shutdown_disconnect_gopool_size" => shutdown_disconnect_pool_size,
    "ping_interval" => ping_interval,
    "stats_refresh_interval" => stats_refresh_interval,
    "broadcast_gopool_size" => hub_gopool_size,
    "ping_timestamp_precision" => ping_timestamp_precision,
    "pong_timeout" => pong_timeout,
    "enable_native_ping" => enable_native_ping,
    "shutdown_timeout" => shutdown_timeout,
    "shutdown_delay" => shutdown_delay,
});

impl Config {
    /// Defaults (upstream `node.NewConfig`).
    pub fn new() -> Self {
        Self {
            ping_interval: 3,
            stats_refresh_interval: 5,
            hub_gopool_size: 16,
            shutdown_disconnect_pool_size: 16,
            ping_timestamp_precision: "s".to_owned(),
            disconnect_mode: DISCONNECT_MODE_AUTO.to_owned(),
            shutdown_timeout: 30,
            ..Default::default()
        }
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Server-to-client heartbeat interval (seconds)\n");
        out.push_str(&format!("ping_interval = {}\n", self.ping_interval));
        out.push_str("# Timestamp format for ping messages (s, ms, or ns)\n");
        out.push_str(&format!("ping_timestamp_precision = {}\n", toml_quote(&self.ping_timestamp_precision)));
        out.push_str("# Client-to-server pong timeout (seconds)\n");
        if self.pong_timeout == 0 {
            out.push_str("# pong_timeout = 6\n");
        } else {
            out.push_str(&format!("pong_timeout = {}\n", self.pong_timeout));
        }
        // Upstream writes the setting on the comment line (no newline) and comments
        // it out when enabled; kept byte for byte.
        out.push_str("# Enable native ping/pong mechanism (e.g., WebSocket PING/PONG frames)");
        if self.enable_native_ping {
            out.push_str("# enable_native_ping = true\n");
        } else {
            out.push_str("enable_native_ping = true\n");
        }
        out.push_str("# Define when to invoke Disconnect RPC callback\n");
        out.push_str(&format!("disconnect_mode = {}\n", toml_quote(&self.disconnect_mode)));
        out.push_str("# Graceful shutdown period (seconds)\n");
        out.push_str(&format!("shutdown_timeout = {}\n", self.shutdown_timeout));
        out.push_str("# How often to refresh system-wide metrics (seconds)\n");
        out.push_str(&format!("stats_refresh_interval = {}\n", self.stats_refresh_interval));
        out.push_str("# The number of Go routines to use for broadcasting (server-to-client fan-out)\n");
        out.push_str(&format!("broadcast_gopool_size = {}\n", self.hub_gopool_size));
        out.push_str("# The number of Go routines to use for Disconnect RPC calls on shutdown\n");
        out.push_str(&format!("shutdown_disconnect_gopool_size = {}\n", self.shutdown_disconnect_pool_size));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        toml_config::decode(self, toml).map_err(|e| TomlError(e.0))?;
        Ok(())
    }
}

/// Disconnect queue settings (upstream `node.DisconnectQueueConfig`, TOML table
/// `[disconnector]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisconnectQueueConfig {
    /// `rate`: max Disconnect calls per second (shared by all workers).
    pub rate: i64,
    /// `workers_num`: concurrent Disconnect calls.
    pub workers_num: i64,
    /// Deprecated channel buffer size (upstream `Backlog`, no TOML tag).
    pub backlog: i64,
    /// `max_backlog`: queued calls beyond this are discarded (0: unlimited).
    pub max_backlog: i64,
    /// Deprecated (upstream `ShutdownTimeout`, no TOML tag).
    pub shutdown_timeout: i64,
}

// Untagged upstream fields decode by their Go names (case-insensitively).
cabby_core::toml_section!(DisconnectQueueConfig, "node.DisconnectQueueConfig" {
    "Rate" => rate,
    "workers_num" => workers_num,
    "Backlog" => backlog,
    "max_backlog" => max_backlog,
    "ShutdownTimeout" => shutdown_timeout,
});

impl DisconnectQueueConfig {
    /// Defaults (upstream `NewDisconnectQueueConfig`).
    pub fn new() -> Self {
        Self { rate: 100, workers_num: 4, max_backlog: 8192, ..Default::default() }
    }

    /// Renders the annotated TOML section (upstream `DisconnectQueueConfig.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Limit the number of Disconnect RPC calls per second\n");
        out.push_str(&format!("rate = {}\n", self.rate));
        out.push_str("# The number of concurrent Disconnect RPC calls\n");
        out.push_str(&format!("workers_num = {}\n", self.workers_num));
        out.push_str("# Max queue size for disconnect requests (new items are discarded when the limit is reached)\n");
        out.push_str(&format!("max_backlog = {}\n", self.max_backlog));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        toml_config::decode(self, toml).map_err(|e| TomlError(e.0))?;
        Ok(())
    }
}
