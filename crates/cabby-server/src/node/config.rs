//! Node and disconnect queue settings (upstream `node/config.go` and the config
//! half of `node/disconnect_queue.go`).

use cabby_core::logger::TomlError;
use cabby_core::pending;

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

impl Config {
    /// Defaults (upstream `node.NewConfig`).
    pub fn new() -> Self {
        pending!("node::new_config")
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("node::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, _toml: &str) -> Result<(), TomlError> {
        pending!("node::config_merge_toml")
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

impl DisconnectQueueConfig {
    /// Defaults (upstream `NewDisconnectQueueConfig`).
    pub fn new() -> Self {
        pending!("node::new_disconnect_queue_config")
    }

    /// Renders the annotated TOML section (upstream `DisconnectQueueConfig.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("node::disconnect_queue_config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, _toml: &str) -> Result<(), TomlError> {
        pending!("node::disconnect_queue_config_merge_toml")
    }
}
