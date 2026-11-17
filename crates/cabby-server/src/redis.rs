//! Redis connection configuration and the Redis Streams consumer (upstream
//! package `redis`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cabby_core::pending;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisError(pub String);

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RedisError {}

/// Redis adapter configuration (upstream `redis.RedisConfig`).
///
/// `Default` is Go's zero value (`RedisConfig{}`); upstream defaults come from
/// [`RedisConfig::new`]. The resolved host list is derived state, filled in by
/// [`RedisConfig::to_client_options`] as upstream does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    /// Instance URL, sentinel master name, or comma-separated cluster URLs.
    pub url: String,
    /// Comma-separated sentinel addresses.
    pub sentinels: String,
    /// Seconds.
    pub sentinel_discovery_interval: i64,
    /// Seconds.
    pub keepalive_ping_interval: i64,
    pub tls_verify: bool,
    pub tls_ca_cert_path: String,
    pub tls_client_cert_path: String,
    pub tls_client_key_path: String,
    pub max_reconnect_attempts: i64,
    pub disable_cache: bool,
    hosts: Vec<String>,
    sentinel_master: String,
}

/// Client options derived from a [`RedisConfig`] (the fields upstream tests read
/// from `rueidis.ClientOption`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientOptions {
    pub init_address: Vec<String>,
    pub select_db: i64,
    pub username: String,
    pub password: String,
    pub keep_alive: Duration,
    pub dial_timeout: Duration,
    pub shuffle_init: bool,
    /// `None`: plain TCP (upstream `TLSConfig == nil`).
    pub tls: Option<TlsOptions>,
    pub sentinel: SentinelOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsOptions {
    pub insecure_skip_verify: bool,
    /// Number of loaded client certificates (mutual TLS).
    pub certificates: usize,
    /// Whether a custom root CA pool is configured.
    pub root_cas: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SentinelOptions {
    pub master_set: String,
    pub username: String,
    pub password: String,
}

impl RedisConfig {
    /// Upstream `redis.NewRedisConfig()` (defaults).
    pub fn new() -> Self {
        pending!("redis::new_redis_config")
    }

    pub fn is_cluster(&self) -> bool {
        let _ = (&self.hosts, &self.sentinel_master);
        pending!("redis::is_cluster")
    }

    pub fn is_sentinel(&self) -> bool {
        pending!("redis::is_sentinel")
    }

    pub fn tls_verification_skipped(&self) -> bool {
        pending!("redis::tls_verification_skipped")
    }

    pub fn hostnames(&self) -> Vec<String> {
        pending!("redis::hostnames")
    }

    pub fn hostname(&self) -> String {
        pending!("redis::hostname")
    }

    pub fn tls_client_cert_available(&self) -> bool {
        pending!("redis::tls_client_cert_available")
    }

    /// Upstream `ToRueidisOptions`; also records the resolved hosts.
    pub fn to_client_options(&mut self) -> Result<ClientOptions, RedisError> {
        pending!("redis::to_client_options")
    }

    pub fn to_toml(&self) -> String {
        pending!("redis::to_toml")
    }

    /// Decodes TOML into `self`, keeping fields the document omits (upstream
    /// `toml.Decode(str, &config)`).
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), RedisError> {
        pending!("redis::decode_toml")
    }
}

/// Handles one stream entry (field/value pairs).
pub type StreamHandler = Arc<dyn Fn(HashMap<String, String>) -> Result<(), RedisError> + Send + Sync>;

/// Upstream `StreamerOption`s.
#[derive(Clone, Default)]
pub struct StreamerOptions {
    pub block_ms: Option<i64>,
    pub consumer_name: Option<String>,
    pub handler: Option<StreamHandler>,
}

/// Redis Streams consumer-group reader with pending-entry auto-claim (upstream
/// `redis.Streamer`).
pub struct Streamer {
    _private: (),
}

impl Streamer {
    pub fn new(_stream: &str, _group: &str, _config: &RedisConfig, _options: StreamerOptions) -> Self {
        pending!("redis::new_streamer")
    }

    pub fn start(&self) -> Result<(), RedisError> {
        pending!("redis::streamer_start")
    }

    pub fn shutdown(&self) -> Result<(), RedisError> {
        pending!("redis::streamer_shutdown")
    }

    /// Waits until the consumer group is set up and reading (upstream
    /// `<-streamer.Ready()`). Returns false if `timeout` elapses first.
    pub fn wait_ready(&self, _timeout: Duration) -> bool {
        pending!("redis::streamer_ready")
    }

    /// Closes the streamer's Redis client (upstream `streamer.Client().Close()`):
    /// entries being handled can no longer be acknowledged, and the reader then
    /// reconnects up to `max_reconnect_attempts` times.
    pub fn close_client(&self) {
        pending!("redis::streamer_close_client")
    }
}
