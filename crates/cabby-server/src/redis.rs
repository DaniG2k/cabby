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

cabby_core::toml_section!(RedisConfig, "redis.RedisConfig" {
    "url" => url,
    "sentinels" => sentinels,
    "sentinel_discovery_interval" => sentinel_discovery_interval,
    "keepalive_ping_interval" => keepalive_ping_interval,
    "tls_verify" => tls_verify,
    "tls_ca_cert_path" => tls_ca_cert_path,
    "tls_client_cert_path" => tls_client_cert_path,
    "tls_client_key_path" => tls_client_key_path,
    "max_reconnect_attempts" => max_reconnect_attempts,
    "disable_cache" => disable_cache,
});

impl RedisConfig {
    /// Upstream `redis.NewRedisConfig()` (defaults).
    pub fn new() -> Self {
        Self {
            url: "redis://localhost:6379".to_owned(),
            keepalive_ping_interval: 30,
            sentinel_discovery_interval: 30,
            tls_verify: false,
            max_reconnect_attempts: 5,
            disable_cache: false,
            ..Default::default()
        }
    }

    pub fn is_cluster(&self) -> bool {
        self.hosts.len() > 1 && !self.is_sentinel()
    }

    pub fn is_sentinel(&self) -> bool {
        !self.sentinels.is_empty() || !self.sentinel_master.is_empty()
    }

    pub fn tls_verification_skipped(&self) -> bool {
        !self.tls_verify && (self.url.contains("rediss://") || self.sentinels.contains("rediss://"))
    }

    pub fn hostnames(&self) -> Vec<String> {
        self.hosts.clone()
    }

    /// The sentinel master, or the first host (upstream panics without hosts;
    /// this returns `""`).
    pub fn hostname(&self) -> String {
        if self.is_sentinel() { self.sentinel_master.clone() } else { self.hosts.first().cloned().unwrap_or_default() }
    }

    pub fn tls_client_cert_available(&self) -> bool {
        !self.tls_client_cert_path.is_empty() && !self.tls_client_key_path.is_empty()
    }

    /// Upstream `ToRueidisOptions`; also records the resolved hosts.
    pub fn to_client_options(&mut self) -> Result<ClientOptions, RedisError> {
        pending!("redis::to_client_options")
    }

    pub fn to_toml(&self) -> String {
        fn optional(out: &mut String, key: &str, value: &str, placeholder: &str) {
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"{placeholder}\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }

        let mut out = String::new();
        out.push_str("# Redis instance URL or master name in case of sentinels usage\n");
        out.push_str("# or list of URLs if cluster usage\n");
        out.push_str(&format!("url = \"{}\"\n", self.url));
        out.push_str("# Sentinel addresses (comma-separated list)\n");
        out.push_str(&format!("sentinels = \"{}\"\n", self.sentinels));
        out.push_str("# Sentinel discovery interval (seconds)\n");
        out.push_str(&format!("sentinel_discovery_interval = {}\n", self.sentinel_discovery_interval));
        out.push_str("# Keepalive ping interval (seconds)\n");
        out.push_str(&format!("keepalive_ping_interval = {}\n", self.keepalive_ping_interval));
        out.push_str("# Enable TLS Verify\n");
        out.push_str(if self.tls_verify { "tls_verify = true\n" } else { "# tls_verify = true\n" });
        out.push_str("# Path to CA certificate file to verify Redis server\n");
        optional(&mut out, "tls_ca_cert_path", &self.tls_ca_cert_path, "/path/to/ca.pem");
        out.push_str("# Path to client TLS certificate file for mutual TLS\n");
        optional(&mut out, "tls_client_cert_path", &self.tls_client_cert_path, "/path/to/cert.pem");
        out.push_str("# Path to client TLS private key file for mutual TLS\n");
        optional(&mut out, "tls_client_key_path", &self.tls_client_key_path, "/path/to/key.pem");
        out.push_str("# Max number of reconnect attempts\n");
        out.push_str(&format!("max_reconnect_attempts = {}\n", self.max_reconnect_attempts));
        out.push_str("# Disable client-side caching\n");
        out.push_str(if self.disable_cache { "disable_cache = true\n" } else { "# disable_cache = true\n" });
        out.push('\n');
        out
    }

    /// Decodes TOML into `self`, keeping fields the document omits (upstream
    /// `toml.Decode(str, &config)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), RedisError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| RedisError(e.0))?;
        Ok(())
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
