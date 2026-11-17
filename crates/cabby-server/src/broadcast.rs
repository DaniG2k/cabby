//! Broadcasters: HTTP endpoint, Redis Streams, legacy Redis pub/sub and legacy
//! NATS (upstream package `broadcast`; interfaces in `cabby_core::broadcast`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::broadcast::{BroadcastError, Broadcaster, Handler};
use cabby_core::pending;
use cabby_core::toml_config::{self, TomlSection};

use crate::nats::NatsConfig;
use crate::redis::RedisConfig as RedisClientConfig;

/// Upstream `broadcast.HTTPConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpConfig {
    pub port: i64,
    pub path: String,
    /// Bearer token required on requests (empty: none).
    pub secret: String,
    /// Application secret the token is derived from when `secret` is empty
    /// (not a TOML field).
    pub secret_base: String,
    pub add_cors_headers: bool,
    /// Comma-separated CORS hosts.
    pub cors_hosts: String,
    /// Bytes; 0 disables the limit.
    pub max_body_size: i64,
}

/// Upstream `defaultHTTPPath`.
pub const DEFAULT_HTTP_PATH: &str = "/_broadcast";
/// Upstream `defaultMaxBodySize` (1 MB).
pub const DEFAULT_MAX_BODY_SIZE: i64 = 1_048_576;

fn decode(target: &mut impl TomlSection, toml: &str) -> Result<(), BroadcastError> {
    toml_config::decode(target, toml).map_err(|e| BroadcastError(e.0))?;
    Ok(())
}

cabby_core::toml_section!(HttpConfig, "broadcast.HTTPConfig" {
    "port" => port,
    "path" => path,
    "secret" => secret,
    "SecretBase" => secret_base,
    "cors_headers" => add_cors_headers,
    "cors_hosts" => cors_hosts,
    "max_body_size" => max_body_size,
});

impl HttpConfig {
    /// Upstream `broadcast.NewHTTPConfig()`.
    pub fn new() -> Self {
        Self { path: DEFAULT_HTTP_PATH.to_owned(), max_body_size: DEFAULT_MAX_BODY_SIZE, ..Default::default() }
    }

    pub fn is_secured(&self) -> bool {
        !self.secret.is_empty() || !self.secret_base.is_empty()
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# HTTP server port (can be the same as the main server port)\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# HTTP endpoint path for broadcasts\n");
        out.push_str(&format!("path = \"{}\"\n", self.path));
        out.push_str("# Secret token to authenticate broadcasting requests\n");
        if self.secret.is_empty() {
            out.push_str("# secret = \"\"\n");
        } else {
            out.push_str(&format!("secret = \"{}\"\n", self.secret));
        }
        out.push_str("# Enable CORS headers (allowed origins are used as allowed hosts)\n");
        out.push_str(if self.add_cors_headers { "cors_headers = true\n" } else { "# cors_headers = false\n" });
        out.push_str("# Maximum allowed size of the request body (in bytes)\n");
        out.push_str(&format!("max_body_size = {}\n", self.max_body_size));
        out.push('\n');
        out
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// An HTTP request as seen by the broadcast handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpRequest {
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP broadcast endpoint (upstream `broadcast.HTTPBroadcaster`).
pub struct HttpBroadcaster {
    _private: (),
}

impl HttpBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &HttpConfig) -> Self {
        pending!("broadcast::new_http_broadcaster")
    }

    /// Derives the authorization token and CORS settings (upstream `Prepare`).
    pub fn prepare(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::http_prepare")
    }

    /// Serves one request and returns the response status (upstream `Handler`).
    pub fn handle(&self, _request: &HttpRequest) -> u16 {
        pending!("broadcast::http_handler")
    }
}

impl Broadcaster for HttpBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::http_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::http_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::http_is_fanout")
    }
}

/// Upstream `broadcast.RedisConfig` (Redis Streams). `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    pub stream: String,
    pub group: String,
    pub stream_read_block_milliseconds: i64,
    pub redis: Option<RedisClientConfig>,
}

cabby_core::toml_section!(RedisConfig, "broadcast.RedisConfig" {
    "stream" => stream,
    "group" => group,
    "stream_read_block_milliseconds" => stream_read_block_milliseconds,
    "redis" => redis,
});

impl RedisConfig {
    /// Upstream `broadcast.NewRedisConfig()`.
    pub fn new() -> Self {
        Self {
            stream: "__anycable__".to_owned(),
            group: "bx".to_owned(),
            stream_read_block_milliseconds: 2000,
            redis: None,
        }
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Redis stream name for broadcasts\n");
        out.push_str(&format!("stream = \"{}\"\n", self.stream));
        out.push_str("# Stream consumer group name\n");
        out.push_str(&format!("group = \"{}\"\n", self.group));
        out.push_str("# Streams read wait time in milliseconds\n");
        out.push_str(&format!("stream_read_block_milliseconds = {}\n", self.stream_read_block_milliseconds));
        out.push('\n');
        out
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Redis Streams broadcaster (upstream `broadcast.RedisBroadcaster`).
pub struct RedisBroadcaster {
    _private: (),
}

impl RedisBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &RedisConfig) -> Self {
        pending!("broadcast::new_redis_broadcaster")
    }
}

impl Broadcaster for RedisBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::redis_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::redis_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::redis_is_fanout")
    }
}

/// Upstream `broadcast.LegacyRedisConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LegacyRedisConfig {
    pub channel: String,
    pub redis: Option<RedisClientConfig>,
}

cabby_core::toml_section!(LegacyRedisConfig, "broadcast.LegacyRedisConfig" {
    "channel" => channel,
    "redis" => redis,
});

impl LegacyRedisConfig {
    /// Upstream `broadcast.NewLegacyRedisConfig()`.
    pub fn new() -> Self {
        Self { channel: "__anycable__".to_owned(), redis: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Legacy Redis pub/sub broadcaster (upstream `broadcast.LegacyRedisBroadcaster`).
pub struct LegacyRedisBroadcaster {
    _private: (),
}

impl LegacyRedisBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &LegacyRedisConfig) -> Self {
        pending!("broadcast::new_legacy_redis_broadcaster")
    }
}

impl Broadcaster for LegacyRedisBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_redis_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_redis_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::legacy_redis_is_fanout")
    }
}

/// Upstream `broadcast.LegacyNATSConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LegacyNatsConfig {
    pub channel: String,
    pub nats: Option<NatsConfig>,
}

cabby_core::toml_section!(LegacyNatsConfig, "broadcast.LegacyNATSConfig" {
    "channel" => channel,
    "nats" => nats,
});

impl LegacyNatsConfig {
    /// Upstream `broadcast.NewLegacyNATSConfig()`.
    pub fn new() -> Self {
        Self { channel: "__anycable__".to_owned(), nats: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Legacy NATS broadcaster (upstream `broadcast.LegacyNATSBroadcaster`).
pub struct LegacyNatsBroadcaster {
    _private: (),
}

impl LegacyNatsBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &LegacyNatsConfig) -> Self {
        pending!("broadcast::new_legacy_nats_broadcaster")
    }
}

impl Broadcaster for LegacyNatsBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_nats_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_nats_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::legacy_nats_is_fanout")
    }
}
