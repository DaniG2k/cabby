//! Broadcasters: HTTP endpoint, Redis Streams, legacy Redis pub/sub and legacy
//! NATS (upstream package `broadcast`; interfaces in `cabby_core::broadcast`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::broadcast::{BroadcastError, Broadcaster, Handler};
use cabby_core::pending;

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

impl HttpConfig {
    /// Upstream `broadcast.NewHTTPConfig()`.
    pub fn new() -> Self {
        pending!("broadcast::new_http_config")
    }

    pub fn is_secured(&self) -> bool {
        pending!("broadcast::http_is_secured")
    }

    pub fn to_toml(&self) -> String {
        pending!("broadcast::http_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), BroadcastError> {
        pending!("broadcast::http_config_decode_toml")
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

impl RedisConfig {
    /// Upstream `broadcast.NewRedisConfig()`.
    pub fn new() -> Self {
        pending!("broadcast::new_redis_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("broadcast::redis_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), BroadcastError> {
        pending!("broadcast::redis_config_decode_toml")
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

impl LegacyRedisConfig {
    /// Upstream `broadcast.NewLegacyRedisConfig()`.
    pub fn new() -> Self {
        pending!("broadcast::new_legacy_redis_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("broadcast::legacy_redis_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_redis_config_decode_toml")
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

impl LegacyNatsConfig {
    /// Upstream `broadcast.NewLegacyNATSConfig()`.
    pub fn new() -> Self {
        pending!("broadcast::new_legacy_nats_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("broadcast::legacy_nats_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_nats_config_decode_toml")
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
