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

/// Redis Streams broadcaster (upstream `broadcast.RedisBroadcaster`): every
/// node reads the stream in one consumer group, so each broadcast is handled
/// by one node.
pub struct RedisBroadcaster {
    stream: String,
    redis: RedisClientConfig,
    streamer: crate::redis::Streamer,
}

impl RedisBroadcaster {
    pub fn new(node: Arc<dyn Handler>, config: &RedisConfig) -> Self {
        let name = crate::redis::connect::nanoid6();
        let redis = crate::redis::or_defaults(config.redis.as_ref());
        let handler: crate::redis::StreamHandler = Arc::new(move |message| match message.get("payload") {
            Some(payload) => {
                crate::redis::log(cabby_core::logger::Level::Debug, "broadcast", "received broadcast", &[]);
                node.handle_broadcast(payload.as_bytes()).map_err(|e| crate::redis::RedisError(e.0))
            }
            None => Err(crate::redis::RedisError("missing payload field".to_owned())),
        });
        let streamer = crate::redis::Streamer::new(
            &config.stream,
            &config.group,
            &redis,
            crate::redis::StreamerOptions {
                block_ms: Some(config.stream_read_block_milliseconds),
                consumer_name: Some(name),
                handler: Some(handler),
            },
        );
        Self { stream: config.stream.clone(), redis, streamer }
    }
}

impl Broadcaster for RedisBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        self.streamer
            .start()
            .map_err(|e| BroadcastError(format!("failed to initialize Redis stream, cause: {}", e.0)))?;

        let mut redis = self.redis.clone();
        let _ = redis.to_client_options();
        let hosts = format!("[{}]", redis.hostnames().join(" "));
        let msg = if redis.is_sentinel() {
            format!("Starting Redis broadcaster at {hosts} (sentinels)")
        } else if redis.is_cluster() {
            format!("Starting Redis broadcaster at {hosts} (cluster)")
        } else {
            format!("Starting Redis broadcaster at {}", redis.hostname())
        };
        crate::redis::log(
            cabby_core::logger::Level::Info,
            "broadcast",
            &msg,
            &[("provider", "redisx".to_owned()), ("stream", self.stream.clone())],
        );
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        self.streamer.shutdown().map_err(|e| BroadcastError(e.0))
    }

    fn is_fanout(&self) -> bool {
        false
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

/// Legacy Redis pub/sub broadcaster (upstream `broadcast.LegacyRedisBroadcaster`):
/// every node subscribes to the channel. The subscription is kept alive like
/// the pub/sub subscriber's (deviation D-003).
pub struct LegacyRedisBroadcaster {
    node: Arc<dyn Handler>,
    redis: RedisClientConfig,
    subscriptions: Arc<crate::redis::subscription::Subscriptions>,
    started: std::sync::atomic::AtomicBool,
}

/// Hands pub/sub payloads to the node (upstream `OnMessage` hook).
struct PubSubSink {
    node: Arc<dyn Handler>,
}

impl crate::redis::subscription::Sink for PubSubSink {
    fn message(&self, _channel: &str, payload: &[u8]) {
        crate::redis::log(cabby_core::logger::Level::Debug, "broadcast", "received pubsub message", &[]);
        self.node.handle_pubsub(payload);
    }
}

impl LegacyRedisBroadcaster {
    pub fn new(node: Arc<dyn Handler>, config: &LegacyRedisConfig) -> Self {
        let redis = crate::redis::or_defaults(config.redis.as_ref());
        let subscriptions = crate::redis::subscription::Subscriptions::new(crate::redis::subscription::Settings {
            context: "broadcast",
            primary: config.channel.clone(),
            max_reconnect_attempts: redis.max_reconnect_attempts,
            connected_msg: "subscribed to Redis channel",
            reconnected_msg: "reconnected to Redis channel",
            exhausted_error: "Redis reconnect attempts exceeded",
        });
        subscriptions.subscribe(&config.channel);
        Self { node, redis, subscriptions, started: std::sync::atomic::AtomicBool::new(false) }
    }
}

impl Broadcaster for LegacyRedisBroadcaster {
    fn start(&self, done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        let mut redis = self.redis.clone();
        redis.to_client_options().map_err(|e| BroadcastError(e.0))?;
        let spec = redis.connect_spec().map_err(|e| BroadcastError(e.0))?;

        let msg = crate::redis::pubsub_start_message("Starting Redis pub/sub", &redis);
        // Upstream logs the single-instance line at debug level.
        let level = if redis.is_sentinel() || redis.is_cluster() {
            cabby_core::logger::Level::Info
        } else {
            cabby_core::logger::Level::Debug
        };
        crate::redis::log(level, "broadcast", &msg, &[("provider", "redis".to_owned())]);

        self.started.store(true, std::sync::atomic::Ordering::SeqCst);
        self.subscriptions.start(
            spec,
            Arc::new(PubSubSink { node: Arc::clone(&self.node) }),
            Box::new(move |err| {
                let _ = done.send(BroadcastError(err));
            }),
        );
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        if !self.started.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        crate::redis::log(cabby_core::logger::Level::Debug, "broadcast", "shutting down Redis pub/sub", &[]);
        self.subscriptions.shutdown(std::time::Duration::from_secs(2));
        Ok(())
    }

    fn is_fanout(&self) -> bool {
        true
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
