//! Facade for upstream package `broadcast`.
//!
//! [`MockHandler`] stands in for upstream's `mocks.Handler`: every
//! `HandleBroadcast`/`HandlePubSub` call is recorded with its argument and
//! returns `nil`. On the Go backend handler and broadcasters live in the bridge
//! (`bridge/broadcast.go`).

use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use cabby_core::broadcast::{self as rs, BroadcastError};
use cabby_server::broadcast::{
    HttpBroadcaster, HttpConfig, HttpRequest, LegacyNatsConfig, LegacyRedisBroadcaster, LegacyRedisConfig,
    RedisBroadcaster, RedisConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::msg_shapes::{
    GoNatsConfig, GoRedisConfig, go_config_decode_toml, go_config_new, go_config_to_toml, go_nats, go_redis, go_unit,
};
use crate::{Backend, backend, bridge};

/// A recorded handler call.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HandlerCall {
    /// `HandleBroadcast` or `HandlePubSub`.
    #[serde(rename = "Kind")]
    pub kind: String,
    #[serde(rename = "Payload")]
    pub payload: String,
}

struct RustHandler {
    calls: Mutex<Vec<HandlerCall>>,
    ready: Condvar,
}

impl RustHandler {
    fn record(&self, kind: &str, json: &[u8]) {
        let payload = String::from_utf8_lossy(json).into_owned();
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(HandlerCall { kind: kind.into(), payload });
        self.ready.notify_all();
    }
}

impl rs::Handler for RustHandler {
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), BroadcastError> {
        self.record("HandleBroadcast", json);
        Ok(())
    }

    fn handle_pubsub(&self, json: &[u8]) {
        self.record("HandlePubSub", json);
    }
}

enum HandlerImpl {
    Rust(Arc<RustHandler>),
    Go(i64),
}

pub struct MockHandler {
    inner: HandlerImpl,
}

impl MockHandler {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Self {
                inner: HandlerImpl::Rust(Arc::new(RustHandler {
                    calls: Mutex::new(Vec::new()),
                    ready: Condvar::new(),
                })),
            },
            Backend::Go => Self { inner: HandlerImpl::Go(bridge::call("broadcast.NewHandler", json!({}))) },
        }
    }

    fn rust(&self) -> Arc<dyn rs::Handler> {
        let HandlerImpl::Rust(h) = &self.inner else { unreachable!() };
        Arc::clone(h) as Arc<dyn rs::Handler>
    }

    fn go(&self) -> i64 {
        let HandlerImpl::Go(id) = &self.inner else { unreachable!() };
        *id
    }

    /// Calls recorded since the last `calls`/`drain`.
    pub fn calls(&self) -> Vec<HandlerCall> {
        match &self.inner {
            HandlerImpl::Rust(h) => std::mem::take(&mut *h.calls.lock().unwrap_or_else(|e| e.into_inner())),
            HandlerImpl::Go(id) => bridge::call("broadcast.HandlerCalls", json!({"Handler": id})),
        }
    }

    /// Upstream `drainBroadcasts`: collects calls until none arrived for `idle`.
    pub fn drain(&self, idle: Duration) -> Vec<HandlerCall> {
        match &self.inner {
            HandlerImpl::Rust(h) => {
                let mut out = Vec::new();
                let mut g = h.calls.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    out.append(&mut g);
                    let (ng, res) =
                        h.ready.wait_timeout_while(g, idle, |c| c.is_empty()).unwrap_or_else(|e| e.into_inner());
                    g = ng;
                    if res.timed_out() && g.is_empty() {
                        return out;
                    }
                }
            }
            HandlerImpl::Go(id) => {
                bridge::call("broadcast.HandlerDrain", json!({"Handler": id, "IdleMs": idle.as_millis() as u64}))
            }
        }
    }
}

impl Default for MockHandler {
    fn default() -> Self {
        Self::new()
    }
}

enum BroadcasterImpl {
    Http(HttpBroadcaster),
    Rust(Box<dyn rs::Broadcaster>),
    Go(i64),
}

/// A `broadcast.Broadcaster`.
pub struct Broadcaster {
    inner: BroadcasterImpl,
}

fn go_new(op: &str, handler: &MockHandler, config: Value) -> Broadcaster {
    Broadcaster { inner: BroadcasterImpl::Go(bridge::call(op, json!({"Handler": handler.go(), "Config": config}))) }
}

impl Broadcaster {
    /// Upstream `NewHTTPBroadcaster(handler, &config, ...)`.
    pub fn http(handler: &MockHandler, config: &HttpConfig) -> Self {
        match backend() {
            Backend::Rust => Self { inner: BroadcasterImpl::Http(HttpBroadcaster::new(handler.rust(), config)) },
            Backend::Go => go_new("broadcast.NewHTTPBroadcaster", handler, json!(GoHttpConfig::from(config))),
        }
    }

    /// Upstream `NewRedisBroadcaster(handler, &config, ...)` (Redis Streams).
    pub fn redis(handler: &MockHandler, config: &RedisConfig) -> Self {
        match backend() {
            Backend::Rust => {
                Self { inner: BroadcasterImpl::Rust(Box::new(RedisBroadcaster::new(handler.rust(), config))) }
            }
            Backend::Go => go_new("broadcast.NewRedisBroadcaster", handler, json!(GoRedisStreamConfig::from(config))),
        }
    }

    /// Upstream `NewLegacyRedisBroadcaster(handler, &config, ...)`.
    pub fn legacy_redis(handler: &MockHandler, config: &LegacyRedisConfig) -> Self {
        match backend() {
            Backend::Rust => {
                Self { inner: BroadcasterImpl::Rust(Box::new(LegacyRedisBroadcaster::new(handler.rust(), config))) }
            }
            Backend::Go => {
                go_new("broadcast.NewLegacyRedisBroadcaster", handler, json!(GoLegacyRedisConfig::from(config)))
            }
        }
    }

    fn rust(&self) -> &dyn rs::Broadcaster {
        match &self.inner {
            BroadcasterImpl::Http(b) => b,
            BroadcasterImpl::Rust(b) => b.as_ref(),
            BroadcasterImpl::Go(_) => unreachable!(),
        }
    }

    /// `Start(done)`; later errors on `done` are not observed by upstream tests.
    pub fn start(&self) -> Result<(), String> {
        match &self.inner {
            BroadcasterImpl::Go(id) => go_unit("broadcast.Start", json!({"Broadcaster": id})),
            _ => {
                let (tx, _rx) = channel();
                self.rust().start(tx).map_err(|e| e.0)
            }
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match &self.inner {
            BroadcasterImpl::Go(id) => go_unit("broadcast.Shutdown", json!({"Broadcaster": id})),
            _ => self.rust().shutdown().map_err(|e| e.0),
        }
    }

    /// HTTP broadcaster only: upstream `Prepare()`.
    pub fn prepare(&self) -> Result<(), String> {
        match &self.inner {
            BroadcasterImpl::Http(b) => b.prepare().map_err(|e| e.0),
            BroadcasterImpl::Go(id) => go_unit("broadcast.HTTPPrepare", json!({"Broadcaster": id})),
            BroadcasterImpl::Rust(_) => panic!("prepare() is only defined for the HTTP broadcaster"),
        }
    }

    /// HTTP broadcaster only: serves one request through `Handler` (upstream
    /// `httptest.NewRecorder()` + `http.HandlerFunc(b.Handler).ServeHTTP`) and
    /// returns the status code.
    pub fn serve(&self, method: &str, headers: &[(&str, &str)], body: &str) -> u16 {
        match &self.inner {
            BroadcasterImpl::Http(b) => b.handle(&HttpRequest {
                method: method.into(),
                headers: headers.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect(),
                body: body.as_bytes().to_vec(),
            }),
            BroadcasterImpl::Go(id) => bridge::call(
                "broadcast.HTTPServe",
                json!({"Broadcaster": id, "Method": method, "Headers": headers, "Body": body}),
            ),
            BroadcasterImpl::Rust(_) => panic!("serve() is only defined for the HTTP broadcaster"),
        }
    }
}

// ------------------------------------------------------------- configs ---

#[derive(Serialize, Deserialize)]
struct GoHttpConfig {
    #[serde(rename = "Port")]
    port: i64,
    #[serde(rename = "Path")]
    path: String,
    #[serde(rename = "Secret")]
    secret: String,
    #[serde(rename = "SecretBase")]
    secret_base: String,
    #[serde(rename = "AddCORSHeaders")]
    add_cors_headers: bool,
    #[serde(rename = "CORSHosts")]
    cors_hosts: String,
    #[serde(rename = "MaxBodySize")]
    max_body_size: i64,
}

impl From<&HttpConfig> for GoHttpConfig {
    fn from(c: &HttpConfig) -> Self {
        Self {
            port: c.port,
            path: c.path.clone(),
            secret: c.secret.clone(),
            secret_base: c.secret_base.clone(),
            add_cors_headers: c.add_cors_headers,
            cors_hosts: c.cors_hosts.clone(),
            max_body_size: c.max_body_size,
        }
    }
}

impl From<GoHttpConfig> for HttpConfig {
    fn from(g: GoHttpConfig) -> Self {
        HttpConfig {
            port: g.port,
            path: g.path,
            secret: g.secret,
            secret_base: g.secret_base,
            add_cors_headers: g.add_cors_headers,
            cors_hosts: g.cors_hosts,
            max_body_size: g.max_body_size,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GoRedisStreamConfig {
    #[serde(rename = "Stream")]
    stream: String,
    #[serde(rename = "Group")]
    group: String,
    #[serde(rename = "StreamReadBlockMilliseconds")]
    stream_read_block_milliseconds: i64,
    #[serde(rename = "Redis")]
    redis: Option<GoRedisConfig>,
}

impl From<&RedisConfig> for GoRedisStreamConfig {
    fn from(c: &RedisConfig) -> Self {
        Self {
            stream: c.stream.clone(),
            group: c.group.clone(),
            stream_read_block_milliseconds: c.stream_read_block_milliseconds,
            redis: go_redis(&c.redis),
        }
    }
}

impl From<GoRedisStreamConfig> for RedisConfig {
    fn from(g: GoRedisStreamConfig) -> Self {
        RedisConfig {
            stream: g.stream,
            group: g.group,
            stream_read_block_milliseconds: g.stream_read_block_milliseconds,
            redis: g.redis.map(Into::into),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GoLegacyRedisConfig {
    #[serde(rename = "Channel")]
    channel: String,
    #[serde(rename = "Redis")]
    redis: Option<GoRedisConfig>,
}

impl From<&LegacyRedisConfig> for GoLegacyRedisConfig {
    fn from(c: &LegacyRedisConfig) -> Self {
        Self { channel: c.channel.clone(), redis: go_redis(&c.redis) }
    }
}

impl From<GoLegacyRedisConfig> for LegacyRedisConfig {
    fn from(g: GoLegacyRedisConfig) -> Self {
        LegacyRedisConfig { channel: g.channel, redis: g.redis.map(Into::into) }
    }
}

#[derive(Serialize, Deserialize)]
struct GoLegacyNatsConfig {
    #[serde(rename = "Channel")]
    channel: String,
    #[serde(rename = "NATS")]
    nats: Option<GoNatsConfig>,
}

impl From<&LegacyNatsConfig> for GoLegacyNatsConfig {
    fn from(c: &LegacyNatsConfig) -> Self {
        Self { channel: c.channel.clone(), nats: go_nats(&c.nats) }
    }
}

impl From<GoLegacyNatsConfig> for LegacyNatsConfig {
    fn from(g: GoLegacyNatsConfig) -> Self {
        LegacyNatsConfig { channel: g.channel, nats: g.nats.map(Into::into) }
    }
}

/// `new_x_config()`, `x_config_to_toml(&c)` and `x_config_decode_toml(c, toml)`
/// (= `toml.Decode(toml, &c)`) for one config type.
macro_rules! config_facade {
    ($new:ident, $to_toml:ident, $decode:ident, $ty:ty, $go:ty, $prefix:literal) => {
        pub fn $new() -> $ty {
            match backend() {
                Backend::Rust => <$ty>::new(),
                Backend::Go => go_config_new::<$go>($prefix).into(),
            }
        }

        pub fn $to_toml(c: &$ty) -> String {
            match backend() {
                Backend::Rust => c.to_toml(),
                Backend::Go => go_config_to_toml($prefix, &<$go>::from(c)),
            }
        }

        pub fn $decode(mut c: $ty, toml: &str) -> Result<$ty, String> {
            match backend() {
                Backend::Rust => c.decode_toml(toml).map(|()| c).map_err(|e| e.0),
                Backend::Go => go_config_decode_toml($prefix, &<$go>::from(&c), toml).map(Into::into),
            }
        }
    };
}

config_facade!(
    new_http_config,
    http_config_to_toml,
    http_config_decode_toml,
    HttpConfig,
    GoHttpConfig,
    "broadcast.HTTPConfig"
);
config_facade!(
    new_redis_config,
    redis_config_to_toml,
    redis_config_decode_toml,
    RedisConfig,
    GoRedisStreamConfig,
    "broadcast.RedisConfig"
);
config_facade!(
    new_legacy_redis_config,
    legacy_redis_config_to_toml,
    legacy_redis_config_decode_toml,
    LegacyRedisConfig,
    GoLegacyRedisConfig,
    "broadcast.LegacyRedisConfig"
);
config_facade!(
    new_legacy_nats_config,
    legacy_nats_config_to_toml,
    legacy_nats_config_decode_toml,
    LegacyNatsConfig,
    GoLegacyNatsConfig,
    "broadcast.LegacyNATSConfig"
);
