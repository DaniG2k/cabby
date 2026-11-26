//! Facade for upstream package `pubsub`.
//!
//! [`TestHandler`] is upstream's `TestHandler` (pubsub/subscriber_test.go).
//! Upstream's `Receive` waits 100 ms, both where a test requires a message and
//! where it checks that none arrives; 100 ms is short for a message that must
//! arrive on a loaded machine. So `receive` returns a [`Received`] that waits
//! when the test looks at it: up to [`EXPECT_TIMEOUT`] for `expect` (a message
//! is required, so the longer wait only gives it time), and upstream's
//! [`RECEIVE_TIMEOUT`] for `is_none`, `is_some` and `is_some_and`. On the Go
//! backend handlers and subscribers live in the bridge (`bridge/pubsub.go`) and
//! talk to the same Redis/NATS servers.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_core::pubsub as rs;
use cabby_server::pubsub::{NatsConfig, NatsSubscriber, RedisConfig, RedisSubscriber};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::msg_shapes::{
    GoNatsConfig, GoParsed, GoRedisConfig, GoRemoteCommand, GoStreamMessage, go_config_decode_toml, go_config_new,
    go_config_to_toml, go_nats, go_redis, go_unit, remote_command_json, stream_message_json,
};
use crate::{Backend, backend, bridge};

/// Upstream `TestHandler.Receive` waits this long; kept where a test checks
/// that nothing arrives.
pub const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// How long [`Received::expect`] waits for a message the test requires.
pub const EXPECT_TIMEOUT: Duration = Duration::from_secs(5);

struct Queue<T> {
    items: Mutex<VecDeque<T>>,
    ready: Condvar,
}

impl<T> Queue<T> {
    fn new() -> Self {
        Self { items: Mutex::new(VecDeque::new()), ready: Condvar::new() }
    }

    fn push(&self, v: T) {
        self.items.lock().unwrap_or_else(|e| e.into_inner()).push_back(v);
        self.ready.notify_all();
    }

    fn pop(&self, timeout: Duration) -> Option<T> {
        let g = self.items.lock().unwrap_or_else(|e| e.into_inner());
        let (mut q, _) = self.ready.wait_timeout_while(g, timeout, |q| q.is_empty()).unwrap_or_else(|e| e.into_inner());
        q.pop_front()
    }
}

struct RustHandler {
    messages: Queue<StreamMessage>,
    commands: Queue<RemoteCommandMessage>,
}

impl rs::Handler for RustHandler {
    fn broadcast(&self, msg: &StreamMessage) {
        self.messages.push(msg.clone());
    }

    fn execute_remote_command(&self, cmd: &RemoteCommandMessage) {
        self.commands.push(cmd.clone());
    }
}

#[derive(Clone)]
enum HandlerImpl {
    Rust(Arc<RustHandler>),
    Go(i64),
}

fn next_message(handler: &HandlerImpl, timeout: Duration) -> Option<StreamMessage> {
    match handler {
        HandlerImpl::Rust(h) => h.messages.pop(timeout),
        HandlerImpl::Go(id) => {
            let v: Vec<GoStreamMessage> =
                bridge::call("pubsub.HandlerReceive", json!({"Handler": id, "TimeoutMs": timeout.as_millis() as u64}));
            v.into_iter().next().map(Into::into)
        }
    }
}

fn next_command(handler: &HandlerImpl, timeout: Duration) -> Option<RemoteCommandMessage> {
    match handler {
        HandlerImpl::Rust(h) => h.commands.pop(timeout),
        HandlerImpl::Go(id) => {
            let v: Vec<GoRemoteCommand> = bridge::call(
                "pubsub.HandlerReceiveCommand",
                json!({"Handler": id, "TimeoutMs": timeout.as_millis() as u64}),
            );
            v.into_iter().next().map(Into::into)
        }
    }
}

/// The next message or command of a [`TestHandler`], received when the test
/// looks at it (upstream: the `*StreamMessage` that `Receive` returns, `nil`
/// after 100 ms). See the module documentation for the waits.
#[must_use = "the receive happens when the result is looked at"]
pub struct Received<T> {
    handler: HandlerImpl,
    next: fn(&HandlerImpl, Duration) -> Option<T>,
}

impl<T> Received<T> {
    /// The next item within `timeout`, if any.
    pub fn within(self, timeout: Duration) -> Option<T> {
        (self.next)(&self.handler, timeout)
    }

    /// The test requires an item: waits up to [`EXPECT_TIMEOUT`], panics with
    /// `msg` without one.
    pub fn expect(self, msg: &str) -> T {
        self.within(EXPECT_TIMEOUT).expect(msg)
    }

    /// Whether nothing arrives within [`RECEIVE_TIMEOUT`].
    pub fn is_none(self) -> bool {
        self.within(RECEIVE_TIMEOUT).is_none()
    }

    /// Whether an item arrives within [`RECEIVE_TIMEOUT`].
    pub fn is_some(self) -> bool {
        self.within(RECEIVE_TIMEOUT).is_some()
    }

    /// Whether an item arrives within [`RECEIVE_TIMEOUT`] and satisfies `f`.
    pub fn is_some_and(self, f: impl FnOnce(T) -> bool) -> bool {
        self.within(RECEIVE_TIMEOUT).is_some_and(f)
    }
}

pub struct TestHandler {
    inner: HandlerImpl,
}

impl TestHandler {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Self {
                inner: HandlerImpl::Rust(Arc::new(RustHandler { messages: Queue::new(), commands: Queue::new() })),
            },
            Backend::Go => Self { inner: HandlerImpl::Go(bridge::call("pubsub.NewTestHandler", json!({}))) },
        }
    }

    fn rust(&self) -> Arc<dyn rs::Handler> {
        match &self.inner {
            HandlerImpl::Rust(h) => Arc::clone(h) as Arc<dyn rs::Handler>,
            HandlerImpl::Go(_) => unreachable!(),
        }
    }

    fn go(&self) -> i64 {
        match &self.inner {
            HandlerImpl::Go(id) => *id,
            HandlerImpl::Rust(_) => unreachable!(),
        }
    }

    /// Next broadcast message (upstream `Receive`); see [`Received`].
    pub fn receive(&self) -> Received<StreamMessage> {
        Received { handler: self.inner.clone(), next: next_message }
    }

    /// Next remote command (upstream `ReceiveCommand`); see [`Received`].
    pub fn receive_command(&self) -> Received<RemoteCommandMessage> {
        Received { handler: self.inner.clone(), next: next_command }
    }
}

impl Default for TestHandler {
    fn default() -> Self {
        Self::new()
    }
}

enum SubscriberImpl {
    Rust(Box<dyn rs::Subscriber>),
    Go(i64),
}

/// A `pubsub.Subscriber`.
pub struct Subscriber {
    inner: SubscriberImpl,
}

fn go_subscriber(op: &str, handler: &TestHandler, config: serde_json::Value) -> Result<Subscriber, String> {
    let p: GoParsed<i64> = bridge::call(op, json!({"Handler": handler.go(), "Config": config}));
    p.into_result().map(|id| Subscriber { inner: SubscriberImpl::Go(id.expect("subscriber handle")) })
}

impl Subscriber {
    /// Upstream `NewLegacySubscriber(handler)`.
    pub fn legacy(handler: &TestHandler) -> Self {
        match backend() {
            Backend::Rust => Self { inner: SubscriberImpl::Rust(Box::new(rs::LegacySubscriber::new(handler.rust()))) },
            Backend::Go => Self {
                inner: SubscriberImpl::Go(bridge::call("pubsub.NewLegacySubscriber", json!({"Handler": handler.go()}))),
            },
        }
    }

    /// Upstream `NewRedisSubscriber(handler, &config, ...)`.
    pub fn redis(handler: &TestHandler, config: &RedisConfig) -> Result<Self, String> {
        match backend() {
            Backend::Rust => RedisSubscriber::new(handler.rust(), config)
                .map(|s| Self { inner: SubscriberImpl::Rust(Box::new(s)) })
                .map_err(|e| e.0),
            Backend::Go => {
                go_subscriber("pubsub.NewRedisSubscriber", handler, json!(GoPubSubRedisConfig::from(config)))
            }
        }
    }

    /// The Postgres subscriber (a Cabby addition, D-070: not observable on Go),
    /// with its [`cabby_server::postgres::pubsub::ListenProbe`].
    pub fn postgres(
        handler: &TestHandler,
        config: &cabby_server::postgres::PostgresConfig,
        metrics: cabby_server::postgres::Metrics,
    ) -> Result<(Self, cabby_server::postgres::pubsub::ListenProbe), String> {
        crate::postgres::rust_only();
        let s = cabby_server::postgres::PostgresSubscriber::new(handler.rust(), config, metrics).map_err(|e| e.0)?;
        let probe = s.probe();
        Ok((Self { inner: SubscriberImpl::Rust(Box::new(s)) }, probe))
    }

    /// Upstream `NewNATSSubscriber(handler, &config, ...)`.
    pub fn nats(handler: &TestHandler, config: &NatsConfig) -> Result<Self, String> {
        match backend() {
            Backend::Rust => NatsSubscriber::new(handler.rust(), config)
                .map(|s| Self { inner: SubscriberImpl::Rust(Box::new(s)) })
                .map_err(|e| e.0),
            Backend::Go => go_subscriber("pubsub.NewNATSSubscriber", handler, json!(GoPubSubNatsConfig::from(config))),
        }
    }

    fn go_call<R: serde::de::DeserializeOwned>(&self, op: &str, mut args: serde_json::Value) -> R {
        let SubscriberImpl::Go(id) = &self.inner else { unreachable!() };
        args["Subscriber"] = json!(id);
        bridge::call(op, args)
    }

    /// `Start(done)`; errors reported later on `done` are not observed by
    /// upstream tests.
    pub fn start(&self) -> Result<(), String> {
        match &self.inner {
            SubscriberImpl::Rust(s) => {
                let (tx, _rx) = std::sync::mpsc::channel();
                s.start(tx).map_err(|e| e.0)
            }
            SubscriberImpl::Go(id) => go_unit("pubsub.Start", json!({"Subscriber": id})),
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.shutdown().map_err(|e| e.0),
            SubscriberImpl::Go(id) => go_unit("pubsub.Shutdown", json!({"Subscriber": id})),
        }
    }

    pub fn subscribe(&self, stream: &str) {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.subscribe(stream),
            SubscriberImpl::Go(_) => {
                let _: bool = self.go_call("pubsub.Subscribe", json!({"Stream": stream}));
            }
        }
    }

    pub fn unsubscribe(&self, stream: &str) {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.unsubscribe(stream),
            SubscriberImpl::Go(_) => {
                let _: bool = self.go_call("pubsub.Unsubscribe", json!({"Stream": stream}));
            }
        }
    }

    pub fn broadcast(&self, msg: &StreamMessage) {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.broadcast(msg),
            SubscriberImpl::Go(_) => {
                let _: bool = self.go_call("pubsub.Broadcast", json!({"Msg": stream_message_json(msg)}));
            }
        }
    }

    pub fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.broadcast_command(cmd),
            SubscriberImpl::Go(_) => {
                let _: bool = self.go_call("pubsub.BroadcastCommand", json!({"Cmd": remote_command_json(cmd)}));
            }
        }
    }

    pub fn is_multi_node(&self) -> bool {
        match &self.inner {
            SubscriberImpl::Rust(s) => s.is_multi_node(),
            SubscriberImpl::Go(_) => self.go_call("pubsub.IsMultiNode", json!({})),
        }
    }
}

// ------------------------------------------------------------- configs ---

#[derive(Serialize, Deserialize)]
struct GoPubSubRedisConfig {
    #[serde(rename = "Channel")]
    channel: String,
    #[serde(rename = "Redis")]
    redis: Option<GoRedisConfig>,
}

impl From<&RedisConfig> for GoPubSubRedisConfig {
    fn from(c: &RedisConfig) -> Self {
        Self { channel: c.channel.clone(), redis: go_redis(&c.redis) }
    }
}

impl From<GoPubSubRedisConfig> for RedisConfig {
    fn from(g: GoPubSubRedisConfig) -> Self {
        RedisConfig { channel: g.channel, redis: g.redis.map(Into::into) }
    }
}

#[derive(Serialize, Deserialize)]
struct GoPubSubNatsConfig {
    #[serde(rename = "Channel")]
    channel: String,
    #[serde(rename = "NATS")]
    nats: Option<GoNatsConfig>,
}

impl From<&NatsConfig> for GoPubSubNatsConfig {
    fn from(c: &NatsConfig) -> Self {
        Self { channel: c.channel.clone(), nats: go_nats(&c.nats) }
    }
}

impl From<GoPubSubNatsConfig> for NatsConfig {
    fn from(g: GoPubSubNatsConfig) -> Self {
        NatsConfig { channel: g.channel, nats: g.nats.map(Into::into) }
    }
}

/// Upstream `pubsub.NewRedisConfig()`.
pub fn new_redis_config() -> RedisConfig {
    match backend() {
        Backend::Rust => RedisConfig::new(),
        Backend::Go => go_config_new::<GoPubSubRedisConfig>("pubsub.RedisConfig").into(),
    }
}

pub fn redis_config_to_toml(c: &RedisConfig) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => go_config_to_toml("pubsub.RedisConfig", &GoPubSubRedisConfig::from(c)),
    }
}

/// `toml.Decode(toml, &config)`, returning the decoded config.
pub fn redis_config_decode_toml(mut c: RedisConfig, toml: &str) -> Result<RedisConfig, String> {
    match backend() {
        Backend::Rust => c.decode_toml(toml).map(|()| c).map_err(|e| e.0),
        Backend::Go => {
            go_config_decode_toml("pubsub.RedisConfig", &GoPubSubRedisConfig::from(&c), toml).map(Into::into)
        }
    }
}

/// Upstream `pubsub.NewNATSConfig()`.
pub fn new_nats_config() -> NatsConfig {
    match backend() {
        Backend::Rust => NatsConfig::new(),
        Backend::Go => go_config_new::<GoPubSubNatsConfig>("pubsub.NATSConfig").into(),
    }
}

pub fn nats_config_to_toml(c: &NatsConfig) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => go_config_to_toml("pubsub.NATSConfig", &GoPubSubNatsConfig::from(c)),
    }
}

pub fn nats_config_decode_toml(mut c: NatsConfig, toml: &str) -> Result<NatsConfig, String> {
    match backend() {
        Backend::Rust => c.decode_toml(toml).map(|()| c).map_err(|e| e.0),
        Backend::Go => go_config_decode_toml("pubsub.NATSConfig", &GoPubSubNatsConfig::from(&c), toml).map(Into::into),
    }
}
