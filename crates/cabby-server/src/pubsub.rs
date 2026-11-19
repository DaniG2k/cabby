//! Redis and NATS pub/sub subscribers (upstream package `pubsub`; the
//! interfaces are in `cabby_core::pubsub`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::common::{PubSubMessage, RemoteCommandMessage, StreamMessage, pub_sub_message_from_json};
use cabby_core::gojson;
use cabby_core::nats::client::{Connection, Incoming};
use cabby_core::pending;
use cabby_core::pubsub::{Handler, PubSubError, Subscriber};
use cabby_core::toml_config::{self, TomlSection};

use crate::nats::NatsConfig as NatsClientConfig;
use crate::redis::RedisConfig as RedisClientConfig;

/// Upstream `pubsub.RedisConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    /// Internal channel for remote commands.
    pub channel: String,
    pub redis: Option<RedisClientConfig>,
}

/// Upstream's internal pub/sub channel name.
const INTERNAL_CHANNEL: &str = "__anycable_internal__";

fn decode(target: &mut impl TomlSection, toml: &str) -> Result<(), PubSubError> {
    toml_config::decode(target, toml).map_err(|e| PubSubError(e.0))?;
    Ok(())
}

// Upstream's `Redis` pointer field is untagged (matched by its Go name).
cabby_core::toml_section!(RedisConfig, "pubsub.RedisConfig" {
    "channel" => channel,
    "Redis" => redis,
});

impl RedisConfig {
    /// Upstream `pubsub.NewRedisConfig()`.
    pub fn new() -> Self {
        Self { channel: INTERNAL_CHANNEL.to_owned(), redis: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), PubSubError> {
        decode(self, toml)
    }
}

/// Upstream `pubsub.NATSConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NatsConfig {
    pub channel: String,
    pub nats: Option<NatsClientConfig>,
}

// Upstream's `NATS` pointer field is untagged (matched by its Go name).
cabby_core::toml_section!(NatsConfig, "pubsub.NATSConfig" {
    "channel" => channel,
    "NATS" => nats,
});

impl NatsConfig {
    /// Upstream `pubsub.NewNATSConfig()`.
    pub fn new() -> Self {
        Self { channel: INTERNAL_CHANNEL.to_owned(), nats: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), PubSubError> {
        decode(self, toml)
    }
}

/// Redis pub/sub subscriber (upstream `pubsub.RedisSubscriber`).
pub struct RedisSubscriber {
    _private: (),
}

impl RedisSubscriber {
    pub fn new(_node: Arc<dyn Handler>, _config: &RedisConfig) -> Result<Self, PubSubError> {
        pending!("pubsub::new_redis_subscriber")
    }
}

impl Subscriber for RedisSubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        pending!("pubsub::redis_start")
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        pending!("pubsub::redis_shutdown")
    }

    fn broadcast(&self, _msg: &StreamMessage) {
        pending!("pubsub::redis_broadcast")
    }

    fn broadcast_command(&self, _cmd: &RemoteCommandMessage) {
        pending!("pubsub::redis_broadcast_command")
    }

    fn subscribe(&self, _stream: &str) {
        pending!("pubsub::redis_subscribe")
    }

    fn unsubscribe(&self, _stream: &str) {
        pending!("pubsub::redis_unsubscribe")
    }

    fn is_multi_node(&self) -> bool {
        pending!("pubsub::redis_is_multi_node")
    }
}

/// NATS subscriber (upstream `pubsub.NATSSubscriber`): streams are NATS
/// subjects, remote commands go to the internal channel subject.
///
/// Calls before `start` (and after `shutdown`) are ignored; upstream requires
/// `Start` first.
pub struct NatsSubscriber {
    node: Arc<dyn Handler>,
    config: NatsConfig,
    conn: std::sync::Mutex<Option<Arc<Connection>>>,
}

impl NatsSubscriber {
    /// Upstream `NewNATSSubscriber(node, config)`.
    pub fn new(node: Arc<dyn Handler>, config: &NatsConfig) -> Result<Self, PubSubError> {
        Ok(Self { node, config: config.clone(), conn: std::sync::Mutex::new(None) })
    }

    fn conn(&self) -> Option<Arc<Connection>> {
        self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn publish(&self, subject: &str, payload: Vec<u8>) {
        if let Some(c) = self.conn() {
            c.publish(subject, payload);
        }
    }
}

/// Upstream `handleMessage`: stream messages and remote commands go to the
/// node; anything else (including batches) is ignored.
fn handle_nats_message(node: &dyn Handler, msg: &Incoming) {
    match pub_sub_message_from_json(&msg.payload) {
        Ok(PubSubMessage::Stream(m)) => node.broadcast(&m),
        Ok(PubSubMessage::RemoteCommand(c)) => node.execute_remote_command(&c),
        Ok(PubSubMessage::Batch(_)) | Err(_) => {}
    }
}

impl Subscriber for NatsSubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        let nats = self.config.nats.clone().ok_or_else(|| PubSubError("NATS configuration is missing".into()))?;
        let node = self.node.clone();
        let conn = Connection::open(&nats, "cabby-nats-pubsub", move |msg| handle_nats_message(node.as_ref(), &msg))
            .map_err(|e| PubSubError(e.0))?;
        conn.subscribe(&self.config.channel);
        *self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(conn));
        Ok(())
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        let conn = self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(c) = conn {
            c.close();
        }
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        self.publish(&msg.stream, gojson::to_vec(msg));
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        self.publish(&self.config.channel, gojson::to_vec(cmd));
    }

    fn subscribe(&self, stream: &str) {
        if let Some(c) = self.conn() {
            c.subscribe(stream);
        }
    }

    fn unsubscribe(&self, stream: &str) {
        if let Some(c) = self.conn() {
            c.unsubscribe(stream);
        }
    }

    fn is_multi_node(&self) -> bool {
        true
    }
}

/// Brokers publish through the subscriber (upstream passes the subscriber
/// where a `broker.Broadcaster` is expected).
impl cabby_core::broker::Broadcaster for NatsSubscriber {
    fn broadcast(&self, msg: &StreamMessage) {
        Subscriber::broadcast(self, msg);
    }

    fn broadcast_command(&self, msg: &RemoteCommandMessage) {
        Subscriber::broadcast_command(self, msg);
    }

    fn subscribe(&self, stream: &str) {
        Subscriber::subscribe(self, stream);
    }

    fn unsubscribe(&self, stream: &str) {
        Subscriber::unsubscribe(self, stream);
    }
}
