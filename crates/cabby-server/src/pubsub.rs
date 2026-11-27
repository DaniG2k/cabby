//! Redis and NATS pub/sub subscribers (upstream package `pubsub`; the
//! interfaces are in `cabby_core::pubsub`).
//!
//! Status: implemented: Redis and NATS multi-node subscribers (the legacy
//! single-node subscriber lives in `cabby_core::pubsub`).

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::common::{PubSubMessage, RemoteCommandMessage, StreamMessage, pub_sub_message_from_json};
use cabby_core::gojson;
use cabby_core::logger::{LogValuer, Logger, compact_value_bytes};
use cabby_core::nats::client::{Connection, Incoming};
use cabby_core::pubsub::{Handler, PubSubError, Published, Subscriber};
use cabby_core::toml_config::{self, TomlSection, toml_quote};
use cabby_core::{log_debug, log_error, log_info, log_warn};

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
        format!("channel = {}\n\n", toml_quote(&self.channel))
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
        format!("channel = {}\n\n", toml_quote(&self.channel))
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), PubSubError> {
        decode(self, toml)
    }
}

/// Redis pub/sub subscriber (upstream `pubsub.RedisSubscriber`).
///
/// Streams map to Redis channels of the same name; the internal channel
/// carries remote commands. Subscriptions are kept alive by
/// [`crate::redis::subscription`] (heartbeat, resubscription after unsolicited
/// unsubscribes and reconnects: deviation D-003); publishing uses a separate
/// connection.
pub struct RedisSubscriber {
    node: Arc<dyn Handler>,
    channel: String,
    redis: RedisClientConfig,
    spec: crate::redis::connect::ConnectSpec,
    subscriptions: Arc<redis_sub::Subscriptions>,
    publisher: std::sync::Mutex<Option<Arc<crate::redis::publisher::Publisher>>>,
    log: Logger,
}

use crate::redis::subscription as redis_sub;

/// Delivers pub/sub messages to the node (upstream `OnMessage` hook).
struct NodeSink {
    node: Arc<dyn Handler>,
    log: Logger,
}

impl redis_sub::Sink for NodeSink {
    fn message(&self, channel: &str, payload: &[u8]) {
        handle_message(self.node.as_ref(), &self.log, channel, payload, false);
    }
}

/// Upstream `OnMessage` (Redis) / `handleMessage` (NATS): stream messages and
/// remote commands go to the node; NATS warns about anything else (batches),
/// Redis ignores it.
fn handle_message(node: &dyn Handler, log: &Logger, channel: &str, payload: &[u8], warn_unknown: bool) {
    match pub_sub_message_from_json(payload) {
        Ok(PubSubMessage::Stream(msg)) => {
            log_debug!(log, "received broadcast message", "channel" => channel);
            node.broadcast(&msg);
        }
        Ok(PubSubMessage::RemoteCommand(cmd)) => {
            log_debug!(log, "received remote command", "channel" => channel);
            node.execute_remote_command(&cmd);
        }
        Ok(PubSubMessage::Batch(_)) => {
            if warn_unknown {
                log_warn!(log, "received unknown message", "channel" => channel, "data" => compact_value_bytes(payload).log_value());
            }
        }
        Err(e) => {
            log_warn!(log, "failed to parse pubsub message", "data" => compact_value_bytes(payload).log_value(), "error" => e.to_string());
        }
    }
}

/// Upstream `Publish`'s debug record (`log.With("channel", stream)`).
fn log_publish(log: &Logger, channel: &str, msg: Published<'_>) {
    log_debug!(log, "publish message", "channel" => channel, "data" => msg.log_value());
}

impl RedisSubscriber {
    pub fn new(node: Arc<dyn Handler>, config: &RedisConfig) -> Result<Self, PubSubError> {
        let mut redis = crate::redis::or_defaults(config.redis.as_ref());
        redis.to_client_options().map_err(|e| PubSubError(e.0))?;
        let spec = redis.connect_spec().map_err(|e| PubSubError(e.0))?;
        let log = Logger::default().with("context", "pubsub");
        let subscriptions = redis_sub::Subscriptions::new(redis_sub::Settings {
            log: log.clone(),
            primary: config.channel.clone(),
            max_reconnect_attempts: redis.max_reconnect_attempts,
            connected_msg: "connected",
            reconnected_msg: "reconnected",
            exhausted_error: "failed to reconnect to Redis: attempts exceeded",
            subscribe_failed: "failed to resubscribe",
            lost: &[(cabby_core::logger::Level::Error, "Redis pub/sub disconnected")],
            activations: node.activations(),
        });
        Ok(Self {
            node,
            channel: config.channel.clone(),
            redis,
            spec,
            subscriptions,
            publisher: std::sync::Mutex::new(None),
            log,
        })
    }

    fn publish(&self, channel: &str, msg: Published<'_>, payload: Vec<u8>) {
        log_publish(&self.log, channel, msg);
        // Publishing may wait for room in the queue: not under the lock.
        let publisher = self.publisher.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(p) = publisher {
            p.publish(channel, payload);
        }
    }
}

impl Subscriber for RedisSubscriber {
    fn start(&self, done: Sender<PubSubError>) -> Result<(), PubSubError> {
        log_info!(self.log, &crate::redis::pubsub_start_message("Starting Redis pub/sub", &self.redis));

        // Add internal channel to subscriptions
        self.subscriptions.subscribe(&self.channel);

        let mut publisher = self.publisher.lock().unwrap_or_else(|e| e.into_inner());
        if publisher.is_none() {
            *publisher = Some(Arc::new(crate::redis::publisher::Publisher::start(self.spec.clone(), "pubsub")));
        }
        drop(publisher);

        let sink = Arc::new(NodeSink { node: Arc::clone(&self.node), log: self.log.clone() });
        self.subscriptions.start(
            self.spec.clone(),
            sink,
            Box::new(move |err| {
                let _ = done.send(PubSubError(err));
            }),
        );
        Ok(())
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        let Some(publisher) = self.publisher.lock().unwrap_or_else(|e| e.into_inner()).take() else {
            return Ok(());
        };
        log_debug!(self.log, "shutting down Redis pub/sub");
        publisher.shutdown();
        self.subscriptions.shutdown(std::time::Duration::from_secs(2));
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        self.publish(&msg.stream, Published::Stream(msg), cabby_core::gojson::to_vec(msg));
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        self.publish(&self.channel, Published::Command(cmd), cabby_core::gojson::to_vec(cmd));
    }

    fn subscribe(&self, stream: &str) {
        log_debug!(self.log, "subscribing", "channel" => stream);
        self.subscriptions.subscribe(stream);
    }

    fn unsubscribe(&self, stream: &str) {
        if self.subscriptions.unsubscribe(stream) {
            log_debug!(self.log, "unsubscribing", "channel" => stream);
        }
    }

    fn is_multi_node(&self) -> bool {
        true
    }
}

/// Brokers publish through the subscriber's `Broadcaster` subset (upstream passes
/// a `pubsub.Subscriber` where a `broker.Broadcaster` is expected).
impl cabby_core::broker::Broadcaster for RedisSubscriber {
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

/// NATS subscriber (upstream `pubsub.NATSSubscriber`): streams are NATS
/// subjects, remote commands go to the internal channel subject.
///
/// Calls before `start` (and after `shutdown`) are ignored; upstream requires
/// `Start` first.
pub struct NatsSubscriber {
    node: Arc<dyn Handler>,
    config: NatsConfig,
    conn: std::sync::Mutex<Option<Arc<Connection>>>,
    log: Logger,
}

impl NatsSubscriber {
    /// Upstream `NewNATSSubscriber(node, config)`.
    pub fn new(node: Arc<dyn Handler>, config: &NatsConfig) -> Result<Self, PubSubError> {
        Ok(Self {
            node,
            config: config.clone(),
            conn: std::sync::Mutex::new(None),
            log: Logger::default().with("context", "pubsub"),
        })
    }

    fn conn(&self) -> Option<Arc<Connection>> {
        self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn publish(&self, subject: &str, msg: Published<'_>, payload: Vec<u8>) {
        log_publish(&self.log, subject, msg);
        if let Some(c) = self.conn()
            && let Err(e) = c.publish(subject, payload)
        {
            log_error!(self.log, "failed to publish message", "error" => e.0);
        }
    }
}

impl Subscriber for NatsSubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        let nats = self.config.nats.clone().ok_or_else(|| PubSubError("NATS configuration is missing".into()))?;
        let node = self.node.clone();
        let log = self.log.clone();
        let conn = Connection::open(&nats, "cabby-nats-pubsub", self.log.clone(), move |msg: Incoming| {
            handle_message(node.as_ref(), &log, &msg.subject, &msg.payload, true)
        })
        .map_err(|e| PubSubError(e.0))?;
        log_info!(self.log, &format!("Starting NATS pub/sub: {}", nats.filtered_servers()));
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
        self.publish(&msg.stream, Published::Stream(msg), gojson::to_vec(msg));
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        self.publish(&self.config.channel, Published::Command(cmd), gojson::to_vec(cmd));
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
