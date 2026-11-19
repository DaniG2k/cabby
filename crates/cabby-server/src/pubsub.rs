//! Redis and NATS pub/sub subscribers (upstream package `pubsub`; the
//! interfaces are in `cabby_core::pubsub`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
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
    publisher: std::sync::Mutex<Option<crate::redis::publisher::Publisher>>,
}

use crate::redis::subscription as redis_sub;

/// Delivers pub/sub messages to the node (upstream `OnMessage` hook).
struct NodeSink {
    node: Arc<dyn Handler>,
}

impl redis_sub::Sink for NodeSink {
    fn message(&self, channel: &str, payload: &[u8]) {
        match cabby_core::common::pub_sub_message_from_json(payload) {
            Ok(cabby_core::common::PubSubMessage::Stream(msg)) => self.node.broadcast(&msg),
            Ok(cabby_core::common::PubSubMessage::RemoteCommand(cmd)) => self.node.execute_remote_command(&cmd),
            // Upstream handles stream messages and remote commands only.
            Ok(cabby_core::common::PubSubMessage::Batch(_)) => {}
            Err(e) => crate::redis::log(
                cabby_core::logger::Level::Warn,
                "pubsub",
                "failed to parse pubsub message",
                &[
                    ("channel", channel.to_owned()),
                    ("data", String::from_utf8_lossy(payload).into_owned()),
                    ("error", e.to_string()),
                ],
            ),
        }
    }
}

impl RedisSubscriber {
    pub fn new(node: Arc<dyn Handler>, config: &RedisConfig) -> Result<Self, PubSubError> {
        let mut redis = crate::redis::or_defaults(config.redis.as_ref());
        redis.to_client_options().map_err(|e| PubSubError(e.0))?;
        let spec = redis.connect_spec().map_err(|e| PubSubError(e.0))?;
        let subscriptions = redis_sub::Subscriptions::new(redis_sub::Settings {
            context: "pubsub",
            primary: config.channel.clone(),
            max_reconnect_attempts: redis.max_reconnect_attempts,
            connected_msg: "connected",
            reconnected_msg: "reconnected",
            exhausted_error: "failed to reconnect to Redis: attempts exceeded",
        });
        Ok(Self {
            node,
            channel: config.channel.clone(),
            redis,
            spec,
            subscriptions,
            publisher: std::sync::Mutex::new(None),
        })
    }

    fn publish(&self, channel: &str, payload: Vec<u8>) {
        if let Some(p) = self.publisher.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            p.publish(channel, payload);
        }
    }
}

impl Subscriber for RedisSubscriber {
    fn start(&self, done: Sender<PubSubError>) -> Result<(), PubSubError> {
        crate::redis::log(
            cabby_core::logger::Level::Info,
            "pubsub",
            &crate::redis::pubsub_start_message("Starting Redis pub/sub", &self.redis),
            &[],
        );

        // Add internal channel to subscriptions
        self.subscriptions.subscribe(&self.channel);

        let mut publisher = self.publisher.lock().unwrap_or_else(|e| e.into_inner());
        if publisher.is_none() {
            *publisher = Some(crate::redis::publisher::Publisher::start(self.spec.clone(), "pubsub"));
        }
        drop(publisher);

        let sink = Arc::new(NodeSink { node: Arc::clone(&self.node) });
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
        crate::redis::log(cabby_core::logger::Level::Debug, "pubsub", "shutting down Redis pub/sub", &[]);
        publisher.shutdown();
        self.subscriptions.shutdown(std::time::Duration::from_secs(2));
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        self.publish(&msg.stream, cabby_core::gojson::to_vec(msg));
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        self.publish(&self.channel, cabby_core::gojson::to_vec(cmd));
    }

    fn subscribe(&self, stream: &str) {
        self.subscriptions.subscribe(stream);
    }

    fn unsubscribe(&self, stream: &str) {
        self.subscriptions.unsubscribe(stream);
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

/// NATS subscriber (upstream `pubsub.NATSSubscriber`).
pub struct NatsSubscriber {
    _private: (),
}

impl NatsSubscriber {
    pub fn new(_node: Arc<dyn Handler>, _config: &NatsConfig) -> Result<Self, PubSubError> {
        pending!("pubsub::new_nats_subscriber")
    }
}

impl Subscriber for NatsSubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        pending!("pubsub::nats_start")
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        pending!("pubsub::nats_shutdown")
    }

    fn broadcast(&self, _msg: &StreamMessage) {
        pending!("pubsub::nats_broadcast")
    }

    fn broadcast_command(&self, _cmd: &RemoteCommandMessage) {
        pending!("pubsub::nats_broadcast_command")
    }

    fn subscribe(&self, _stream: &str) {
        pending!("pubsub::nats_subscribe")
    }

    fn unsubscribe(&self, _stream: &str) {
        pending!("pubsub::nats_unsubscribe")
    }

    fn is_multi_node(&self) -> bool {
        pending!("pubsub::nats_is_multi_node")
    }
}
