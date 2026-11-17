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
