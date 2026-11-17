//! Redis and NATS pub/sub subscribers (upstream package `pubsub`; the
//! interfaces are in `cabby_core::pubsub`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_core::pending;
use cabby_core::pubsub::{Handler, PubSubError, Subscriber};

use crate::nats::NatsConfig as NatsClientConfig;
use crate::redis::RedisConfig as RedisClientConfig;

/// Upstream `pubsub.RedisConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    /// Internal channel for remote commands.
    pub channel: String,
    pub redis: Option<RedisClientConfig>,
}

impl RedisConfig {
    /// Upstream `pubsub.NewRedisConfig()`.
    pub fn new() -> Self {
        pending!("pubsub::new_redis_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("pubsub::redis_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), PubSubError> {
        pending!("pubsub::redis_config_decode_toml")
    }
}

/// Upstream `pubsub.NATSConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NatsConfig {
    pub channel: String,
    pub nats: Option<NatsClientConfig>,
}

impl NatsConfig {
    /// Upstream `pubsub.NewNATSConfig()`.
    pub fn new() -> Self {
        pending!("pubsub::new_nats_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("pubsub::nats_config_to_toml")
    }

    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), PubSubError> {
        pending!("pubsub::nats_config_decode_toml")
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
