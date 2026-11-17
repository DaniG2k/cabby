//! Cross-node pub/sub interfaces (upstream package `pubsub`). Redis and NATS
//! subscribers live in `cabby_server::pubsub`.
//!
//! Status: interfaces only; every behavior is [`pending!`].

use std::sync::Arc;
use std::sync::mpsc::Sender;

use crate::common::{RemoteCommandMessage, StreamMessage};
use crate::pending;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PubSubError(pub String);

/// Receives messages from pub/sub (upstream `pubsub.Handler`, implemented by the node).
pub trait Handler: Send + Sync {
    fn broadcast(&self, msg: StreamMessage);
    fn execute_remote_command(&self, cmd: RemoteCommandMessage);
}

/// Subscribes to individual streams and publishes messages to streams
/// (upstream `pubsub.Subscriber`).
pub trait Subscriber: Send + Sync {
    /// Starts the subscriber; fatal errors after start are sent to `done`.
    fn start(&self, done: Sender<PubSubError>) -> Result<(), PubSubError>;
    fn shutdown(&self) -> Result<(), PubSubError>;
    fn broadcast(&self, msg: &StreamMessage);
    fn broadcast_command(&self, cmd: &RemoteCommandMessage);
    fn subscribe(&self, stream: &str);
    fn unsubscribe(&self, stream: &str);
    fn is_multi_node(&self) -> bool;
}

/// Single-node subscriber used with legacy broadcasters (upstream `pubsub.LegacySubscriber`).
pub struct LegacySubscriber {
    _node: Arc<dyn Handler>,
}

impl LegacySubscriber {
    pub fn new(node: Arc<dyn Handler>) -> Self {
        Self { _node: node }
    }
}

impl Subscriber for LegacySubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        pending!("pubsub::legacy_start")
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        pending!("pubsub::legacy_shutdown")
    }

    fn broadcast(&self, _msg: &StreamMessage) {
        pending!("pubsub::legacy_broadcast")
    }

    fn broadcast_command(&self, _cmd: &RemoteCommandMessage) {
        pending!("pubsub::legacy_broadcast_command")
    }

    fn subscribe(&self, _stream: &str) {
        pending!("pubsub::legacy_subscribe")
    }

    fn unsubscribe(&self, _stream: &str) {
        pending!("pubsub::legacy_unsubscribe")
    }

    fn is_multi_node(&self) -> bool {
        pending!("pubsub::legacy_is_multi_node")
    }
}
