//! Cross-node pub/sub interfaces and the single-node subscriber (upstream package
//! `pubsub`). Redis and NATS subscribers live in `cabby_server::pubsub`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use crate::broker::Broadcaster;
use crate::common::{RemoteCommandMessage, StreamMessage};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PubSubError(pub String);

/// Receives messages from pub/sub (upstream `pubsub.Handler`, implemented by the
/// node). Messages are borrowed, as upstream passes pointers.
pub trait Handler: Send + Sync {
    fn broadcast(&self, msg: &StreamMessage);
    fn execute_remote_command(&self, cmd: &RemoteCommandMessage);
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

/// Single-node subscriber used with legacy broadcasters (upstream
/// `pubsub.LegacySubscriber`): publications go straight back to the node and
/// subscriptions are no-ops.
pub struct LegacySubscriber {
    node: Arc<dyn Handler>,
}

impl LegacySubscriber {
    /// Upstream `NewLegacySubscriber(node)`.
    pub fn new(node: Arc<dyn Handler>) -> Self {
        Self { node }
    }
}

impl Subscriber for LegacySubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        self.node.broadcast(msg);
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        self.node.execute_remote_command(cmd);
    }

    fn subscribe(&self, _stream: &str) {}

    fn unsubscribe(&self, _stream: &str) {}

    fn is_multi_node(&self) -> bool {
        false
    }
}

/// Brokers publish through the subscriber's `Broadcaster` subset (upstream passes
/// a `pubsub.Subscriber` where a `broker.Broadcaster` is expected).
impl Broadcaster for LegacySubscriber {
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
