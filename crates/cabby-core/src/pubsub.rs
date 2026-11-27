//! Cross-node pub/sub interfaces and the single-node subscriber (upstream package
//! `pubsub`). Redis and NATS subscribers live in `cabby_server::pubsub`.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use crate::broker::Broadcaster;
use crate::common::{RemoteCommandMessage, StreamMessage};
use crate::logger::{LogValue, LogValuer};

mod activations;

pub use activations::Activations;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PubSubError(pub String);

/// Upstream `RemoteCommandMessage.LogValue()`: the command and the raw payload.
pub fn remote_command_log_value(m: &RemoteCommandMessage) -> LogValue {
    m.log_value()
}

/// What subscribers log as a published message's `data` (upstream passes the
/// message, a `slog.LogValuer`).
pub enum Published<'a> {
    Stream(&'a StreamMessage),
    Command(&'a RemoteCommandMessage),
}

impl Published<'_> {
    pub fn log_value(&self) -> LogValue {
        match self {
            Published::Stream(m) => LogValuer::log_value(*m),
            Published::Command(m) => remote_command_log_value(m),
        }
    }
}

/// Receives messages from pub/sub (upstream `pubsub.Handler`, implemented by the
/// node). Messages are borrowed, as upstream passes pointers.
pub trait Handler: Send + Sync {
    fn broadcast(&self, msg: &StreamMessage);
    fn execute_remote_command(&self, cmd: &RemoteCommandMessage);

    /// Where a subscriber reports the stream subscriptions that are not
    /// active yet, so the node waits for them before confirming a
    /// subscription (D-105). `None` (the default): nothing waits.
    fn activations(&self) -> Option<Arc<Activations>> {
        None
    }
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
