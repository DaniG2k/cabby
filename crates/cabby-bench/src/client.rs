//! Benchi subscriber client (upstream `lib/client.go`): a [`crate::cable::Client`]
//! with one merged message queue and subscribe-with-confirm semantics over
//! public streams (`{"channel":"$pubsub","stream_name":"<stream>"}`).

use std::time::Duration;

use cabby_core::pending;

use crate::BenchError;

/// Merged queue capacity (upstream `defaultMergedCapacity`).
pub const DEFAULT_MERGED_CAPACITY: usize = 256;
/// Per-subscription queue capacity (upstream `defaultSubscriptionCapacity`).
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 16;
/// Confirmation wait per stream (upstream `defaultSubscribeTimeout`).
pub const DEFAULT_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

/// A broadcast on a subscribed stream. String payloads are decoded; other JSON is
/// kept as raw text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    pub stream: String,
    pub data: String,
}

/// Outcome of a bounded receive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    Message(Message),
    /// The client was closed and every in-flight message has been drained
    /// (upstream: `Receive` returns `(Message{}, false)`).
    Closed,
    TimedOut,
}

/// The `$pubsub` identifier for `stream`, as JSON text.
pub fn stream_identifier(_stream: &str) -> String {
    pending!("benchi::stream_identifier")
}

/// A public-streams subscriber. `Sync`: `close` may race with `receive` from other
/// threads (upstream asserts `Close` is safe from multiple goroutines).
pub struct Client {
    _inner: (),
}

impl Client {
    /// Rejects an empty URL; does not dial.
    pub fn build(_server_url: &str) -> Result<Client, BenchError> {
        pending!("benchi::build_client")
    }

    /// Dials and consumes the welcome message, giving up after `timeout` if set
    /// (upstream: context cancellation).
    pub fn connect(&self, _timeout: Option<Duration>) -> Result<(), BenchError> {
        pending!("benchi::client_connect")
    }

    /// Subscribes to each stream not yet subscribed and waits for its confirmation.
    /// Already-subscribed streams send nothing. The error names every stream that
    /// timed out or was rejected.
    pub fn subscribe(&self, _streams: &[String]) -> Result<(), BenchError> {
        pending!("benchi::client_subscribe")
    }

    /// Blocks until the next message, or until the client is closed and drained.
    pub fn receive(&self) -> Option<Message> {
        pending!("benchi::client_receive")
    }

    pub fn receive_timeout(&self, _timeout: Duration) -> Received {
        pending!("benchi::client_receive_timeout")
    }

    /// Closes the connection, drains the forwarders and closes the merged queue.
    /// Idempotent.
    pub fn close(&self) {
        pending!("benchi::client_close")
    }
}
