//! Broadcast ingestion interfaces (upstream package `broadcast`). HTTP, Redis
//! Streams and legacy Redis/NATS broadcasters live in `cabby_server::broadcast`.
//!
//! Status: interfaces only.

use std::sync::mpsc::Sender;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BroadcastError(pub String);

/// Receives broadcast payloads (upstream `broadcast.Handler`, implemented by the node).
pub trait Handler: Send + Sync {
    /// A broadcast delivered only to this node (single-node broadcasters).
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), BroadcastError>;
    /// A broadcast delivered to all nodes (fan-out broadcasters).
    fn handle_pubsub(&self, json: &[u8]);
}

/// Consumes broadcasts from the outside world (upstream `broadcast.Broadcaster`).
pub trait Broadcaster: Send + Sync {
    fn start(&self, done: Sender<BroadcastError>) -> Result<(), BroadcastError>;
    fn shutdown(&self) -> Result<(), BroadcastError>;
    /// Whether every node receives the same event (such broadcasters must not be
    /// combined with a real pub/sub engine).
    fn is_fanout(&self) -> bool;
}
