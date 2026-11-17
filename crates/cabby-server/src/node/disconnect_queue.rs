//! Deferred Disconnect callbacks (upstream `node/disconnect_queue.go` and
//! `node/disconnector.go`).

use std::sync::Arc;

use cabby_core::pending;

use super::{DisconnectQueueConfig, Node, NodeError, Session};
use crate::rpc::Context;

/// Where the node sends sessions whose Disconnect callback must run (upstream
/// `node.Disconnector`).
pub trait Disconnector: Send + Sync {
    /// Processes the queue until shutdown (blocks; run it on its own thread).
    fn run(&self) -> Result<(), NodeError>;
    /// Stops accepting sessions and invokes the remaining calls one by one until
    /// `ctx` is cancelled or past its deadline.
    fn shutdown(&self, ctx: &Context) -> Result<(), NodeError>;
    fn enqueue(&self, session: Arc<Session>) -> Result<(), NodeError>;
    fn size(&self) -> usize;
}

/// Does nothing (upstream `NoopDisconnectQueue` / `NewNoopDisconnector`).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopDisconnectQueue;

impl Disconnector for NoopDisconnectQueue {
    fn run(&self) -> Result<(), NodeError> {
        pending!("node::noop_disconnector_run")
    }
    fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        pending!("node::noop_disconnector_shutdown")
    }
    fn enqueue(&self, _session: Arc<Session>) -> Result<(), NodeError> {
        pending!("node::noop_disconnector_enqueue")
    }
    fn size(&self) -> usize {
        pending!("node::noop_disconnector_size")
    }
}

/// Calls Disconnect synchronously on enqueue (upstream `InlineDisconnector`).
pub struct InlineDisconnector {
    _node: Arc<Node>,
}

impl InlineDisconnector {
    pub fn new(node: Arc<Node>) -> Self {
        Self { _node: node }
    }
}

impl Disconnector for InlineDisconnector {
    fn run(&self) -> Result<(), NodeError> {
        pending!("node::inline_disconnector_run")
    }
    fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        pending!("node::inline_disconnector_shutdown")
    }
    fn enqueue(&self, _session: Arc<Session>) -> Result<(), NodeError> {
        pending!("node::inline_disconnector_enqueue")
    }
    fn size(&self) -> usize {
        pending!("node::inline_disconnector_size")
    }
}

/// A rate-limited, bounded queue of Disconnect calls run by a fixed number of
/// workers that share one throttle ticker (upstream `node.DisconnectQueue`).
///
/// The ticker comes from the node's [`super::Clock`], so tests drive the rate
/// with a controlled clock.
pub struct DisconnectQueue {
    _private: (),
}

impl DisconnectQueue {
    /// Upstream `NewDisconnectQueue(node, config, logger)`: period `1000 / rate` ms,
    /// at least one worker, `max_backlog` limit (0: unlimited).
    pub fn new(_node: Arc<Node>, _config: &DisconnectQueueConfig) -> Self {
        pending!("node::new_disconnect_queue")
    }

    /// Removes the front session without calling Disconnect (upstream tests call
    /// the unexported `q.disconnect.Remove()`).
    pub fn remove(&self) -> Option<Arc<Session>> {
        pending!("node::disconnect_queue_remove")
    }
}

impl Disconnector for DisconnectQueue {
    fn run(&self) -> Result<(), NodeError> {
        pending!("node::disconnect_queue_run")
    }
    fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        pending!("node::disconnect_queue_shutdown")
    }
    /// Fails (and counts `disconnect_queue_discarded_total`) when the backlog
    /// limit is reached; silently drops sessions after shutdown.
    fn enqueue(&self, _session: Arc<Session>) -> Result<(), NodeError> {
        pending!("node::disconnect_queue_enqueue")
    }
    fn size(&self) -> usize {
        pending!("node::disconnect_queue_size")
    }
}
