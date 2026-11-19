//! Deferred Disconnect callbacks (upstream `node/disconnect_queue.go` and
//! `node/disconnector.go`).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use cabby_core::utils::{Item, Queue};

use super::{DisconnectQueueConfig, METRICS_DISCONNECT_DISCARDED, Node, NodeError, Session};
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
        Ok(())
    }
    fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        Ok(())
    }
    fn enqueue(&self, _session: Arc<Session>) -> Result<(), NodeError> {
        Ok(())
    }
    fn size(&self) -> usize {
        0
    }
}

/// Calls Disconnect synchronously on enqueue (upstream `InlineDisconnector`).
pub struct InlineDisconnector {
    node: Arc<Node>,
}

impl InlineDisconnector {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }
}

impl Disconnector for InlineDisconnector {
    fn run(&self) -> Result<(), NodeError> {
        Ok(())
    }
    fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        Ok(())
    }
    fn enqueue(&self, session: Arc<Session>) -> Result<(), NodeError> {
        self.node.disconnect_now(&session)
    }
    fn size(&self) -> usize {
        0
    }
}

/// Whether `ctx` is cancelled or past its deadline (Go `ctx.Err() != nil`).
pub(super) fn context_done(ctx: &Context) -> bool {
    ctx.cancel.is_cancelled() || ctx.deadline.is_some_and(|d| tokio::time::Instant::now() >= d)
}

/// A rate-limited, bounded queue of Disconnect calls run by a fixed number of
/// workers that share one throttle ticker (upstream `node.DisconnectQueue`).
///
/// The ticker comes from the node's [`super::Clock`], so tests drive the rate
/// with a controlled clock. Enqueueing never blocks: the queue is a growable
/// ring buffer, and over `max_backlog` new sessions are discarded.
pub struct DisconnectQueue {
    node: Arc<Node>,
    rate: Duration,
    workers: usize,
    queue: Queue<Arc<Session>>,
    limit: usize,
    stopped: Mutex<bool>,
}

impl DisconnectQueue {
    /// Upstream `NewDisconnectQueue(node, config, logger)`: period `1000 / rate` ms,
    /// at least one worker, `max_backlog` limit (0: unlimited).
    pub fn new(node: Arc<Node>, config: &DisconnectQueueConfig) -> Self {
        // Upstream divides by the rate (a zero rate panics) and a rate above
        // 1000/s makes a zero period (`time.NewTicker` panics); both are clamped
        // to the fastest valid period instead.
        let millis = 1000 / config.rate.max(1);
        let rate = Duration::from_millis(millis.max(1) as u64);
        let workers = config.workers_num.max(1) as usize;
        let capacity = (config.max_backlog / 8).max(512) as usize;

        Self {
            node,
            rate,
            workers,
            queue: Queue::new(capacity),
            limit: config.max_backlog.max(0) as usize,
            stopped: Mutex::new(false),
        }
    }

    fn stopped(&self) -> MutexGuard<'_, bool> {
        self.stopped.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Removes the front session without calling Disconnect (upstream tests call
    /// the unexported `q.disconnect.Remove()`).
    pub fn remove(&self) -> Option<Arc<Session>> {
        self.queue.remove().map(|item| item.data)
    }
}

impl Disconnector for DisconnectQueue {
    /// Starts the workers; they share the throttle ticker, so the total rate of
    /// calls stays within the limit.
    fn run(&self) -> Result<(), NodeError> {
        let throttle = self.node.clock().ticker(self.rate);

        std::thread::scope(|scope| {
            for _ in 0..self.workers {
                let throttle = throttle.clone();
                scope.spawn(move || {
                    loop {
                        if !self.queue.wait() {
                            return;
                        }
                        // Another worker took the item, or the queue closed (`wait` reports it next).
                        let Some(item) = self.queue.remove() else { continue };
                        throttle.recv();
                        let _ = self.node.disconnect_now(&item.data);
                    }
                });
            }
        });

        throttle.stop();
        Ok(())
    }

    /// Stops throttling and makes the remaining calls one by one.
    fn shutdown(&self, ctx: &Context) -> Result<(), NodeError> {
        let sessions = {
            let mut stopped = self.stopped();
            if *stopped {
                return Ok(());
            }
            *stopped = true;

            let mut sessions = Vec::with_capacity(self.queue.len());
            while let Some(item) = self.queue.remove() {
                sessions.push(item.data);
            }
            self.queue.close();
            sessions
        };

        let left = sessions.len();
        for (actual, session) in sessions.iter().enumerate() {
            if context_done(ctx) {
                return Err(NodeError(format!("had no time to invoke Disconnect calls: ~{}", left - actual)));
            }
            let _ = self.node.disconnect_now(session);
        }

        Ok(())
    }

    /// Fails (and counts `disconnect_queue_discarded_total`) when the backlog
    /// limit is reached; silently drops sessions after shutdown.
    fn enqueue(&self, session: Arc<Session>) -> Result<(), NodeError> {
        if *self.stopped() {
            return Ok(());
        }

        if self.limit > 0 && self.size() >= self.limit {
            self.node.instrumenter().counter_increment(METRICS_DISCONNECT_DISCARDED);
            return Err(NodeError("disconnect queue is full, discarding Disconnect".into()));
        }

        // A closed queue (shutdown raced past the check above) drops the session.
        self.queue.add(Item { size: 1, data: session });
        Ok(())
    }

    fn size(&self) -> usize {
        self.queue.size() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Config;

    fn queue_with_rate(rate: i64) -> DisconnectQueue {
        let node = Node::new(Config::new(), Vec::new());
        let mut config = DisconnectQueueConfig::new();
        config.rate = rate;
        DisconnectQueue::new(node, &config)
    }

    #[test]
    fn period_is_one_second_divided_by_the_rate() {
        assert_eq!(Duration::from_millis(1000), queue_with_rate(1).rate);
        assert_eq!(Duration::from_millis(10), queue_with_rate(100).rate);
        assert_eq!(Duration::from_millis(3), queue_with_rate(300).rate);
    }

    /// Deviation D-015: a zero rate or one above 1000 calls/s (Go: division by
    /// zero, or a zero ticker period; both panic) runs at the fastest valid period.
    #[test]
    fn deviation_d015_out_of_range_rates_are_clamped() {
        assert_eq!(Duration::from_millis(1000), queue_with_rate(0).rate);
        assert_eq!(Duration::from_millis(1), queue_with_rate(5000).rate);
    }
}
