//! Deferred Disconnect callbacks (upstream `node/disconnect_queue.go` and
//! `node/disconnector.go`).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use cabby_core::logger::Logger;
use cabby_core::utils::{Item, Queue};
use cabby_core::{log_debug, log_info};

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

/// Go's `time.Duration.String()` for the whole-millisecond periods of the queue.
pub(super) fn go_duration(d: Duration) -> String {
    let millis = d.as_millis();
    if millis >= 1000 && millis.is_multiple_of(1000) {
        format!("{}s", millis / 1000)
    } else if millis >= 1000 {
        format!("{}s", d.as_secs_f64())
    } else {
        format!("{millis}ms")
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
    /// Upstream `l.With("context", "disconnector")`.
    log: Logger,
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

        let log = Logger::default().with("context", "disconnector");
        log_debug!(log, "calls rate", "rate" => go_duration(rate), "workers" => workers);

        Self {
            node,
            rate,
            workers,
            queue: Queue::new(capacity),
            limit: config.max_backlog.max(0) as usize,
            stopped: Mutex::new(false),
            log,
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
        if left == 0 {
            return Ok(());
        }

        match ctx.deadline {
            Some(deadline) => {
                let time_left = deadline.saturating_duration_since(tokio::time::Instant::now());
                log_info!(self.log, "invoking remaining disconnects",
                    "interval" => cabby_core::logger::LogValue::String(time_left.as_secs_f64().to_string()),
                    "num" => left);
            }
            None => log_info!(self.log, "invoking remaining disconnects", "num" => left),
        }

        let mut actual = 0;
        let res = loop {
            let Some(session) = sessions.get(actual) else { break Ok(()) };
            if context_done(ctx) {
                break Err(NodeError(format!("had no time to invoke Disconnect calls: ~{}", left - actual)));
            }
            let _ = self.node.disconnect_now(session);
            actual += 1;
        };

        log_info!(self.log, "disconnected sessions", "num" => actual);
        res
    }

    /// Fails (and counts `disconnect_queue_discarded_total`) when the backlog
    /// limit is reached; silently drops sessions after shutdown.
    ///
    /// The stopped flag's lock is held from the checks to the insertion, so
    /// concurrent calls cannot all pass the size check and exceed the limit
    /// together, and a shutdown cannot start between the checks and the
    /// insertion. Workers remove sessions without this lock: the size only
    /// goes down meanwhile.
    fn enqueue(&self, session: Arc<Session>) -> Result<(), NodeError> {
        let stopped = self.stopped();
        if *stopped {
            return Ok(());
        }

        if self.limit > 0 && self.size() >= self.limit {
            drop(stopped);
            self.node.instrumenter().counter_increment(METRICS_DISCONNECT_DISCARDED);
            return Err(NodeError("disconnect queue is full, discarding Disconnect".into()));
        }
        #[cfg(test)]
        tests::AFTER_BACKLOG_CHECK.with(|hook| hook.borrow().as_ref().map(|hook| hook()));

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
    use crate::node::{Config, Connection, ConnectionError};
    use cabby_core::common::SessionEnv;
    use std::cell::RefCell;
    use std::sync::Barrier;
    use std::sync::mpsc;
    use std::time::Instant;

    thread_local! {
        /// Runs in `enqueue` between the backlog check and the insertion, on
        /// the thread that set it: a test pauses a producer there.
        pub(super) static AFTER_BACKLOG_CHECK: RefCell<Option<Box<dyn Fn()>>> = const { RefCell::new(None) };
    }

    struct Idle;

    impl Connection for Idle {
        fn write(&self, _: &[u8], _: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn write_binary(&self, _: &[u8], _: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn write_ping(&self, _: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn read(&self) -> Result<Vec<u8>, ConnectionError> {
            Err(ConnectionError("websocket: close 1000 (normal)".into()))
        }
        fn close(&self, _: i32, _: &str) {}
    }

    fn session() -> Arc<Session> {
        Session::build(Arc::new(Idle), SessionEnv::new("/cable", None))
    }

    /// A queue with room for one session whose workers do not run (`run` is
    /// not called), so enqueued sessions stay; and its metrics.
    fn queue_of_one() -> (Arc<DisconnectQueue>, Arc<crate::metrics::Metrics>) {
        let metrics = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        let node = Node::new(
            Config::new(),
            vec![crate::node::NodeOption::Instrumenter(metrics.clone() as Arc<dyn crate::metrics::Instrumenter>)],
        );
        let mut config = DisconnectQueueConfig::new();
        config.max_backlog = 1;
        (Arc::new(DisconnectQueue::new(node, &config)), metrics)
    }

    fn discarded(metrics: &crate::metrics::Metrics) -> u64 {
        metrics.counter(METRICS_DISCONNECT_DISCARDED).expect("the counter").value()
    }

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

    /// Two sessions enqueued at the same time into a queue with room for one:
    /// one is queued, the other is discarded (an error and one count of
    /// `disconnect_queue_discarded_total`). The two producers of a round start
    /// together at a barrier.
    #[test]
    fn concurrent_enqueues_respect_the_backlog_limit() {
        const ROUNDS: usize = 200;
        let (queue, metrics) = queue_of_one();

        for round in 0..ROUNDS {
            let barrier = Arc::new(Barrier::new(2));
            let producers: Vec<_> = (0..2)
                .map(|_| {
                    let (queue, barrier, s) = (queue.clone(), barrier.clone(), session());
                    std::thread::spawn(move || {
                        barrier.wait();
                        queue.enqueue(s)
                    })
                })
                .collect();
            let results: Vec<_> = producers.into_iter().map(|p| p.join().expect("a producer")).collect();

            let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
            assert_eq!(1, errors.len(), "round {round}: exactly one session is discarded: {results:?}");
            assert_eq!("disconnect queue is full, discarding Disconnect", errors[0].0);
            assert_eq!(1, queue.size(), "round {round}: the backlog limit holds");
            assert!(queue.remove().is_some());
        }
        assert_eq!(ROUNDS as u64, discarded(&metrics), "one discard counted per round");
    }

    /// The race of the test above, made deterministic: producer A passes the
    /// backlog check and is held right after it; producer B enqueues
    /// meanwhile. B waits for A's insertion, then finds the queue full and is
    /// discarded. Before, the check and the insertion took the locks
    /// separately: B passed the check too and both sessions were queued.
    #[test]
    fn an_enqueue_between_the_check_and_the_insertion_of_another_is_discarded() {
        let (queue, metrics) = queue_of_one();
        let (checked_tx, checked) = mpsc::channel::<()>();
        let (release, released) = mpsc::channel::<()>();

        let a = {
            let queue = queue.clone();
            std::thread::spawn(move || {
                AFTER_BACKLOG_CHECK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        let _ = checked_tx.send(());
                        let _ = released.recv();
                    }));
                });
                queue.enqueue(session())
            })
        };
        checked.recv_timeout(Duration::from_secs(5)).expect("A passed the backlog check");

        let (b_tx, b_result) = mpsc::channel();
        let b = {
            let queue = queue.clone();
            std::thread::spawn(move || {
                let _ = b_tx.send(queue.enqueue(session()));
            })
        };
        // B cannot finish while A is held: it waits for A's insertion. (Before,
        // it was queued at once; the wait bounds that case.)
        let early = b_result.recv_timeout(Duration::from_millis(200)).ok();
        assert_eq!(None, early, "B finished while A was between its check and its insertion");

        release.send(()).expect("A is held");
        assert_eq!(Ok(()), a.join().expect("producer A"));
        let b_result = b_result.recv_timeout(Duration::from_secs(5)).expect("B finished");
        b.join().expect("producer B");
        assert_eq!(Err(NodeError("disconnect queue is full, discarding Disconnect".into())), b_result);
        assert_eq!(1, queue.size());
        assert_eq!(1, discarded(&metrics));
    }
}
