//! The bounded write queue between the broker and the persister thread (spec §3).
//!
//! Enqueueing never blocks the broadcast path. When the queue would hold more
//! than `capacity` ops, it drops the oldest history op; only if none is
//! queued, the oldest session touch, then the oldest session commit. The
//! incoming op counts as queued, so a history op arriving at a queue full of
//! session ops is itself the one dropped. `push` returns the kind it dropped,
//! so the broker counts every drop (spec §7).
//!
//! A dropped history op leaves a gap the file can't fill, so the queue also
//! remembers that history was dropped during this run (the shutdown is then
//! never marked clean, and the next start rotates the epoch) and each stream's
//! highest dropped offset (still written to `streams.max_seq`, so the stream
//! loads at its true offset).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::broker::lock;

/// One write for the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteOp {
    /// A stored broadcast: `ts_secs` is its unix-seconds timestamp.
    History { stream: String, offset: u64, ts_secs: i64, data: String },
    /// Session state with its absolute expiry (unix milliseconds).
    SessionCommit { sid: String, data: Vec<u8>, expires_at_ms: i64 },
    /// A new expiry (unix milliseconds) for a stored session.
    SessionTouch { sid: String, expires_at_ms: i64 },
}

/// The kind of a [`WriteOp`] (what an overflow dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OpKind {
    History,
    SessionTouch,
    SessionCommit,
}

impl WriteOp {
    pub(crate) fn kind(&self) -> OpKind {
        match self {
            WriteOp::History { .. } => OpKind::History,
            WriteOp::SessionTouch { .. } => OpKind::SessionTouch,
            WriteOp::SessionCommit { .. } => OpKind::SessionCommit,
        }
    }
}

/// Per stream, the highest offset and its timestamp (unix seconds).
pub(crate) type StreamMaxima = BTreeMap<String, (u64, i64)>;

/// Raises `stream`'s entry in `maxima` to `offset` (and `ts_secs`).
pub(crate) fn raise(maxima: &mut StreamMaxima, stream: &str, offset: u64, ts_secs: i64) {
    match maxima.get_mut(stream) {
        Some(entry) => *entry = (entry.0.max(offset), entry.1.max(ts_secs)),
        None => {
            maxima.insert(stream.to_owned(), (offset, ts_secs));
        }
    }
}

/// Which kind goes first when the queue is full.
const DROP_ORDER: [OpKind; 3] = [OpKind::History, OpKind::SessionTouch, OpKind::SessionCommit];

type Lane = VecDeque<(u64, WriteOp)>;

#[derive(Default)]
struct Lanes {
    history: Lane,
    touches: Lane,
    commits: Lane,
    next_seq: u64,
    /// Set by [`WriteQueue::close`]: the deadline of the final flush.
    closed: Option<Instant>,
    /// A history op was dropped during this run.
    history_dropped: bool,
    /// The highest dropped offset per stream, not yet taken by the writer.
    dropped_streams: StreamMaxima,
}

impl Lanes {
    fn lane(&mut self, kind: OpKind) -> &mut Lane {
        match kind {
            OpKind::History => &mut self.history,
            OpKind::SessionTouch => &mut self.touches,
            OpKind::SessionCommit => &mut self.commits,
        }
    }

    fn len(&self) -> usize {
        self.history.len() + self.touches.len() + self.commits.len()
    }

    /// Remembers a dropped op (only history ops leave a gap).
    fn record_drop(&mut self, op: &WriteOp) {
        if let WriteOp::History { stream, offset, ts_secs, .. } = op {
            self.history_dropped = true;
            raise(&mut self.dropped_streams, stream, *offset, *ts_secs);
        }
    }

    fn front_seq(&self, kind: OpKind) -> Option<u64> {
        let lane = match kind {
            OpKind::History => &self.history,
            OpKind::SessionTouch => &self.touches,
            OpKind::SessionCommit => &self.commits,
        };
        lane.front().map(|(seq, _)| *seq)
    }

    /// The oldest op across the lanes.
    fn pop_oldest(&mut self) -> Option<WriteOp> {
        let (_, kind) = DROP_ORDER.into_iter().filter_map(|kind| self.front_seq(kind).map(|seq| (seq, kind))).min()?;
        self.lane(kind).pop_front().map(|(_, op)| op)
    }

    fn take(&mut self, max: usize) -> Vec<WriteOp> {
        let mut out = Vec::with_capacity(max.min(self.len()));
        while out.len() < max {
            match self.pop_oldest() {
                Some(op) => out.push(op),
                None => break,
            }
        }
        out
    }
}

/// The queue; see the module docs.
pub(crate) struct WriteQueue {
    lanes: Mutex<Lanes>,
    ready: Condvar,
    capacity: usize,
    batch: usize,
}

impl WriteQueue {
    /// At most `capacity` ops; a waiting taker wakes once `batch` are queued.
    pub(crate) fn new(capacity: usize, batch: usize) -> Self {
        Self { lanes: Mutex::new(Lanes::default()), ready: Condvar::new(), capacity, batch: batch.max(1) }
    }

    /// Adds `op` without blocking; returns the kind of the op dropped to stay
    /// within capacity, if any. Ignored once closed.
    pub(crate) fn push(&self, op: WriteOp) -> Option<OpKind> {
        let mut lanes = lock(&self.lanes);
        if lanes.closed.is_some() {
            return None;
        }
        let seq = lanes.next_seq;
        lanes.next_seq += 1;
        lanes.lane(op.kind()).push_back((seq, op));
        let mut dropped = None;
        if lanes.len() > self.capacity {
            for kind in DROP_ORDER {
                if let Some((_, op)) = lanes.lane(kind).pop_front() {
                    lanes.record_drop(&op);
                    dropped = Some(kind);
                    break;
                }
            }
        }
        if lanes.len() >= self.batch {
            self.ready.notify_one();
        }
        dropped
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.lanes).len()
    }

    /// Whether a history op was dropped during this run.
    pub(crate) fn history_dropped(&self) -> bool {
        lock(&self.lanes).history_dropped
    }

    /// Takes the highest dropped offset per stream since the last call.
    pub(crate) fn take_dropped_streams(&self) -> StreamMaxima {
        std::mem::take(&mut lock(&self.lanes).dropped_streams)
    }

    /// Waits up to `wait` for a full batch (or for `close`), then takes up to
    /// one batch, oldest first.
    pub(crate) fn take_batch(&self, wait: Duration) -> Vec<WriteOp> {
        let lanes = lock(&self.lanes);
        let (mut lanes, _) = self
            .ready
            .wait_timeout_while(lanes, wait, |l| l.len() < self.batch && l.closed.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        lanes.take(self.batch)
    }

    /// Takes everything queued, oldest first.
    pub(crate) fn take_all(&self) -> Vec<WriteOp> {
        let mut lanes = lock(&self.lanes);
        let n = lanes.len();
        lanes.take(n)
    }

    /// Waits up to `wait` unless closed: the pause before retrying a failed write.
    pub(crate) fn pause(&self, wait: Duration) {
        let lanes = lock(&self.lanes);
        let _ = self.ready.wait_timeout_while(lanes, wait, |l| l.closed.is_none());
    }

    /// Stops accepting ops and wakes the taker; `deadline` bounds the final
    /// flush. The first call's deadline wins.
    pub(crate) fn close(&self, deadline: Instant) {
        lock(&self.lanes).closed.get_or_insert(deadline);
        self.ready.notify_all();
    }

    pub(crate) fn closed(&self) -> Option<Instant> {
        lock(&self.lanes).closed
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::broker::sqlite::testing::{commit, history, history_at, touch};
    use crate::broker::test_clock::T0;

    #[test]
    fn overflow_drops_history_then_touches_then_commits() {
        let q = WriteQueue::new(3, 1000);
        assert_eq!(None, q.push(history("s", 1)));
        assert_eq!(None, q.push(touch("a", 1)));
        assert_eq!(None, q.push(commit("b", 1)));

        assert_eq!(Some(OpKind::History), q.push(commit("c", 1)), "the history op goes first");
        assert_eq!(Some(OpKind::SessionTouch), q.push(commit("d", 1)), "then the touch");
        assert_eq!(Some(OpKind::SessionCommit), q.push(commit("e", 1)), "then the oldest commit");

        assert_eq!(3, q.len());
        assert_eq!(vec![commit("c", 1), commit("d", 1), commit("e", 1)], q.take_all());
    }

    #[test]
    fn a_history_op_arriving_at_a_queue_full_of_sessions_is_the_one_dropped() {
        let q = WriteQueue::new(2, 1000);
        q.push(commit("a", 1));
        q.push(touch("a", 2));

        assert_eq!(Some(OpKind::History), q.push(history("s", 1)));
        assert_eq!(vec![commit("a", 1), touch("a", 2)], q.take_all());
    }

    #[test]
    fn the_queue_never_exceeds_its_capacity_and_reports_every_drop() {
        let q = WriteQueue::new(100, 1000);
        let mut dropped = 0;
        for offset in 0..1_000 {
            if q.push(history("s", offset)) == Some(OpKind::History) {
                dropped += 1;
            }
            assert!(q.len() <= 100);
        }
        assert_eq!(100, q.len());
        assert_eq!(900, dropped);
        assert_eq!(history("s", 900), q.take_all()[0], "the newest are kept");
    }

    #[test]
    fn batches_keep_enqueue_order_across_kinds() {
        let q = WriteQueue::new(10, 1000);
        let ops = vec![commit("a", 1), history("s", 1), touch("a", 2), history("s", 2)];
        for op in &ops {
            q.push(op.clone());
        }
        assert_eq!(ops, q.take_all());
    }

    #[test]
    fn take_batch_returns_a_full_batch_at_once_and_a_partial_one_after_the_wait() {
        let q = WriteQueue::new(100, 3);
        for offset in 1..=5 {
            q.push(history("s", offset));
        }

        let started = Instant::now();
        assert_eq!(vec![history("s", 1), history("s", 2), history("s", 3)], q.take_batch(Duration::from_secs(10)));
        assert!(started.elapsed() < Duration::from_secs(5), "a full batch does not wait");

        let started = Instant::now();
        assert_eq!(2, q.take_batch(Duration::from_millis(50)).len());
        assert!(started.elapsed() >= Duration::from_millis(50), "a partial batch waits for the tick");
    }

    #[test]
    fn a_full_batch_wakes_the_waiting_taker() {
        let q = Arc::new(WriteQueue::new(100, 2));
        let taker = {
            let q = q.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                (q.take_batch(Duration::from_secs(10)), started.elapsed())
            })
        };
        q.push(history("s", 1));
        q.push(history("s", 2));

        let (batch, waited) = taker.join().unwrap();
        assert_eq!(2, batch.len());
        assert!(waited < Duration::from_secs(5), "woken by the full batch, not the timeout");
    }

    #[test]
    fn close_wakes_the_taker_and_later_pushes_are_ignored() {
        let q = Arc::new(WriteQueue::new(100, 1000));
        q.push(history("s", 1));
        let taker = {
            let q = q.clone();
            std::thread::spawn(move || q.take_batch(Duration::from_secs(10)))
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        q.close(deadline);
        q.close(deadline + Duration::from_secs(5));

        assert_eq!(1, taker.join().unwrap().len());
        assert_eq!(Some(deadline), q.closed(), "the first deadline wins");
        assert_eq!(None, q.push(history("s", 2)));
        assert_eq!(0, q.len());
    }

    #[test]
    fn pause_returns_early_when_closed() {
        let q = WriteQueue::new(100, 1000);
        q.close(Instant::now());
        let started = Instant::now();
        q.pause(Duration::from_secs(10));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn dropped_history_is_remembered_with_each_streams_highest_offset() {
        let q = WriteQueue::new(1, 1000);
        assert!(!q.history_dropped());
        q.push(history_at("s", 1, T0));
        q.push(history_at("s", 2, T0 + 1)); // drops s 1
        q.push(history_at("t", 7, T0 + 2)); // drops s 2
        q.push(commit("a", 1)); // drops t 7

        assert!(q.history_dropped());
        let expected = StreamMaxima::from([("s".to_owned(), (2, T0 + 1)), ("t".to_owned(), (7, T0 + 2))]);
        assert_eq!(expected, q.take_dropped_streams());
        assert!(q.take_dropped_streams().is_empty(), "taken once");
        assert!(q.history_dropped(), "the flag stays set for the run");
    }

    #[test]
    fn dropping_session_ops_is_not_dropped_history() {
        let q = WriteQueue::new(1, 1000);
        q.push(commit("a", 1));
        q.push(touch("a", 2));
        assert!(!q.history_dropped());
        assert!(q.take_dropped_streams().is_empty());
    }
}
