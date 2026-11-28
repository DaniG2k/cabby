//! The bounded write queue between the broker and the persister thread.
//!
//! Enqueueing never blocks the broadcast path. The queue is bounded twice: by
//! `capacity` ops and by `max_bytes` of payload ([`WriteOp::approx_bytes`],
//! 64 MiB by default). When it would exceed either bound, it drops the oldest
//! history op; only if none is queued, the oldest session touch, then the
//! oldest session commit, until it is within both. The incoming op counts as
//! queued, so a history op arriving at a queue full of session ops is itself
//! the one dropped; an op larger than `max_bytes` on its own is dropped
//! without evicting anything. Taken ops no longer count toward either bound.
//! Every drop is reported to the caller, so the broker counts it.
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
use crate::log_warn;

/// One write for the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteOp {
    /// A stored broadcast: `ts_secs` is its unix-seconds timestamp.
    History { stream: String, offset: u64, ts_secs: i64, data: String },
    /// Session state with its absolute expiry (unix milliseconds).
    SessionCommit { sid: String, data: Vec<u8>, expires_at_ms: i64 },
    /// A new expiry (unix milliseconds) for a stored session.
    SessionTouch { sid: String, expires_at_ms: i64 },
    /// A deleted session (queued and dropped with the commits, whose lane it shares).
    SessionDelete { sid: String },
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
            WriteOp::SessionCommit { .. } | WriteOp::SessionDelete { .. } => OpKind::SessionCommit,
        }
    }

    /// Payload bytes held by this op (used to bound total queued memory).
    pub(crate) fn approx_bytes(&self) -> usize {
        match self {
            WriteOp::History { stream, data, .. } => stream.len() + data.len(),
            WriteOp::SessionCommit { sid, data, .. } => sid.len() + data.len(),
            WriteOp::SessionTouch { sid, .. } | WriteOp::SessionDelete { sid } => sid.len(),
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
    bytes: usize,
    next_seq: u64,
    /// Set by [`WriteQueue::close`]: the deadline of the final flush.
    closed: Option<Instant>,
    /// A history op was dropped during this run (on overflow or after close).
    history_dropped: bool,
    /// The first op pushed after close was logged.
    late_logged: bool,
    /// The highest dropped offset per stream, not yet taken by the writer.
    dropped_streams: StreamMaxima,
    /// Wakeups sent by `push` (tests check they stay rare).
    #[cfg(test)]
    notifies: u64,
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
        let (_, op) = self.lane(kind).pop_front()?;
        self.bytes = self.bytes.saturating_sub(op.approx_bytes());
        Some(op)
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
    max_bytes: usize,
    batch: usize,
}

impl WriteQueue {
    /// At most `capacity` ops (and [`super::QUEUE_MAX_BYTES`] payload bytes); a
    /// waiting taker wakes once `batch` are queued.
    #[cfg(test)]
    pub(crate) fn new(capacity: usize, batch: usize) -> Self {
        Self::with_max_bytes(capacity, super::QUEUE_MAX_BYTES, batch)
    }

    /// At most `capacity` ops and `max_bytes` payload bytes; a waiting taker
    /// wakes once `batch` are queued.
    pub(crate) fn with_max_bytes(capacity: usize, max_bytes: usize, batch: usize) -> Self {
        Self { lanes: Mutex::new(Lanes::default()), ready: Condvar::new(), capacity, max_bytes, batch: batch.max(1) }
    }

    /// Adds `op` without blocking, calling `on_drop` for every op dropped to
    /// stay within `capacity` and `max_bytes` (or for `op` itself once closed
    /// or when a single op exceeds `max_bytes`).
    pub(crate) fn push_with_drops(&self, op: WriteOp, mut on_drop: impl FnMut(OpKind)) {
        let op_bytes = op.approx_bytes();
        let mut dropped = [0usize; 3];
        let mut late_kind = None;
        {
            let mut lanes = lock(&self.lanes);
            if lanes.closed.is_some() {
                lanes.record_drop(&op);
                let first = !std::mem::replace(&mut lanes.late_logged, true);
                let kind = op.kind();
                drop(lanes);
                if first {
                    log_warn!(super::log(), "a write arrived after the SQLite broker shut down and is not persisted",
                        "kind" => format!("{kind:?}"));
                }
                on_drop(kind);
                return;
            }
            if op_bytes > self.max_bytes || self.capacity == 0 {
                lanes.record_drop(&op);
                late_kind = Some(op.kind());
            } else {
                let before = lanes.len();
                let seq = lanes.next_seq;
                lanes.next_seq += 1;
                lanes.bytes += op_bytes;
                lanes.lane(op.kind()).push_back((seq, op));
                while lanes.len() > self.capacity || lanes.bytes > self.max_bytes {
                    let mut evicted = false;
                    for kind in DROP_ORDER {
                        if let Some((_, op)) = lanes.lane(kind).pop_front() {
                            lanes.bytes = lanes.bytes.saturating_sub(op.approx_bytes());
                            lanes.record_drop(&op);
                            dropped[kind as usize] += 1;
                            evicted = true;
                            break;
                        }
                    }
                    if !evicted {
                        break;
                    }
                }
                // Wake the taker only when this push fills a batch: it rechecks under
                // the lock, and a partial batch is taken on its tick anyway.
                if before < self.batch && lanes.len() >= self.batch {
                    #[cfg(test)]
                    {
                        lanes.notifies += 1;
                    }
                    self.ready.notify_one();
                }
            }
        }
        if let Some(kind) = late_kind {
            on_drop(kind);
        }
        for kind in DROP_ORDER {
            for _ in 0..dropped[kind as usize] {
                on_drop(kind);
            }
        }
    }

    /// Adds `op` without blocking; returns the kind of the op dropped to stay
    /// within capacity, if any. Once closed, `op` itself is dropped (and
    /// returned as such, the first time with a warning).
    #[cfg(test)]
    pub(crate) fn push(&self, op: WriteOp) -> Option<OpKind> {
        let mut last = None;
        self.push_with_drops(op, |kind| last = Some(kind));
        last
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.lanes).len()
    }

    /// Whether a history op was dropped during this run.
    pub(crate) fn history_dropped(&self) -> bool {
        lock(&self.lanes).history_dropped
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        lock(&self.lanes).bytes
    }

    #[cfg(test)]
    fn notifies(&self) -> u64 {
        lock(&self.lanes).notifies
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
        assert_eq!(Some(OpKind::History), q.push(history("s", 2)), "dropped, and reported");
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

    #[test]
    fn a_backlog_wakes_the_taker_once_per_batch_crossing() {
        let q = WriteQueue::new(10_000, 1000);
        for offset in 0..2500 {
            q.push(history("s", offset));
        }
        assert_eq!(1, q.notifies(), "one wakeup when the batch filled up");

        assert_eq!(1000, q.take_batch(Duration::ZERO).len());
        for offset in 2500..2510 {
            q.push(history("s", offset));
        }
        assert_eq!(1, q.notifies(), "still a full batch queued: no more wakeups");

        while !q.take_batch(Duration::ZERO).is_empty() {}
        for offset in 0..1000 {
            q.push(history("t", offset));
        }
        assert_eq!(2, q.notifies(), "the next crossing wakes it again");
    }

    #[test]
    fn a_full_queue_at_the_batch_size_does_not_wake_on_every_drop() {
        let q = WriteQueue::new(3, 3);
        for offset in 0..10 {
            q.push(history("s", offset));
        }
        assert_eq!(1, q.notifies());
    }

    #[test]
    fn ops_after_close_are_dropped_reported_and_history_counts_as_dropped() {
        let q = WriteQueue::new(100, 1000);
        q.close(Instant::now());
        assert_eq!(Some(OpKind::SessionCommit), q.push(commit("a", 1)));
        assert!(!q.history_dropped(), "a late session op leaves no gap in the history");
        assert_eq!(Some(OpKind::History), q.push(history_at("s", 4, T0)));
        assert!(q.history_dropped());
        assert_eq!(StreamMaxima::from([("s".to_owned(), (4, T0))]), q.take_dropped_streams());
    }

    #[test]
    fn byte_limit_evicts_oldest_in_drop_order_and_drops_oversized_ops() {
        let q = WriteQueue::with_max_bytes(100, 10, 1000);
        let mut drops = Vec::new();
        // Each history("s", n) has approx_bytes = 1 + 2 = 3.
        for offset in 1..=3 {
            q.push_with_drops(history("s", offset), |k| drops.push(k));
        }
        assert!(drops.is_empty());
        assert_eq!(3, q.len());

        // A 7-byte commit forces two 3-byte history ops out (9 + 7 = 16 -> 10).
        q.push_with_drops(WriteOp::SessionCommit { sid: "a".into(), data: vec![b'x'; 6], expires_at_ms: 1 }, |k| {
            drops.push(k);
        });
        assert_eq!(vec![OpKind::History, OpKind::History], drops);
        assert!(q.history_dropped());
        assert_eq!(2, q.len());

        // A single op larger than max_bytes is dropped directly without evicting queued ops.
        drops.clear();
        q.push_with_drops(WriteOp::History { stream: "s".into(), offset: 9, ts_secs: T0, data: "x".repeat(20) }, |k| {
            drops.push(k)
        });
        assert_eq!(vec![OpKind::History], drops);
        assert_eq!(2, q.len());
    }

    fn session_commit(sid: &str, data_len: usize) -> WriteOp {
        WriteOp::SessionCommit { sid: sid.into(), data: vec![b'x'; data_len], expires_at_ms: 1 }
    }

    #[test]
    fn taken_ops_release_their_bytes() {
        let q = WriteQueue::with_max_bytes(100, 10, 2);
        let mut drops = Vec::new();
        for offset in 1..=3 {
            q.push_with_drops(history("s", offset), |k| drops.push(k)); // 3 bytes each
        }
        assert_eq!(9, q.bytes());

        assert_eq!(2, q.take_batch(Duration::ZERO).len());
        assert_eq!(3, q.bytes(), "a batch gives its bytes back");
        assert_eq!(1, q.take_all().len());
        assert_eq!(0, q.bytes());

        // A full byte budget again, with nothing dropped.
        for offset in 4..=6 {
            q.push_with_drops(history("s", offset), |k| drops.push(k));
        }
        assert!(drops.is_empty(), "{drops:?}");
        assert!(!q.history_dropped());
        assert_eq!(9, q.bytes());
    }

    #[test]
    fn the_byte_bound_drops_touches_before_commits() {
        let q = WriteQueue::with_max_bytes(100, 10, 1000);
        let mut drops = Vec::new();
        q.push_with_drops(session_commit("c1", 3), |k| drops.push(k)); // 5 bytes
        q.push_with_drops(touch("t1", 1), |k| drops.push(k)); // 2 bytes
        assert!(drops.is_empty());

        // 5 + 2 + 4 = 11: the touch goes, although the first commit is older.
        q.push_with_drops(session_commit("c2", 2), |k| drops.push(k));
        assert_eq!(vec![OpKind::SessionTouch], drops);
        assert_eq!(9, q.bytes());

        // With no touch left, the oldest commit goes: 5 + 4 + 4 = 13 -> 8.
        drops.clear();
        q.push_with_drops(session_commit("c3", 2), |k| drops.push(k));
        assert_eq!(vec![OpKind::SessionCommit], drops);
        assert_eq!(8, q.bytes());
        assert!(!q.history_dropped(), "dropping session ops is not dropped history");
        assert_eq!(vec![session_commit("c2", 2), session_commit("c3", 2)], q.take_all());
    }
}
