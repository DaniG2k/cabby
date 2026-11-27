//! Stream subscriptions a pub/sub subscriber asked for and that are not active
//! yet (D-105).
//!
//! A multi-node subscriber sets a node's first subscription to a stream up
//! asynchronously: the Redis `SUBSCRIBE` is confirmed, or the Postgres `LISTEN`
//! committed, a little after the node registered the subscriber in its hub. A
//! message published in between never comes back to the node. The subscriber
//! reports here which streams are in that state ([`Activations::requested`],
//! then [`Activations::settled`] once the subscription is active or will not
//! become active, [`Activations::settle_all`] when the connection is lost),
//! and the node waits for them ([`Activations::wait`]) before it confirms a
//! subscription.
//!
//! Nothing is tracked for a stream the node is already subscribed to, so the
//! registry is empty almost all the time; checking it then is one atomic load.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Streams whose pub/sub subscription is not active yet (see the module docs).
#[derive(Debug, Default)]
pub struct Activations {
    pending: Mutex<HashSet<String>>,
    /// `pending.len()`, read without the lock.
    len: AtomicUsize,
    /// Callers inside [`Activations::wait`].
    waiting: AtomicUsize,
    changed: Condvar,
}

impl Activations {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashSet<String>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The subscriber asked for `stream`; its subscription is not active yet.
    pub fn requested(&self, stream: &str) {
        let mut pending = self.lock();
        if pending.insert(stream.to_owned()) {
            self.len.store(pending.len(), Ordering::Release);
        }
    }

    /// `stream`'s subscription is active, failed, or is no longer wanted.
    pub fn settled(&self, stream: &str) {
        if self.len.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut pending = self.lock();
        if pending.remove(stream) {
            self.len.store(pending.len(), Ordering::Release);
            drop(pending);
            self.changed.notify_all();
        }
    }

    /// Nothing that was requested will become active (the connection is gone).
    pub fn settle_all(&self) {
        let mut pending = self.lock();
        if !pending.is_empty() {
            pending.clear();
            self.len.store(0, Ordering::Release);
            drop(pending);
            self.changed.notify_all();
        }
    }

    /// Whether nothing is pending (one atomic load).
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    /// Whether any of `streams` is not active yet; one atomic load when
    /// nothing is pending.
    pub fn any_pending(&self, streams: &[String]) -> bool {
        if self.len.load(Ordering::Acquire) == 0 {
            return false;
        }
        let pending = self.lock();
        streams.iter().any(|s| pending.contains(s))
    }

    /// Waits until none of `streams` is pending, at most `timeout`; returns the
    /// streams still pending then (empty when all are active).
    pub fn wait(&self, streams: &[String], timeout: Duration) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        let mut pending = self.lock();
        self.waiting.fetch_add(1, Ordering::AcqRel);
        let left = loop {
            let left: Vec<String> = streams.iter().filter(|s| pending.contains(*s)).cloned().collect();
            let now = Instant::now();
            if left.is_empty() || now >= deadline {
                break left;
            }
            pending = self.changed.wait_timeout(pending, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
        };
        self.waiting.fetch_sub(1, Ordering::AcqRel);
        left
    }

    /// How many callers are inside [`Activations::wait`] (for tests and
    /// diagnostics).
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn streams(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn nothing_pending_needs_no_wait() {
        let a = Activations::new();
        assert!(!a.any_pending(&streams(&["a"])));
        assert!(a.wait(&streams(&["a"]), Duration::ZERO).is_empty());
        a.settled("a");
        a.settle_all();
    }

    #[test]
    fn a_wait_ends_when_its_streams_are_settled() {
        let a = Arc::new(Activations::new());
        a.requested("a");
        a.requested("b");
        a.requested("c");
        assert!(a.any_pending(&streams(&["x", "b"])));
        assert!(!a.any_pending(&streams(&["x"])));

        let waiter = {
            let a = a.clone();
            std::thread::spawn(move || a.wait(&streams(&["a", "b"]), Duration::from_secs(10)))
        };
        while a.waiting() == 0 {
            std::thread::yield_now();
        }
        a.settled("a");
        a.settled("c");
        assert!(a.any_pending(&streams(&["b"])));
        a.settled("b");
        assert!(waiter.join().unwrap().is_empty());
        assert!(!a.any_pending(&streams(&["a", "b", "c"])));
        assert_eq!(0, a.waiting());
    }

    #[test]
    fn a_wait_ends_at_its_deadline_with_what_is_still_pending() {
        let a = Activations::new();
        a.requested("a");
        a.requested("b");
        a.settled("a");
        assert_eq!(streams(&["b"]), a.wait(&streams(&["a", "b"]), Duration::from_millis(20)));
        a.settle_all();
        assert!(a.wait(&streams(&["b"]), Duration::from_secs(10)).is_empty());
    }
}
