//! Identifiers disconnected remotely, and the cached sessions this makes
//! unusable (D-084).
//!
//! A remote disconnect for identifiers X is recorded with the wall-clock time
//! it arrived. Until the record expires, a cached session of X authenticated
//! before that time is authenticated by the application instead of restored:
//! its snapshot predates the disconnect. A session of X authenticated
//! afterwards is a new one and is restored as usual. Sessions authenticated
//! within [`CLOCK_MARGIN_MICROS`] after the record count as older, which covers
//! small clock differences between nodes.
//!
//! The record lasts the sessions TTL plus [`EXPIRY_MARGIN`], so it outlives
//! every cache entry written before it: brokers may return an entry a little
//! after its TTL (the memory and SQLite brokers until their once-a-second sweep
//! removes it, NATS KV until the server's age check runs).
//!
//! The record holds at most `capacity` identifiers. Expired records are
//! dropped as they are met and whenever the earliest one has expired. When the
//! record is full, the oldest revocation is folded into one overflow record
//! that refuses every session authenticated before it, whatever its
//! identifiers: a full record refuses more restores (they fall back to
//! authentication), never fewer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Identifiers kept in the record.
pub(super) const CAPACITY: usize = 16_384;

/// Sessions authenticated up to this long after a revocation (by the wall
/// clocks of the nodes involved) count as authenticated before it.
pub(super) const CLOCK_MARGIN_MICROS: i64 = 1_000_000;

/// How long a record outlives the sessions TTL: covers brokers that return an
/// entry after its TTL, with room to spare.
pub(super) const EXPIRY_MARGIN: Duration = Duration::from_secs(30);

/// Where expiries beyond what `Instant` can represent go.
const FAR_FUTURE: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Revocation {
    /// Wall-clock time of the remote disconnect (unix microseconds).
    at: i64,
    /// The record is dropped from then on.
    until: Instant,
}

impl Revocation {
    /// Whether a session authenticated at `authenticated_at` (`None`: at an
    /// unknown time) predates this revocation.
    fn covers(&self, authenticated_at: Option<i64>, now: Instant) -> bool {
        now < self.until && authenticated_at.is_none_or(|t| t <= self.at.saturating_add(CLOCK_MARGIN_MICROS))
    }

    fn merge(&mut self, other: Revocation) {
        self.at = self.at.max(other.at);
        self.until = self.until.max(other.until);
    }
}

#[derive(Default)]
struct State {
    by_identifiers: HashMap<String, Revocation>,
    /// Revocations evicted from a full record, merged.
    overflow: Option<Revocation>,
    /// The earliest expiry in `by_identifiers`.
    next_expiry: Option<Instant>,
}

impl State {
    fn prune(&mut self, now: Instant) {
        if self.next_expiry.is_some_and(|at| at <= now) {
            self.by_identifiers.retain(|_, r| r.until > now);
            self.next_expiry = self.by_identifiers.values().map(|r| r.until).min();
        }
        if self.overflow.is_some_and(|r| r.until <= now) {
            self.overflow = None;
        }
    }

    fn is_empty(&self) -> bool {
        self.by_identifiers.is_empty() && self.overflow.is_none()
    }

    fn covers(&self, identifiers: &str, authenticated_at: Option<i64>, now: Instant) -> bool {
        self.by_identifiers.get(identifiers).is_some_and(|r| r.covers(authenticated_at, now))
            || self.overflow.is_some_and(|r| r.covers(authenticated_at, now))
    }
}

/// The node's record of remote disconnects (see the module docs).
pub(super) struct Revocations {
    capacity: usize,
    /// Something is recorded: without it, checks take no lock.
    active: AtomicBool,
    state: Mutex<State>,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// When a record kept for the sessions TTL `ttl` from `now` expires.
fn expiry(now: Instant, ttl: Duration) -> Instant {
    let keep = ttl.saturating_add(EXPIRY_MARGIN);
    now.checked_add(keep).or_else(|| now.checked_add(FAR_FUTURE)).unwrap_or(now)
}

impl Revocations {
    pub(super) fn new() -> Self {
        Self::with_capacity(CAPACITY)
    }

    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), active: AtomicBool::new(false), state: Mutex::new(State::default()) }
    }

    /// Records a remote disconnect of `identifiers` at wall-clock time `at`
    /// (unix microseconds), kept for the sessions TTL `ttl` (plus
    /// [`EXPIRY_MARGIN`]) from `now`.
    pub(super) fn revoke(&self, identifiers: &str, at: i64, now: Instant, ttl: Duration) {
        let revocation = Revocation { at, until: expiry(now, ttl) };
        let mut st = lock(&self.state);
        st.prune(now);
        if let Some(existing) = st.by_identifiers.get_mut(identifiers) {
            existing.merge(revocation);
        } else {
            if st.by_identifiers.len() >= self.capacity {
                let oldest = st.by_identifiers.iter().min_by_key(|(_, r)| (r.at, r.until)).map(|(k, _)| k.clone());
                if let Some(evicted) = oldest.and_then(|k| st.by_identifiers.remove(&k)) {
                    st.overflow.get_or_insert(evicted).merge(evicted);
                }
            }
            st.by_identifiers.insert(identifiers.to_owned(), revocation);
        }
        st.next_expiry = Some(st.next_expiry.map_or(revocation.until, |at| at.min(revocation.until)));
        self.active.store(true, Ordering::Release);
    }

    /// Whether a cached session of `identifiers`, authenticated at
    /// `authenticated_at` (`None`: at an unknown time), must not be restored
    /// or written to the cache.
    pub(super) fn refuses(&self, identifiers: &str, authenticated_at: Option<i64>, now: Instant) -> bool {
        if !self.active.load(Ordering::Acquire) {
            return false;
        }
        let mut st = lock(&self.state);
        st.prune(now);
        if st.is_empty() {
            self.active.store(false, Ordering::Release);
            return false;
        }
        st.covers(identifiers, authenticated_at, now)
    }

    /// After a cache write of such a session that raced with a remote
    /// disconnect (it was allowed, then the disconnect was recorded), keeps
    /// what refuses it for the sessions TTL `ttl` (plus [`EXPIRY_MARGIN`])
    /// from `now`, longer than the entry it wrote.
    pub(super) fn extend(&self, identifiers: &str, authenticated_at: Option<i64>, now: Instant, ttl: Duration) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let until = expiry(now, ttl);
        let mut st = lock(&self.state);
        if let Some(r) = st.by_identifiers.get_mut(identifiers)
            && r.covers(authenticated_at, now)
        {
            r.until = r.until.max(until);
        }
        if let Some(r) = st.overflow.as_mut()
            && r.covers(authenticated_at, now)
        {
            r.until = r.until.max(until);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.state).by_identifiers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(300);
    /// How long a record revoked with [`TTL`] lasts.
    const KEPT: Duration = TTL.saturating_add(EXPIRY_MARGIN);
    const T0: i64 = 1_800_000_000_000_000;
    const SEC: i64 = 1_000_000;

    #[test]
    fn refuses_sessions_authenticated_before_the_revocation_until_it_expires() {
        let r = Revocations::new();
        let now = Instant::now();
        assert!(!r.refuses("x", Some(T0), now), "nothing recorded");

        r.revoke("x", T0, now, TTL);
        assert!(r.refuses("x", Some(T0 - 60 * SEC), now));
        assert!(r.refuses("x", None, now), "authenticated at an unknown time");
        assert!(r.refuses("x", Some(T0 + CLOCK_MARGIN_MICROS), now), "within the clock margin");
        assert!(!r.refuses("x", Some(T0 + CLOCK_MARGIN_MICROS + 1), now), "authenticated afterwards");
        assert!(!r.refuses("y", Some(T0 - 60 * SEC), now), "other identifiers");

        assert!(r.refuses("x", None, now + TTL + Duration::from_secs(1)), "an entry read just after its TTL");
        assert!(r.refuses("x", None, now + KEPT - Duration::from_millis(1)));
        assert!(!r.refuses("x", None, now + KEPT), "expired");
        assert_eq!(0, r.len(), "expired records are dropped");
    }

    #[test]
    fn a_second_revocation_moves_the_time_and_the_expiry() {
        let r = Revocations::new();
        let now = Instant::now();
        r.revoke("x", T0, now, TTL);
        r.revoke("x", T0 + 10 * SEC, now + Duration::from_secs(10), TTL);
        assert_eq!(1, r.len());
        assert!(r.refuses("x", Some(T0 + 5 * SEC), now + KEPT), "the later revocation's time and expiry");
        assert!(!r.refuses("x", None, now + KEPT + Duration::from_secs(10)));
    }

    #[test]
    fn a_racing_write_extends_the_record() {
        let r = Revocations::new();
        let now = Instant::now();
        r.revoke("x", T0, now, TTL);
        let later = now + Duration::from_secs(100);
        r.extend("x", Some(T0 - SEC), later, TTL);
        assert!(r.refuses("x", Some(T0 - SEC), later + KEPT - Duration::from_millis(1)));
        assert!(!r.refuses("x", Some(T0 - SEC), later + KEPT));

        // Writes of sessions it does not refuse leave it as it is.
        r.revoke("y", T0, now, TTL);
        r.extend("y", Some(T0 + 10 * SEC), later, TTL);
        assert!(!r.refuses("y", None, now + KEPT));
    }

    #[test]
    fn stays_bounded_and_refuses_more_when_full() {
        let r = Revocations::with_capacity(3);
        let now = Instant::now();
        for (i, ids) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            r.revoke(ids, T0 + i as i64 * 10 * SEC, now, TTL);
            assert!(r.len() <= 3);
        }
        assert_eq!(3, r.len());

        // "a" and "b" were evicted: sessions authenticated before the later of
        // them are refused whatever their identifiers.
        assert!(r.refuses("a", Some(T0), now));
        assert!(r.refuses("b", Some(T0 + 10 * SEC), now));
        assert!(r.refuses("z", Some(T0 + 5 * SEC), now));
        assert!(!r.refuses("z", Some(T0 + 15 * SEC), now));
        assert!(r.refuses("e", Some(T0 + 35 * SEC), now), "kept records still apply");

        assert!(!r.refuses("a", None, now + KEPT), "the overflow record expires too");
        assert!(!r.refuses("e", None, now + KEPT));
        assert_eq!(0, r.len());
    }

    #[test]
    fn expired_records_make_room() {
        let r = Revocations::with_capacity(2);
        let now = Instant::now();
        r.revoke("a", T0, now, TTL);
        r.revoke("b", T0, now, TTL);
        let later = now + KEPT;
        r.revoke("c", T0 + 400 * SEC, later, TTL);
        assert_eq!(1, r.len(), "the expired records are dropped, nothing is evicted");
        assert!(!r.refuses("z", None, later), "no overflow record");
        assert!(r.refuses("c", None, later));
    }
}
