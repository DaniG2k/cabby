//! A controlled time source for node tests (`docs/translating-tests.md`, "Hard rules"): session timers, the
//! disconnect queue throttle ticker and broker TTLs only move when the test
//! advances time.
//!
//! [`ManualTimers`] implements both `cabby_server::node::Clock` (timers and
//! tickers) and `cabby_core::broker::Clock` (wall-clock timestamps), so one
//! `advance` moves a node and its memory broker together. Timer callbacks run
//! on the advancing thread, in due-time order. Tickers follow Go's
//! `time.Ticker`: one tick is buffered and further ticks are dropped while
//! nobody receives.
//!
//! Every wait has a deadline; nothing sleeps or polls.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use cabby_server::node::{Clock, Ticker, Timer};

use crate::sync::Watched;

type Callback = Box<dyn FnOnce() + Send>;

struct PendingTimer {
    id: u64,
    due: Duration,
    callback: Option<Callback>,
}

#[derive(Debug, Default)]
struct TickerState {
    period: Duration,
    next_due: Duration,
    buffered: bool,
    stopped: bool,
    /// Receivers blocked in `recv`.
    waiting: usize,
    /// Ticks taken by receivers.
    received: usize,
}

#[derive(Default)]
struct State {
    elapsed: Duration,
    next_id: u64,
    timers: Vec<PendingTimer>,
    tickers: Vec<TickerState>,
}

struct Inner {
    base_instant: Instant,
    base_system: SystemTime,
    state: Watched<State>,
}

/// See the module documentation.
#[derive(Clone)]
pub struct ManualTimers {
    inner: Arc<Inner>,
}

impl Default for ManualTimers {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ManualTimers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.state.read(|s| {
            f.debug_struct("ManualTimers")
                .field("elapsed", &s.elapsed)
                .field("timers", &s.timers.len())
                .field("tickers", &s.tickers)
                .finish()
        })
    }
}

/// How long a receiver blocked on a manual ticker waits for a tick before the
/// test is considered stuck. Generous: ticks come from the test thread.
const RECV_DEADLINE: Duration = Duration::from_secs(120);

impl ManualTimers {
    /// Starts at the current wall-clock time (so timestamps a Go reference would
    /// compute with real time still make sense).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                base_instant: Instant::now(),
                base_system: SystemTime::now(),
                state: Watched::new(State::default()),
            }),
        }
    }

    /// Time elapsed since creation.
    pub fn elapsed(&self) -> Duration {
        self.inner.state.read(|s| s.elapsed)
    }

    /// Unix seconds (Go `time.Now().Unix()`).
    pub fn unix(&self) -> i64 {
        cabby_core::broker::Clock::now(self).duration_since(SystemTime::UNIX_EPOCH).expect("after epoch").as_secs()
            as i64
    }

    /// Moves time forward by `d` (replaces an upstream `time.Sleep(d)`), firing
    /// due timers and ticks in order.
    pub fn advance(&self, d: Duration) {
        let target = self.elapsed() + d;
        loop {
            // The earliest event at or before the target: a timer or a tick.
            let next = self.inner.state.update(|s| {
                let timer = s.timers.iter().filter(|t| t.due <= target).min_by_key(|t| (t.due, t.id)).map(|t| t.due);
                let tick = s.tickers.iter().filter(|t| !t.stopped && t.next_due <= target).map(|t| t.next_due).min();
                match (timer, tick) {
                    (None, None) => {
                        s.elapsed = target;
                        None
                    }
                    (Some(a), Some(b)) if b < a => {
                        s.elapsed = b;
                        Self::fire_ticks(s, b);
                        Some(None)
                    }
                    (None, Some(b)) => {
                        s.elapsed = b;
                        Self::fire_ticks(s, b);
                        Some(None)
                    }
                    (Some(a), _) => {
                        s.elapsed = a;
                        let i = s
                            .timers
                            .iter()
                            .enumerate()
                            .filter(|(_, t)| t.due == a)
                            .min_by_key(|(_, t)| t.id)
                            .map(|(i, _)| i)
                            .expect("a due timer");
                        Some(s.timers.remove(i).callback)
                    }
                }
            });
            match next {
                None => return,
                Some(Some(callback)) => callback(),
                Some(None) => {}
            }
        }
    }

    fn fire_ticks(s: &mut State, at: Duration) {
        for t in s.tickers.iter_mut().filter(|t| !t.stopped && t.next_due == at) {
            t.buffered = true;
            t.next_due += t.period;
        }
    }

    /// Receivers currently blocked on any ticker.
    pub fn ticker_waiters(&self) -> usize {
        self.inner.state.read(|s| s.tickers.iter().map(|t| t.waiting).sum())
    }

    /// Ticks taken by receivers so far (all tickers).
    pub fn ticks_received(&self) -> usize {
        self.inner.state.read(|s| s.tickers.iter().map(|t| t.received).sum())
    }

    /// Blocks until exactly `n` receivers wait on tickers; panics after `deadline`.
    pub fn await_ticker_waiters(&self, n: usize, deadline: Duration) {
        assert!(
            self.inner.state.wait_until(deadline, |s| s.tickers.iter().map(|t| t.waiting).sum::<usize>() == n),
            "manual timers: {n} ticker receiver(s) expected within {deadline:?}, state {self:?}"
        );
    }

    /// Blocks until `n` ticks have been taken; panics after `deadline`.
    pub fn await_ticks_received(&self, n: usize, deadline: Duration) {
        assert!(
            self.inner.state.wait_until(deadline, |s| s.tickers.iter().map(|t| t.received).sum::<usize>() >= n),
            "manual timers: {n} received tick(s) expected within {deadline:?}, state {self:?}"
        );
    }

    /// Pending (unfired, unstopped) timers.
    pub fn pending_timers(&self) -> usize {
        self.inner.state.read(|s| s.timers.len())
    }
}

struct ManualTimer {
    inner: Arc<Inner>,
    id: u64,
}

impl Timer for ManualTimer {
    fn stop(&self) -> bool {
        self.inner.state.update(|s| {
            let before = s.timers.len();
            s.timers.retain(|t| t.id != self.id);
            before != s.timers.len()
        })
    }
}

struct ManualTicker {
    inner: Arc<Inner>,
    index: usize,
}

impl Ticker for ManualTicker {
    fn recv(&self) {
        self.inner.state.update(|s| s.tickers[self.index].waiting += 1);
        // The tick is taken under the same lock that observed it, so one tick
        // wakes exactly one receiver (as a Go ticker channel delivers it once).
        let ticked = self
            .inner
            .state
            .wait_and_update(
                RECV_DEADLINE,
                |s| s.tickers[self.index].buffered,
                |s| {
                    let t = &mut s.tickers[self.index];
                    t.waiting -= 1;
                    t.buffered = false;
                    t.received += 1;
                },
            )
            .is_some();
        if !ticked {
            self.inner.state.update(|s| s.tickers[self.index].waiting -= 1);
        }
        assert!(ticked, "manual ticker: no tick within {RECV_DEADLINE:?}; the test never advanced time");
    }

    fn stop(&self) {
        self.inner.state.update(|s| s.tickers[self.index].stopped = true);
    }
}

impl Clock for ManualTimers {
    fn now(&self) -> Instant {
        self.inner.base_instant + self.elapsed()
    }

    fn after(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer> {
        let id = self.inner.state.update(|s| {
            s.next_id += 1;
            let id = s.next_id;
            s.timers.push(PendingTimer { id, due: s.elapsed + delay, callback: Some(f) });
            id
        });
        Box::new(ManualTimer { inner: self.inner.clone(), id })
    }

    fn ticker(&self, period: Duration) -> Arc<dyn Ticker> {
        assert!(!period.is_zero(), "ticker period must be positive (Go panics too)");
        let index = self.inner.state.update(|s| {
            s.tickers.push(TickerState { period, next_due: s.elapsed + period, ..Default::default() });
            s.tickers.len() - 1
        });
        Arc::new(ManualTicker { inner: self.inner.clone(), index })
    }
}

impl cabby_core::broker::Clock for ManualTimers {
    fn now(&self) -> SystemTime {
        self.inner.base_system + self.elapsed()
    }
}
