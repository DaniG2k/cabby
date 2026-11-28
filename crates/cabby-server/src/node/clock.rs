//! The operating system clock behind [`super::SystemClock`].
//!
//! Upstream uses `time.AfterFunc` (a runtime timer, the callback on a fresh
//! goroutine) and `time.NewTicker`. Here:
//!
//! * **Timers** live in one process-wide map ordered by deadline, served by a single
//!   thread. Due callbacks run on the node's task pool (see `tasks`), so a slow callback
//!   (a ping write waiting on its deadline) never delays other sessions' timers. A
//!   stopped timer is removed at once, so the map holds the timers that can still fire
//!   and no others, whatever the rate at which timers are armed and stopped, and its lock
//!   is held for one insertion or removal, or to take the timers that are due.
//! * **Tickers** get a thread each (the node uses one, for the disconnect queue)
//!   and follow Go's `time.Ticker`: one tick is buffered, further ticks are
//!   dropped while nobody receives, and a stopped ticker never ticks again.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use super::{Ticker, Timer};

type Callback = Box<dyn FnOnce() + Send>;

/// Where timers with unrepresentable deadlines go.
const FAR_FUTURE: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A timer's deadline and its ID, which keeps timers with equal deadlines in the order
/// they were armed.
type Key = (Instant, u64);

#[derive(Default)]
struct WheelState {
    pending: BTreeMap<Key, Callback>,
    next_id: u64,
}

struct Wheel {
    state: Mutex<WheelState>,
    changed: Condvar,
}

fn wheel() -> &'static Arc<Wheel> {
    static WHEEL: OnceLock<Arc<Wheel>> = OnceLock::new();
    WHEEL.get_or_init(Wheel::start)
}

impl WheelState {
    /// Removes the timers due at `now`; returns their callbacks, earliest first.
    fn take_due(&mut self, now: Instant) -> Vec<Callback> {
        let mut due = Vec::new();
        while let Some(first) = self.pending.first_entry()
            && first.key().0 <= now
        {
            due.push(first.remove());
        }
        due
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.pending.len()
    }
}

impl Wheel {
    fn new() -> Arc<Self> {
        Arc::new(Wheel { state: Mutex::new(WheelState::default()), changed: Condvar::new() })
    }

    /// A wheel and the thread that serves it.
    fn start() -> Arc<Self> {
        let wheel = Self::new();
        let w = wheel.clone();
        std::thread::Builder::new()
            .name("cabby-timers".into())
            .spawn(move || w.serve())
            .expect("spawn the timer thread");
        wheel
    }

    fn serve(&self) {
        let mut state = lock(&self.state);
        loop {
            let now = Instant::now();
            let due = state.take_due(now);
            if !due.is_empty() {
                drop(state);
                for cb in due {
                    super::tasks::spawn(cb);
                }
                state = lock(&self.state);
                continue;
            }
            state = match state.pending.first_key_value() {
                None => self.changed.wait(state).unwrap_or_else(PoisonError::into_inner),
                Some(((at, _), _)) => {
                    let wait = at.saturating_duration_since(now);
                    self.changed.wait_timeout(state, wait).unwrap_or_else(PoisonError::into_inner).0
                }
            };
        }
    }

    fn add(self: &Arc<Self>, delay: Duration, f: Callback) -> SystemTimer {
        // A delay past what `Instant` can represent fires "never" (in 30 years).
        let now = Instant::now();
        let at = now.checked_add(delay).or_else(|| now.checked_add(FAR_FUTURE)).unwrap_or(now);
        let mut state = lock(&self.state);
        state.next_id += 1;
        let key = (at, state.next_id);
        let earliest = state.pending.first_key_value().is_none_or(|((first, _), _)| at < *first);
        state.pending.insert(key, f);
        drop(state);
        if earliest {
            self.changed.notify_one();
        }
        SystemTimer { wheel: self.clone(), key }
    }
}

/// A pending callback of the system clock.
pub(super) struct SystemTimer {
    wheel: Arc<Wheel>,
    key: Key,
}

impl Timer for SystemTimer {
    fn stop(&self) -> bool {
        let callback = lock(&self.wheel.state).pending.remove(&self.key);
        // Dropped without the lock.
        callback.is_some()
    }
}

/// Upstream `time.AfterFunc(delay, f)`.
pub(super) fn after(delay: Duration, f: Callback) -> Box<dyn Timer> {
    Box::new(wheel().add(delay, f))
}

#[derive(Default)]
struct TickerState {
    buffered: bool,
    stopped: bool,
}

struct SystemTickerInner {
    state: Mutex<TickerState>,
    changed: Condvar,
}

/// Upstream `time.NewTicker(period)`.
pub(super) struct SystemTicker {
    inner: Arc<SystemTickerInner>,
}

pub(super) fn ticker(period: Duration) -> Arc<dyn Ticker> {
    assert!(!period.is_zero(), "non-positive interval for a ticker (Go panics too)");
    let inner = Arc::new(SystemTickerInner { state: Mutex::new(TickerState::default()), changed: Condvar::new() });
    let ticking = inner.clone();
    std::thread::Builder::new()
        .name("cabby-ticker".into())
        .spawn(move || {
            let mut next = Instant::now() + period;
            let mut state = lock(&ticking.state);
            loop {
                if state.stopped {
                    return;
                }
                let now = Instant::now();
                if now >= next {
                    state.buffered = true;
                    ticking.changed.notify_all();
                    // Ticks missed while nobody received are dropped (Go adjusts too),
                    // skipped in one step: a period of a few nanoseconds (a disconnect
                    // rate near its maximum) misses millions of ticks per millisecond.
                    let missed = (now - next).as_nanos() / period.as_nanos() + 1;
                    next += Duration::from_nanos(u64::try_from(missed * period.as_nanos()).unwrap_or(u64::MAX));
                    continue;
                }
                state = ticking.changed.wait_timeout(state, next - now).unwrap_or_else(PoisonError::into_inner).0;
            }
        })
        .expect("spawn a ticker thread");
    Arc::new(SystemTicker { inner })
}

impl Ticker for SystemTicker {
    fn recv(&self) {
        let mut state = lock(&self.inner.state);
        while !state.buffered {
            state = self.inner.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
        state.buffered = false;
    }

    fn stop(&self) {
        lock(&self.inner.state).stopped = true;
        self.inner.changed.notify_all();
    }
}

impl Drop for SystemTicker {
    fn drop(&mut self) {
        // The thread holds the inner state only; stop it with the last handle.
        self.stop();
    }
}

/// A wheel of its own for a test, as a session's clock.
#[cfg(test)]
#[derive(Clone)]
pub(super) struct TestWheel(Arc<Wheel>);

#[cfg(test)]
impl TestWheel {
    pub(super) fn new() -> Self {
        Self(Wheel::start())
    }

    /// Entries the wheel keeps.
    pub(super) fn entries(&self) -> usize {
        lock(&self.0.state).entries()
    }
}

#[cfg(test)]
impl super::Clock for TestWheel {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn after(&self, delay: Duration, f: Callback) -> Box<dyn Timer> {
        Box::new(self.0.add(delay, f))
    }

    fn ticker(&self, _period: Duration) -> Arc<dyn Ticker> {
        unimplemented!("not used by sessions")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn timers_fire_in_order_and_stop_cancels() {
        let (tx, rx) = mpsc::channel();
        let t1 = tx.clone();
        let t2 = tx.clone();
        let _a = after(Duration::from_millis(30), Box::new(move || t1.send(2).unwrap()));
        let _b = after(Duration::from_millis(5), Box::new(move || t2.send(1).unwrap()));
        let c = after(Duration::from_millis(10), Box::new(move || tx.send(3).unwrap()));
        assert!(c.stop());
        assert!(!c.stop());
        assert_eq!(1, rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert_eq!(2, rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn ticker_buffers_one_tick() {
        let start = Instant::now();
        let t = ticker(Duration::from_millis(2));
        t.recv();
        t.recv();
        assert!(start.elapsed() >= Duration::from_millis(4));
        t.stop();
    }

    const HOUR: Duration = Duration::from_secs(3600);

    /// A wheel keeps the timers that can still fire, and nothing of the others.
    #[test]
    fn stopped_timers_leave_nothing_behind() {
        let wheel = Wheel::new();
        let kept = wheel.add(HOUR, Box::new(|| {}));
        for _ in 0..10_000 {
            assert!(wheel.add(HOUR, Box::new(|| {})).stop());
        }
        assert_eq!(1, lock(&wheel.state).entries());
        assert!(kept.stop());
        assert_eq!(0, lock(&wheel.state).entries());
    }

    /// Timers that come due together are taken in order; later ones stay.
    #[test]
    fn due_timers_are_taken_in_order() {
        const TIMERS: usize = 10_000;
        let wheel = Wheel::new();
        let fired = Arc::new(Mutex::new(Vec::new()));
        wheel.add(HOUR, Box::new(|| {}));
        for i in 0..TIMERS {
            let fired = fired.clone();
            wheel.add(Duration::ZERO, Box::new(move || lock(&fired).push(i)));
        }
        let due = lock(&wheel.state).take_due(Instant::now());
        due.into_iter().for_each(|cb| cb());
        assert!(lock(&fired).iter().copied().eq(0..TIMERS), "in order, each once");
        assert_eq!(1, lock(&wheel.state).entries());
    }

    /// How long the lock is held when timers come due does not depend on how many were
    /// stopped. The shortest of several holds: a busy machine stretches some of them.
    #[test]
    fn stopped_timers_do_not_lengthen_the_hold_of_the_lock() {
        let shortest = (0..5)
            .map(|_| {
                let wheel = Wheel::new();
                for _ in 0..5000 {
                    wheel.add(Duration::ZERO, Box::new(|| {}));
                    for _ in 0..10 {
                        wheel.add(Duration::ZERO, Box::new(|| {})).stop();
                    }
                }
                let started = Instant::now();
                let due = lock(&wheel.state).take_due(started);
                let held = started.elapsed();
                assert_eq!(5000, due.len());
                held
            })
            .min()
            .unwrap();
        assert!(shortest < Duration::from_millis(5), "{shortest:?}");
    }

    /// The serving thread wakes up for a timer earlier than the one it waits for.
    #[test]
    fn a_timer_earlier_than_the_next_one_fires() {
        let wheel = Wheel::start();
        wheel.add(HOUR, Box::new(|| {}));
        std::thread::sleep(Duration::from_millis(20));
        let (tx, rx) = mpsc::channel();
        wheel.add(Duration::from_millis(1), Box::new(move || tx.send(()).unwrap()));
        assert!(rx.recv_timeout(Duration::from_secs(60)).is_ok());
    }

    /// Timers armed while many others come due fire, and not early.
    #[test]
    fn timers_armed_while_others_come_due_fire() {
        const TIMERS: usize = 20_000;
        let wheel = Wheel::start();
        let (tx, rx) = mpsc::channel();
        let soon = Instant::now() + Duration::from_millis(100);
        for _ in 0..TIMERS {
            let tx = tx.clone();
            wheel.add(soon.saturating_duration_since(Instant::now()), Box::new(move || tx.send(None).unwrap()));
        }
        std::thread::sleep(soon.saturating_duration_since(Instant::now()));
        for i in 0..100 {
            let tx = tx.clone();
            let at = Instant::now() + Duration::from_millis(i % 20);
            wheel.add(at - Instant::now(), Box::new(move || tx.send(Some(Instant::now() >= at)).unwrap()));
        }
        for _ in 0..TIMERS + 100 {
            let fired = rx.recv_timeout(Duration::from_secs(60)).expect("every timer fires");
            assert_ne!(Some(false), fired, "early");
        }
        assert_eq!(0, lock(&wheel.state).entries());
    }
}
