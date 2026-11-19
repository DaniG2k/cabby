//! The operating system clock behind [`super::SystemClock`].
//!
//! Upstream uses `time.AfterFunc` (a runtime timer, the callback on a fresh
//! goroutine) and `time.NewTicker`. Here:
//!
//! * **Timers** live in one process-wide heap served by a single thread. Due
//!   callbacks run on the node's task pool (see `tasks`), so a slow callback (a
//!   ping write waiting on its deadline) never delays other sessions' timers. Stopping a timer removes its callback; the heap entry
//!   is skipped when it comes due, so rescheduling costs one heap push and no
//!   search.
//! * **Tickers** get a thread each (the node uses one, for the disconnect queue)
//!   and follow Go's `time.Ticker`: one tick is buffered, further ticks are
//!   dropped while nobody receives, and a stopped ticker never ticks again.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use super::{Ticker, Timer};

type Callback = Box<dyn FnOnce() + Send>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct WheelState {
    heap: BinaryHeap<Reverse<(Instant, u64)>>,
    pending: HashMap<u64, Callback>,
    next_id: u64,
}

struct Wheel {
    state: Mutex<WheelState>,
    changed: Condvar,
}

fn wheel() -> &'static Arc<Wheel> {
    static WHEEL: OnceLock<Arc<Wheel>> = OnceLock::new();
    WHEEL.get_or_init(|| {
        let wheel = Arc::new(Wheel { state: Mutex::new(WheelState::default()), changed: Condvar::new() });
        let w = wheel.clone();
        std::thread::Builder::new()
            .name("cabby-timers".into())
            .spawn(move || w.serve())
            .expect("spawn the timer thread");
        wheel
    })
}

impl Wheel {
    fn serve(&self) {
        let mut state = lock(&self.state);
        loop {
            let now = Instant::now();
            let mut due = Vec::new();
            while let Some(Reverse((at, id))) = state.heap.peek().copied() {
                if at > now {
                    break;
                }
                state.heap.pop();
                // Stopped timers left their heap entry behind; skip it.
                if let Some(cb) = state.pending.remove(&id) {
                    due.push(cb);
                }
            }
            if !due.is_empty() {
                drop(state);
                for cb in due {
                    super::tasks::spawn(cb);
                }
                state = lock(&self.state);
                continue;
            }
            state = match state.heap.peek() {
                None => self.changed.wait(state).unwrap_or_else(PoisonError::into_inner),
                Some(Reverse((at, _))) => {
                    let wait = at.saturating_duration_since(now);
                    self.changed.wait_timeout(state, wait).unwrap_or_else(PoisonError::into_inner).0
                }
            };
        }
    }

    fn add(self: &Arc<Self>, delay: Duration, f: Callback) -> SystemTimer {
        let at = Instant::now() + delay;
        let mut state = lock(&self.state);
        state.next_id += 1;
        let id = state.next_id;
        let earliest = state.heap.peek().is_none_or(|Reverse((first, _))| at < *first);
        state.heap.push(Reverse((at, id)));
        state.pending.insert(id, f);
        drop(state);
        if earliest {
            self.changed.notify_one();
        }
        SystemTimer { wheel: self.clone(), id }
    }
}

/// A pending callback of the system clock.
pub(super) struct SystemTimer {
    wheel: Arc<Wheel>,
    id: u64,
}

impl Timer for SystemTimer {
    fn stop(&self) -> bool {
        lock(&self.wheel.state).pending.remove(&self.id).is_some()
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
                    // Ticks missed while nobody received are dropped (Go adjusts too).
                    while next <= now {
                        next += period;
                    }
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
        let t = ticker(Duration::from_millis(2));
        let start = Instant::now();
        t.recv();
        t.recv();
        assert!(start.elapsed() >= Duration::from_millis(4));
        t.stop();
    }
}
