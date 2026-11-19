//! Synchronization primitives for tests: observable shared state and gates.
//!
//! Every wait takes a deadline. Nothing here sleeps or polls: waiters are woken
//! by the state change itself (condition variable for test threads, a watch
//! channel for async code on the testkit runtime).

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// State that test threads and async tasks can wait on.
pub struct Watched<T> {
    state: Mutex<T>,
    cv: Condvar,
    version: watch::Sender<u64>,
}

impl<T> Watched<T> {
    pub fn new(value: T) -> Self {
        Self { state: Mutex::new(value), cv: Condvar::new(), version: watch::Sender::new(0) }
    }

    fn lock(&self) -> MutexGuard<'_, T> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Mutates the state and wakes every waiter.
    pub fn update<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let r = f(&mut self.lock());
        self.cv.notify_all();
        self.version.send_modify(|v| *v += 1);
        r
    }

    pub fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.lock())
    }

    /// Blocks the calling (non-runtime) thread until `pred` holds; `false` on deadline.
    pub fn wait_until(&self, deadline: Duration, mut pred: impl FnMut(&T) -> bool) -> bool {
        let end = Instant::now() + deadline;
        let mut guard = self.lock();
        loop {
            if pred(&guard) {
                return true;
            }
            let now = Instant::now();
            if now >= end {
                return false;
            }
            guard = self.cv.wait_timeout(guard, end - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Blocks until `pred` holds, then applies `f` under the same lock, so no
    /// other thread sees the state in between (a check-and-take); `None` on
    /// deadline. Wakes every waiter after `f` ran.
    pub fn wait_and_update<R>(
        &self,
        deadline: Duration,
        mut pred: impl FnMut(&T) -> bool,
        f: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        let end = Instant::now() + deadline;
        let mut guard = self.lock();
        loop {
            if pred(&guard) {
                let r = f(&mut guard);
                drop(guard);
                self.cv.notify_all();
                self.version.send_modify(|v| *v += 1);
                return Some(r);
            }
            let now = Instant::now();
            if now >= end {
                return None;
            }
            guard = self.cv.wait_timeout(guard, end - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Async variant of [`Watched::wait_until`] without a deadline of its own;
    /// callers race it against their own cancellation.
    pub async fn changed_until(&self, mut pred: impl FnMut(&T) -> bool) {
        let mut rx = self.version.subscribe();
        loop {
            if self.read(&mut pred) {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Debug, Default)]
struct GateState {
    open: bool,
    permits: usize,
    /// Calls currently held at the gate.
    held: usize,
    /// Calls that ever reached the gate.
    arrived: usize,
    /// Calls that passed the gate.
    passed: usize,
}

/// A barrier that holds calls until the test releases them, so a test can make
/// a call "in flight" for exactly as long as it needs (no sleeps).
///
/// A held call that its client abandons (cancellation, deadline) stops counting
/// as held. Dropping the owning fake service unblocks every held call.
#[derive(Clone)]
pub struct Gate {
    inner: Arc<Watched<GateState>>,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.read(|s| f.debug_struct("Gate").field("state", s).finish())
    }
}

struct HeldGuard<'a>(&'a Gate);

impl Drop for HeldGuard<'_> {
    fn drop(&mut self) {
        self.0.inner.update(|s| s.held -= 1);
    }
}

impl Gate {
    /// A closed gate.
    pub fn new() -> Self {
        Self { inner: Arc::new(Watched::new(GateState::default())) }
    }

    /// Lets one held (or future) call through.
    pub fn release_one(&self) {
        self.release(1);
    }

    pub fn release(&self, n: usize) {
        self.inner.update(|s| s.permits += n);
    }

    /// Opens the gate for good.
    pub fn open(&self) {
        self.inner.update(|s| s.open = true);
    }

    pub fn held(&self) -> usize {
        self.inner.read(|s| s.held)
    }

    pub fn arrived(&self) -> usize {
        self.inner.read(|s| s.arrived)
    }

    pub fn passed(&self) -> usize {
        self.inner.read(|s| s.passed)
    }

    /// Blocks until at least `n` calls are held; panics after `deadline`.
    pub fn await_held(&self, n: usize, deadline: Duration) {
        assert!(
            self.inner.wait_until(deadline, |s| s.held >= n),
            "gate: {n} held call(s) expected within {deadline:?}, state {self:?}"
        );
    }

    /// Holds the caller until released. Returns `false` if `shutdown` fired first.
    pub async fn pass(&self, shutdown: &CancellationToken) -> bool {
        self.inner.update(|s| {
            s.arrived += 1;
            s.held += 1;
        });
        let guard = HeldGuard(self);
        let released = async {
            loop {
                self.inner.changed_until(|s| s.open || s.permits > 0).await;
                let took = self.inner.update(|s| {
                    if s.open {
                        true
                    } else if s.permits > 0 {
                        s.permits -= 1;
                        true
                    } else {
                        false
                    }
                });
                if took {
                    return;
                }
            }
        };
        let ok = tokio::select! {
            () = released => true,
            () = shutdown.cancelled() => false,
        };
        drop(guard);
        if ok {
            self.inner.update(|s| s.passed += 1);
        }
        ok
    }
}
