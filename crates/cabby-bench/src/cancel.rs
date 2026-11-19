//! Cancellation for long-running harness loops (upstream: `context.Context`).

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::runtime::rt;

struct Inner {
    token: CancellationToken,
    /// Mirrors the token for synchronous waiters.
    flag: Mutex<bool>,
    cv: Condvar,
}

/// A cloneable cancellation flag that loops can also wait on with a deadline, so
/// backoff sleeps end as soon as the run is cancelled.
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self { inner: Arc::new(Inner { token: CancellationToken::new(), flag: Mutex::new(false), cv: Condvar::new() }) }
    }

    /// A token cancelled automatically after `timeout` (upstream `context.WithTimeout`).
    pub fn with_timeout(timeout: Duration) -> Self {
        let t = Self::new();
        let c = t.clone();
        rt().spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(timeout) => c.cancel(),
                () = c.inner.token.cancelled() => {}
            }
        });
        t
    }

    pub fn cancel(&self) {
        self.inner.token.cancel();
        *self.inner.flag.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.inner.cv.notify_all();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    /// Sleeps up to `d`; returns `false` if the token was cancelled first
    /// (upstream `sleepCtx`).
    pub fn sleep(&self, d: Duration) -> bool {
        if d.is_zero() {
            return !self.is_cancelled();
        }
        let guard = self.inner.flag.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = self.inner.cv.wait_timeout_while(guard, d, |c| !*c).unwrap_or_else(|e| e.into_inner());
        !*guard
    }

    /// Async [`CancelToken::sleep`].
    pub(crate) async fn sleep_async(&self, d: Duration) -> bool {
        if d.is_zero() {
            return !self.is_cancelled();
        }
        tokio::select! {
            () = self.inner.token.cancelled() => false,
            () = tokio::time::sleep(d) => !self.is_cancelled(),
        }
    }

    /// Completes when the token is cancelled.
    pub(crate) async fn cancelled(&self) {
        self.inner.token.cancelled().await;
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}
