//! Cancellation for long-running harness loops (upstream: `context.Context`).

use std::time::Duration;

use cabby_core::pending;

/// A cloneable cancellation flag that loops can also wait on with a deadline, so
/// backoff sleeps end as soon as the run is cancelled.
#[derive(Clone)]
pub struct CancelToken {
    _state: (),
}

impl CancelToken {
    pub fn new() -> Self {
        pending!("benchi::cancel::new")
    }

    /// A token cancelled automatically after `timeout` (upstream `context.WithTimeout`).
    pub fn with_timeout(_timeout: Duration) -> Self {
        pending!("benchi::cancel::with_timeout")
    }

    pub fn cancel(&self) {
        pending!("benchi::cancel::cancel")
    }

    pub fn is_cancelled(&self) -> bool {
        pending!("benchi::cancel::is_cancelled")
    }

    /// Sleeps up to `d`; returns `false` if the token was cancelled first
    /// (upstream `sleepCtx`).
    pub fn sleep(&self, _d: Duration) -> bool {
        pending!("benchi::cancel::sleep")
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}
