//! The session context of a controller call.
//!
//! Upstream passes the session's `closeCtx` to `Authenticate`, `Subscribe`,
//! `Unsubscribe` and `Perform`, so a session that closes cancels its pending and
//! in-flight RPC calls. The [`Controller`](cabby_core::common::Controller)
//! interface is synchronous and has no context parameter (identity, streams and
//! the router wrap it on the same thread), so the node publishes the session's
//! close signal for the duration of the call ([`with_close_context`]) and
//! controllers that can be cancelled (the RPC controller) read it
//! ([`close_context`]). `Disconnect` runs without one, as upstream
//! (`context.Background()`).

use std::cell::RefCell;

use tokio_util::sync::CancellationToken;

thread_local! {
    static CURRENT: RefCell<Option<CancellationToken>> = const { RefCell::new(None) };
}

/// Runs `call` with `token` as the close signal of the session it serves.
pub fn with_close_context<R>(token: CancellationToken, call: impl FnOnce() -> R) -> R {
    struct Restore(Option<CancellationToken>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let prev = self.0.take();
            CURRENT.with(|c| *c.borrow_mut() = prev);
        }
    }
    let _restore = Restore(CURRENT.with(|c| c.borrow_mut().replace(token)));
    call()
}

/// The close signal of the session the current controller call serves, if any.
pub fn close_context() -> Option<CancellationToken> {
    CURRENT.with(|c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_context_is_scoped_to_the_call() {
        assert!(close_context().is_none());
        let outer = CancellationToken::new();
        let inner = CancellationToken::new();
        inner.cancel();
        with_close_context(outer.clone(), || {
            assert!(!close_context().unwrap().is_cancelled());
            with_close_context(inner.clone(), || assert!(close_context().unwrap().is_cancelled()));
            assert!(!close_context().unwrap().is_cancelled());
        });
        assert!(close_context().is_none());
    }
}
