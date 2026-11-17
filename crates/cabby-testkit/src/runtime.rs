//! The shared Tokio runtime for test infrastructure (fake services, async facades).
//!
//! Tests are ordinary synchronous `#[test]` functions; they drive async code with
//! [`block_on`] and spawn background work with [`spawn`]. Fakes run on this
//! runtime's worker threads, so a test thread blocked on a fake never starves it.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::runtime::Runtime;

/// Default deadline for waits in test infrastructure.
pub const DEADLINE: Duration = Duration::from_secs(10);

pub fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("cabby-testkit")
            .enable_all()
            .build()
            .expect("building the testkit runtime")
    })
}

/// Runs `f` to completion on the shared runtime. Must not be called from inside
/// the runtime (call it from test threads).
pub fn block_on<F: Future>(f: F) -> F::Output {
    runtime().block_on(f)
}

/// Like [`block_on`], failing with a panic if `f` does not finish within `deadline`.
pub fn block_on_within<F: Future>(deadline: Duration, f: F) -> F::Output {
    block_on(async move {
        match tokio::time::timeout(deadline, f).await {
            Ok(v) => v,
            Err(_) => panic!("operation did not finish within {deadline:?}"),
        }
    })
}

pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    runtime().spawn(f)
}
