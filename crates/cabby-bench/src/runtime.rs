//! The harness's own async runtime and clock.
//!
//! Every network operation of the load generator (WebSocket clients, publisher
//! workers, the fake RPC application) runs on one multi-threaded Tokio runtime, so
//! thousands of connections cost tasks, not threads. The synchronous public API
//! blocks the calling thread on that runtime.
//!
//! Worker threads default to the number of CPUs; `CABBY_BENCH_THREADS` overrides
//! it (for example to keep the generator on cores the server under test does not
//! use, together with `taskset`).
//!
//! Timings use a monotonic clock: [`now_ns`] counts from a process-wide
//! [`Instant`], so delivery latencies stamped by the publisher and measured by the
//! receivers are immune to wall-clock adjustments.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Instant;

use tokio::runtime::Runtime;

/// Environment variable that sets the number of runtime worker threads.
pub const THREADS_ENV: &str = "CABBY_BENCH_THREADS";

pub(crate) fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads())
            .thread_name("cabby-bench")
            .enable_all()
            .build()
            .expect("building the harness runtime")
    })
}

/// Worker threads of the harness runtime.
pub fn worker_threads() -> usize {
    std::env::var(THREADS_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()))
}

/// Runs `f` to completion on the harness runtime, blocking the calling thread.
pub(crate) fn block_on<F: Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        // Called from inside the runtime (a callback): don't nest `block_on`.
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f)),
        Err(_) => rt().block_on(f),
    }
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Monotonic nanoseconds since the harness epoch.
pub(crate) fn now_ns() -> u64 {
    u64::try_from(epoch().elapsed().as_nanos()).unwrap_or(u64::MAX)
}
