//! Utilities (upstream package `utils`): worker pool, message verifier,
//! priority queue and the session send queue.

pub mod backpressure;
mod gopool;
pub mod localtime;
mod message_verifier;
mod priority_queue;
mod queue;
mod redact;

pub use gopool::{
    GoPool, PoolInfo, ScheduleTimeoutError, Task, WORKER_IDLE_TIMEOUT, WORKER_RESPAWN_THRESHOLD, WorkerId, all_pools,
};
pub use localtime::LocalTime;
pub use message_verifier::{MessageVerifier, VerifierError};
pub use priority_queue::{ItemId, PriorityQueue, PriorityQueueItem};
pub use queue::{Item, PushResult, Queue};
pub use redact::{redact_refused_url, url_list, userinfo_may_be_cut};

/// Byte-slice comparison through `subtle::ConstantTimeEq`.
///
/// Slices of different lengths compare unequal right away. For equal lengths
/// the comparison is constant-time on a best-effort basis: `subtle` uses
/// optimization barriers so the compiler is unlikely to turn it back into an
/// early-exit loop, but neither it nor Rust guarantees constant-time code.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    subtle::ConstantTimeEq::ct_eq(a, b).into()
}
