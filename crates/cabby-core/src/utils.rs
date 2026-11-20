//! Utilities (upstream package `utils`): worker pool, message verifier,
//! priority queue and the session send queue.

pub mod backpressure;
mod gopool;
pub mod localtime;
mod message_verifier;
mod priority_queue;
mod queue;

pub use gopool::{
    GoPool, PoolInfo, ScheduleTimeoutError, Task, WORKER_IDLE_TIMEOUT, WORKER_RESPAWN_THRESHOLD, WorkerId, all_pools,
};
pub use localtime::LocalTime;
pub use message_verifier::{MessageVerifier, VerifierError};
pub use priority_queue::{ItemId, PriorityQueue, PriorityQueueItem};
pub use queue::{Item, PushResult, Queue};
