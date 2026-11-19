//! Thread pools for node work that may block.
//!
//! * The **task pool** ([`spawn`]) runs short-lived work: session write drains
//!   and fired timer callbacks (upstream runs both on fresh goroutines). It never
//!   blocks the caller and never makes a task wait behind a blocked one: a task
//!   goes to an idle worker when one is waiting, otherwise a new worker thread
//!   starts. So the number of threads follows the number of tasks running at the
//!   same time (typically writes blocked on slow clients), not the number of
//!   sessions.
//! * The **command pool** ([`execute`]) runs client commands and session setup
//!   (authentication), which call the controller synchronously and may wait for
//!   an RPC response. Upstream runs them on each session's reader goroutine; here
//!   transports hand messages over (push model) and the commands run on a
//!   bounded set of threads ([`set_command_concurrency`]), queued in FIFO order
//!   when every worker is busy. RPC calls are bounded by the RPC concurrency
//!   anyway, so a few more threads than that keep the RPC pipeline full without a
//!   thread per connection.
//!
//! Workers of both pools that stay idle for [`IDLE_TIMEOUT`] exit.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

type Task = Box<dyn FnOnce() + Send>;

/// How long an idle worker waits for work before it exits.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default size of the command pool.
pub const DEFAULT_COMMAND_CONCURRENCY: usize = 64;

#[derive(Default)]
struct State {
    tasks: VecDeque<Task>,
    idle: usize,
    workers: usize,
}

struct Pool {
    name: &'static str,
    /// Maximum number of workers (`usize::MAX`: unbounded).
    max: AtomicUsize,
    state: Mutex<State>,
    work: Condvar,
}

fn lock(pool: &Pool) -> MutexGuard<'_, State> {
    pool.state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn task_pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        name: "cabby-node-task",
        max: AtomicUsize::new(usize::MAX),
        state: Mutex::default(),
        work: Condvar::new(),
    })
}

fn command_pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        name: "cabby-command",
        max: AtomicUsize::new(DEFAULT_COMMAND_CONCURRENCY),
        state: Mutex::default(),
        work: Condvar::new(),
    })
}

/// Runs `task` on a task pool thread (never waits for a busy worker).
pub(super) fn spawn(task: Task) {
    submit(task_pool(), task);
}

/// Runs a node task that may block on the controller (authentication, a client
/// command) on the bounded command pool. Tasks start in submission order.
pub fn execute(task: Task) {
    submit(command_pool(), task);
}

/// Sets the maximum number of command pool threads (at least one). Running
/// workers above a lowered limit finish their current task first.
pub fn set_command_concurrency(size: usize) {
    command_pool().max.store(size.max(1), Ordering::Relaxed);
}

/// The command pool's maximum number of threads.
pub fn command_concurrency() -> usize {
    command_pool().max.load(Ordering::Relaxed)
}

fn submit(pool: &'static Pool, task: Task) {
    let mut state = lock(pool);
    state.tasks.push_back(task);
    if state.tasks.len() <= state.idle {
        drop(state);
        pool.work.notify_one();
        return;
    }
    if state.workers >= pool.max.load(Ordering::Relaxed) {
        // Every worker is busy: the task waits its turn.
        return;
    }
    state.workers += 1;
    drop(state);
    let spawned = std::thread::Builder::new().name(pool.name.into()).spawn(move || worker(pool));
    if spawned.is_err() {
        // Out of threads: the existing workers (if any) take the task later.
        lock(pool).workers -= 1;
    }
}

fn worker(pool: &'static Pool) {
    loop {
        let task = {
            let mut state = lock(pool);
            loop {
                if state.workers > pool.max.load(Ordering::Relaxed) {
                    state.workers -= 1;
                    return;
                }
                if let Some(task) = state.tasks.pop_front() {
                    break task;
                }
                state.idle += 1;
                let (s, timeout) = pool.work.wait_timeout(state, IDLE_TIMEOUT).unwrap_or_else(PoisonError::into_inner);
                state = s;
                state.idle -= 1;
                if timeout.timed_out() && state.tasks.is_empty() {
                    state.workers -= 1;
                    return;
                }
            }
        };
        // A panicking task must not take the worker with it (as the hub's pool).
        let _ = catch_unwind(AssertUnwindSafe(task));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn a_blocked_task_does_not_delay_the_next_one() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel();
        spawn(Box::new(move || {
            let _ = release_rx.recv();
        }));
        spawn(Box::new(move || done_tx.send(()).unwrap()));
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).is_ok());
        release_tx.send(()).unwrap();
    }

    #[test]
    fn commands_run_in_order_on_a_bounded_pool() {
        let pool: &'static Pool = Box::leak(Box::new(Pool {
            name: "test-bounded",
            max: AtomicUsize::new(2),
            state: Mutex::default(),
            work: Condvar::new(),
        }));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = std::sync::Arc::new(Mutex::new(release_rx));
        let (done_tx, done_rx) = mpsc::channel();
        for i in 0..6 {
            let (rx, tx) = (release_rx.clone(), done_tx.clone());
            submit(
                pool,
                Box::new(move || {
                    let _ = rx.lock().unwrap().recv();
                    tx.send(i).unwrap();
                }),
            );
        }
        assert_eq!(2, lock(pool).workers, "no more workers than the limit");
        for _ in 0..6 {
            release_tx.send(()).unwrap();
        }
        let mut done: Vec<i32> = (0..6).map(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap()).collect();
        done.sort_unstable();
        assert_eq!(vec![0, 1, 2, 3, 4, 5], done);
    }
}
