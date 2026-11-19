//! A cached thread pool for short-lived node work: session write drains and
//! fired timer callbacks (upstream runs both on fresh goroutines).
//!
//! The pool never blocks the caller and never makes a task wait behind a
//! blocked one: a task goes to an idle worker when one is waiting, otherwise a
//! new worker thread starts. Workers that stay idle for [`IDLE_TIMEOUT`] exit,
//! so the number of threads follows the number of tasks running at the same
//! time (typically writes blocked on slow clients), not the number of sessions.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

type Task = Box<dyn FnOnce() + Send>;

/// How long an idle worker waits for work before it exits.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct State {
    tasks: VecDeque<Task>,
    idle: usize,
}

struct Pool {
    state: Mutex<State>,
    work: Condvar,
}

fn pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool { state: Mutex::new(State::default()), work: Condvar::new() })
}

fn lock(pool: &Pool) -> MutexGuard<'_, State> {
    pool.state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `task` on a pool thread.
pub(super) fn spawn(task: Task) {
    let pool = pool();
    let mut state = lock(pool);
    state.tasks.push_back(task);
    if state.tasks.len() <= state.idle {
        drop(state);
        pool.work.notify_one();
        return;
    }
    drop(state);
    std::thread::Builder::new()
        .name("cabby-node-task".into())
        .spawn(move || worker(pool))
        .expect("spawn a node task thread");
}

fn worker(pool: &'static Pool) {
    loop {
        let task = {
            let mut state = lock(pool);
            loop {
                if let Some(task) = state.tasks.pop_front() {
                    break task;
                }
                state.idle += 1;
                let (s, timeout) = pool.work.wait_timeout(state, IDLE_TIMEOUT).unwrap_or_else(PoisonError::into_inner);
                state = s;
                state.idle -= 1;
                if timeout.timed_out() && state.tasks.is_empty() {
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
}
