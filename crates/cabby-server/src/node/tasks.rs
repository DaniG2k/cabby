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
//!   (authentication). Upstream runs them on each session's reader goroutine;
//!   here transports hand messages over (push model), and a session with nothing
//!   to execute holds no thread.
//!
//! # Command pool: seats and blocking calls
//!
//! The command pool has a bounded number of *seats*
//! ([`set_command_concurrency`]): a task needs a seat to start, and tasks
//! waiting for one start in FIFO order. A seat stands for a thread doing work,
//! not for a thread waiting: a controller call that may block (an RPC) runs
//! inside [`blocking`], which gives the seat up for the duration of the call, as
//! Go's scheduler hands a blocked goroutine's processor over. The next queued
//! task starts right away, on an idle worker or a new thread. So a command waits
//! only for its own session's earlier commands and, if it calls the controller,
//! for an RPC slot (bounded by `rpc_wait_timeout`, as upstream): other sessions'
//! RPC backlog never delays it. Otherwise every seat could be held by sessions
//! queued at the RPC barrier, and commands that need no RPC (a public stream
//! subscription, a no-auth welcome) would wait behind them.
//!
//! When the call returns, the thread takes its seat back, even if that puts the
//! pool over its limit for a moment; the session's reader then hands the rest of
//! its messages back to the queue ([`should_yield`]) and the surplus thread
//! retires, so work outside blocking calls stays bounded by the seats.
//!
//! The trade-off: one thread per session that is *inside* an RPC call (waiting
//! for a slot or for the response), where upstream has a goroutine. Their number
//! is the RPC backlog: at most the RPC concurrency in flight plus the callers
//! queued for a slot, each for at most the wait timeout. Idle sessions, and
//! sessions running commands that need no RPC, hold no thread of their own. A
//! parked thread costs the touched pages of its stack (a few KiB), but threads
//! are a process-wide resource: past [`MAX_BLOCKED`] blocked threads, callers
//! keep their seat (the pool then queues commands like a plain bounded pool)
//! rather than exhaust the OS thread limit the rest of the server relies on.
//! The alternatives were an asynchronous controller interface (every node
//! command path, the controllers and their test facades would become async) or
//! taking the RPC slot before a command is scheduled (which needs to know
//! whether a command will call the controller before running it).
//!
//! Workers of both pools that stay idle for [`IDLE_TIMEOUT`] exit.

use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

type Task = Box<dyn FnOnce() + Send>;

/// How long an idle worker waits for work before it exits.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of command pool seats.
pub const DEFAULT_COMMAND_CONCURRENCY: usize = 64;

/// At most this many command pool threads give their seat up while blocked in
/// a controller call (see the module docs).
pub const MAX_BLOCKED: usize = 8192;

#[derive(Default)]
struct State {
    tasks: VecDeque<Task>,
    idle: usize,
    /// Threads holding a seat (busy or idle).
    workers: usize,
    /// Threads that gave their seat up inside [`blocking`].
    blocked: usize,
}

struct Pool {
    name: &'static str,
    /// Maximum number of seats (`usize::MAX`: unbounded).
    max: AtomicUsize,
    /// Maximum number of threads without a seat inside [`blocking`].
    max_blocked: usize,
    state: Mutex<State>,
    work: Condvar,
}

impl Pool {
    fn bounded(&self) -> bool {
        self.max.load(Ordering::Relaxed) != usize::MAX
    }
}

thread_local! {
    /// The pool the current thread is a worker of.
    static WORKER_OF: Cell<Option<&'static Pool>> = const { Cell::new(None) };
    /// Set when a [`blocking`] call returned with the pool over its seat limit.
    static OVER_LIMIT: Cell<bool> = const { Cell::new(false) };
}

fn lock(pool: &Pool) -> MutexGuard<'_, State> {
    pool.state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn task_pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        name: "cabby-node-task",
        max: AtomicUsize::new(usize::MAX),
        max_blocked: 0,
        state: Mutex::default(),
        work: Condvar::new(),
    })
}

fn command_pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        name: "cabby-command",
        max: AtomicUsize::new(DEFAULT_COMMAND_CONCURRENCY),
        max_blocked: MAX_BLOCKED,
        state: Mutex::default(),
        work: Condvar::new(),
    })
}

/// Runs `task` on a task pool thread (never waits for a busy worker).
pub(super) fn spawn(task: Task) {
    submit(task_pool(), task);
}

/// Runs a node task that may block on the controller (authentication, a client
/// command) on the command pool. Tasks start in submission order.
pub fn execute(task: Task) {
    submit(command_pool(), task);
}

/// Sets the number of command pool seats (at least one). Running workers above
/// a lowered limit finish their current task first.
pub fn set_command_concurrency(size: usize) {
    command_pool().max.store(size.max(1), Ordering::Relaxed);
}

/// The number of command pool seats.
pub fn command_concurrency() -> usize {
    command_pool().max.load(Ordering::Relaxed)
}

/// Runs `call`, a controller call that may wait for a long time (an RPC), with
/// the current command pool seat given up meanwhile, so queued commands of
/// other sessions start without waiting for it (see the module docs). On any
/// other thread (or nested in another `blocking` call) it just runs `call`.
pub fn blocking<R>(call: impl FnOnce() -> R) -> R {
    /// Takes the seat back when the call returns (or panics: the worker loop
    /// catches panics and must find its seat).
    struct Retake(&'static Pool);
    impl Drop for Retake {
        fn drop(&mut self) {
            WORKER_OF.with(|w| w.set(Some(self.0)));
            let mut state = lock(self.0);
            state.blocked -= 1;
            state.workers += 1;
            if state.workers > self.0.max.load(Ordering::Relaxed) {
                OVER_LIMIT.with(|o| o.set(true));
            }
        }
    }

    let pool = WORKER_OF.with(Cell::get).filter(|p| p.bounded());
    let _retake = pool.filter(|p| release_seat(p)).map(|p| {
        // Without a seat until the call returns (a nested call has none to give up).
        WORKER_OF.with(|w| w.set(None));
        Retake(p)
    });
    call()
}

/// Whether the current command pool task should hand the rest of its work back
/// to the queue: the pool was over its seat limit when the thread came back
/// from a [`blocking`] call (its seat had been taken over). Reading clears it.
pub(super) fn should_yield() -> bool {
    OVER_LIMIT.with(|o| o.replace(false))
}

/// Gives the current worker's seat up and lets a queued task have it. `false`
/// when the seat is kept (too many blocked threads already).
fn release_seat(pool: &'static Pool) -> bool {
    let mut state = lock(pool);
    if state.blocked >= pool.max_blocked {
        return false;
    }
    state.workers -= 1;
    state.blocked += 1;
    dispatch(pool, state);
    true
}

fn submit(pool: &'static Pool, task: Task) {
    let mut state = lock(pool);
    state.tasks.push_back(task);
    dispatch(pool, state);
}

/// Hands queued tasks to idle workers, or starts a worker when there are more
/// tasks than idle workers and a seat is free.
fn dispatch(pool: &'static Pool, mut state: MutexGuard<'_, State>) {
    if state.tasks.is_empty() {
        return;
    }
    if state.tasks.len() <= state.idle {
        drop(state);
        pool.work.notify_one();
        return;
    }
    if state.workers >= pool.max.load(Ordering::Relaxed) {
        // Every seat is taken: the task waits its turn.
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
    WORKER_OF.with(|w| w.set(Some(pool)));
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
        OVER_LIMIT.with(|o| o.set(false));
        // A panicking task must not take the worker with it (as the hub's pool).
        let _ = catch_unwind(AssertUnwindSafe(task));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn bounded_pool(name: &'static str, max: usize) -> &'static Pool {
        Box::leak(Box::new(Pool {
            name,
            max: AtomicUsize::new(max),
            max_blocked: MAX_BLOCKED,
            state: Mutex::default(),
            work: Condvar::new(),
        }))
    }

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
        let pool = bounded_pool("test-bounded", 2);
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

    /// H4 (review 2026-11): tasks blocked in a controller call do not hold the
    /// seats other tasks need.
    #[test]
    fn tasks_blocked_in_a_call_give_their_seat_up() {
        let pool = bounded_pool("test-blocking", 1);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = std::sync::Arc::new(Mutex::new(release_rx));
        let (done_tx, done_rx) = mpsc::channel();
        // Five tasks block "in RPC", one after another on the single seat (a
        // nested blocking call has no seat to give up).
        for _ in 0..5 {
            let rx = release_rx.clone();
            submit(
                pool,
                Box::new(move || {
                    blocking(|| {
                        blocking(|| {
                            let _ = rx.lock().unwrap().recv();
                        })
                    });
                }),
            );
        }
        // A task that needs no RPC still starts.
        let tx = done_tx.clone();
        submit(pool, Box::new(move || tx.send(should_yield()).unwrap()));
        assert_eq!(Ok(false), done_rx.recv_timeout(Duration::from_secs(5)));

        // The blocked tasks come back: the pool shrinks to its seat limit again.
        for _ in 0..5 {
            release_tx.send(()).unwrap();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (workers, blocked) = {
                let s = lock(pool);
                (s.workers, s.blocked)
            };
            if blocked == 0 && workers <= 1 {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{workers} workers, {blocked} blocked");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// D-036: past the cap, a blocked task keeps its seat, and queued tasks wait
    /// for it as on a plain bounded pool.
    #[test]
    fn deviation_d036_blocked_tasks_past_the_cap_keep_their_seat() {
        let pool: &'static Pool = Box::leak(Box::new(Pool {
            name: "test-blocked-cap",
            max: AtomicUsize::new(1),
            max_blocked: 1,
            state: Mutex::default(),
            work: Condvar::new(),
        }));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = std::sync::Arc::new(Mutex::new(release_rx));
        let (done_tx, done_rx) = mpsc::channel();
        for _ in 0..2 {
            let rx = release_rx.clone();
            submit(
                pool,
                Box::new(move || {
                    blocking(|| {
                        let _ = rx.lock().unwrap().recv();
                    });
                }),
            );
        }
        let tx = done_tx.clone();
        submit(pool, Box::new(move || tx.send(()).unwrap()));
        // One blocked task gave its seat up; the second kept it: the third waits.
        assert!(done_rx.recv_timeout(Duration::from_millis(300)).is_err(), "ran past the cap");
        assert_eq!((1, 1), {
            let s = lock(pool);
            (s.workers, s.blocked)
        });
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).is_ok());
    }

    #[test]
    fn blocking_outside_a_command_worker_just_runs_the_call() {
        assert_eq!(7, blocking(|| 7));
        assert!(!should_yield());
    }
}
