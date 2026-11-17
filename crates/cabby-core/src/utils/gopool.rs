//! Worker pool (upstream `utils.GoPool`).
//!
//! # Design
//!
//! Upstream bounds the number of goroutines running scheduled tasks with a semaphore
//! channel (`size` slots) and buffers tasks for busy workers in a channel of
//! `size / 2` slots; `size / 5` workers (at most 1024) are started eagerly.
//! `Schedule` picks at random between handing the task to the buffer and spawning
//! a worker, and blocks while both are full.
//!
//! Cabby keeps the same limits and the same blocking contract, with one mutex-guarded
//! state and two condition variables instead of channels:
//!
//! * Workers are **OS threads**, not Tokio tasks. Scheduled tasks are synchronous
//!   closures that may block (for example a session disconnect that waits for its
//!   writer), and running them on dedicated threads keeps them off the async runtime's
//!   worker threads. The pool is `Send + Sync`; the hub can share it behind an `Arc`.
//! * Dispatch is deterministic and FIFO: an idle worker gets the task first; otherwise
//!   a new worker is spawned while fewer than `size` exist; otherwise the task is
//!   buffered while the buffer has room; otherwise [`GoPool::schedule`] blocks.
//!   Upstream may buffer a task behind busy workers while spawn slots are free;
//!   Cabby never does, so a burst starts parallel workers sooner.
//! * Idle workers park on a condition variable and are woken with an explicit token,
//!   so a schedule wakes exactly one of them and nothing is signalled when no one waits.
//!   Thread spawns happen outside the lock.
//! * After [`WORKER_RESPAWN_THRESHOLD`] tasks a worker hands over to a fresh thread
//!   (new [`WorkerId`]) and exits, like upstream's goroutine restart; this releases
//!   whatever per-thread state the tasks accumulated.
//! * From async code, use [`GoPool::try_schedule`] (never blocks) or call
//!   [`GoPool::schedule`] from `spawn_blocking`: `schedule` blocks the calling thread
//!   while the pool is saturated, which is the upstream back-pressure contract.
//! * A panicking task is caught and its worker keeps going (a Go panic would end the
//!   process). Dropping the pool lets workers finish the buffered tasks, then exit.

use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

/// Tasks a worker runs before it is replaced by a fresh one (upstream
/// `workerRespawnThreshold`).
pub const WORKER_RESPAWN_THRESHOLD: usize = 1 << 16;

/// Identity of a pool worker (upstream tests compare goroutine IDs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(pub u64);

pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// Returned by [`GoPool::schedule_timeout`] (upstream `ErrScheduleTimeout`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("schedule error: timed out")]
pub struct ScheduleTimeoutError;

/// Name and size of a live pool (upstream `AllPools()` entries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolInfo {
    pub name: String,
    pub size: usize,
}

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);
static POOLS: Mutex<Vec<Weak<Shared>>> = Mutex::new(Vec::new());

thread_local! {
    static CURRENT_WORKER: Cell<Option<WorkerId>> = const { Cell::new(None) };
}

/// The pools that are still alive, in creation order (upstream `AllPools`).
pub fn all_pools() -> Vec<PoolInfo> {
    let mut pools = lock(&POOLS);
    pools.retain(|p| p.strong_count() > 0);
    pools
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|p| !lock(&p.state).closed)
        .map(|p| PoolInfo { name: p.name.clone(), size: p.size })
        .collect()
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Tasks never run under the lock, so poisoning cannot leave the state inconsistent.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

struct State {
    /// Tasks waiting for a worker.
    queue: VecDeque<Task>,
    /// Live workers (bounded by `size`).
    workers: usize,
    /// Parked workers that have not been handed a wake-up token.
    idle: usize,
    /// Wake-up tokens handed to parked workers and not consumed yet.
    wakeups: usize,
    /// Schedulers blocked on a saturated pool.
    blocked: usize,
    /// The pool was dropped: workers drain the queue and exit.
    closed: bool,
}

struct Shared {
    name: String,
    size: usize,
    queue_cap: usize,
    state: Mutex<State>,
    /// Parked workers wait here.
    work: Condvar,
    /// Blocked schedulers wait here.
    space: Condvar,
}

/// A bounded pool of reusable workers (upstream `utils.GoPool`). See the module docs.
pub struct GoPool {
    shared: Arc<Shared>,
}

/// What a scheduling attempt did with the task.
enum Placement {
    Done,
    Spawn(Option<Task>),
    Full(Task),
}

impl GoPool {
    /// Creates a pool with up to `size` workers (at least one). Starts `size / 5`
    /// workers (at least one, at most 1024) and buffers up to `size / 2` tasks (at
    /// least one), like upstream `NewGoPool`.
    pub fn new(name: &str, size: usize) -> Self {
        let size = size.max(1);
        let queue_cap = (size / 2).max(1);
        let spawn = (size / 5).clamp(1, 1024);

        let shared = Arc::new(Shared {
            name: name.to_owned(),
            size,
            queue_cap,
            state: Mutex::new(State {
                queue: VecDeque::with_capacity(queue_cap),
                workers: spawn,
                idle: 0,
                wakeups: 0,
                blocked: 0,
                closed: false,
            }),
            work: Condvar::new(),
            space: Condvar::new(),
        });

        for _ in 0..spawn {
            // Upstream starts them with a no-op task, which counts towards the threshold.
            spawn_worker(&shared, None, 1);
        }

        lock(&POOLS).push(Arc::downgrade(&shared));

        GoPool { shared }
    }

    pub fn name(&self) -> String {
        self.shared.name.clone()
    }

    pub fn size(&self) -> usize {
        self.shared.size
    }

    /// Schedules a task on the pool's workers, blocking while the pool is saturated.
    pub fn schedule(&self, task: Task) {
        let mut st = lock(&self.shared.state);
        let mut task = task;
        loop {
            match self.place(&mut st, task) {
                Placement::Done => return,
                Placement::Spawn(first) => {
                    drop(st);
                    spawn_worker(&self.shared, first, 0);
                    return;
                }
                Placement::Full(t) => {
                    task = t;
                    st.blocked += 1;
                    st = self.shared.space.wait(st).unwrap_or_else(|e| e.into_inner());
                    st.blocked -= 1;
                }
            }
        }
    }

    /// Like [`schedule`](Self::schedule), but gives up after `timeout` (upstream
    /// `ScheduleTimeout`). The task is dropped on timeout.
    pub fn schedule_timeout(&self, timeout: Duration, task: Task) -> Result<(), ScheduleTimeoutError> {
        let deadline = Instant::now() + timeout;
        let mut st = lock(&self.shared.state);
        let mut task = task;
        loop {
            match self.place(&mut st, task) {
                Placement::Done => return Ok(()),
                Placement::Spawn(first) => {
                    drop(st);
                    spawn_worker(&self.shared, first, 0);
                    return Ok(());
                }
                Placement::Full(t) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(ScheduleTimeoutError);
                    }
                    task = t;
                    st.blocked += 1;
                    st = self.shared.space.wait_timeout(st, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
                    st.blocked -= 1;
                }
            }
        }
    }

    /// Schedules a task without blocking; gives the task back if the pool is saturated.
    /// This is the variant to call from async code.
    pub fn try_schedule(&self, task: Task) -> Result<(), Task> {
        let mut st = lock(&self.shared.state);
        match self.place(&mut st, task) {
            Placement::Done => Ok(()),
            Placement::Spawn(first) => {
                drop(st);
                spawn_worker(&self.shared, first, 0);
                Ok(())
            }
            Placement::Full(t) => Err(t),
        }
    }

    fn place(&self, st: &mut State, task: Task) -> Placement {
        if st.idle > 0 {
            st.queue.push_back(task);
            st.idle -= 1;
            st.wakeups += 1;
            self.shared.work.notify_one();
            return Placement::Done;
        }
        if st.workers < self.shared.size {
            st.workers += 1;
            if st.queue.is_empty() {
                return Placement::Spawn(Some(task));
            }
            // Keep FIFO order: the new worker starts from the front of the queue.
            st.queue.push_back(task);
            return Placement::Spawn(None);
        }
        if st.queue.len() < self.shared.queue_cap {
            st.queue.push_back(task);
            return Placement::Done;
        }
        Placement::Full(task)
    }

    /// The worker executing the calling task (`None` outside pool workers). This is
    /// Cabby's equivalent of the goroutine ID upstream tests observe.
    pub fn current_worker_id() -> Option<WorkerId> {
        CURRENT_WORKER.with(Cell::get)
    }
}

impl Drop for GoPool {
    fn drop(&mut self) {
        let mut st = lock(&self.shared.state);
        st.closed = true;
        drop(st);
        self.shared.work.notify_all();
        self.shared.space.notify_all();
    }
}

impl std::fmt::Debug for GoPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoPool").field("name", &self.shared.name).field("size", &self.shared.size).finish()
    }
}

/// Starts a worker thread. The caller has already counted it in `State::workers`.
fn spawn_worker(shared: &Arc<Shared>, first: Option<Task>, counter: usize) {
    let id = WorkerId(NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed));
    let shared = Arc::clone(shared);
    std::thread::Builder::new()
        .name(format!("pool-{}", id.0))
        .spawn(move || worker(shared, id, first, counter))
        .expect("failed to spawn a pool worker thread");
}

fn run(task: Task) {
    // A panicking task must not take the worker (and its pool slot) with it.
    let _ = catch_unwind(AssertUnwindSafe(task));
}

fn worker(shared: Arc<Shared>, id: WorkerId, first: Option<Task>, mut counter: usize) {
    CURRENT_WORKER.with(|c| c.set(Some(id)));

    if let Some(task) = first {
        run(task);
        counter += 1;
    }

    loop {
        if counter >= WORKER_RESPAWN_THRESHOLD {
            // Hand the pool slot over to a fresh worker.
            spawn_worker(&shared, None, 0);
            return;
        }

        let mut st = lock(&shared.state);
        let task = loop {
            if let Some(task) = st.queue.pop_front() {
                if st.blocked > 0 {
                    shared.space.notify_one();
                }
                break task;
            }
            if st.closed {
                st.workers -= 1;
                return;
            }
            st.idle += 1;
            while st.wakeups == 0 && !st.closed {
                st = shared.work.wait(st).unwrap_or_else(|e| e.into_inner());
            }
            if st.wakeups > 0 {
                st.wakeups -= 1;
            } else {
                st.idle -= 1;
            }
        };
        drop(st);

        run(task);
        counter += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    #[test]
    fn runs_every_task_with_bounded_parallelism() {
        let pool = GoPool::new("t", 4);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        for _ in 0..200 {
            let (running, peak, tx) = (running.clone(), peak.clone(), tx.clone());
            pool.schedule(Box::new(move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_micros(200));
                running.fetch_sub(1, Ordering::SeqCst);
                tx.send(()).unwrap();
            }));
        }
        for _ in 0..200 {
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= 4);
        assert!(GoPool::current_worker_id().is_none());
    }

    #[test]
    fn schedule_timeout_and_try_schedule_report_saturation() {
        let pool = GoPool::new("t", 1);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (started_tx, started_rx) = mpsc::channel::<()>();
        pool.schedule(Box::new(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // One buffer slot.
        assert!(pool.try_schedule(Box::new(|| {})).is_ok());
        assert!(pool.try_schedule(Box::new(|| {})).is_err());
        let err = pool.schedule_timeout(Duration::from_millis(20), Box::new(|| {})).unwrap_err();
        assert_eq!("schedule error: timed out", err.to_string());
        release_tx.send(()).unwrap();
    }

    #[test]
    fn survives_a_panicking_task() {
        let pool = GoPool::new("t", 1);
        pool.schedule(Box::new(|| panic!("boom")));
        let (tx, rx) = mpsc::channel();
        pool.schedule(Box::new(move || tx.send(GoPool::current_worker_id()).unwrap()));
        assert!(rx.recv_timeout(Duration::from_secs(10)).unwrap().is_some());
    }

    #[test]
    fn lists_live_pools() {
        let pool = GoPool::new("listed-pool", 7);
        assert!(all_pools().contains(&PoolInfo { name: "listed-pool".into(), size: 7 }));
        drop(pool);
        assert!(!all_pools().iter().any(|p| p.name == "listed-pool"));
    }
}
