//! upstream file: utils/gopool_test.go
//!
//! Upstream compares goroutine IDs (parsed from `runtime.Stack`) to show that a
//! worker is replaced after `workerRespawnThreshold` tasks. Cabby's equivalent is
//! `GoPool::current_worker_id()`.

use std::sync::mpsc;
use std::time::Duration;

use cabby_core::utils::{GoPool, WORKER_RESPAWN_THRESHOLD};
use cabby_testkit::reference::utils::{WorkerRespawn, rust_go_pool, worker_respawn};

const DEADLINE: Duration = Duration::from_secs(30);

/// The schedule on a Rust pool of size 1: probe, threshold+2 data tasks, probe.
fn rust_schedule() -> WorkerRespawn {
    let pool = rust_go_pool("test", 1);

    let (res_tx, res_rx) = mpsc::channel();
    let (data_tx, data_rx) = mpsc::channel::<()>();
    // Upstream's WaitGroup is marked done by the last task but never waited on.

    let tx = res_tx.clone();
    pool.schedule(Box::new(move || {
        tx.send(GoPool::current_worker_id()).unwrap();
    }));

    for _ in 0..WORKER_RESPAWN_THRESHOLD + 2 {
        let tx = data_tx.clone();
        pool.schedule(Box::new(move || {
            tx.send(()).unwrap();
        }));
    }

    pool.schedule(Box::new(move || {
        res_tx.send(GoPool::current_worker_id()).unwrap();
    }));

    let initial = res_rx.recv_timeout(DEADLINE).expect("first task ran");
    let current = res_rx.recv_timeout(DEADLINE).expect("last task ran");
    drop(data_tx);
    WorkerRespawn { initial: initial.map(|w| w.0), current: current.map(|w| w.0), data_len: data_rx.try_iter().count() }
}

/// upstream: utils/gopool_test.go#TestWorkerRespawn
/// note: the schedule (GID probe, workerRespawnThreshold+2 data tasks, GID probe) runs as one bridge op on the exported NewGoPool("test", 1)/Schedule with upstream's getGID; the Rust worker identity is GoPool::current_worker_id().
#[test]
fn test_worker_respawn() {
    let out = worker_respawn(rust_schedule);

    assert_ne!(out.initial, out.current);
    // len(dataChan): the pool has a single worker slot, so every data task ran before
    // the last one.
    assert_eq!(WORKER_RESPAWN_THRESHOLD + 2, out.data_len);
}
