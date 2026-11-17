//! upstream file: utils/gopool_test.go
//!
//! Upstream compares goroutine IDs (parsed from `runtime.Stack`) to show that a
//! worker is replaced after `workerRespawnThreshold` tasks. Cabby's equivalent is
//! `GoPool::current_worker_id()`. Tasks are closures, which cannot cross the Go
//! bridge, so this case is not observable against Go.

use std::sync::mpsc;
use std::time::Duration;

use cabby_core::utils::{GoPool, WORKER_RESPAWN_THRESHOLD};
use cabby_testkit::reference::utils::rust_go_pool;

const DEADLINE: Duration = Duration::from_secs(30);

/// upstream: utils/gopool_test.go#TestWorkerRespawn
#[test]
fn test_worker_respawn() {
    let pool = rust_go_pool("test", 1, "GoPool tasks are Go closures and goroutine IDs are runtime internals");

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

    assert_ne!(initial, current);
    // len(dataChan): the pool has a single worker slot, so every data task ran before
    // the last one.
    drop(data_tx);
    assert_eq!(WORKER_RESPAWN_THRESHOLD + 2, data_rx.try_iter().count());
}
