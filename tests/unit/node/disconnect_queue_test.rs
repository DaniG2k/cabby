//! upstream file: node/disconnect_queue_test.go
//!
//! Upstream drives the queue on wall-clock time: `Rate` sets a real
//! `time.Ticker` shared by the workers, and the tests sleep or poll
//! (`require.Eventually`, `runtime.Gosched`) around it. Here the Rust node runs
//! on a [`ManualTimers`] clock: the throttle only ticks when the test advances
//! time, and the observable readiness points are "N workers wait for a tick"
//! (the ticker's receivers) and "N Disconnect calls are in flight" (a gate in
//! the controller). The original Go timings are kept in comments.
//!
//! Against Go, cases that only use exported API (`NewDisconnectQueue`,
//! `Enqueue`, `Size`, `Run`, `Shutdown`, metrics) run through the bridge; cases
//! built on testify controller callbacks, the unexported queue, or concurrent
//! callers are `not_observable` there.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cabby_core::common::ControllerError;
use cabby_server::node::Disconnector;
use cabby_testkit::reference::node::*;
use cabby_testkit::sync::Watched;
use cabby_testkit::timers::ManualTimers;

use super::support::DEADLINE;

/// Upstream `newQueue()`: a disconnect queue with `Rate = 1` over its own
/// `NewMockNode()` (upstream reaches the node as `q.node`).
fn new_queue(node: &TestNode) -> TestQueue<'_> {
    let mut config = new_disconnect_queue_config();
    config.rate = 1;
    new_disconnect_queue(node, &config)
}

/// `defer q.Shutdown(ctx)`.
struct ShutdownOnDrop<'a, 'b>(&'a TestQueue<'b>);

impl Drop for ShutdownOnDrop<'_, '_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = self.0.shutdown();
        }
    }
}

fn timers_of<'a>(q: &'a TestQueue<'_>) -> &'a ManualTimers {
    q.node().timers()
}

/// In-flight bookkeeping of upstream's blocking `Disconnect` mock.
#[derive(Debug, Default)]
struct Inflight {
    inflight: i32,
    peak: i32,
    calls: i32,
}

mod test_disconnect_queue_run {
    use super::*;

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run/Disconnects_sessions
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run
    #[test]
    fn disconnects_sessions() {
        let node = new_mock_node();
        let q = new_queue(&node);
        let _shutdown = ShutdownOnDrop(&q);

        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());
        assert_eq!(1, q.size());

        q.run_in_background();

        // Upstream: `for q.Size() > 0 { runtime.Gosched() }` ("TODO: We need a
        // trully node mock to control disconnect operation"). The worker takes
        // the session and then waits for the (1s) throttle tick.
        q.await_drained(1);
        assert_eq!(0, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run/Runs_calls_concurrently_up_to_the_number_of_workers
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run
    #[test]
    fn runs_calls_concurrently_up_to_the_number_of_workers() {
        let controller = MockNodeController::new();
        let config = new_config();
        let node = new_node_with_controller(config, controller.clone());

        let release = BlockingGate::new();
        let state = Arc::new(Watched::new(Inflight::default()));

        {
            let release = release.clone();
            let state = state.clone();
            controller.on("Disconnect", vec![any(), any(), any(), any()], move |_| {
                state.update(|s| {
                    s.inflight += 1;
                    if s.inflight > s.peak {
                        s.peak = s.inflight;
                    }
                });
                release.pass();
                state.update(|s| {
                    s.inflight -= 1;
                    s.calls += 1;
                });
                Ok::<(), ControllerError>(())
            });
        }

        let mut dconfig = new_disconnect_queue_config();
        dconfig.rate = 1000;
        dconfig.workers_num = 2;
        let q = new_disconnect_queue(&node, &dconfig);
        let timers = timers_of(&q);

        for i in 0..3 {
            q.enqueue(&new_mock_session(&i.to_string(), &node, vec![])).expect("enqueue");
        }

        q.run_in_background();

        // Upstream: require.Eventually(inflight == 2, 1s, 1ms) on a 1ms ticker.
        // Both workers hold a session and wait for the throttle; each tick lets
        // one call through.
        timers.await_ticker_waiters(2, DEADLINE);
        timers.advance(Duration::from_millis(1));
        release.await_held(1, DEADLINE);
        timers.advance(Duration::from_millis(1));
        release.await_held(2, DEADLINE);
        assert_eq!(2, state.read(|s| s.inflight));

        // The third call waits for a free worker (upstream: time.Sleep(50ms),
        // i.e. 50 throttle periods).
        for _ in 0..50 {
            timers.advance(Duration::from_millis(1));
        }
        assert_eq!(2, state.read(|s| s.inflight));
        assert_eq!(2, release.arrived(), "no third Disconnect call may start while both workers are busy");
        assert_eq!(0, timers.ticker_waiters(), "the third session is not taken while both workers are busy");

        release.open();

        // Upstream: require.Eventually(calls == 3, 1s, 1ms). The tick the ticker
        // buffered while both workers were busy (Go tickers buffer one) lets the
        // freed worker run the third call without another advance.
        assert!(state.wait_until(DEADLINE, |s| s.calls == 3), "3 calls expected, {:?}", state.read(|s| s.calls));
        assert_eq!(2, state.read(|s| s.peak));

        q.shutdown().expect("shutdown");
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run/Respects_the_rate_with_multiple_workers
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Run
    #[test]
    fn respects_the_rate_with_multiple_workers() {
        let controller = MockNodeController::new();
        let config = new_config();
        let node = new_node_with_controller(config, controller.clone());

        let calls = Arc::new(Watched::new(0usize));
        {
            let calls = calls.clone();
            controller.on("Disconnect", vec![any(), any(), any(), any()], move |_| {
                calls.update(|c| *c += 1);
                Ok::<(), ControllerError>(())
            });
        }

        let mut dconfig = new_disconnect_queue_config();
        dconfig.rate = 20;
        dconfig.workers_num = 4;
        let q = new_disconnect_queue(&node, &dconfig);
        let timers = timers_of(&q);

        for i in 0..5 {
            q.enqueue(&new_mock_session(&i.to_string(), &node, vec![])).expect("enqueue");
        }

        let start = timers.elapsed();
        q.run_in_background();

        // Upstream: require.Eventually(calls == 5, 2s, 1ms), then "5 calls at
        // 20 calls/s take at least 5 ticks of 50ms": elapsed >= 240ms.
        //
        // Here: all four workers hold a session and wait for the shared 50ms
        // throttle; every tick admits exactly one call, whichever worker gets it.
        timers.await_ticker_waiters(4, DEADLINE);
        assert_eq!(0, calls.read(|c| *c));
        for k in 1..=5usize {
            timers.advance(Duration::from_millis(50));
            assert!(calls.wait_until(DEADLINE, |c| *c >= k), "call {k} expected after tick {k}");
            // Quiescence: the remaining sessions are held by workers waiting for a tick.
            timers.await_ticker_waiters(4.min(5 - k), DEADLINE);
            assert_eq!(k, calls.read(|c| *c), "the workers share one rate: one call per tick");
        }

        assert!(timers.elapsed() - start >= Duration::from_millis(240));

        q.shutdown().expect("shutdown");
    }
}

mod test_disconnect_queue_shutdown {
    use super::*;

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Disconnects_sessions
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown
    #[test]
    fn disconnects_sessions() {
        let node = new_mock_node();
        let q = new_queue(&node);

        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());
        assert!(q.enqueue(&new_mock_session("2", q.node(), vec![])).is_ok());
        assert_eq!(2, q.size());

        assert!(q.shutdown().is_ok());
        assert_eq!(0, q.size());
    }

    /// Upstream skips this case: `t.Skip("TODO: We need a trully node mock to
    /// control disconnect operation")` (the whole body; nothing else runs).
    ///
    /// This translation runs what the name describes with the upstream test
    /// tools alone (`newQueue()`, whose mock controller cannot be held): the
    /// shutdown context's deadline has already passed, so `Shutdown` drains the
    /// queue without invoking any remaining Disconnect call and reports how
    /// many it skipped. The in-flight variant the TODO asks for, with a
    /// controllable controller, is `stops_after_timeout_while_a_call_is_in_flight`.
    ///
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Stops_after_timeout
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown
    #[test]
    fn stops_after_timeout() {
        let node = new_mock_node();
        let q = new_queue(&node);

        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());
        assert!(q.enqueue(&new_mock_session("2", q.node(), vec![])).is_ok());
        assert_eq!(2, q.size());

        assert_eq!(Err(NodeError("had no time to invoke Disconnect calls: ~2".into())), q.shutdown_expired());
        assert_eq!(0, q.size());
    }

    /// Deterministic active equivalent of the skipped upstream case (provenance:
    /// `TestDisconnectQueue_Shutdown/Stops_after_timeout`, skipped upstream with
    /// "TODO: We need a trully node mock to control disconnect operation").
    ///
    /// The controller's Disconnect is held at a gate, so the first remaining
    /// call is in flight when the shutdown context ends (cancellation stands in
    /// for the deadline firing; upstream checks `ctx.Done()` before each call).
    /// `Shutdown` must finish that call, invoke no further ones, and report the
    /// two it had no time for.
    ///
    /// regression-for: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Stops_after_timeout
    #[test]
    fn stops_after_timeout_while_a_call_is_in_flight() {
        let controller = MockNodeController::new();
        let node = new_node_with_controller(new_config(), controller.clone());
        let hold = BlockingGate::new();
        {
            let hold = hold.clone();
            controller.on("Disconnect", vec![any(), any(), any(), any()], move |_| {
                hold.pass();
                Ok::<(), ControllerError>(())
            });
        }

        let q = new_queue(&node);
        for i in 1..=3 {
            q.enqueue(&new_mock_session(&i.to_string(), &node, vec![])).expect("enqueue");
        }
        assert_eq!(3, q.size());

        let ctx = background();
        let result = std::thread::scope(|sc| {
            let shutdown = sc.spawn(|| q.rust_with("testify controller").shutdown(&ctx));

            hold.await_held(1, DEADLINE);
            assert_eq!(0, q.size(), "Shutdown takes every queued session before invoking them");

            ctx.cancel.cancel();
            hold.open();

            shutdown.join().expect("shutdown thread")
        });

        assert_eq!(Err(NodeError("had no time to invoke Disconnect calls: ~2".into())), result);
        controller.mock.assert_number_of_calls("Disconnect", 1);
        assert_eq!(0, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Allows_multiple_entering
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown
    #[test]
    fn allows_multiple_entering() {
        let node = new_mock_node();
        let q = new_queue(&node);

        for _ in 1..=10 {
            let _ = q.shutdown(); // upstream ignores the error
        }
    }

    /// Upstream ignores the results; repeated shutdowns are no-ops that succeed.
    ///
    /// regression-for: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Allows_multiple_entering
    #[test]
    fn repeated_shutdowns_succeed() {
        let node = new_mock_node();
        let q = new_queue(&node);
        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());

        for _ in 1..=10 {
            assert_eq!(Ok(()), q.shutdown());
        }
        assert_eq!(0, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown/Does_not_block_when_sessions_are_enqueued_concurrently
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Shutdown
    #[test]
    fn does_not_block_when_sessions_are_enqueued_concurrently() {
        for _ in 0..100 {
            let node = new_mock_node();
            let q = new_queue(&node);
            q.rust_with("the bridge serializes calls, so Enqueue cannot race Shutdown in the Go reference");

            for j in 0..100 {
                q.enqueue(&new_mock_session(&j.to_string(), q.node(), vec![])).expect("enqueue");
            }

            // Keep enqueueing until shutdown completes, so some Enqueue calls
            // pass the stopped check before Shutdown and add items during it
            let stop = AtomicBool::new(false);

            std::thread::scope(|sc| {
                for j in 0..8 {
                    let (q, stop) = (&q, &stop);
                    sc.spawn(move || {
                        let s = new_mock_session(&format!("c-{j}"), q.node(), vec![]);

                        while !stop.load(Ordering::Acquire) {
                            let _ = q.enqueue(&s);
                        }
                    });
                }

                let (tx, rx) = std::sync::mpsc::channel();
                let q = &q;
                sc.spawn(move || {
                    let _ = tx.send(q.shutdown());
                });

                let result = rx.recv_timeout(Duration::from_secs(5));
                stop.store(true, Ordering::Release);
                match result {
                    Ok(r) => r.expect("shutdown"),
                    Err(_) => panic!("Shutdown is blocked"),
                }
            });
        }
    }
}

mod test_disconnect_queue_enqueue {
    use super::*;

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue/Adds_sessions_to_the_queue
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue
    #[test]
    fn adds_sessions_to_the_queue() {
        let node = new_mock_node();
        let q = new_queue(&node);

        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());
        assert_eq!(1, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue/After_shutdown
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue
    #[test]
    fn after_shutdown() {
        let node = new_mock_node();
        let q = new_queue(&node);
        let _ = q.shutdown();

        assert!(q.enqueue(&new_mock_session("1", q.node(), vec![])).is_ok());
        assert_eq!(0, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue/Discards_sessions_over_the_limit
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue
    #[test]
    fn discards_sessions_over_the_limit() {
        let node = new_mock_node();
        let mut config = new_disconnect_queue_config();
        config.max_backlog = 2;
        let q = new_disconnect_queue(&node, &config);

        assert!(q.enqueue(&new_mock_session("1", &node, vec![])).is_ok());
        assert!(q.enqueue(&new_mock_session("2", &node, vec![])).is_ok());
        assert!(q.enqueue(&new_mock_session("3", &node, vec![])).is_err());
        assert!(q.enqueue(&new_mock_session("4", &node, vec![])).is_err());

        assert_eq!(2, q.size());
        assert_eq!(2, node.counter_value(METRICS_DISCONNECT_DISCARDED));

        let item = q.rust_with("DisconnectQueue.disconnect is unexported").remove();
        let item = item.expect("a queued session");
        assert_eq!("1", item.get_id());

        assert!(q.enqueue(&new_mock_session("5", &node, vec![])).is_ok());
        assert_eq!(2, q.size());
    }

    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue/Does_not_limit_the_queue_when_max_backlog_is_zero
    /// upstream: node/disconnect_queue_test.go#TestDisconnectQueue_Enqueue
    #[test]
    fn does_not_limit_the_queue_when_max_backlog_is_zero() {
        let node = new_mock_node();
        let mut config = new_disconnect_queue_config();
        config.max_backlog = 0;
        let q = new_disconnect_queue(&node, &config);

        for i in 0..1000 {
            q.enqueue(&new_mock_session(&i.to_string(), &node, vec![])).expect("enqueue");
        }

        assert_eq!(1000, q.size());
        assert_eq!(0, node.counter_value(METRICS_DISCONNECT_DISCARDED));
    }
}

/// upstream: node/disconnect_queue_test.go#TestDisconnectQueueConfig_ToToml
#[test]
fn test_disconnect_queue_config_to_toml() {
    let mut conf = new_disconnect_queue_config();
    conf.rate = 50;
    conf.workers_num = 8;
    conf.max_backlog = 2048;

    let toml_str = disconnect_queue_config_to_toml(&conf);

    for expected in ["rate = 50", "workers_num = 8", "max_backlog = 2048"] {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }

    // Round-trip test
    let mut conf2 = new_disconnect_queue_config();

    disconnect_queue_config_merge_toml(&mut conf2, &toml_str).expect("decode");

    assert_eq!(conf, conf2);
}
