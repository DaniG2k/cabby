//! upstream file: etc/benchi/lib/publisher_test.go

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use cabby_testkit::http_stub::{HttpStub, Request, Response};
use cabby_testkit::reference::benchi::*;

/// Upstream `newSlowHandler`: answers 201 after `delay` (simulated HTTP backpressure;
/// the sleep is the workload, not synchronization).
fn slow_handler(delay: Duration) -> HttpStub {
    HttpStub::start(move |_| {
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        Response::status(201)
    })
}

/// Number of broadcasts in a JSON-array body (upstream: `json.Unmarshal` into
/// `[]map[string]any`; non-arrays count as zero).
fn broadcasts_in(req: &Request) -> i64 {
    serde_json::from_slice::<Vec<serde_json::Map<String, serde_json::Value>>>(&req.body).map_or(0, |a| a.len() as i64)
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_IssuedAndDroppedCounts
#[test]
fn test_publisher_issued_and_dropped_counts() {
    let srv = slow_handler(Duration::from_millis(100));

    // A single worker makes the queue-full path observable.
    let p = Publisher::new(&srv.url(), 2, PublisherOptions { workers: Some(1), batch_size: Some(1) });

    let start = Instant::now();
    for _ in 0..10 {
        p.publish("foo", "x");
    }
    let enqueue_elapsed = start.elapsed();
    assert!(
        enqueue_elapsed < Duration::from_millis(50),
        "enqueueing 10 should be fast, not gated on HTTP latency ({enqueue_elapsed:?})"
    );

    let issued = p.issued_count();
    let dropped = p.dropped_count();
    assert_eq!(10, issued + dropped, "issued+dropped must equal total Publish calls");
    assert!(dropped > 0, "expected at least one drop with maxInflight=2 and 100ms-slow handler");

    p.close();
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_CadenceNotGatedByHTTPLatency
#[test]
fn test_publisher_cadence_not_gated_by_http_latency() {
    let srv = slow_handler(Duration::from_millis(50));

    // maxInflight large enough that the queue never fills during the window.
    let p = Publisher::new(&srv.url(), 1024, PublisherOptions::default());

    // 100 Hz ticker for 200ms (upstream: time.NewTicker(10ms) until a 200ms deadline).
    ticker_until(Duration::from_millis(10), Duration::from_millis(200), || p.publish("foo", "x"));

    let issued = p.issued_count();
    assert!(issued >= 15, "expected ~20 ticks in 200ms; got {issued} (cadence may be gated on HTTP)");
    assert!(issued <= 25, "expected ~20 ticks in 200ms; got {issued} (clock drift?)");
    assert_eq!(0, p.dropped_count(), "queue should not have dropped any with maxInflight=1024");

    p.close();
}

/// Calls `f` on every tick of a `tick` ticker until `window` has passed, with
/// `time.Ticker` semantics: ticks fall on the fixed grid `start + k·tick`, one tick
/// missed while `f` runs is buffered and fires at once, further missed ticks are
/// dropped. So a slow `f` lowers the call count, as upstream's ticker loop would.
fn ticker_until(tick: Duration, window: Duration, mut f: impl FnMut()) {
    let start = Instant::now();
    let deadline = start + window;
    // First grid tick strictly after `t`.
    let tick_after = |t: Instant| {
        let k = (t - start).as_nanos() / tick.as_nanos() + 1;
        start + tick * u32::try_from(k).expect("tick count fits in u32")
    };
    let mut next = start + tick;
    loop {
        let now = Instant::now();
        if next > now {
            if next >= deadline {
                break;
            }
            std::thread::sleep(next - now);
        }
        // The tick is read now; ticks until then were the buffered one or dropped.
        let read_at = Instant::now();
        if read_at >= deadline {
            break;
        }
        f();
        next = tick_after(read_at);
    }
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_TargetCountsSnapshot
#[test]
fn test_publisher_target_counts_snapshot() {
    let srv = slow_handler(Duration::ZERO);

    let p = Publisher::new(&srv.url(), 16, PublisherOptions::default());
    for s in ["a", "b", "a", "c"] {
        p.publish(s, "x");
    }
    p.close();

    let counts = p.target_counts();
    let expected: HashMap<String, i64> = [("a".into(), 2), ("b".into(), 1), ("c".into(), 1)].into();
    assert_eq!(expected, counts);
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_ClosedThenPublishIsNoOp
#[test]
fn test_publisher_closed_then_publish_is_no_op() {
    let srv = slow_handler(Duration::ZERO);

    let p = Publisher::new(&srv.url(), 4, PublisherOptions::default());
    p.publish("a", "x");
    // Let the worker drain so the accounting after Close is deterministic.
    assert!(eventually(Duration::from_secs(1), Duration::from_millis(5), || p.issued_count() == 1));
    p.close();

    let issued_before = p.issued_count();
    let dropped_before = p.dropped_count();

    // Upstream `assert.NotPanics`: a panic fails this test.
    p.publish("a", "x");

    assert_eq!(issued_before, p.issued_count(), "Publish after Close must not increase issued");
    assert_eq!(dropped_before + 1, p.dropped_count(), "Publish after Close is counted as dropped");
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_EmptyStream
#[test]
fn test_publisher_empty_stream() {
    let srv = slow_handler(Duration::ZERO);

    let p = Publisher::new(&srv.url(), 4, PublisherOptions::default());

    // Upstream `assert.NotPanics`.
    p.publish("", "x");

    p.close();
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_HandlerReturns500_RecordsAsIssuedNotDropped
#[test]
fn test_publisher_handler_returns500_records_as_issued_not_dropped() {
    let broadcasts_seen = Arc::new(AtomicI64::new(0));
    let seen = Arc::clone(&broadcasts_seen);
    let srv = HttpStub::start(move |req| {
        seen.fetch_add(broadcasts_in(&req), Ordering::SeqCst);
        Response::status(500)
    });

    let p = Publisher::new(&srv.url(), 16, PublisherOptions::default());
    for _ in 0..5 {
        p.publish("a", "x");
    }
    p.close();

    assert_eq!(5, p.issued_count(), "issued counts all enqueued tasks, regardless of HTTP status");
    assert_eq!(0, p.dropped_count(), "non-2xx is not a drop — drops only count scheduler-fell-behind");
    assert_eq!(0, p.completed_count(), "non-2xx must not increment completed");
    assert_eq!(
        5,
        broadcasts_seen.load(Ordering::SeqCst),
        "server should have received all 5 broadcasts (possibly batched into fewer POSTs)"
    );
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_CompletedCountTracks2xx
#[test]
fn test_publisher_completed_count_tracks2xx() {
    let srv = slow_handler(Duration::ZERO);

    let p = Publisher::new(&srv.url(), 16, PublisherOptions::default());
    for _ in 0..7 {
        p.publish("a", "x");
    }
    p.close();

    assert_eq!(7, p.issued_count());
    assert_eq!(7, p.completed_count(), "all 2xx-responded broadcasts should be completed");
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_BatchingCoalescesUnderBackpressure
#[test]
fn test_publisher_batching_coalesces_under_backpressure() {
    // One slow worker; 20 tasks arrive faster than dispatch, so the worker packs
    // queued tasks into batches.
    let posts = Arc::new(AtomicI64::new(0));
    let broadcasts = Arc::new(AtomicI64::new(0));
    let (posts2, broadcasts2) = (Arc::clone(&posts), Arc::clone(&broadcasts));
    let srv = HttpStub::start(move |req| {
        posts2.fetch_add(1, Ordering::SeqCst);
        broadcasts2.fetch_add(broadcasts_in(&req), Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(50));
        Response::status(201)
    });

    let p = Publisher::new(&srv.url(), 1024, PublisherOptions { workers: Some(1), batch_size: Some(64) });
    for _ in 0..20 {
        p.publish("a", "x");
    }
    p.close();

    assert_eq!(20, p.issued_count());
    assert_eq!(20, broadcasts.load(Ordering::SeqCst), "server must still see every broadcast, just packed");
    assert_eq!(20, p.completed_count());
    assert!(posts.load(Ordering::SeqCst) < 20, "batching should yield fewer POSTs than broadcasts under backpressure");
}

/// upstream: etc/benchi/lib/publisher_test.go#TestPublisher_AgainstBuildServer
/// note: adaptation: upstream's "no goroutine leak" remark becomes: a second close from another thread returns (workers already joined).
#[test]
fn test_publisher_against_build_server() {
    let srv = build_server(ServerConfig::default()).expect("BuildServer");

    let p = Publisher::new(&srv.broadcast_url(), 4, PublisherOptions::default());

    p.publish("foo", "hello");

    // Without a subscriber, settle for "the publisher round-tripped without dropping
    // the task and the worker drained".
    assert!(eventually(Duration::from_secs(1), Duration::from_millis(5), || p.issued_count() == 1));

    p.close();

    assert_eq!(1, p.issued_count());
    assert_eq!(0, p.dropped_count());

    // A second Close from another thread is fine. Upstream: "keeps the integration
    // test from leaking goroutines"; the Rust equivalent is that the call returns
    // (the workers were joined by the first Close).
    std::thread::scope(|s| {
        s.spawn(|| p.close());
    });

    let _ = srv.shutdown();
}
