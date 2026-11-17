//! upstream file: etc/benchi/lib/pool_test.go

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use cabby_testkit::reference::benchi::*;

fn server() -> Server {
    build_server(ServerConfig::default()).expect("BuildServer")
}

/// upstream: etc/benchi/lib/pool_test.go#TestBuildPool_AllSubscribeOk
#[test]
fn test_build_pool_all_subscribe_ok() {
    let srv = server();

    let start = Instant::now();
    let built = build_pool(PoolConfig {
        server_url: srv.websocket_url(),
        c: 10,
        s: 3,
        big_s: 20,
        tolerance: 0,
        seed: 42,
        ..Default::default()
    });
    assert_eq!(None, built.error);
    let pool = built.pool.expect("pool");

    assert!(start.elapsed() < Duration::from_secs(1), "c=10 happy-path setup must complete in <1s");
    assert_eq!(10, pool.size());

    for (i, streams) in pool.streams().iter().enumerate() {
        assert_eq!(3, streams.len(), "client {i} has wrong subscription count");
        let mut seen = HashSet::new();
        for s in streams {
            let n: i64 = s.parse().unwrap_or_else(|_| panic!("stream name must be a 1..S integer, got {s:?}"));
            assert!(n >= 1);
            assert!(n <= 20);
            assert!(seen.insert(s.clone()), "client {i} has duplicate {s:?} in subset");
        }
    }

    pool.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/pool_test.go#TestBuildPool_AccumulatorIntegration
#[test]
fn test_build_pool_accumulator_integration() {
    let srv = server();

    // Hand-picked subsets: clients 0, 4, 7 subscribe to "target"; everyone else to a
    // unique singleton stream.
    let target = "target";
    let subscribers: HashSet<&str> = ["0", "4", "7"].into();
    let streams: Vec<Vec<String>> = (0..10)
        .map(|i| {
            let id = i.to_string();
            if subscribers.contains(id.as_str()) { vec![target.to_owned()] } else { vec![format!("solo-{id}")] }
        })
        .collect();

    let acc = Accumulator::new();
    let built = build_pool(PoolConfig {
        server_url: srv.websocket_url(),
        c: 10,
        s: 1,
        big_s: 1,
        streams: Some(streams),
        accumulator: Some(&acc),
        ..Default::default()
    });
    assert_eq!(None, built.error);
    let pool = built.pool.expect("pool");

    let publisher = Publisher::new(&srv.broadcast_url(), 16, PublisherOptions::default());
    for _ in 0..5 {
        publisher.publish(target, "ping");
    }

    let all_five = |snap: &HashMap<String, i64>| subscribers.iter().all(|id| snap.get(*id) == Some(&5));
    assert!(
        eventually(Duration::from_secs(3), Duration::from_millis(25), || all_five(&acc.snapshot())),
        "subscribers should each accumulate 5 messages"
    );

    let snap = acc.snapshot();
    for id in &subscribers {
        assert_eq!(Some(&5), snap.get(*id), "client {id} should receive 5");
    }
    for i in 0..10 {
        let id = i.to_string();
        if subscribers.contains(id.as_str()) {
            continue;
        }
        assert_eq!(0, snap.get(&id).copied().unwrap_or(0), "client {id} should receive 0");
    }

    publisher.close();
    pool.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/pool_test.go#TestBuildPool_StreamSubsetUniformity_Smoke
#[test]
fn test_build_pool_stream_subset_uniformity_smoke() {
    const C: usize = 1000;
    const S: usize = 10;
    const BIG_S: usize = 100;
    let subsets = build_stream_subsets(0xDECAFC0FFEE, C, S, BIG_S);
    assert_eq!(C, subsets.len());

    let mut stream_hits: HashMap<String, i64> = HashMap::new();
    for sub in &subsets {
        assert_eq!(S, sub.len());
        let mut seen = HashSet::new();
        for name in sub {
            assert!(seen.insert(name.clone()), "duplicate {name:?} within a single subset");
            *stream_hits.entry(name.clone()).or_default() += 1;
        }
    }

    // Expected per-stream count is c*s/bigS = 100; the count is roughly
    // Binomial(c, s/bigS) with stdev ≈ 9.5. The tolerance absorbs 3-4σ outliers but
    // still flags degenerate distributions.
    const EXPECTED: i64 = (C * S / BIG_S) as i64;
    const TOLERANCE: i64 = 60;
    for i in 1..=BIG_S {
        let name = i.to_string();
        let hits = stream_hits.get(&name).copied().unwrap_or(0);
        assert!(
            (EXPECTED - hits).abs() <= TOLERANCE,
            "stream {name:?} appeared in {hits} clients; expected ~{EXPECTED} (±{TOLERANCE})"
        );
    }
}

/// upstream: etc/benchi/lib/pool_test.go#TestBuildPool_SetupFailureUnderTolerance
#[test]
fn test_build_pool_setup_failure_under_tolerance() {
    let srv = server();

    // One client subscribes to an empty stream name, which the server rejects; the
    // other nine each pick a valid singleton stream.
    let mut streams: Vec<Vec<String>> = (0..10).map(|i| vec![format!("s-{i}")]).collect();
    streams[5] = vec![String::new()];

    let built = build_pool(PoolConfig {
        server_url: srv.websocket_url(),
        c: 10,
        s: 1,
        big_s: 1,
        tolerance: 1,
        streams: Some(streams),
        ..Default::default()
    });
    assert_eq!(None, built.error);
    let pool = built.pool.expect("pool");

    assert_eq!(9, pool.size(), "one failure tolerated; nine clients should remain");

    pool.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/pool_test.go#TestBuildPool_SetupFailureOverTolerance
#[test]
fn test_build_pool_setup_failure_over_tolerance() {
    let srv = server();

    let mut streams: Vec<Vec<String>> = (0..5).map(|i| vec![format!("s-{i}")]).collect();
    streams[2] = vec![String::new()];

    let built = build_pool(PoolConfig {
        server_url: srv.websocket_url(),
        c: 5,
        s: 1,
        big_s: 1,
        tolerance: 0,
        streams: Some(streams),
        ..Default::default()
    });
    let err = built.error.expect("BuildPool must fail");
    assert!(built.pool.is_none());
    assert!(err.contains("client 2"), "error must name the failed client: {err}");
    assert!(err.contains("subscribe"), "error must indicate the failing phase: {err}");

    let _ = srv.shutdown();
}
