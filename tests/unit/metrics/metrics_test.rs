//! upstream file: metrics/metrics_test.go

use std::time::Duration;

use cabby_testkit::reference::metrics::*;

/// upstream: metrics/metrics_test.go#TestMetricsSnapshot
/// note: rotate interval 1s instead of upstream's 10 (no assertion depends on it): on Go, rotate() is driven through the exported Run loop with a bridge writer that blocks in Write after each rotation, so each rotate() costs one interval of wall time.
#[test]
fn test_metrics_snapshot() {
    let m = Metrics::new(1);

    m.register_counter("test_count", "");
    m.register_gauge("test_gauge", "");

    for _ in 0..1000 {
        m.counter("test_count").expect("counter").inc();
    }

    m.gauge("test_gauge").expect("gauge").set(123);

    m.rotate();

    assert_eq!(Some(&1000), m.interval_snapshot().get("test_count"));
    assert_eq!(Some(&123), m.interval_snapshot().get("test_gauge"));

    m.counter("test_count").expect("counter").inc();

    m.rotate();

    assert_eq!(Some(&1), m.interval_snapshot().get("test_count"));
    assert_eq!(Some(&123), m.interval_snapshot().get("test_gauge"));
}

/// upstream: metrics/metrics_test.go#TestMetrics_EachGauge
#[test]
fn test_metrics_each_gauge() {
    let m = Metrics::new(10);

    m.register_gauge("test_gauge", "First");
    m.register_gauge("test_gauge_2", "Second");

    m.gauge("test_gauge").expect("gauge").set(123);
    m.gauge("test_gauge_2").expect("gauge").set(321);

    m.each_gauge(|gauge| match gauge.name.as_str() {
        "test_gauge" => assert_eq!(123, gauge.value),
        "test_gauge_2" => assert_eq!(321, gauge.value),
        other => panic!("Unknown gauge: {other}"),
    });
}

/// upstream: metrics/metrics_test.go#TestMetrics_EachCounter
#[test]
fn test_metrics_each_counter() {
    let m = Metrics::new(10);

    m.register_counter("test_counter", "First");
    m.register_counter("test_counter_2", "Second");

    m.counter("test_counter").expect("counter").inc();
    m.counter("test_counter_2").expect("counter").add(3);

    m.each_counter(|counter| match counter.name.as_str() {
        "test_counter" => assert_eq!(1, counter.value),
        "test_counter_2" => assert_eq!(3, counter.value),
        other => panic!("Unknown counter: {other}"),
    });
}

/// upstream: metrics/metrics_test.go#TestRunWithDedicatedServerStartsRotation
///
/// Regression test: with a dedicated metrics HTTP server configured, Run() must
/// still reach the interval writers / rotation loop. Upstream assigns the
/// unexported `m.server` a `server.NewServer("127.0.0.1", "0", ...)`; here the
/// same server comes from the exported `NewFromConfig` (HTTP path set, host
/// different from the main server's), and the writer double is added with
/// `RegisterWriter`.
#[test]
fn test_run_with_dedicated_server_starts_rotation() {
    let mut config = new_config();
    config.http = "/metrics".into();
    config.host = "127.0.0.1".into();
    config.port = 0;
    config.rotate_interval = 1;
    let m = Metrics::from_config(&config).expect("metrics");
    let w = m.register_signal_writer();

    let run = m.spawn_run();

    // rotation started — the server did not block Run
    let started = w.await_run(Duration::from_secs(2));
    m.shutdown().expect("shutdown");
    let result = run.wait();
    assert!(started, "rotation loop never started: the metrics server start is blocking Run (Run returned {result:?})");
}
