//! upstream file: metrics/gauge_test.go

use cabby_testkit::reference::metrics::Gauge;

/// upstream: metrics/gauge_test.go#TestGauge
#[test]
fn test_gauge() {
    let g = Gauge::new("test", "");
    assert_eq!(0, g.value());
    g.set(20);
    assert_eq!(20, g.value());
}

/// upstream: metrics/gauge_test.go#TestGaugeIncDec
/// note: the 20 Inc / 13 Dec goroutines run inside one facade call (Rust: scoped threads; Go: goroutines inside the bridge op), so the Go gauge sees real concurrency rather than serialized bridge calls.
#[test]
fn test_gauge_inc_dec() {
    let g = Gauge::new("test", "");

    g.inc_dec_concurrently(20, 13);

    assert_eq!(7, g.value());
}
