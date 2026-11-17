//! upstream file: metrics/gauge_test.go

use std::sync::Arc;

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
///
/// 20 concurrent increments and 13 concurrent decrements. (Against Go the
/// bridge serializes the calls, so only the Rust run exercises real contention.)
#[test]
fn test_gauge_inc_dec() {
    let g = Arc::new(Gauge::new("test", ""));

    let handles: Vec<_> = (0..20)
        .map(|_| {
            let g = Arc::clone(&g);
            std::thread::spawn(move || {
                g.inc();
            })
        })
        .chain((0..13).map(|_| {
            let g = Arc::clone(&g);
            std::thread::spawn(move || {
                g.dec();
            })
        }))
        .collect();

    for h in handles {
        h.join().unwrap_or_else(|e| std::panic::resume_unwind(e));
    }

    assert_eq!(7, g.value());
}
