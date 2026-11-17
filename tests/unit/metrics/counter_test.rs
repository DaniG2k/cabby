//! upstream file: metrics/counter_test.go

use cabby_testkit::reference::metrics::Counter;

/// upstream: metrics/counter_test.go#TestCounter
#[test]
fn test_counter() {
    let cnt = Counter::new("test", "");
    assert_eq!(0, cnt.interval_value());
    for _ in 0..1000 {
        cnt.inc();
    }
    assert_eq!(1000, cnt.value());
    cnt.add(500);
    assert_eq!(1500, cnt.value());
    cnt.update_delta();
    cnt.inc();
    assert_eq!(1501, cnt.value());
    assert_eq!(1500, cnt.interval_value());
    cnt.update_delta();
    assert_eq!(1501, cnt.value());
    assert_eq!(1, cnt.interval_value());
    cnt.update_delta();
    assert_eq!(1501, cnt.value());
    assert_eq!(0, cnt.interval_value());
}
