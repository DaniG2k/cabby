//! upstream file: metrics/prometheus_test.go

use cabby_testkit::reference::metrics::*;
use regex::Regex;

/// upstream: metrics/prometheus_test.go#TestPrometheus
#[test]
fn test_prometheus() {
    let m = Metrics::new(10);

    m.register_counter("test_total", "Total number of smth");
    m.register_counter("any_total", "Total number of anything");
    m.register_gauge("tests", "Number of active smth");
    m.register_gauge("any_tests", "Number of active anything");

    m.gauge("tests").expect("gauge").set(123);
    m.counter("test_total").expect("counter").add(3);

    let actual = m.prometheus();

    assert!(actual.contains(
        "
# HELP anycable_go_test_total Total number of smth
# TYPE anycable_go_test_total counter
anycable_go_test_total 3
"
    ));

    assert!(actual.contains(
        "
# HELP anycable_go_any_total Total number of anything
# TYPE anycable_go_any_total counter
anycable_go_any_total 0
"
    ));

    assert!(actual.contains(
        "
# HELP anycable_go_tests Number of active smth
# TYPE anycable_go_tests gauge
anycable_go_tests 123
"
    ));

    assert!(actual.contains(
        "
# HELP anycable_go_any_tests Number of active anything
# TYPE anycable_go_any_tests gauge
anycable_go_any_tests 0
"
    ));
}

/// upstream: metrics/prometheus_test.go#TestPrometheusWithTags
#[test]
fn test_prometheus_with_tags() {
    let m = Metrics::new(10);
    m.default_tags(&[("env", "dev"), ("instance", "R2D2")]);

    m.register_counter("test_total", "Total number of smth");
    m.register_gauge("tests", "Number of active smth");

    m.gauge("tests").expect("gauge").set(123);
    m.counter("test_total").expect("counter").add(3);

    let actual = m.prometheus();

    let r = Regex::new(r"anycable_go_test_total\{(.+)\}\s+(\d+)").expect("regex");
    let matches = r.captures(&actual);

    let matches = matches.expect("a tagged anycable_go_test_total line");

    let tags_str = &matches[1];
    let val_str = &matches[2];

    let tags: Vec<&str> = tags_str.split(", ").collect();

    assert!(tags.contains(&r#"env="dev""#));
    assert!(tags.contains(&r#"instance="R2D2""#));

    assert_eq!("3", val_str);
}

/// upstream: metrics/prometheus_test.go#TestPrometheusHandler
#[test]
fn test_prometheus_handler() {
    let m = Metrics::new(10);

    m.register_counter("test_total", "Total number of smth");
    m.register_counter("any_total", "Total number of anything");

    m.counter("test_total").expect("counter").add(3);

    let (code, body) = m.prometheus_handler();

    assert_eq!(200, code);

    assert!(body.contains("anycable_go_test_total 3"));
    assert!(body.contains("anycable_go_any_total 0"));
}
