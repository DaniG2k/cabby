//! upstream file: etc/benchi/lib/throughput_test.go

use std::time::{Duration, SystemTime};

use cabby_testkit::reference::benchi::*;

fn in_delta(want: f64, got: f64, delta: f64, msg: &str) {
    assert!((want - got).abs() <= delta, "{msg}: {got} not within {delta} of {want}");
}

fn at(start: SystemTime, offset: Duration, n: i64) -> ReceiveSample {
    ReceiveSample { t: start + offset, n }
}

/// 1000 msg/s for 3s, sampled every 100ms.
fn constant_rate() -> Vec<ReceiveSample> {
    let start = unix_epoch();
    (0..=30).map(|i| at(start, Duration::from_millis(i * 100), (i * 100) as i64)).collect()
}

/// upstream: etc/benchi/lib/throughput_test.go#TestReceiveSampler_CapturesGrowingCounter
#[test]
fn test_receive_sampler_captures_growing_counter() {
    let n = Counter::new();
    let s = ReceiveSampler::new(Duration::from_millis(20), &n);
    s.start();
    // The sleeps are the workload (a counter that grows over ~125ms), not
    // synchronization.
    for i in 1..=5 {
        std::thread::sleep(Duration::from_millis(25));
        n.store(i * 10);
    }
    s.stop();

    let samples = s.samples();
    assert!(samples.len() >= 5, "should have at least 5 samples in ~125ms with 20ms tick, got {}", samples.len());
    assert_eq!(0, samples[0].n, "first sample taken at Start, before any increments");
    assert_eq!(50, samples[samples.len() - 1].n, "final sample should reflect last counter value");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestReceiveSampler_DoubleStopIsSafe
#[test]
fn test_receive_sampler_double_stop_is_safe() {
    let n = Counter::new();
    let s = ReceiveSampler::new(Duration::from_millis(20), &n);
    s.start();
    s.stop();
    // Upstream `assert.NotPanics`: a panic fails this test.
    s.stop();
}

/// upstream: etc/benchi/lib/throughput_test.go#TestOverall_ConstantRate
#[test]
fn test_overall_constant_rate() {
    let samples = constant_rate();
    in_delta(1000.0, overall(Some(&samples)), 1.0, "overall");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestOverall_EmptyOrSingle
#[test]
fn test_overall_empty_or_single() {
    assert_eq!(0.0, overall(None));
    assert_eq!(0.0, overall(Some(&[ReceiveSample { t: SystemTime::now(), n: 5 }])));
}

/// upstream: etc/benchi/lib/throughput_test.go#TestComputeWindowStats_ConstantRate
#[test]
fn test_compute_window_stats_constant_rate() {
    let samples = constant_rate();
    let stats = compute_window_stats(Some(&samples), Duration::from_secs(1));
    assert!(stats.valid);
    in_delta(1000.0, stats.min, 1.0, "min");
    in_delta(1000.0, stats.max, 1.0, "max");
    in_delta(1000.0, stats.p50, 1.0, "p50");
    in_delta(1000.0, stats.p95, 1.0, "p95");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestComputeWindowStats_BurstThenIdle
#[test]
fn test_compute_window_stats_burst_then_idle() {
    // 0..1s: 2000 msg/s; 1s..3s: 0 msg/s.
    let start = unix_epoch();
    let mut samples: Vec<ReceiveSample> =
        (0..=10).map(|i| at(start, Duration::from_millis(i * 100), (i * 200) as i64)).collect();
    samples.extend((11..=30).map(|i| at(start, Duration::from_millis(i * 100), 2000)));

    let stats = compute_window_stats(Some(&samples), Duration::from_secs(1));
    assert!(stats.valid);
    in_delta(2000.0, stats.max, 10.0, "max should catch the burst window");
    in_delta(0.0, stats.min, 10.0, "min should catch the idle window");
    // More idle windows than burst windows, so P50 should sit near 0.
    in_delta(0.0, stats.p50, 200.0, "p50 should be near idle (more idle windows than burst windows)");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestComputeWindowStats_PercentilesBetweenMinAndMax
#[test]
fn test_compute_window_stats_percentiles_between_min_and_max() {
    // Step rates 100, 200, ..., 1000 msg/s for 1s each, sampled every 100ms.
    let start = unix_epoch();
    let mut samples = Vec::new();
    let mut n: i64 = 0;
    for step in 1..=10u64 {
        let rate = (step * 100) as i64;
        for i in 0..10u64 {
            samples.push(at(start, Duration::from_millis(((step - 1) * 10 + i) * 100), n));
            n += rate / 10;
        }
    }
    samples.push(at(start, Duration::from_secs(10), n));

    let stats = compute_window_stats(Some(&samples), Duration::from_secs(1));
    assert!(stats.valid);
    assert!(stats.min <= stats.p50, "{stats:?}");
    assert!(stats.p50 <= stats.p95, "{stats:?}");
    assert!(stats.p95 <= stats.max, "{stats:?}");
    in_delta(1000.0, stats.max, 50.0, "max");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestComputeWindowStats_TooShort
#[test]
fn test_compute_window_stats_too_short() {
    let start = unix_epoch();
    let samples = [at(start, Duration::ZERO, 0), at(start, Duration::from_millis(100), 10)];
    let stats = compute_window_stats(Some(&samples), Duration::from_secs(1));
    assert!(!stats.valid, "sampling shorter than the window must report Valid=false");
}

/// upstream: etc/benchi/lib/throughput_test.go#TestComputeWindowStats_EmptyOrInvalid
#[test]
fn test_compute_window_stats_empty_or_invalid() {
    assert!(!compute_window_stats(None, Duration::from_secs(1)).valid);
    let now = SystemTime::now();
    assert!(!compute_window_stats(Some(&[ReceiveSample { t: now, n: 0 }]), Duration::from_secs(1)).valid);
    // Upstream calls time.Now() twice; the two samples are ~equal and the window is 0.
    let pair = [ReceiveSample { t: SystemTime::now(), n: 0 }, ReceiveSample { t: SystemTime::now(), n: 0 }];
    assert!(!compute_window_stats(Some(&pair), Duration::ZERO).valid);
}
