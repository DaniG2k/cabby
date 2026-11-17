//! upstream file: telemetry/telemetry_test.go
//!
//! The upstream test is split in two Rust cases mapped to the same upstream
//! case: the boot event (exported API, verified against Go) and the full
//! sequence including the usage event, which upstream drives through the
//! unexported `observeUsage` / `collectUsage`.
//!
//! The environment upstream sets with `t.Setenv` is passed explicitly, and the
//! endpoint is always a local recorder: no test reaches the real telemetry
//! service (the facade refuses other endpoints).

use std::time::Duration;

use cabby_testkit::fake_http::HttpRecorder;
use cabby_testkit::reference::metrics::{self, Metrics};
use cabby_testkit::reference::telemetry::*;
use serde_json::Value;

const DEADLINE: Duration = Duration::from_secs(10);

/// t.Setenv("AWS_EXECUTION_ENV", "AWS_ECS_FARGATE")
const ENV: &[(&str, &str)] = &[("AWS_EXECUTION_ENV", "AWS_ECS_FARGATE")];

struct Setup {
    metrics: Metrics,
    ts: HttpRecorder,
    tracker: Tracker,
}

fn setup() -> Setup {
    let mconfig = metrics::new_config();
    let metrics = Metrics::from_config(&mconfig).expect("metrics");

    metrics.register_gauge("clients_num", "");
    metrics.register_gauge("mem_sys_bytes", "");
    metrics.gauge_set("clients_num", 10);
    metrics.gauge_set("mem_sys_bytes", 100);

    // Records each event; answers 200.
    let ts = HttpRecorder::start();

    let mut tconf = new_config(ENV);
    tconf.token = "secret".into();
    tconf.endpoint = ts.url();
    tconf.custom_props.insert("distro".into(), "testo".into());

    let tracker = Tracker::new(&metrics, &tconf, ENV);
    Setup { metrics, ts, tracker }
}

fn captured(ts: &HttpRecorder) -> Vec<Value> {
    ts.requests().iter().map(|r| r.body_json()).collect()
}

fn assert_boot_event(s: &Setup) {
    let captured = captured(&s.ts);
    assert_eq!(1, captured.len());
    let event = &captured[0];

    assert_eq!("boot", event["event"]);
    assert_eq!(Value::String(version()), event["version"]);
    assert_eq!("testo", event["distro"]);
}

mod test_tracking {
    use super::*;

    /// upstream: telemetry/telemetry_test.go#TestTracking
    /// (first half: the boot event sent by `Collect`)
    /// note: the upstream test is split: this case (boot event, exported API) runs against Go; usage_event covers the rest.
    #[test]
    fn boot_event() {
        let s = setup();

        s.tracker.collect();

        assert_boot_event(&s);

        assert_eq!(Ok(()), s.tracker.shutdown());
    }

    /// upstream: telemetry/telemetry_test.go#TestTracking
    /// (whole test, including the usage event)
    /// note: not_observable on Go: the usage half calls the unexported observeUsage/collectUsage and depends on the unexported usage-monitor goroutine (otherwise the usage event fires only after 30 minutes); the boot half runs against Go in boot_event.
    #[test]
    fn usage_event() {
        let s = setup();

        s.tracker.collect();

        assert_boot_event(&s);

        // Upstream sleeps 100ms so the usage monitor started by Collect records
        // its first observation (clients 10, memory 100); here we wait for it.
        assert!(s.tracker.wait_for_observations(1, DEADLINE), "the usage monitor made no observation");

        s.metrics.gauge_set("clients_num", 14);
        s.metrics.gauge_set("mem_sys_bytes", 90);

        s.tracker.observe_usage();
        s.tracker.collect_usage();

        let captured = captured(&s.ts);
        assert_eq!(2, captured.len());
        let event = &captured[1];

        assert_eq!("usage", event["event"]);
        assert_eq!("ecs-fargate", event["deploy"]);
        assert_eq!(Some(14), event["clients_max"].as_f64().map(|v| v as i64));
        assert_eq!(Some(100), event["mem_sys_max"].as_f64().map(|v| v as i64));
        assert_eq!("testo", event["distro"]);

        assert_eq!(Ok(()), s.tracker.shutdown());
    }
}
