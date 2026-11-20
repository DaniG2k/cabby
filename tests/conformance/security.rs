//! Hardening found by the 2026-11 security review
//! (`docs/review/2026-11-security-review.md`), observed from outside. Other
//! findings are pinned next to the features they touch: `pusher.rs` (F2, D-055)
//! and `client_messages.rs` (F3, D-056).

use std::time::Duration;

use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::sut::Sut;
use serde_json::json;

/// `?pi=` values that Go survives: `1` and a day keep upstream's semantics
/// (pings every so many seconds); zero and negative values disable pings; the
/// others are absurd (Go wraps `val * time.Second` and gets a non-positive
/// interval, so no pings, while Cabby clamps them to a day).
const SURVIVED_BY_GO: &[&str] = &[
    "1",
    "86400",
    "86401",
    "0",
    "-1",
    "-9223372036854775808",
    "9223372036854775807",
    "9223372037",
    "3000000000000000000",
    "3074457345",
];

/// `?pi=` values whose wrapped `time.Duration` makes Go's first-ping jitter call
/// `rand.Int63n` with a non-positive argument: `panic: invalid argument to
/// Int63n` stops the Go process.
const CRASH_GO: &[&str] = &["6148914691236517206", "6148914691236517205", "9223372036"];

fn connect(sut: &Sut, pi: &str) -> Result<CableClient, String> {
    let opts = Options { ignore: vec!["ping".into()], qs: format!("pi={pi}"), ..Options::default() };
    CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).map_err(|e| format!("{e:?}"))
}

/// The session gets its welcome and serves a subscription.
fn assert_session_works(sut: &Sut, pi: &str) {
    let mut c = connect(sut, pi).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("pi={pi}: connect failed: {e}\nSUT output:\n{}", sut.process.output())
    });
    let welcome = c.receive().unwrap_or_else(|e| panic!("pi={pi}: no welcome: {e:?}\n{}", sut.process.output()));
    assert_eq!(Some("welcome"), welcome["type"].as_str(), "pi={pi}: {welcome}");
    let identifier = json!({"channel": "$pubsub", "stream_name": format!("pi-{pi}")}).to_string();
    c.send(&json!({"command": "subscribe", "identifier": identifier}));
    let ack = c.receive().unwrap_or_else(|e| panic!("pi={pi}: no subscription reply: {e:?}"));
    assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "pi={pi}: {ack}");
    c.close(true);
}

/// regression-for: features/ping.testfile#scenario
/// note: Deviation D-054: every `?pi=` value (any i64, zero, negative, huge) gives a working session on Cabby, with no panic in the server output; values over a day are clamped to a day. Go survives the values in SURVIVED_BY_GO but stops with `panic: invalid argument to Int63n` for those in CRASH_GO (wrapped `time.Duration` arithmetic in `scheduleInitialPing`), which the Go arm pins.
#[test]
fn deviation_d054_extreme_ping_intervals_keep_the_session_working() {
    let mut sut = Sut::start(&["--public", "--broadcast_adapter=http"]);
    for pi in SURVIVED_BY_GO {
        assert_session_works(&sut, pi);
    }
    match cabby_testkit::backend() {
        cabby_testkit::Backend::Rust => {
            for pi in CRASH_GO {
                assert_session_works(&sut, pi);
            }
            // Still serving, and nothing panicked on the way.
            assert_session_works(&sut, "3");
            let status = sut.process.stop(Duration::from_secs(10)).expect("stop");
            let output = sut.process.output();
            assert!(!output.contains("panicked"), "a panic in the server output ({status}):\n{output}");
        }
        cabby_testkit::Backend::Go => {
            // The first crashing value stops the process.
            let _ = connect(&sut, CRASH_GO[0]).map(|mut c| c.receive_within(Duration::from_secs(2)));
            let status = sut.process.wait_exit(Duration::from_secs(10)).expect("Go exits");
            let output = sut.process.output();
            assert!(!status.success(), "{status}\n{output}");
            assert!(output.contains("panic: invalid argument to Int63n"), "{output}");
        }
    }
}
