//! Client ping intervals (`?pi=`), observed from outside.

use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::sut::Sut;
use serde_json::json;

/// `?pi=` values: `1` and a day keep their meaning (pings every so many
/// seconds), larger values are a day, and zero and negative values are
/// ignored with a warning (the server's interval applies), as in AnyCable 1.6.22.
const PING_INTERVALS: &[&str] = &["1", "86400", "86401", "0", "-1"];

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
/// note: every `?pi=` value in PING_INTERVALS (in range, over a day, zero, negative) gives a working session; zero and negative values are logged as invalid.
#[test]
fn ping_intervals_in_and_out_of_range_give_working_sessions() {
    let mut sut = Sut::start(&["--public", "--broadcast_adapter=http"]);
    for pi in PING_INTERVALS {
        assert_session_works(&sut, pi);
    }
    for pi in ["0", "-1"] {
        let line = sut.process.wait_output(std::time::Duration::from_secs(5), |out| {
            out.lines()
                .find(|l| {
                    l.contains("invalid ping interval value, must be a positive integer")
                        && l.contains(&format!("val={pi}"))
                })
                .map(str::to_owned)
        });
        assert!(line.is_ok(), "pi={pi}: no warning\nSUT output:\n{}", sut.process.output());
    }
}
