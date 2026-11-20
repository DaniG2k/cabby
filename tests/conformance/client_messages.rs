//! Client messages nested deeply, observed from outside (review 2026-11, Low):
//! Go's `encoding/json` decodes documents nested up to 10000 levels and rejects
//! deeper ones, which ends the session's reader (upstream `Serve` returns on a
//! decode error). Cabby used to stop reading at serde_json's 128 levels.

use std::time::Duration;

use cabby_testkit::cable_client::{CableClient, ClientError, Options};
use cabby_testkit::sut::Sut;
use serde_json::json;

/// Go's `encoding/json` nesting limit.
const GO_MAX_DEPTH: usize = 10_000;

fn connect(sut: &Sut) -> CableClient {
    let opts = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    assert_eq!(Some("welcome"), c.receive().expect("welcome")["type"].as_str());
    c
}

/// A `message` command whose `data` makes the whole document `depth` levels deep
/// (the command object is the first level).
fn nested_message(depth: usize) -> String {
    let identifier = json!({"channel": "$pubsub", "stream_name": "deep"}).to_string();
    let inner = depth - 1;
    format!(
        r#"{{"command":"message","identifier":{},"data":{}{}}}"#,
        serde_json::to_string(&identifier).expect("identifier"),
        "[".repeat(inner),
        "]".repeat(inner)
    )
}

fn subscribe_public(c: &mut CableClient, stream: &str) {
    let identifier = json!({"channel": "$pubsub", "stream_name": stream}).to_string();
    c.send(&json!({"command": "subscribe", "identifier": identifier}));
}

/// external-for: encoders/json_test.go#TestJSONEncoder/.Decode
/// note: Messages nested 129 and 10000 levels deep are decoded and handled (the perform fails: its data is not a string); the session keeps reading, so a following $pubsub subscription is confirmed.
#[test]
fn messages_nested_up_to_go_limit_are_processed() {
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let mut c = connect(&sut);

    for depth in [129, 1000, GO_MAX_DEPTH] {
        c.send_raw(&nested_message(depth));
        let stream = format!("after-{depth}");
        subscribe_public(&mut c, &stream);
        let ack = c.receive().unwrap_or_else(|e| panic!("no reply after a message {depth} levels deep: {e}"));
        assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
        assert!(ack["identifier"].as_str().unwrap_or_default().contains(&stream), "unexpected reply {ack}");
    }
}

/// external-for: encoders/json_test.go#TestJSONEncoder/.Decode
/// note: A message nested 10001 levels deep fails to decode (Go: `exceeded max depth`), which ends the session's reader as any decode error does: a following subscription gets no reply within AnyT's 0.5s window.
#[test]
fn messages_nested_deeper_than_go_limit_stop_the_reader() {
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let mut c = connect(&sut);

    c.send_raw(&nested_message(GO_MAX_DEPTH + 1));
    subscribe_public(&mut c, "after");
    match c.receive_within(Duration::from_millis(500)) {
        Err(ClientError::Timeout) => {}
        other => panic!("expected no reply, got {other:?}"),
    }
}
