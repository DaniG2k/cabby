//! upstream file: etc/benchi/lib/client_test.go

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cabby_testkit::http_stub::StalledCable;
use cabby_testkit::reference::benchi::*;

/// Upstream `receiveWithin`: one message with a deadline, so a wedged receive fails the
/// test instead of hanging the suite.
pub fn receive_within(c: &Client, d: Duration) -> (Message, bool) {
    match c.receive_within(d) {
        Received::Message(m) => (m, true),
        Received::Closed => (Message::default(), false),
        Received::TimedOut => panic!("Receive did not return within {d:?}"),
    }
}

fn server() -> Server {
    build_server(ServerConfig::default()).expect("BuildServer")
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_ConnectSubscribeReceive
#[test]
fn test_client_connect_subscribe_receive() {
    let srv = server();

    let cli = build_client(&srv.websocket_url()).expect("BuildClient");

    cli.connect(None).expect("Connect");
    cli.subscribe(&["foo"]).expect("Subscribe");

    let publisher = Publisher::new(&srv.broadcast_url(), 4, PublisherOptions::default());
    publisher.publish("foo", "hello");

    let (msg, ok) = receive_within(&cli, Duration::from_secs(2));
    assert!(ok);
    assert_eq!("foo", msg.stream);
    assert_eq!("hello", msg.data);

    publisher.close();
    cli.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_MultiStreamSubscribe
#[test]
fn test_client_multi_stream_subscribe() {
    let srv = server();

    let cli = build_client(&srv.websocket_url()).expect("BuildClient");
    cli.connect(None).expect("Connect");

    let streams = ["a", "b", "c", "d", "e"];
    cli.subscribe(&streams).expect("Subscribe");

    let publisher = Publisher::new(&srv.broadcast_url(), 16, PublisherOptions::default());
    for s in streams {
        publisher.publish(s, &format!("{s}-payload"));
    }

    let mut got = HashMap::new();
    for _ in streams {
        let (msg, ok) = receive_within(&cli, Duration::from_secs(2));
        assert!(ok);
        got.insert(msg.stream, msg.data);
    }
    for s in streams {
        assert_eq!(Some(&format!("{s}-payload")), got.get(s), "missing message on stream {s:?}");
    }

    publisher.close();
    cli.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_SubscribeIdempotency
#[test]
fn test_client_subscribe_idempotency() {
    let srv = server();

    let cli = build_client(&srv.websocket_url()).expect("BuildClient");
    cli.connect(None).expect("Connect");
    cli.subscribe(&["foo"]).expect("Subscribe");
    assert_eq!(Ok(()), cli.subscribe(&["foo"]), "second Subscribe must not error");

    let publisher = Publisher::new(&srv.broadcast_url(), 4, PublisherOptions::default());
    publisher.publish("foo", "hello");

    let (msg, ok) = receive_within(&cli, Duration::from_secs(2));
    assert!(ok);
    assert_eq!("foo", msg.stream);
    assert_eq!("hello", msg.data);

    // No duplicate delivery: a second receive within a short window must time out,
    // proving messages are delivered once per publish.
    match cli.receive_within(Duration::from_millis(200)) {
        Received::TimedOut => {}
        other => panic!("unexpected second message: {other:?}"),
    }

    publisher.close();
    cli.close();
    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_CloseStopsReceive
#[test]
fn test_client_close_stops_receive() {
    let srv = server();

    let cli = build_client(&srv.websocket_url()).expect("BuildClient");
    cli.connect(None).expect("Connect");
    cli.subscribe(&["foo"]).expect("Subscribe");

    std::thread::scope(|scope| {
        let receiver = scope.spawn(|| cli.receive_within(Duration::from_secs(2)));

        cli.close();

        match receiver.join().expect("receiver thread") {
            Received::Closed => {
                // Upstream: ok == false and msg == Message{}; `Closed` carries no message.
            }
            Received::Message(m) => panic!("Receive after Close must return ok=false, got {m:?}"),
            Received::TimedOut => panic!("Receive did not unblock after Close"),
        }
    });

    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_ConnectToInvalidURL
#[test]
fn test_client_connect_to_invalid_url() {
    // Port 1 is reserved on most systems and almost never listens; even if a rogue
    // process bound to it, the WS upgrade would still fail and Connect would error.
    let cli = build_client("ws://127.0.0.1:1/cable").expect("BuildClient");

    let conn = cli.connect(Some(Duration::from_secs(3)));
    assert!(conn.is_err());

    cli.close();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_SubscribeTimeout
#[test]
fn test_client_subscribe_timeout() {
    // Stalled mock: accepts the WS upgrade, sends a welcome, then ignores every
    // incoming frame, so the subscription is never confirmed.
    let mock = StalledCable::start();

    let cli = build_client(&mock.url()).expect("BuildClient");
    cli.connect(None).expect("Connect");

    // Subscribe uses the client's default 5s confirmation timeout.
    let start = Instant::now();
    let sub = cli.subscribe(&["never-confirmed"]);
    let elapsed = start.elapsed();

    let err = sub.expect_err("Subscribe must time out");
    assert!(err.contains("never-confirmed"), "error must name the offending stream: {err}");
    assert!(elapsed < Duration::from_secs(7), "Subscribe took {elapsed:?}; expected to time out near the 5s default");
    assert!(elapsed >= Duration::from_secs(4), "Subscribe returned in {elapsed:?}; expected to wait near 5s");

    // Sanity: the mock server accepted at least one upgrade.
    assert!(mock.accepted() >= 1, "mock server never received a connection");

    cli.close();
}

/// upstream: etc/benchi/lib/client_test.go#TestClient_BuildClient_RejectsEmptyURL
#[test]
fn test_client_build_client_rejects_empty_url() {
    assert!(build_client("").is_err());
}

// Upstream also has a compile-time assertion that `Close` is safe to call from several
// goroutines (a `var _ = func() bool {...}`, not a test case). Rust equivalent: the
// harness client is `Sync`, so `close` can race with itself and with `receive`.
const _: fn() = || {
    fn sync<T: Sync + Send>() {}
    sync::<cabby_bench::client::Client>();
};
