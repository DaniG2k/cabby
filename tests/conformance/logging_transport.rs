//! Log lines of the pub/sub, broadcaster and broker layers (review finding H7,
//! `docs/review/2026-11-fable-review.md`): the same records as Go, with the same
//! levels, messages, attributes and context attributes. They run against both
//! servers; Go is the reference.
//!
//! Lines are compared without the timestamp and the `nodeid` attribute; error
//! texts that come from the client libraries (Go's vs Rust's) are not compared.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::raw_http;
use cabby_testkit::services::{self, NatsServer};
use cabby_testkit::sut::Sut;
use regex::Regex;
use serde_json::json;

const LOG_DEADLINE: Duration = Duration::from_secs(20);

/// The server's output lines without the timestamp and `nodeid=…`.
fn lines(sut: &Sut) -> Vec<String> {
    let nodeid = Regex::new(r" nodeid=\S+").expect("regex");
    let stamp = Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} ").expect("regex");
    sut.process.output().lines().map(|l| nodeid.replace(&stamp.replace(l, ""), "").into_owned()).collect()
}

/// Waits until a line starting with `prefix` is logged.
fn wait_line(sut: &Sut, prefix: &str) {
    let deadline = Instant::now() + LOG_DEADLINE;
    while Instant::now() < deadline {
        if lines(sut).iter().any(|l| l.starts_with(prefix)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("no line starting with {prefix:?} within {LOG_DEADLINE:?}\nSUT output:\n{}", sut.process.output());
}

fn assert_line(sut: &Sut, expected: &str) {
    wait_line(sut, expected);
    assert!(lines(sut).iter().any(|l| l == expected), "no line {expected:?}\nSUT output:\n{}", sut.process.output());
}

fn identifier(stream: &str) -> String {
    json!({"channel": "$pubsub", "stream_name": stream}).to_string()
}

fn connect(sut: &Sut) -> CableClient {
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), Options::default()).expect("connect");
    assert_eq!(Some("welcome"), c.receive().expect("welcome")["type"].as_str());
    c
}

fn subscribe(c: &mut CableClient, stream: &str) {
    c.send(&json!({"command": "subscribe", "identifier": identifier(stream)}));
    assert_eq!(Some("confirm_subscription"), c.receive().expect("confirmation")["type"].as_str());
}

fn unsubscribe(c: &mut CableClient, stream: &str) {
    c.send(&json!({"command": "unsubscribe", "identifier": identifier(stream)}));
}

fn broadcast(sut: &Sut, body: &serde_json::Value) {
    let body = body.to_string();
    let addr = SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let res = raw_http::request_until(
        addr,
        "POST",
        "/_broadcast",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
        Instant::now() + Duration::from_secs(5),
    )
    .expect("broadcast request");
    assert_eq!(201, res.status);
}

/// Publishes raw payloads with the NATS text protocol.
fn nats_publish(addr: SocketAddr, messages: &[(&str, &str)]) {
    let mut s = TcpStream::connect(addr).expect("connecting to NATS");
    s.set_read_timeout(Some(Duration::from_secs(5))).expect("timeout");
    let mut reader = BufReader::new(s.try_clone().expect("clone"));
    let mut line = String::new();
    reader.read_line(&mut line).expect("INFO");
    let mut out = String::from("CONNECT {\"verbose\":false}\r\n");
    for (subject, payload) in messages {
        out.push_str(&format!("PUB {subject} {}\r\n{payload}\r\n", payload.len()));
    }
    out.push_str("PING\r\n");
    s.write_all(out.as_bytes()).expect("publishing");
    loop {
        line.clear();
        reader.read_line(&mut line).expect("PONG");
        if line.starts_with("PONG") {
            return;
        }
    }
}

fn nats_addr(url: &str) -> SocketAddr {
    url.trim_start_matches("nats://").parse().expect("NATS address")
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Review finding H7: Cabby's Redis pub/sub and legacy broadcaster logged no per-message trace and logged the subscription confirmation before the `connected` line.
/// note: With --debug: subscribe, publish, receive, an unparseable payload, a legacy broadcast, unsubscribe and shutdown; every line is compared without timestamp and nodeid (the parse error text comes from the JSON decoder and is not compared).
#[test]
fn redis_pubsub_and_broadcaster_log_like_go() {
    let redis = services::redis().scope("logredis");
    let channel = redis.name("bcast");
    let stream = redis.name("stream");
    let mut sut = Sut::start(&[
        "--public",
        "--debug",
        "--broadcast_adapter=http,redis",
        "--pubsub=redis",
        &format!("--redis_url={}", redis.url()),
        &format!("--redis_channel={channel}"),
    ]);
    assert_line(&sut, "INF connected context=pubsub channel=__anycable_internal__");
    assert_line(&sut, &format!("INF subscribed to Redis channel context=broadcast provider=redis channel={channel}"));
    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    assert_line(&sut, &format!("DBG subscribing context=pubsub channel={stream}"));
    assert_line(&sut, &format!("DBG subscribe context=pubsub channel={stream}"));

    broadcast(&sut, &json!({"stream": stream, "data": "hi"}));
    assert_line(
        &sut,
        &format!("DBG publish message context=pubsub channel={stream} data.stream={stream} data.data=hi"),
    );
    assert_line(&sut, &format!("DBG received broadcast message context=pubsub channel={stream}"));

    redis.publish(&stream, "garbage").expect("PUBLISH");
    wait_line(&sut, "WRN failed to parse pubsub message context=pubsub data=garbage error=");

    redis.publish(&channel, &json!({"stream": stream, "data": "legacy"}).to_string()).expect("PUBLISH");
    assert_line(&sut, "DBG received pubsub message context=broadcast provider=redis");

    unsubscribe(&mut c, &stream);
    assert_line(&sut, &format!("DBG unsubscribing context=pubsub channel={stream}"));
    assert_line(&sut, &format!("DBG unsubscribe context=pubsub channel={stream}"));

    sut.process.stop(Duration::from_secs(10)).expect("stopping the server");
    for expected in [
        "DBG shutting down Redis pub/sub context=pubsub",
        "DBG close pub/sub channel context=pubsub",
        "DBG shutting down Redis pub/sub context=broadcast provider=redis",
        "DBG close pub/sub channel context=broadcast provider=redis",
    ] {
        assert!(
            lines(&sut).iter().any(|l| l == expected),
            "no line {expected:?}\nSUT output:\n{}",
            sut.process.output()
        );
    }
    // The confirmation of the primary channel is announced before its debug record (Go's hook order).
    let all = lines(&sut);
    let connected = all.iter().position(|l| l == "INF connected context=pubsub channel=__anycable_internal__");
    let confirmed = all.iter().position(|l| l == "DBG subscribe context=pubsub channel=__anycable_internal__");
    assert!(connected < confirmed, "SUT output:\n{}", sut.process.output());
}

/// regression-for: pubsub/nats_test.go#TestNATSCommon/Broadcast
/// note: Review finding H7: Cabby's NATS pub/sub and legacy NATS broadcaster logged nothing (start-up, per-message trace, unknown messages).
/// note: With --debug against a private nats-server: start-up, publish, receive, an unparseable payload, a batch (unknown to pub/sub) and a legacy broadcast.
#[test]
fn nats_pubsub_and_broadcaster_log_like_go() {
    let nats = NatsServer::start();
    let channel = services::unique("natsbcast");
    let stream = services::unique("natslog");
    let sut = Sut::start(&[
        "--public",
        "--debug",
        "--broadcast_adapter=http,nats",
        "--pubsub=nats",
        &format!("--nats_servers={}", nats.url()),
        &format!("--nats_channel={channel}"),
    ]);
    assert_line(&sut, &format!("INF Starting NATS pub/sub: {} context=pubsub", nats.url()));
    assert_line(&sut, &format!("INF subscribing for broadcasts context=broadcast provider=nats channel={channel}"));
    let mut c = connect(&sut);
    subscribe(&mut c, &stream);

    broadcast(&sut, &json!({"stream": stream, "data": "hi"}));
    assert_line(
        &sut,
        &format!("DBG publish message context=pubsub channel={stream} data.stream={stream} data.data=hi"),
    );
    assert_line(&sut, &format!("DBG received broadcast message context=pubsub channel={stream}"));

    let batch = format!("[{{\"stream\":\"{stream}\",\"data\":\"b\"}}]");
    let legacy = json!({"stream": stream, "data": "legacy"}).to_string();
    nats_publish(nats_addr(&nats.url()), &[(&stream, "garbage"), (&stream, &batch), (&channel, &legacy)]);
    wait_line(&sut, "WRN failed to parse pubsub message context=pubsub data=garbage error=");
    assert_line(
        &sut,
        &format!(
            "WRN received unknown message context=pubsub channel={stream} data=\"[{{\\\"stream\\\":\\\"{stream}\\\",\\\"data\\\":\\\"b\\\"}}]\""
        ),
    );
    assert_line(&sut, "DBG received pubsub message context=broadcast provider=nats");
}

/// regression-for: pubsub/nats_test.go#TestNATSReconnect
/// note: Review finding H7: Cabby logged nothing when a NATS connection was lost or restored (Go: `connection failed` and `connection restored` from nats.go's handlers, per component).
/// note: The private nats-server is stopped and started again on the same port; the `error` text is the client library's (Go `EOF`, Cabby `disconnected`) and is not compared.
#[test]
fn nats_reconnect_is_logged_per_component() {
    let mut nats = NatsServer::start();
    let sut = Sut::start(&[
        "--public",
        "--broadcast_adapter=http,nats",
        "--pubsub=nats",
        &format!("--nats_servers={}", nats.url()),
        &format!("--nats_channel={}", services::unique("natsreconnect")),
    ]);
    wait_line(&sut, "INF subscribing for broadcasts context=broadcast provider=nats");
    nats.shutdown();
    wait_line(&sut, "WRN connection failed context=pubsub error=");
    wait_line(&sut, "WRN connection failed context=broadcast provider=nats error=");
    nats.start_again();
    assert_line(&sut, &format!("INF connection restored context=pubsub url={}", nats.url()));
    assert_line(&sut, &format!("INF connection restored context=broadcast provider=nats url={}", nats.url()));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Review finding H7: Cabby's NATS broker did not log its JetStream consumers (Go: `created JetStream consumer` at debug).
/// note: Runs against the shared NATS service (JetStream enabled).
#[test]
fn nats_broker_logs_its_jetstream_consumers() {
    let url = services::nats().url.clone();
    let stream = services::unique("natsbroker");
    let sut = Sut::start(&[
        "--public",
        "--debug",
        "--broadcast_adapter=http",
        "--broker=nats",
        "--pubsub=nats",
        &format!("--nats_servers={url}"),
    ]);
    wait_line(&sut, "INF NATS broker is ready context=broker provider=nats epoch=");
    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    let consumer = Regex::new(&format!(
        r"^DBG created JetStream consumer context=broker provider=nats consumer=\S+ stream={}$",
        regex::escape(&stream)
    ))
    .expect("regex");
    let deadline = Instant::now() + LOG_DEADLINE;
    while !lines(&sut).iter().any(|l| consumer.is_match(l)) {
        assert!(Instant::now() < deadline, "no consumer line\nSUT output:\n{}", sut.process.output());
        std::thread::sleep(Duration::from_millis(50));
    }
}
