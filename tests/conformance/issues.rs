//! Scenarios pinning fixes for open upstream AnyCable issues (see
//! compat/deviations.md, "Proposed"). They run against both servers: a failure
//! on Go is the upstream defect, recorded as `upstream_failure`; Cabby must pass.
//!
//! Setup shared by all scenarios: the server uses Redis pub/sub for broadcasts
//! (`--pubsub=redis`), a fake RPC app that confirms subscriptions and streams
//! from the channel's `stream` parameter, and the HTTP broadcast endpoint.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use cabby_core::protos;
use cabby_testkit::cable_client::{CableClient, ClientError, Options};
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
use cabby_testkit::fault_proxy::FaultProxy;
use cabby_testkit::raw_http;
use cabby_testkit::services::{self, RedisScope};
use cabby_testkit::sut::Sut;
use serde_json::{Value, json};

/// The fake application: authenticate everyone, stream from the identifier's
/// `stream` parameter on subscribe, stop streams on unsubscribe.
fn fake_app() -> FakeRpc {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(protos::ConnectionResponse {
        status: protos::Status::Success as i32,
        identifiers: "user=1".into(),
        transmissions: vec![json!({"type": "welcome"}).to_string()],
        ..Default::default()
    });
    fake.on(Method::Command).reply(Reply::with(|call| {
        let cmd = call.request.command();
        let id: Value = serde_json::from_str(&cmd.identifier).unwrap_or(Value::Null);
        let stream = id.get("stream").and_then(Value::as_str).unwrap_or_default().to_owned();
        let resp = match cmd.command.as_str() {
            "subscribe" => protos::CommandResponse {
                status: protos::Status::Success as i32,
                streams: vec![stream],
                transmissions: vec![json!({"identifier": cmd.identifier, "type": "confirm_subscription"}).to_string()],
                ..Default::default()
            },
            "unsubscribe" => protos::CommandResponse {
                status: protos::Status::Success as i32,
                stop_streams: true,
                ..Default::default()
            },
            _ => protos::CommandResponse { status: protos::Status::Success as i32, ..Default::default() },
        };
        resp.into()
    }));
    fake.on(Method::Disconnect)
        .reply(protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() });
    fake
}

fn start_server(fake: &FakeRpc, redis_url: &str) -> Sut {
    Sut::start(&[
        &format!("--rpc_host={}", fake.grpc_target()),
        "--pubsub=redis",
        &format!("--redis_url={redis_url}"),
        "--broadcast_adapter=http",
    ])
}

fn identifier(stream: &str) -> String {
    json!({"channel": "IssueChannel", "stream": stream}).to_string()
}

fn connect(sut: &Sut) -> CableClient {
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), Options::default()).expect("connect");
    assert_eq!(Some("welcome"), c.receive().expect("welcome")["type"].as_str());
    c
}

fn subscribe(c: &mut CableClient, stream: &str) {
    c.send(&json!({"command": "subscribe", "identifier": identifier(stream)}));
    let ack = c.receive().expect("confirmation");
    assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
}

fn broadcast(sut: &Sut, stream: &str, text: &str) {
    let addr = SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let body = json!({"stream": stream, "data": json!({"text": text}).to_string()}).to_string();
    let r =
        raw_http::request(addr, "POST", "/_broadcast", &[("Content-Type", "application/json")], Some(body.as_bytes()))
            .expect("broadcast request");
    assert!((200..300).contains(&r.status), "broadcast rejected: {}", r.status);
}

/// Broadcasts `text` every 250ms until the client receives it or `within` passes.
fn delivered_within(sut: &Sut, c: &mut CableClient, stream: &str, text: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        broadcast(sut, stream, text);
        match c.receive_within(Duration::from_millis(250)) {
            Ok(m) if m["message"]["text"] == text => return true,
            Ok(_) | Err(ClientError::Timeout) => {}
            Err(e) => panic!("client error while waiting for delivery: {e}"),
        }
    }
    false
}

fn pubsub_channels(redis: &RedisScope, pattern: &str) -> Vec<String> {
    let r = redis.command(&["PUBSUB", "CHANNELS", pattern]).expect("PUBSUB CHANNELS");
    r.items().iter().filter_map(|i| i.as_text()).collect()
}

/// issue: anycable/anycable/issues/263
/// note: Redis memory grew without bound because pub/sub channels stayed subscribed after the last local subscriber left.
/// note: Cabby invariant: once no client is subscribed to a stream, the node holds no Redis subscription for it.
#[test]
fn redis_pubsub_channels_are_released_when_streams_have_no_subscribers() {
    let redis = services::redis().scope("issue263");
    let fake = fake_app();
    let sut = start_server(&fake, &redis.url());
    let streams: Vec<String> = (0..20).map(|i| redis.name(&format!("s{i}"))).collect();
    let pattern = format!("{}:*", redis.prefix);

    let mut c = connect(&sut);
    for s in &streams {
        subscribe(&mut c, s);
    }
    // Sanity: subscriptions really go through Redis (otherwise the check below is vacuous).
    let held = services::wait_until("Redis subscriptions for all streams", Duration::from_secs(10), || {
        let chans = pubsub_channels(&redis, &pattern);
        (chans.len() == streams.len()).then_some(chans)
    })
    .expect("streams subscribed in Redis");
    assert_eq!(streams.len(), held.len());

    // Churn: unsubscribe half explicitly, re-subscribe and drop the rest by disconnecting.
    for s in &streams[..10] {
        c.send(&json!({"command": "unsubscribe", "identifier": identifier(s)}));
    }
    for s in &streams[..10] {
        subscribe(&mut c, s);
        c.send(&json!({"command": "unsubscribe", "identifier": identifier(s)}));
    }
    c.close(true);

    let left = services::wait_until("Redis channels released", Duration::from_secs(10), || {
        let chans = pubsub_channels(&redis, &pattern);
        chans.is_empty().then_some(())
    });
    assert!(left.is_ok(), "leaked Redis subscriptions: {:?}", pubsub_channels(&redis, &pattern));
}

/// issue: anycable/anycable/issues/275
/// note: Symptom upstream: a node keeps its Redis connections but holds no pub/sub subscription (CLIENT LIST sub=0) and silently stops delivering broadcasts until restarted.
/// note: Reproduced deterministically: the fault proxy injects UNSUBSCRIBE for the stream on the node's subscriber connection.
/// note: Cabby invariant (proposed D-003): delivery resumes within 15s without a restart.
#[test]
fn delivery_recovers_after_redis_drops_the_stream_subscription() {
    let redis = services::redis().scope("issue275u");
    let proxy = FaultProxy::start(services::redis().addr());
    let fake = fake_app();
    let sut = start_server(&fake, &redis.url_via(proxy.addr()));
    let stream = redis.name("orders");

    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    assert!(proxy.await_subscriber(Duration::from_secs(10)), "no Redis subscriber connection seen");
    assert!(delivered_within(&sut, &mut c, &stream, "before", Duration::from_secs(10)), "baseline delivery failed");

    assert!(proxy.inject_command(&["UNSUBSCRIBE", &stream]) > 0, "nothing to inject into");
    services::wait_until("Redis drops the subscription", Duration::from_secs(5), || {
        (!pubsub_channels(&redis, &stream).contains(&stream)).then_some(())
    })
    .expect("subscription dropped");

    assert!(
        delivered_within(&sut, &mut c, &stream, "after", Duration::from_secs(15)),
        "broadcasts were not delivered within 15s after Redis dropped the subscription"
    );
}

/// issue: anycable/anycable/issues/275
/// note: Variant: the Redis side of the subscriber connection closes while the node's socket stays half-open (no FIN/RST), as after a silent network failure.
/// note: Cabby invariant (proposed D-003): the subscriber detects the dead connection, reconnects and resubscribes; delivery resumes within 30s.
#[test]
fn delivery_recovers_after_the_subscriber_connection_goes_half_open() {
    let redis = services::redis().scope("issue275h");
    let proxy = FaultProxy::start(services::redis().addr());
    let fake = fake_app();
    let sut = start_server(&fake, &redis.url_via(proxy.addr()));
    let stream = redis.name("orders");

    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    assert!(proxy.await_subscriber(Duration::from_secs(10)), "no Redis subscriber connection seen");
    assert!(delivered_within(&sut, &mut c, &stream, "before", Duration::from_secs(10)), "baseline delivery failed");

    assert!(proxy.sever_subscribers() > 0, "nothing to sever");
    assert!(
        delivered_within(&sut, &mut c, &stream, "after", Duration::from_secs(30)),
        "broadcasts were not delivered within 30s after the subscriber connection went half-open"
    );
}
