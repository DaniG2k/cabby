//! Pusher compatibility (`--pusher_app_id`, `--pusher_app_key`, `--pusher_secret`),
//! observed externally: the Pusher WebSocket protocol at `/app/<key>` and the REST
//! API at `/apps/<app_id>/...` on the main port. Signatures are computed client-side
//! (`cabby_testkit::pusher_auth`), as Pusher client libraries do.

use std::time::{Duration, Instant};

use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::pusher_auth::{channel_auth, get_channel_users, md5_hex, now_timestamp, signed_target};
use cabby_testkit::raw_http;
use cabby_testkit::sut::Sut;
use cabby_testkit::text_ws::TextWs;
use serde_json::json;

// The worked example from https://pusher.com/docs/channels/library_auth_reference/auth-signatures/
// (used by the upstream verifier and executor tests): key and secret.
const APP_ID: &str = "my-test-id";
const KEY: &str = "278d425bdf160c739803";
const SECRET: &str = "7ad3773142a6692b25b8";
const SIGNATURE: &str = "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";
const PRESENCE_SIGNATURE: &str =
    "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800";
const PRESENCE_DATA: &str = "{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}";

const FRAME_DEADLINE: Duration = Duration::from_secs(5);

fn start(extra: &[&str]) -> Sut {
    let app = format!("--pusher_app_id={APP_ID}");
    let key = format!("--pusher_app_key={KEY}");
    let secret = format!("--pusher_secret={SECRET}");
    let mut args = vec![app.as_str(), key.as_str(), secret.as_str(), "--broadcast_adapter=http"];
    args.extend_from_slice(extra);
    Sut::start(&args)
}

/// Connects with `X-Request-ID: <socket_id>` (a valid Pusher socket ID is kept).
fn connect(sut: &Sut, socket_id: &str) -> TextWs {
    TextWs::connect(sut.addr(), &format!("/app/{KEY}"), &[("X-Request-ID", socket_id)]).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("connecting failed: {e:#}\nSUT output:\n{}", sut.process.output())
    })
}

fn recv(ws: &mut TextWs) -> String {
    ws.recv_text(FRAME_DEADLINE).expect("frame")
}

fn subscribe(ws: &mut TextWs, data: serde_json::Value) {
    ws.send_text(&json!({"event": "pusher:subscribe", "data": data}).to_string()).expect("send");
}

fn api(sut: &Sut, method: &str, target: &str, body: &[u8]) -> raw_http::Response {
    let body = (!body.is_empty() || method == "POST").then_some(body);
    raw_http::request(sut.addr(), method, target, &[("Content-Type", "application/json")], body).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("REST request failed: {e:#}\nSUT output:\n{}", sut.process.output())
    })
}

/// external-for: pusher/encoder_test.go#TestPusherEncode/Welcome
/// external-for: pusher/encoder_test.go#TestPusherEncode/Confirmed
/// external-for: pusher/encoder_test.go#TestPusherEncode/Rejected
/// external-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(public)
/// external-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private)
/// external-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
/// external-for: pusher/controller_test.go#TestPusherController/Subscribe_public_(success)
/// external-for: pusher/controller_test.go#TestPusherController/Subscribe_private_(success)
#[test]
fn websocket_subscriptions() {
    let sut = start(&[]);
    let mut ws = connect(&sut, "1234.1234");

    assert_eq!(
        r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"1234.1234\",\"activity_timeout\":30}"}"#,
        recv(&mut ws)
    );

    subscribe(&mut ws, json!({"channel": "all-chat", "auth": ""}));
    assert_eq!(r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"all-chat"}"#, recv(&mut ws));

    subscribe(&mut ws, json!({"channel": "private-foobar", "auth": SIGNATURE}));
    assert_eq!(
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-foobar"}"#,
        recv(&mut ws)
    );

    let mut other = connect(&sut, "1234.4321");
    recv(&mut other);
    // The signature is for socket 1234.1234.
    subscribe(&mut other, json!({"channel": "private-foobar", "auth": SIGNATURE}));
    assert_eq!(r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#, recv(&mut other));

    subscribe(&mut other, json!({"channel": "private-foobar", "auth": "bla-bla"}));
    assert_eq!(r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#, recv(&mut other));
}

/// external-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(presence)
/// external-for: pusher/controller_test.go#TestPusherController/Subscribe_presence_(success)
#[test]
fn presence_subscription() {
    let sut = start(&["--broker=memory"]);
    let mut ws = connect(&sut, "1234.1234");
    recv(&mut ws);

    subscribe(
        &mut ws,
        json!({"channel": "presence-foobar", "auth": PRESENCE_SIGNATURE, "channel_data": PRESENCE_DATA}),
    );

    assert_eq!(
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":1,\"ids\":[\"10\"],\"hash\":{\"10\":{\"name\":\"Mr. Channels\"}}}}","channel":"presence-foobar"}"#,
        recv(&mut ws)
    );
}

fn events_payload() -> Vec<u8> {
    br#"{"name":"test-event","channel":"test-channel","data":"{\"message\":\"hello world\"}"}"#.to_vec()
}

fn events_path() -> String {
    format!("/apps/{APP_ID}/events")
}

/// external-for: pusher/http_test.go#TestHttpHandler/Handles_broadcasts
/// external-for: pusher/encoder_test.go#TestPusherEncode/Reply_with_event_and_data
#[test]
fn rest_api_event_reaches_subscribers() {
    let sut = start(&[]);
    let mut ws = connect(&sut, "1234.1234");
    recv(&mut ws);
    subscribe(&mut ws, json!({"channel": "test-channel", "auth": ""}));
    recv(&mut ws);

    let payload = events_payload();
    let target = signed_target("POST", &events_path(), KEY, SECRET, &now_timestamp(), Some(&payload));
    let res = api(&sut, "POST", &target, &payload);

    assert_eq!(200, res.status);
    assert_eq!("{}", res.text());
    assert_eq!(
        r#"{"event":"test-event","data":"{\"message\":\"hello world\"}","channel":"test-channel"}"#,
        recv(&mut ws)
    );
}

/// external-for: pusher/http_test.go#TestHttpHandler/Rejects_unsupported_methods
/// external-for: pusher/http_test.go#TestHttpHandler/Rejects_when_authorization_data_is_incorrect
/// external-for: pusher/http_test.go#TestHttpHandler/Rejects_when_auth_timestamp_is_too_far_from_current_time
/// external-for: pusher/http_test.go#TestHttpHandler/Rejects_when_body_does_not_match_signature
#[test]
fn rest_api_rejections() {
    let sut = start(&[]);
    let payload = events_payload();
    let path = events_path();

    assert_eq!(422, api(&sut, "DELETE", &path, &payload).status);

    let auth_params =
        format!("auth_key={KEY}&body_md5={}&auth_version=1.0&auth_timestamp={}", md5_hex(&payload), now_timestamp());
    let bad = format!("{path}?{auth_params}&auth_signature=qwewe123wqee1eqweqweqwe");
    assert_eq!(401, api(&sut, "POST", &bad, &payload).status);

    let stale = (now_timestamp().parse::<i64>().unwrap() - 15 * 60).to_string();
    let stale_target = signed_target("POST", &path, KEY, SECRET, &stale, Some(&payload));
    assert_eq!(401, api(&sut, "POST", &stale_target, &payload).status);

    let target = signed_target("POST", &path, KEY, SECRET, &now_timestamp(), Some(&payload));
    let another = br#"{"name":"test-event","channel":"test-channel","data":"{\"message\":\"hola!\"}"}"#;
    assert_eq!(401, api(&sut, "POST", &target, another).status);
}

fn users(sut: &Sut, channel: &str) -> Result<Vec<String>, String> {
    get_channel_users(APP_ID, KEY, SECRET, channel, |method, target| {
        let r = api(sut, method, target, b"");
        (r.status, r.body)
    })
}

/// external-for: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_presence_channel_with_users
/// external-for: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_empty_channel
/// external-for: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_unknown_channel
/// external-for: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_non-presence_channel_returns_400
#[test]
fn rest_api_channel_users() {
    let sut = start(&["--broker=memory"]);
    let channel = "presence-room-1";

    // Two members join over WebSocket (the upstream test adds them to the broker).
    let mut members = Vec::new();
    for (socket, user) in [("2025.1", "user-1"), ("2025.2", "user-2")] {
        let mut ws = connect(&sut, socket);
        recv(&mut ws);
        let channel_data = json!({"user_id": user, "user_info": {"name": user}}).to_string();
        let auth = channel_auth(KEY, SECRET, &format!("{socket}:{channel}:{channel_data}"));
        subscribe(&mut ws, json!({"channel": channel, "auth": auth, "channel_data": channel_data}));
        let confirmed = recv(&mut ws);
        assert!(confirmed.contains("pusher_internal:subscription_succeeded"), "{confirmed}");
        members.push(ws);
    }

    // Presence is recorded asynchronously after the confirmation: poll (bounded).
    let deadline = Instant::now() + Duration::from_secs(5);
    let ids = loop {
        let ids = users(&sut, channel).expect("no error");
        if ids.len() >= 2 || Instant::now() >= deadline {
            break ids;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(2, ids.len(), "{ids:?}");
    assert!(ids.contains(&"user-1".to_owned()));
    assert!(ids.contains(&"user-2".to_owned()));

    assert_eq!(0, users(&sut, "presence-empty-room").expect("no error").len());
    assert_eq!(0, users(&sut, "presence-unknown").expect("no error").len());

    let err = users(&sut, "private-room").expect_err("an error");
    assert!(err.contains("400"), "{err}");
}

/// A `subscribe` on the Action Cable endpoint; returns the reply type.
fn cable_subscribe(cable: &mut CableClient, identifier: &str) -> String {
    cable.send(&json!({"command": "subscribe", "identifier": identifier}));
    loop {
        let reply = cable.receive().unwrap_or_else(|e| panic!("no reply to {identifier}: {e:?}"));
        // On Go the bypassing subscriptions also get the channels' presence events.
        if reply["type"] == "presence" {
            continue;
        }
        assert_eq!(Some(identifier), reply["identifier"].as_str(), "{reply}");
        return reply["type"].as_str().unwrap_or_default().to_owned();
    }
}

/// regression-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
/// regression-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(presence)
/// note: Deviation D-055: with Pusher enabled, a `$pusher` identifier subscribed on /cable (no Pusher signature: the check lives in the Pusher endpoint's executor) is rejected by Cabby, for private, presence and public channels and for an escaped channel name; Go confirms them (the reviewed bypass, pinned by the Go arm). Signed private and presence subscriptions at /app/<key> still succeed on both, and a bad signature is still rejected.
#[test]
fn deviation_d055_pusher_channel_is_refused_outside_the_pusher_endpoint() {
    let sut = start(&["--noauth", "--norpc", "--broker=memory"]);

    let opts = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut cable = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    assert_eq!(Some("welcome"), cable.receive().expect("welcome")["type"].as_str());

    let expected = match cabby_testkit::backend() {
        cabby_testkit::Backend::Rust => "reject_subscription",
        cabby_testkit::Backend::Go => "confirm_subscription",
    };
    for identifier in [
        r#"{"channel":"$pusher","stream":"private-foobar"}"#,
        r#"{"channel":"$pusher","stream":"presence-foobar"}"#,
        r#"{"channel":"$pusher","stream":"all-chat"}"#,
        r#"{"channel":"$pusher","stream":"private-escaped"}"#,
    ] {
        assert_eq!(expected, cable_subscribe(&mut cable, identifier), "{identifier}");
    }

    // Legitimate Pusher subscriptions are unaffected.
    let mut ws = connect(&sut, "1234.1234");
    recv(&mut ws);
    subscribe(&mut ws, json!({"channel": "private-foobar", "auth": SIGNATURE}));
    assert_eq!(
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-foobar"}"#,
        recv(&mut ws)
    );
    subscribe(
        &mut ws,
        json!({"channel": "presence-foobar", "auth": PRESENCE_SIGNATURE, "channel_data": PRESENCE_DATA}),
    );
    let presence = recv(&mut ws);
    assert!(presence.starts_with(r#"{"event":"pusher_internal:subscription_succeeded""#), "{presence}");
    assert!(presence.contains(r#"\"10\":{\"name\":\"Mr. Channels\"}"#), "{presence}");
    subscribe(&mut ws, json!({"channel": "private-other", "auth": "bla-bla"}));
    let rejected = loop {
        let frame = recv(&mut ws);
        // The member's own join event may come first.
        if !frame.contains("pusher_internal:member_added") {
            break frame;
        }
    };
    assert_eq!(r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#, rejected);

    // `$pubsub` on /cable is unaffected (and needs a signature without `--public`).
    let pubsub = json!({"channel": "$pubsub", "stream_name": "private-foobar"}).to_string();
    assert_eq!("reject_subscription", cable_subscribe(&mut cable, &pubsub));
}
