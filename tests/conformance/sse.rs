//! Server-sent events endpoint (`--sse`), observed externally. Counterparts of the
//! `sse/handler_test.go` cases, which drive the handler with an RPC mock; here the
//! application side is the server's own: `--noauth` (or JWT) authentication,
//! public/signed `$pubsub` streams and Turbo Streams, and HTTP broadcasts. Expected
//! events are therefore the server's own welcome/confirmation messages.

use std::time::Duration;

use base64::Engine;
use cabby_testkit::pusher_auth::hmac_sha256_hex;
use cabby_testkit::raw_http::{self, EventStream};
use cabby_testkit::sut::Sut;

const EVENT_DEADLINE: Duration = Duration::from_secs(2);

fn start(extra: &[&str]) -> Sut {
    let mut args = vec!["--sse", "--broadcast_adapter=http"];
    args.extend_from_slice(extra);
    let mut sut = Sut::start(&args);
    let broadcast_addr = std::net::SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    sut.process.wait_listening(broadcast_addr, Duration::from_secs(10)).unwrap_or_else(|e| panic!("{e:#}"));
    sut
}

fn open(sut: &Sut, method: &str, target: &str, headers: &[(&str, &str)], body: Option<&[u8]>) -> EventStream {
    EventStream::open(sut.addr(), method, target, headers, body).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("SSE request failed: {e:#}\nSUT output:\n{}", sut.process.output())
    })
}

fn request(sut: &Sut, method: &str, target: &str, headers: &[(&str, &str)]) -> raw_http::Response {
    raw_http::request(sut.addr(), method, target, headers, None).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("request failed: {e:#}\nSUT output:\n{}", sut.process.output())
    })
}

fn next(stream: &mut EventStream) -> String {
    stream.next_event(EVENT_DEADLINE).expect("event").expect("stream still open")
}

/// HTTP broadcast (`POST /_broadcast` on the broadcast port, no key configured).
fn broadcast(sut: &Sut, stream: &str, data: &str) {
    let body = serde_json::json!({"stream": stream, "data": data}).to_string();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let res =
        raw_http::request(addr, "POST", "/_broadcast", &[("Content-Type", "application/json")], Some(body.as_bytes()))
            .expect("broadcast request");
    assert!((200..300).contains(&res.status), "broadcast returned {}", res.status);
}

fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// A signed stream name as Rails/AnyCable sign it (`utils.MessageVerifier.Generate`):
/// `base64(json(stream)) + "--" + hex(hmac_sha256(secret, base64))`.
fn signed_stream(secret: &str, stream: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::json!(stream).to_string());
    let sig = hmac_sha256_hex(secret, &encoded);
    format!("{encoded}--{sig}")
}

fn assert_welcome(event: &str) {
    assert!(event.starts_with("event: welcome\ndata: {\"type\":\"welcome\""), "{event}");
}

fn assert_confirmed(event: &str, identifier: &str) {
    let expected = format!(
        "event: confirm_subscription\ndata: {}",
        serde_json::json!({"type": "confirm_subscription", "identifier": identifier})
    );
    // Go field order is type, identifier; serde_json sorts keys, so compare parsed.
    let (head, data) = event.split_once("\ndata: ").expect("event with data");
    let (ehead, edata) = expected.split_once("\ndata: ").unwrap();
    assert_eq!(ehead, head, "{event}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(edata).unwrap(),
        serde_json::from_str::<serde_json::Value>(data).expect("JSON data"),
        "{event}"
    );
}

/// external-for: sse/handler_test.go#TestSSEHandler/headers
/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_without_channel_or_identifier
#[test]
fn headers_and_missing_channel() {
    let sut = start(&["--noauth", "--public_streams"]);

    // A keep-alive request: servers answer `Connection: close` to a client that asked
    // to close.
    let res = request(&sut, "GET", "/events", &[("Connection", "keep-alive")]);

    assert_eq!(400, res.status);
    assert!(res.body.is_empty());
    assert_eq!(Some("text/event-stream; charset=utf-8"), res.header("Content-Type"));
    assert_eq!(Some("private, no-cache, no-store, must-revalidate, max-age=0"), res.header("Cache-Control"));
    assert_eq!(Some("no-cache"), res.header("Pragma"));
    assert_eq!(Some("keep-alive"), res.header("Connection"));
}

/// external-for: sse/handler_test.go#TestSSEHandler/OPTIONS
/// external-for: sse/handler_test.go#TestSSEHandler/non-GET/OPTIONS/POST
#[test]
fn preflight_and_unsupported_methods() {
    let sut = start(&["--noauth", "--public_streams"]);

    assert_eq!(200, request(&sut, "OPTIONS", "/events", &[]).status);
    assert_eq!(405, request(&sut, "PUT", "/events", &[]).status);
}

/// external-for: sse/handler_test.go#TestSSEHandler/headers_+_CORS
#[test]
fn cors_allowed_origin() {
    let sut = start(&["--noauth", "--public_streams", "--allowed_origins=*.example.com"]);

    let res = request(&sut, "GET", "/events", &[("Origin", "http://www.example.com")]);

    assert_eq!(Some("http://www.example.com"), res.header("Access-Control-Allow-Origin"));
}

/// external-for: sse/handler_test.go#TestSSEHandler/when_authentication_fails
#[test]
fn authentication_failure() {
    let sut = start(&["--public_streams", "--jwt_secret=qwerty", "--enforce_jwt"]);

    let res = request(&sut, "GET", "/events?stream=room_1", &[]);

    assert_eq!(401, res.status);
    assert!(res.body.is_empty());
}

/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_stream
/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_identifier
#[test]
fn get_public_stream_and_identifier() {
    let sut = start(&["--noauth", "--public_streams"]);
    let identifier = r#"{"channel":"$pubsub","stream_name":"chat_1"}"#;

    let mut by_stream = open(&sut, "GET", "/events?stream=chat_1", &[], None);
    assert_eq!(200, by_stream.status);
    assert_welcome(&next(&mut by_stream));
    assert_confirmed(&next(&mut by_stream), identifier);

    let mut by_identifier = open(&sut, "GET", &format!("/events?identifier={}", pct(identifier)), &[], None);
    assert_eq!(200, by_identifier.status);
    assert_welcome(&next(&mut by_identifier));
    assert_confirmed(&next(&mut by_identifier), identifier);

    broadcast(&sut, "chat_1", r#"{"content":"hello"}"#);

    // GET requests receive the unwrapped message.
    assert_eq!(r#"data: {"content":"hello"}"#, next(&mut by_stream));
    assert_eq!(r#"data: {"content":"hello"}"#, next(&mut by_identifier));
}

/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_signed_stream
/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_turbo_signed_stream_name
/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_channel_+_rejected
#[test]
fn signed_streams_and_rejection() {
    let sut = start(&["--noauth", "--streams_secret=sekret", "--turbo_streams"]);
    let signed = signed_stream("sekret", "chat_1");

    let mut s = open(&sut, "GET", &format!("/events?signed_stream={}", pct(&signed)), &[], None);
    assert_eq!(200, s.status);
    assert_welcome(&next(&mut s));
    assert_confirmed(
        &next(&mut s),
        &serde_json::json!({"channel": "$pubsub", "signed_stream_name": signed}).to_string(),
    );

    let mut t = open(&sut, "GET", &format!("/events?turbo_signed_stream_name={}", pct(&signed)), &[], None);
    assert_eq!(200, t.status);
    assert_welcome(&next(&mut t));
    assert_confirmed(
        &next(&mut t),
        &serde_json::json!({"channel": "Turbo::StreamsChannel", "signed_stream_name": signed}).to_string(),
    );

    // A subscription the application rejects: 400 with an empty body.
    let rejected = request(&sut, "GET", "/events?signed_stream=secretto", &[]);
    assert_eq!(400, rejected.status);
    assert!(rejected.body.is_empty());
}

/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t
#[test]
fn raw_mode_sends_only_data() {
    let sut = start(&["--noauth", "--public_streams"]);

    // The status line is written after the subscription succeeded.
    let mut s = open(&sut, "GET", "/events?stream=chat_1&raw=t", &[], None);
    assert_eq!(200, s.status);

    broadcast(&sut, "chat_1", r#"{"content":"hello"}"#);

    assert_eq!(r#"data: {"content":"hello"}"#, next(&mut s));
}

/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t_sends_comment_pings
#[test]
fn raw_mode_sends_comment_pings() {
    let sut = start(&["--noauth", "--public_streams", "--ping_interval=1"]);

    let mut s = open(&sut, "GET", "/events?stream=chat_1&raw=t", &[], None);
    assert_eq!(200, s.status);

    assert_eq!(": ping", next(&mut s));
}

/// external-for: sse/handler_test.go#TestSSEHandler/GET_request_with_comment_pings
#[test]
fn comment_pings() {
    let sut = start(&["--noauth", "--public_streams", "--sse_comment_pings", "--ping_interval=1"]);

    let mut s = open(&sut, "GET", "/events?stream=chat_1", &[], None);
    assert_eq!(200, s.status);

    // Protocol events are still sent
    assert_welcome(&next(&mut s));
    assert_confirmed(&next(&mut s), r#"{"channel":"$pubsub","stream_name":"chat_1"}"#);
    assert_eq!(": ping", next(&mut s));
}

/// external-for: sse/handler_test.go#TestSSEHandler/POST_request_with_multiple_subscriptions
#[test]
fn post_with_multiple_subscriptions() {
    let sut = start(&["--noauth", "--public_streams"]);
    let chat = r#"{"channel":"$pubsub","stream_name":"messages_1"}"#;
    let presence = r#"{"channel":"$pubsub","stream_name":"presence_1"}"#;
    let body = format!(
        "{}\n{}",
        serde_json::json!({"command": "subscribe", "identifier": chat}),
        serde_json::json!({"command": "subscribe", "identifier": presence})
    );

    let mut s = open(&sut, "POST", "/events", &[], Some(body.as_bytes()));
    assert_eq!(200, s.status);

    assert_welcome(&next(&mut s));
    assert_confirmed(&next(&mut s), chat);
    assert_confirmed(&next(&mut s), presence);

    broadcast(&sut, "messages_1", r#"{"content":"hello"}"#);
    assert_eq!(
        format!("data: {{\"identifier\":{},\"message\":{{\"content\":\"hello\"}}}}", serde_json::json!(chat)),
        next(&mut s)
    );

    broadcast(&sut, "presence_1", r#"{"type":"join","user_id":1}"#);
    assert_eq!(
        format!(
            "data: {{\"identifier\":{},\"message\":{{\"type\":\"join\",\"user_id\":1}}}}",
            serde_json::json!(presence)
        ),
        next(&mut s)
    );
}

/// external-for: sse/handler_test.go#TestSSEHandler/POST_request_without_commands_+_server_shutdown
#[test]
fn post_without_commands_then_server_shutdown() {
    let mut sut = start(&["--noauth", "--public_streams"]);

    let mut s = open(&sut, "POST", "/events", &[], None);
    assert_eq!(200, s.status);
    assert_welcome(&next(&mut s));

    // Graceful shutdown (SIGTERM); the event is buffered by the client socket.
    let _ = sut.process.stop(Duration::from_secs(10));

    assert_eq!(
        "event: disconnect\n".to_owned() + r#"data: {"type":"disconnect","reason":"server_restart","reconnect":true}"#,
        next(&mut s)
    );
}
