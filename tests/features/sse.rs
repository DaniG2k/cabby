//! features/sse.testfile: SSE sessions with JWT identification, broadcasts via
//! the API port, reconnection with `Last-Event-ID`, `history_since`, and raw
//! mode.

use std::time::Duration;

use cabby_testkit::jwt;
use cabby_testkit::pusher_auth::hmac_sha256_hex;
use cabby_testkit::raw_http::{self, EventStream};
use serde_json::{Value, json};

use crate::support::*;

/// `Net::HTTP.start(..., read_timeout: 2)`.
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// echo -n 'api-cable' | openssl dgst -sha256 -hmac 'qwerty' | awk '{print $2}'
const BROADCAST_KEY: &str = "bdd0244b3664404d03b7d0c2f1b946dc4fc6c13fdcbfa2be62a4a28deb04d096";

#[derive(Debug, Default)]
struct Event {
    r#type: Option<String>,
    data: Option<Value>,
    id: Option<String>,
}

/// `parse_sse_chunk`.
fn parse(chunk: &str) -> Event {
    let mut event = Event::default();
    for line in chunk.split('\n') {
        let (field, value) = line.split_once(':').map_or((line.trim(), ""), |(f, v)| (f.trim(), v.trim()));
        match field {
            "data" => event.data = Some(serde_json::from_str(value).expect("JSON data")),
            "event" => event.r#type = Some(value.to_owned()),
            "id" => event.id = Some(value.to_owned()),
            _ => {}
        }
    }
    event
}

/// `stream.resume`: the next non-ping event.
fn resume(stream: &mut EventStream) -> Event {
    loop {
        let chunk = stream.next_event(READ_TIMEOUT).expect("an event").expect("the stream is open");
        let event = parse(&chunk);
        if event.r#type.as_deref() != Some("ping") {
            return event;
        }
    }
}

fn encode_www_form_component(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => (b as char).to_string(),
            b' ' => "+".into(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// upstream: features/sse.testfile#scenario
/// note: inherent window: Net::HTTP read_timeout (2 s) per event
/// note: --api_port gets a reserved port with the server start, which waits until it listens and is retried on new ports if one is taken (upstream: the fixed port 8082)
#[test]
fn sse() {
    let (sut, api_port) = launch_with_port(
        &["--sse", "--public_streams", "--secret=qwerty", "--broadcast_adapter=http", "--presets=broker"],
        &[],
        "--api_port={port}",
    );
    assert_eq!(BROADCAST_KEY, hmac_sha256_hex("qwerty", "api-cable"));

    let token = jwt::sign_hs256(&json!({"ext": "{}", "exp": jwt::unix_from_now(60)}), "qwerty");
    let stream_name = "chat/2023";
    let identifier = encode_www_form_component(&format!(r#"{{"channel":"$pubsub","stream_name":"{stream_name}"}}"#));
    let url = format!("/events?identifier={identifier}");
    let open = |target: &str, headers: &[(&str, &str)]| {
        let s = EventStream::open(sut.addr(), "GET", target, headers, None).unwrap_or_else(|e| {
            sut.process.propagate_pending();
            panic!("SSE request failed: {e:#}\nSUT output:\n{}", sut.process.output())
        });
        assert_eq!(200, s.status);
        s
    };
    let broadcast = |data: &Value| {
        let body = json!({"stream": stream_name, "data": data.to_string()}).to_string();
        let auth = format!("Bearer {BROADCAST_KEY}");
        let res = raw_http::request(
            local(api_port),
            "POST",
            "/api/publish",
            &[("Content-Type", "application/json"), ("Authorization", &auth)],
            Some(body.as_bytes()),
        )
        .expect("publish request");
        assert_eq!(201, res.status, "Broadcast returned unexpected status: {}", res.status);
    };
    let start_time = jwt::unix_from_now(0);

    let last_id = {
        let mut stream = open(&format!("{url}&jid={token}"), &[]);
        let first_event = resume(&mut stream);
        assert_eq!(Some("welcome"), first_event.r#type.as_deref(), "Expected welcome, got: {first_event:?}");
        let second_event = resume(&mut stream);
        assert_eq!(
            Some("confirm_subscription"),
            second_event.r#type.as_deref(),
            "Expected confirm_subscription, got: {second_event:?}"
        );

        broadcast(&json!({"text": "Hello, stream!"}));

        let broadcast_event = resume(&mut stream);
        assert_eq!(Some(json!({"text": "Hello, stream!"})), broadcast_event.data, "Expected broadcast data");
        broadcast_event.id.expect("event id")
        // Stop first session
    };

    // Broadcast another message
    broadcast(&json!({"text": "Where are you, stream?"}));

    // Start new session with last ID
    {
        let mut stream = open(&format!("{url}&jid={token}"), &[("Last-Event-ID", &last_id)]);
        assert_eq!(Some("welcome"), resume(&mut stream).r#type.as_deref(), "Expected welcome");
        assert_eq!(Some("confirm_subscription"), resume(&mut stream).r#type.as_deref(), "Expected confirmation");
        let missed_message = resume(&mut stream);
        assert_eq!(Some(json!({"text": "Where are you, stream?"})), missed_message.data, "Expected missed message");
    }

    // Start new session with history_since
    {
        let mut stream = open(&format!("{url}&jid={token}&history_since={start_time}"), &[]);
        assert_eq!(Some("welcome"), resume(&mut stream).r#type.as_deref(), "Expected welcome");
        assert_eq!(Some("confirm_subscription"), resume(&mut stream).r#type.as_deref(), "Expected confirmation");
        for msg in ["Hello, stream!", "Where are you, stream?"] {
            let missed_message = resume(&mut stream);
            assert_eq!(Some(json!({"text": msg})), missed_message.data, "Expected missed message {msg}");
        }
        assert_eq!(Some("confirm_history"), resume(&mut stream).r#type.as_deref(), "Expected history confirmation");
    }

    // Start new raw session with history_since
    {
        let mut stream = open(&format!("{url}&raw=1&history_since={start_time}"), &[("X-JID", &token)]);
        for msg in ["Hello, stream!", "Where are you, stream?"] {
            let missed_message = resume(&mut stream);
            assert_eq!(Some(json!({"text": msg})), missed_message.data, "Expected missed message {msg}");
        }

        broadcast(&json!({"text": "Keep rawing, rawing, rawing!"}));

        let broadcast_event = resume(&mut stream);
        assert_eq!(
            Some(json!({"text": "Keep rawing, rawing, rawing!"})),
            broadcast_event.data,
            "Expected broadcast data"
        );
    }
    drop(sut);
}
