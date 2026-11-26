//! Feature scenarios without an RPC application: JWT identification,
//! `--norpc` standalone mode, public and signed streams, Turbo Streams.

use std::time::Duration;

use base64::Engine;
use cabby_testkit::jwt;
use cabby_testkit::pusher_auth::hmac_sha256_hex;
use cabby_testkit::wsdirector::{Connection, Expect, Protocol, identifier};
use serde_json::json;

use crate::support::*;

/// `{ext: {}.to_json, exp: (Time.now.to_i + 60)}` signed with HS256.
fn token(secret: &str) -> String {
    jwt::sign_hs256(&json!({"ext": "{}", "exp": jwt::unix_from_now(60)}), secret)
}

/// `ActiveSupport::MessageVerifier.new(secret, digest: "SHA256", serializer: JSON).generate(value)`.
fn signed_stream_name(secret: &str, value: &str) -> String {
    let data = base64::engine::general_purpose::STANDARD.encode(serde_json::Value::from(value).to_string());
    format!("{data}--{}", hmac_sha256_hex(secret, &data))
}

/// upstream: features/jwt.testfile#scenario
#[test]
fn jwt() {
    let sut = launch(
        &[
            "--secret=qwerty",
            "--jwt_param=token",
            "--enforce_jwt",
            "--metrics_rotate_interval=1",
            "--metrics_log",
            "--metrics_log_filter=rpc_call_total,rpc_error_total,rpc_retries_total",
        ],
        &[],
    );
    let url = ws_url(&sut, "/cable");
    let token = token("qwerty");
    let welcome = Expect::Partial(json!({"type": "welcome"}));
    let run = |conn: Connection, expected: &Expect| -> Result<(), cabby_testkit::wsdirector::StepError> {
        let mut c = wsd(&url, Protocol::Base, conn)?;
        c.receive(expected).map(drop)
    };

    let results = [
        ("header", run(Connection { query: vec![("token".into(), token.clone())], ..Default::default() }, &welcome)),
        (
            "protocol",
            run(Connection { subprotocol: Some(format!("anycable-token.{token}")), ..Default::default() }, &welcome),
        ),
        ("param", run(Connection { headers: vec![("x-token".into(), token.clone())], ..Default::default() }, &welcome)),
        (
            "noauth",
            run(Connection::default(), &Expect::Partial(json!({"type": "disconnect", "reason": "unauthorized"}))),
        ),
    ];

    assert_groups(&results);
}

/// upstream: features/standalone.testfile#scenario
#[test]
fn standalone() {
    let sut = launch(&["--secret=s3Krit", "--norpc"], &[]);
    let url = ws_url(&sut, "/cable");
    let token = token("s3Krit");
    let signed = signed_stream_name("s3Krit", "chat/2023");

    // Authenticated client + subscription
    let turbo = (|| {
        let mut c = wsd(
            &url,
            Protocol::ActionCable,
            Connection { query: vec![("jid".into(), token.clone())], ..Default::default() },
        )?;
        c.subscribe("$pubsub", &[("signed_stream_name", json!(signed))]).map(drop)
    })();
    assert_groups(&[("turbo", turbo)]);

    // Unauthenticated client
    let unauthenticated = (|| {
        let mut c = wsd(&url, Protocol::Base, Connection::default())?;
        c.receive(&Expect::Exact(json!({"type": "disconnect", "reason": "unauthorized", "reconnect": false}))).map(drop)
    })();
    assert_groups(&[("default", unauthenticated)]);
}

/// regression-for: features/standalone.testfile#scenario
/// note: Cabby-only: the confirmation and the disconnect frame must have exactly the expected keys (wsdirector's exact match, kept in the translation, accepts `{}` or a frame with keys missing)
#[test]
fn standalone_frames_are_exact() {
    let sut = launch(&["--secret=s3Krit", "--norpc"], &[]);
    let url = ws_url(&sut, "/cable");
    let token = token("s3Krit");
    let signed = signed_stream_name("s3Krit", "chat/2023");

    let turbo = (|| {
        let mut c = wsd(
            &url,
            Protocol::ActionCable,
            Connection { query: vec![("jid".into(), token.clone())], ..Default::default() },
        )?;
        let id = identifier("$pubsub", &[("signed_stream_name", json!(signed))]);
        c.send(&json!({"command": "subscribe", "identifier": id}));
        c.receive(&Expect::Strict(json!({"type": "confirm_subscription", "identifier": id}))).map(drop)
    })();
    let unauthenticated = (|| {
        let mut c = wsd(&url, Protocol::Base, Connection::default())?;
        c.receive(&Expect::Strict(json!({"type": "disconnect", "reason": "unauthorized", "reconnect": false})))
            .map(drop)
    })();

    assert_groups(&[("turbo", turbo), ("default", unauthenticated)]);
}

/// `logs =~ /rpc_call_total=[1-9] rpc_error_total=[1-9] rpc_retries_total=[1-9]/`.
fn has_nonzero_rpc_triplet(logs: &str) -> bool {
    let digit = |s: &str| s.chars().next().is_some_and(|c| ('1'..='9').contains(&c));
    logs.match_indices("rpc_call_total=").any(|(i, m)| {
        let rest = &logs[i + m.len()..];
        if !digit(rest) {
            return false;
        }
        let Some(rest) = rest[1..].strip_prefix(" rpc_error_total=") else { return false };
        if !digit(rest) {
            return false;
        }
        let Some(rest) = rest[1..].strip_prefix(" rpc_retries_total=") else { return false };
        digit(rest)
    })
}

/// `sleep 2 # Wait for metrics to be logged`, then `stop :anycable`: waits for a
/// metrics line printed after `mark` (the rotation interval is 1 s).
fn logs_after_next_metrics(sut: &mut cabby_testkit::sut::Sut, mark: usize) -> String {
    wait_output(sut, "a metrics log line", READY_DEADLINE, |out| {
        out.get(mark..).is_some_and(|tail| tail.contains("rpc_call_total="))
    });
    stop(sut, Duration::from_secs(10))
}

const METRICS: [&str; 3] = [
    "--metrics_rotate_interval=1",
    "--metrics_log",
    "--metrics_log_filter=rpc_call_total,rpc_error_total,rpc_retries_total",
];

/// upstream: features/streams.testfile#scenario
/// note: the `sleep 2` before stopping waits for the next metrics line (rotation 1 s); upstream's log check is weak: it only fails when all three counters are non-zero single digits
#[test]
fn streams() {
    let mut args = vec!["--public"];
    args.extend(METRICS);
    let mut sut = launch(&args, &[]);
    let url = ws_url(&sut, "/cable");

    let streamer = (|| {
        let mut c = wsd(&url, Protocol::ActionCable, Connection::default())?;
        c.subscribe("$pubsub", &[("stream_name", json!("stream/2023"))]).map(drop)
    })();
    assert_groups(&[("streamer", streamer)]);

    let mark = sut.process.output().len();
    let logs = logs_after_next_metrics(&mut sut, mark);

    // We should not see any rpc calls. (Upstream's check is weak: it only fails
    // when all three counters are non-zero single digits on one line.)
    assert!(!has_nonzero_rpc_triplet(&logs), "Expected metrics logs not found:\n{logs}");
}

/// upstream: features/turbo_rpc_less.testfile#scenario
/// note: the `sleep 2` before stopping waits for the next metrics line (rotation 1 s); upstream's log check is weak: it only fails when all three counters are non-zero single digits
#[test]
fn turbo_rpc_less() {
    let mut args = vec!["--turbo_rails_key=s3Krit", "--jwt_id_key=qwerty"];
    args.extend(METRICS);
    let mut sut = launch(&args, &[]);
    let url = ws_url(&sut, "/cable");
    let token = token("qwerty");
    let signed = signed_stream_name("s3Krit", "chat/2023");

    let turbo = (|| {
        let mut c = wsd(
            &url,
            Protocol::ActionCable,
            Connection { query: vec![("jid".into(), token.clone())], ..Default::default() },
        )?;
        c.subscribe("Turbo::StreamsChannel", &[("signed_stream_name", json!(signed))]).map(drop)
    })();
    assert_groups(&[("turbo", turbo)]);

    let mark = sut.process.output().len();
    let logs = logs_after_next_metrics(&mut sut, mark);

    // We should not see any rpc calls (same weak upstream check as streams).
    assert!(!has_nonzero_rpc_triplet(&logs), "Expected metrics logs not found:\n{logs}");
}
