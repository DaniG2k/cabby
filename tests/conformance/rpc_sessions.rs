//! Sessions and the RPC controller under load, observed from outside (review
//! 2026-11, H4 and M2): a backlog of RPC calls must not delay other sessions'
//! commands, and a session that closes cancels its RPC call. The server talks
//! to a fake HTTP RPC application whose command calls are held at a gate, as a
//! hung application would; clients authenticate without RPC (`--noauth`).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use cabby_core::protos;
use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::fake_rpc::{CallState, FakeRpc, Method};
use cabby_testkit::raw_http;
use cabby_testkit::sut::Sut;
use cabby_testkit::sync::Gate;
use serde_json::json;

const DEADLINE: Duration = Duration::from_secs(10);
/// Upstream's default `rpc_concurrency`.
const RPC_CONCURRENCY: usize = 28;

/// A fake application holding every command call at the returned gate (it
/// would confirm the subscription once released).
fn hung_app() -> (FakeRpc, Gate) {
    let fake = FakeRpc::start();
    let gate = fake.gate();
    fake.on(Method::Command)
        .held_by(&gate)
        .reply(protos::CommandResponse { status: protos::Status::Success as i32, ..Default::default() });
    fake.on(Method::Disconnect)
        .reply(protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() });
    (fake, gate)
}

/// A no-auth server on `fake`'s HTTP RPC with public streams and metrics; RPC
/// calls are limited only by minute-long command and HTTP request timeouts (the
/// wait timeout defaults to the former), so held calls stay pending while a
/// test observes them.
fn start_server(fake: &FakeRpc, extra: &[&str]) -> Sut {
    let rpc_host = format!("--rpc_host={}", fake.http_url());
    let mut args = vec![
        rpc_host.as_str(),
        "--noauth",
        "--public_streams",
        "--broadcast_adapter=http",
        "--disconnect_mode=never",
        "--metrics_http=/metrics",
        "--rpc_command_timeout=60000",
        "--http_rpc_timeout=60000",
    ];
    args.extend_from_slice(extra);
    Sut::start(&args)
}

/// Connects and returns the client with its session ID (from the welcome).
fn connect(sut: &Sut) -> (CableClient, String) {
    let opts = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    let welcome = c.receive().expect("welcome");
    assert_eq!(Some("welcome"), welcome["type"].as_str(), "unexpected first message {welcome}");
    let sid = welcome["sid"].as_str().unwrap_or_default().to_owned();
    (c, sid)
}

/// Subscribes to a channel of the (hung) application: an RPC-bound command.
fn subscribe_slow(c: &mut CableClient, n: usize) {
    let identifier = json!({"channel": "SlowChannel", "n": n}).to_string();
    c.send(&json!({"command": "subscribe", "identifier": identifier}));
}

/// The value of `anycable_go_<name>` on the metrics endpoint.
fn metric(sut: &Sut, name: &str) -> Option<u64> {
    let r = raw_http::request(sut.addr(), "GET", "/metrics", &[], None).expect("metrics request");
    let body = String::from_utf8_lossy(&r.body).into_owned();
    let prefix = format!("anycable_go_{name} ");
    body.lines().find_map(|l| l.strip_prefix(&prefix)).and_then(|v| v.trim().parse().ok())
}

/// Polls `metric` until it equals `want`; returns the last value seen.
fn await_metric(sut: &Sut, name: &str, want: u64, within: Duration) -> Option<u64> {
    let deadline = Instant::now() + within;
    loop {
        let got = metric(sut, name);
        if got == Some(want) || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Disconnects session `sid` from the server side (a remote disconnect
/// broadcast), as an application would: the session closes while its reader is
/// still busy with the RPC-bound command.
fn remote_disconnect(sut: &Sut, sid: &str) {
    let addr = SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let identifier = json!({"sid": sid}).to_string();
    let body = json!({"command": "disconnect", "payload": {"identifier": identifier, "reconnect": false}}).to_string();
    let r =
        raw_http::request(addr, "POST", "/_broadcast", &[("Content-Type", "application/json")], Some(body.as_bytes()))
            .expect("broadcast request");
    assert!((200..300).contains(&r.status), "remote disconnect rejected: {}", r.status);
}

/// external-for: rpc/rpc_test.go#TestWaitTimeout/Subscribe_times_out_waiting_for_a_slot
/// note: Review 2026-11 H4 (repro docs/review/repro/core/hol2.py): with the application hung, 150 sessions each send an RPC-bound subscribe; 28 calls are in flight and the other 122 wait for an RPC slot (rpc_pending_num), each session on its own (Go: goroutine per session).
/// note: A fresh no-auth client then gets its welcome and a $pubsub confirmation within 2s: commands that need no RPC never wait behind other sessions' RPC backlog (Go: 0.00s; Cabby before the fix: 17.5s with 600 sessions, as its bounded command pool was held by RPC-bound commands).
#[test]
fn rpc_backlog_does_not_delay_other_sessions() {
    const BACKLOG: usize = 150;
    let (fake, gate) = hung_app();
    let sut = start_server(&fake, &[]);

    let mut clients = Vec::new();
    for n in 0..BACKLOG {
        let (mut c, _) = connect(&sut);
        subscribe_slow(&mut c, n);
        clients.push(c);
    }
    gate.await_held(RPC_CONCURRENCY, DEADLINE);
    let pending = (BACKLOG - RPC_CONCURRENCY) as u64;
    assert_eq!(
        Some(pending),
        await_metric(&sut, "rpc_pending_num", pending, DEADLINE),
        "every session beyond the RPC concurrency waits for a slot"
    );

    let started = Instant::now();
    let (mut fresh, _) = connect(&sut);
    let welcome_after = started.elapsed();
    let identifier = json!({"channel": "$pubsub", "stream_name": "public"}).to_string();
    fresh.send(&json!({"command": "subscribe", "identifier": identifier}));
    let ack = fresh.receive_within(DEADLINE).expect("confirmation");
    let confirmed_after = started.elapsed();
    assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
    eprintln!("fresh client: welcome after {welcome_after:?}, confirmation after {confirmed_after:?}");
    assert!(confirmed_after < Duration::from_secs(2), "the fresh client waited {confirmed_after:?}");
    assert_eq!(RPC_CONCURRENCY, gate.held(), "the held calls are still in flight");

    drop(sut);
    gate.open();
}

/// external-for: rpc/rpc_test.go#TestAuthenticate/Canceled_during_RPC
/// note: Review 2026-11 M2 (repro docs/review/repro/core/cancel.py): a session closed (remote disconnect) while its subscribe RPC is in flight aborts the HTTP call (the application sees it abandoned) and counts it at once, as Go does with the session's closeCtx: rpc_call_total 1, rpc_error_total 1 (HTTP RPC reports the aborted request as a request timeout), rpc_canceled_total 0.
#[test]
fn closing_a_session_aborts_its_rpc_call() {
    let (fake, gate) = hung_app();
    let sut = start_server(&fake, &[]);

    let (mut c, sid) = connect(&sut);
    subscribe_slow(&mut c, 0);
    gate.await_held(1, DEADLINE);

    remote_disconnect(&sut, &sid);
    assert!(c.wait_for_close(DEADLINE), "the session was not closed");

    let within = Duration::from_secs(3);
    assert_eq!(Some(1), await_metric(&sut, "rpc_error_total", 1, within), "the aborted call is counted as failed");
    assert_eq!(Some(1), metric(&sut, "rpc_call_total"));
    assert_eq!(Some(0), metric(&sut, "rpc_canceled_total"));
    fake.await_in_state(CallState::Abandoned, 1, within);

    drop(sut);
    gate.open();
}

/// external-for: rpc/rpc_test.go#TestWaitTimeout/Session_closes_while_waiting_for_a_slot
/// note: Review 2026-11 M2: with --rpc_concurrency=1 and the only slot held, a second session's subscribe waits for a slot; closing that session (remote disconnect) ends the wait at once and counts it in rpc_canceled_total, as Go does with the session's closeCtx.
#[test]
fn closing_a_session_cancels_its_wait_for_an_rpc_slot() {
    let (fake, gate) = hung_app();
    let sut = start_server(&fake, &["--rpc_concurrency=1"]);

    let (mut holder, _) = connect(&sut);
    subscribe_slow(&mut holder, 0);
    gate.await_held(1, DEADLINE);

    let (mut waiter, sid) = connect(&sut);
    subscribe_slow(&mut waiter, 1);
    assert_eq!(Some(1), await_metric(&sut, "rpc_pending_num", 1, DEADLINE), "the second call waits for the slot");

    remote_disconnect(&sut, &sid);
    assert!(waiter.wait_for_close(DEADLINE), "the session was not closed");

    let within = Duration::from_secs(3);
    assert_eq!(Some(1), await_metric(&sut, "rpc_canceled_total", 1, within), "the wait is cancelled");
    assert_eq!(Some(0), metric(&sut, "rpc_pending_num"));
    assert_eq!(1, fake.call_count(Method::Command), "the cancelled call never reached the application");

    drop(sut);
    gate.open();
}
