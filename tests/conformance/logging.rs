//! Log output parity (review finding H7, `docs/review/2026-11-fable-review.md`;
//! coverage in `docs/logging.md`). The same client script runs against the server
//! at the default level and with `--debug`; the server's log lines, normalised
//! (timestamps, `nodeid`, session ids, ports, epochs) and compared as a multiset,
//! must equal the lines Go prints. Lines tied to recorded deviations are left
//! out, each with its deviation ID.
//!
//! The expected lines were taken from the Go reference; the tests run against
//! both servers.

use std::time::{Duration, Instant};

use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::raw_http;
use cabby_testkit::sut::Sut;
use serde_json::{Value, json};

const DEADLINE: Duration = Duration::from_secs(10);

fn identifier(stream: &str) -> String {
    json!({"channel": "$pubsub", "stream_name": stream}).to_string()
}

fn connect(sut: &Sut) -> (CableClient, String) {
    let opts = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    let welcome = c.receive().expect("welcome");
    assert_eq!(Some("welcome"), welcome["type"].as_str(), "{welcome}");
    let sid = welcome["sid"].as_str().expect("sid").to_owned();
    (c, sid)
}

fn receive_type(c: &mut CableClient, kind: &str) -> Value {
    let m = c.receive().expect("reply");
    assert_eq!(Some(kind), m["type"].as_str(), "unexpected reply {m}");
    m
}

/// Waits until the server's output has a line containing `text`.
fn wait_for_log(sut: &Sut, text: &str) {
    let deadline = Instant::now() + DEADLINE;
    while !sut.process.output().contains(text) {
        assert!(Instant::now() < deadline, "no {text:?} logged\nSUT output:\n{}", sut.process.output());
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits until the server's output has a line containing all of `parts`.
fn wait_for_log_line(sut: &Sut, parts: &[&str]) {
    let deadline = Instant::now() + DEADLINE;
    while !sut.process.output().lines().any(|l| parts.iter().all(|p| l.contains(p))) {
        assert!(Instant::now() < deadline, "no {parts:?} logged\nSUT output:\n{}", sut.process.output());
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Stops the server (SIGTERM, a graceful shutdown) and returns its output.
fn stop(mut sut: Sut) -> String {
    sut.process.stop(DEADLINE).expect("server stops");
    sut.process.output()
}

/// Lines left out of the comparison: environment-dependent or tied to a
/// recorded deviation.
fn ignored(line: &str) -> bool {
    // D-035: the start line names the server; pid and limits vary.
    line.starts_with("INF Starting ")
}

/// Normalises a log line: no timestamp, no `nodeid`, session ids replaced by
/// their order of appearance, ports and the broker epoch masked.
fn normalize(line: &str, sids: &[String], ports: &[u16], epoch: Option<&str>) -> Option<String> {
    // tint: `2006-01-02 15:04:05.000 LVL msg ...`
    let rest = line.get(24..)?;
    if !matches!(rest.get(..4), Some("DBG " | "INF " | "WRN " | "ERR ")) {
        return None;
    }
    let mut out: Vec<&str> = Vec::new();
    for token in rest.split(' ') {
        if !token.starts_with("nodeid=") {
            out.push(token);
        }
    }
    let mut s = out.join(" ");
    for (i, sid) in sids.iter().enumerate() {
        s = s.replace(sid.as_str(), &format!("SID{}", i + 1));
    }
    for (i, port) in ports.iter().enumerate() {
        s = s.replace(&format!(":{port}"), &format!(":PORT{}", i + 1));
    }
    if let Some(epoch) = epoch {
        s = s.replace(epoch, "EPOCH");
    }
    // D-050: connection errors are the transport library's texts.
    if (s.starts_with("DBG WebSocket close error ") || s.starts_with("DBG WebSocket closed "))
        && let Some(i) = s.find(" error=")
    {
        s.truncate(i);
    }
    // The WebSocket and API servers share the main port: which of them starts
    // the listener (and which finds it started) is a race in Go too.
    if s.contains(" HTTP server") {
        s = s.replace("name=\"API server\"", "name=MAIN").replace("name=\"WebSocket server\"", "name=MAIN");
    }
    (!ignored(&s)).then(|| s.trim_end().to_owned())
}

fn normalized(output: &str, sids: &[String], ports: &[u16], epoch: Option<&str>) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().filter_map(|l| normalize(l, sids, ports, epoch)).collect();
    lines.sort();
    lines
}

/// Compares the lines as multisets: each line must appear as many times as
/// Go printed it.
fn assert_lines(actual: &[String], expected: &[&str], output: &str) {
    let mut expected: Vec<&str> = expected.to_vec();
    expected.sort_unstable();
    let mut sorted: Vec<&str> = actual.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    // Walk both sorted lists: what only one side has is missing or unexpected,
    // counting repeated lines.
    let (mut missing, mut extra): (Vec<&str>, Vec<&str>) = (Vec::new(), Vec::new());
    let (mut e, mut a) = (expected.iter().peekable(), sorted.iter().peekable());
    loop {
        match (e.peek(), a.peek()) {
            (Some(x), Some(y)) if x == y => {
                e.next();
                a.next();
            }
            (Some(x), Some(y)) if x < y => missing.extend(e.next().copied()),
            (Some(_), Some(_)) | (None, Some(_)) => extra.extend(a.next().copied()),
            (Some(_), None) => missing.extend(e.next().copied()),
            (None, None) => break,
        }
    }
    assert!(
        missing.is_empty() && extra.is_empty(),
        "log lines differ from Go's\nmissing:\n{}\nunexpected:\n{}\nall (normalised):\n{}\nSUT output:\n{output}",
        missing.join("\n"),
        extra.join("\n"),
        actual.join("\n"),
    );
}

/// The review's `logprobe.py`: one public session sends a duplicate subscription,
/// commands for unknown channels, an unknown command, non-string perform data, a
/// history request (no broker), an unsubscribe (presence removal fails without a
/// broker), a ping and finally a frame that is not JSON (the reader stops, the
/// session stays registered), then the server shuts down with it still active.
fn logprobe(debug: bool) -> (String, Vec<String>, Vec<u16>) {
    let mut args = vec!["--public", "--norpc", "--broadcast_adapter=http"];
    if debug {
        args.push("--debug");
    }
    let sut = Sut::start(&args);
    let ports = vec![sut.port, sut.broadcast_port];
    let (mut c, sid) = connect(&sut);
    let id = identifier("public1");

    c.send(&json!({"command": "subscribe", "identifier": id}));
    receive_type(&mut c, "confirm_subscription");
    c.send(&json!({"command": "subscribe", "identifier": id}));
    c.send(&json!({"command": "message", "identifier": "nope", "data": "{}"}));
    c.send(&json!({"command": "bogus", "identifier": "x"}));
    c.send(&json!({"command": "message", "identifier": id, "data": {"a": 1}}));
    c.send(&json!({"command": "history", "identifier": id, "history": {"since": 1}}));
    receive_type(&mut c, "reject_history");
    c.send(&json!({"command": "unsubscribe", "identifier": id}));
    c.send(&json!({"command": "ping"}));
    receive_type(&mut c, "pong");
    c.send_raw("this is not json");
    if debug {
        // At the default level nothing is logged for it.
        wait_for_log(&sut, "WebSocket session completed");
    }

    let output = stop(sut);
    drop(c);
    (output, vec![sid], ports)
}

/// regression-for: node/node_test.go#TestUnsubscribe
/// note: Review finding H7: the node, session and hub layers logged nothing. The review's logprobe scenario at the default level must print the same lines as Go: `failed to handle incoming message` warnings, `failed to remove presence`, `no RPC configured`, `closing active connections`.
#[test]
fn logprobe_lines_match_go_at_the_default_level() {
    let (output, sids, ports) = logprobe(false);
    let lines = normalized(&output, &sids, &ports, None);
    assert_lines(&lines, LOGPROBE_DEFAULT, &output);
}

/// regression-for: node/node_test.go#TestUnsubscribe
/// note: Review finding H7: with `--debug`, the per-message trace (incoming message, controller results, hub registrations and subscriptions, presence removal, shutdown) must match Go's lines and attributes.
#[test]
fn logprobe_lines_match_go_at_debug_level() {
    let (output, sids, ports) = logprobe(true);
    let lines = normalized(&output, &sids, &ports, None);
    assert_lines(&lines, LOGPROBE_DEBUG, &output);
}

fn broadcast(sut: &Sut, body: &Value) {
    let headers = [("Content-Type", "application/json")];
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let body = body.to_string();
    let res = raw_http::request_until(
        addr,
        "POST",
        "/_broadcast",
        &headers,
        Some(body.as_bytes()),
        Instant::now() + DEADLINE,
    )
    .expect("broadcast request");
    assert_eq!(201, res.status);
}

/// Broadcasts, remote commands, whispers and presence with the memory broker at
/// debug level; one client stays connected through shutdown.
fn broadcasts_and_presence() -> (String, Vec<String>, Vec<u16>, String) {
    let sut = Sut::start(&["--public", "--norpc", "--broadcast_adapter=http", "--broker=memory", "--debug"]);
    let ports = vec![sut.port, sut.broadcast_port];
    let id = identifier("s1");
    let (mut a, sid_a) = connect(&sut);
    a.send(&json!({"command": "subscribe", "identifier": id}));
    receive_type(&mut a, "confirm_subscription");

    a.send(&json!({"command": "whisper", "identifier": id, "data": {"x": 1}}));
    a.send(&json!({"command": "join", "identifier": id, "presence": {"id": "42", "info": {"name": "Sasha"}}}));
    receive_type(&mut a, "presence");
    a.send(&json!({"command": "presence", "identifier": id}));
    receive_type(&mut a, "presence");
    a.send(&json!({"command": "leave", "identifier": id}));
    receive_type(&mut a, "presence");

    broadcast(&sut, &json!({"stream": "s1", "data": "{\"text\":\"hello\"}"}));
    let m = a.receive().expect("broadcast");
    assert_eq!(json!({"text": "hello"}), m["message"], "{m}");
    broadcast(&sut, &json!({"stream": "nobody", "data": "\"two\""}));
    wait_for_log(&sut, "no sessions");

    broadcast(
        &sut,
        &json!({"command": "disconnect", "payload": {"identifier": "{\"sid\":\"nobody\"}", "reconnect": false}}),
    );
    wait_for_log(&sut, "cannot disconnect session");
    broadcast(&sut, &json!("not a broadcast"));
    wait_for_log(&sut, "failed to parse pubsub message");
    broadcast(&sut, &json!({"command": "disconnect", "payload": "garbage"}));
    wait_for_log(&sut, "failed to parse remote disconnect command");

    let (mut b, sid_b) = connect(&sut);
    b.send(&json!({"command": "subscribe", "identifier": id}));
    receive_type(&mut b, "confirm_subscription");

    broadcast(
        &sut,
        &json!({"command": "disconnect", "payload": {"identifier": format!("{{\"sid\":\"{sid_a}\"}}"), "reconnect": true}}),
    );
    receive_type(&mut a, "disconnect");
    assert!(a.wait_for_close(DEADLINE), "a is disconnected");
    wait_for_log_line(&sut, &["DBG unregistered ", &format!("sid={sid_a}")]);

    let output = sut.process.output();
    let epoch = output
        .split("in-memory broker (epoch: ")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .expect("broker epoch")
        .to_owned();
    let output = stop(sut);
    drop(b);
    (output, vec![sid_a, sid_b], ports, epoch)
}

/// regression-for: node/node_test.go#TestBroadcasting/HandleBroadcast
/// note: Review finding H7: broadcast fan-out (handle/incoming broadcast message, the gate's schedule broadcast and no sessions), remote commands (incoming remote command, cannot disconnect session, parse failures), whispers, presence events, the disconnect call and shutdown with an active session must log Go's lines at debug level.
#[test]
fn broadcast_and_presence_lines_match_go_at_debug_level() {
    let (output, sids, ports, epoch) = broadcasts_and_presence();
    let mut lines = normalized(&output, &sids, &ports, Some(&epoch));
    // The session still connected at shutdown: whether its reader logs the
    // closed connection (and the handler the session's end) before the process
    // exits is a race in Go.
    let shutdown_race = ["DBG WebSocket close error ", "DBG WebSocket session completed "];
    lines.retain(|l| !(shutdown_race.iter().any(|p| l.starts_with(p)) && l.contains("sid=SID2")));
    assert_lines(&lines, BROADCASTS_DEBUG, &output);
}

/// An HTTP RPC application that accepts connections and subscriptions but
/// answers performs and disconnects with a 500.
fn failing_rpc() -> cabby_testkit::http_stub::HttpStub {
    use cabby_testkit::http_stub::{HttpStub, Response};
    HttpStub::start(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let ok = |v: Value| Response { status: 200, body: v.to_string().into_bytes() };
        if req.path.ends_with("/connect") {
            return ok(json!({"status": 1, "identifiers": "u1", "transmissions": ["{\"type\":\"welcome\"}"]}));
        }
        if req.path.ends_with("/command") && body["command"] == "subscribe" {
            let confirm = json!({"type": "confirm_subscription", "identifier": body["identifier"]}).to_string();
            return ok(json!({"status": 1, "streams": ["chat"], "transmissions": [confirm]}));
        }
        Response { status: 500, body: b"boom".to_vec() }
    })
}

/// regression-for: node/node_test.go#TestPerform/Error_during_perform
/// note: Review finding H7: a failing RPC call must log Go's default-level lines: `failed to handle incoming message` with the decorated RPC error for the perform, and `controller disconnect failed` for the Disconnect call (the 500 answers come from a stub application).
#[test]
fn failing_rpc_calls_are_logged_like_go() {
    let rpc = failing_rpc();
    let rpc_host = format!("--rpc_host={}/_anycable", rpc.url());
    let sut = Sut::start(&[&rpc_host, "--broadcast_adapter=http", "--disconnect_mode=always"]);
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), Options::default()).expect("connect");
    receive_type(&mut c, "welcome");
    let id = json!({"channel": "ChatChannel"}).to_string();
    c.send(&json!({"command": "subscribe", "identifier": id}));
    receive_type(&mut c, "confirm_subscription");
    c.send(&json!({"command": "message", "identifier": id, "data": "{\"action\":\"speak\"}"}));
    wait_for_log(&sut, "failed to handle incoming message");
    drop(c);
    wait_for_log(&sut, "controller disconnect failed");

    let output = stop(sut);
    let mask_sid = |l: &String| {
        l.split(' ').map(|t| if t.starts_with("sid=") { "sid=X" } else { t }).collect::<Vec<_>>().join(" ")
    };
    let lines: Vec<String> = normalized(&output, &[], &[], None).iter().map(mask_sid).collect();
    for expected in [
        r#"WRN failed to handle incoming message context=node sid=X data="{\"command\":\"message\",\"data\":\"{\\\"action\\\":\\\"speak\\\"}\",\"identifier\":\"{\\\"channel\\\":\\\"ChatChannel\\\"}\"}" error="perform failed for {\"channel\":\"ChatChannel\"}, cause: rpc error: code = Unknown desc = boom""#,
        r#"ERR controller disconnect failed context=node sid=X error="rpc error: code = Unknown desc = boom""#,
    ] {
        assert!(
            lines.iter().any(|l| l == expected),
            "missing {expected}\nnormalised:\n{}\nSUT output:\n{output}",
            lines.join("\n")
        );
    }
}

/// Reads one RESP command (an array of bulk strings) from `r`.
fn read_resp_command(r: &mut impl std::io::BufRead) -> Option<Vec<String>> {
    let mut line = String::new();
    let mut read_line = |line: &mut String| {
        line.clear();
        r.read_line(line).ok().filter(|n| *n > 0).map(|_| line.trim_end().to_owned())
    };
    let header = read_line(&mut line)?;
    let n: usize = header.strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        let _len = read_line(&mut line)?;
        args.push(read_line(&mut line)?);
    }
    Some(args)
}

/// A Redis stand-in that answers every command with `+OK` (subscriptions with
/// their confirmation) but rejects `PUBLISH` like a read-only replica.
fn read_only_redis() -> u16 {
    use std::io::{BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(conn) = conn else { continue };
            std::thread::spawn(move || {
                let mut w = conn.try_clone().expect("clone");
                let mut r = BufReader::new(conn);
                while let Some(args) = read_resp_command(&mut r) {
                    let cmd = args.first().map(|c| c.to_ascii_uppercase()).unwrap_or_default();
                    let reply = match cmd.as_str() {
                        "PUBLISH" => "-READONLY You can't write against a read only replica.\r\n".to_owned(),
                        "SUBSCRIBE" | "UNSUBSCRIBE" => {
                            let ch = args.get(1).cloned().unwrap_or_default();
                            let kind = cmd.to_ascii_lowercase();
                            format!("*3\r\n${}\r\n{kind}\r\n${}\r\n{ch}\r\n:1\r\n", kind.len(), ch.len())
                        }
                        "PING" => "+PONG\r\n".to_owned(),
                        _ => "+OK\r\n".to_owned(),
                    };
                    if w.write_all(reply.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Deviation D-052: when Redis rejects a broadcast's PUBLISH (here a stand-in answering like a read-only replica), Cabby logs `ERR failed to publish` with `context=pubsub` and Redis' error; Go ignores the error of rueidis' `Do` and logs nothing.
#[test]
fn deviation_d052_rejected_redis_publications_are_logged() {
    let port = read_only_redis();
    let url = format!("--redis_url=redis://127.0.0.1:{port}");
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--pubsub=redis", &url]);
    broadcast(&sut, &json!({"stream": "s1", "data": "\"x\""}));
    match cabby_testkit::backend() {
        cabby_testkit::Backend::Rust => {
            wait_for_log_line(&sut, &["ERR failed to publish ", "context=pubsub", "read only replica"]);
        }
        cabby_testkit::Backend::Go => {
            // A second broadcast after the first: Go has had time to handle it.
            broadcast(&sut, &json!({"stream": "s1", "data": "\"y\""}));
            let output = stop(sut);
            assert!(!output.contains("failed to publish"), "SUT output:\n{output}");
        }
    }
}

const LOGPROBE_DEFAULT: &[&str] = &[
    "ERR failed to remove presence context=node sid=SID1 error=\"presence not supported\"",
    "INF API server is running without authentication",
    "INF Accept broadcast requests at http://127.0.0.1:PORT2/_broadcast (no authorization) context=broadcast provider=http",
    "INF Handle API requests at http://127.0.0.1:PORT1/api (no authorization) context=api",
    "INF Handle WebSocket connections at http://127.0.0.1:PORT1/cable",
    "INF Handle health requests at http://127.0.0.1:PORT1/health",
    "INF Shutting down... (hit Ctrl-C to stop immediately or wait for up to 30s for graceful shutdown)",
    "INF Using channels router: $pubsub",
    "INF Using no-op (legacy) broker",
    "INF all active connections closed context=node",
    "INF closing active connections context=node num=1",
    "INF connection authentication is disabled",
    "INF no RPC configured context=rpc impl=null",
    "WRN Server is running in the public mode",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"bogus\\\",\\\"identifier\\\":\\\"x\\\"}\" error=\"unknown command: bogus\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"history\\\",\\\"history\\\":{\\\"since\\\":1},\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"...(12)\" error=\"history not supported\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"message\\\",\\\"data\\\":\\\"{}\\\",\\\"identifier\\\":\\\"nope\\\"}\" error=\"unknown subscription nope\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"message\\\",\\\"data\\\":{\\\"a\\\":1},\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"public1...(5)\" error=\"perform data must be a string, got map[a:1]\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"subscribe\\\",\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"public1\\\\\\\"}\\\"}\" error=\"already subscribed to {\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\"",
];
const LOGPROBE_DEBUG: &[&str] = &[
    "DBG  context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" stream=public1",
    "DBG Go pools initialized (remote commands: 256)",
    "DBG HTTP server has been already started context=http name=MAIN addr=http://127.0.0.1:PORT1",
    "DBG WebSocket read failed context=node sid=SID1 error=\"invalid character 'h' in literal true (expecting 'r')\"",
    "DBG WebSocket session completed sid=SID1",
    "DBG WebSocket session established sid=SID1",
    "DBG calls rate context=disconnector rate=10ms workers=4",
    "DBG controller authenticate context=node sid=SID1 response.status=success response.transmissions=\"[{\\\"type\\\":\\\"welcome\\\",\\\"sid\\\":\\\"SID1\\\"}]\" response.broadcasts=[] response.identifier=\"{\\\"sid\\\":\\\"SID1\\\"}\" response.disconnect_interest=-1 response.cstate=[] response.istate=[] err=<nil>",
    "DBG controller subscribe context=node sid=SID1 response.status=success response.streams=[public1] response.transmissions=\"[{\\\"type\\\":\\\"confirm_subscription\\\",\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"public1\\\\\\\"}\\\"}]\" response.stopped_streams=[] response.stop_all_streams=false response.broadcasts=[] response.disconnect=false response.disconnect_interest=-1 response.cstate=[] response.istate=\"[$p $w]\" err=<nil>",
    "DBG controller unsubscribe context=node sid=SID1 response.status=success response.streams=[] response.transmissions=[] response.stopped_streams=[] response.stop_all_streams=true response.broadcasts=[] response.disconnect=false response.disconnect_interest=0 response.cstate=[] response.istate=[] err=<nil>",
    "DBG incoming message context=node sid=SID1 data.command=bogus data.identifier=x data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=history data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" data.data=<nil> data.history=\"{Since:1 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=message data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" data.data=map[a:1] data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=message data.identifier=nope data.data={} data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=ping data.identifier=\"\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=subscribe data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=subscribe data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=unsubscribe data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG no metrics writers, disabling metrics rotation context=metrics",
    "DBG ping received context=node sid=SID1",
    "DBG registered context=node component=hub sid=SID1 ids=\"{\\\"sid\\\":\\\"SID1\\\"}\"",
    "DBG remove presence context=node sid=SID1 stream=public1",
    "DBG starting HTTP server context=http name=\"broadcasting HTTP server\" addr=http://127.0.0.1:PORT2",
    "DBG starting HTTP server context=http name=MAIN addr=http://127.0.0.1:PORT1",
    "DBG subscribed context=node component=hub sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" stream=public1",
    "DBG subscribed context=node sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\"",
    "DBG unregistered context=node component=hub sid=SID1",
    "DBG unsigned context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\" stream=public1",
    "DBG unsubscribed context=node component=hub sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\"",
    "DBG unsubscribed context=node sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\"",
    "DBG 🔧 🔧 🔧 Debug mode is on 🔧 🔧 🔧",
    "ERR failed to remove presence context=node sid=SID1 error=\"presence not supported\"",
    "INF API server is running without authentication",
    "INF Accept broadcast requests at http://127.0.0.1:PORT2/_broadcast (no authorization) context=broadcast provider=http",
    "INF Handle API requests at http://127.0.0.1:PORT1/api (no authorization) context=api",
    "INF Handle WebSocket connections at http://127.0.0.1:PORT1/cable",
    "INF Handle health requests at http://127.0.0.1:PORT1/health",
    "INF Shutting down... (hit Ctrl-C to stop immediately or wait for up to 30s for graceful shutdown)",
    "INF Using channels router: $pubsub",
    "INF Using no-op (legacy) broker",
    "INF all active connections closed context=node",
    "INF closing active connections context=node num=1",
    "INF connection authentication is disabled",
    "INF no RPC configured context=rpc impl=null",
    "WRN Server is running in the public mode",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"bogus\\\",\\\"identifier\\\":\\\"x\\\"}\" error=\"unknown command: bogus\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"history\\\",\\\"history\\\":{\\\"since\\\":1},\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"...(12)\" error=\"history not supported\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"message\\\",\\\"data\\\":\\\"{}\\\",\\\"identifier\\\":\\\"nope\\\"}\" error=\"unknown subscription nope\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"message\\\",\\\"data\\\":{\\\"a\\\":1},\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"public1...(5)\" error=\"perform data must be a string, got map[a:1]\"",
    "WRN failed to handle incoming message context=node sid=SID1 data=\"{\\\"command\\\":\\\"subscribe\\\",\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"public1\\\\\\\"}\\\"}\" error=\"already subscribed to {\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"public1\\\"}\"",
];
const BROADCASTS_DEBUG: &[&str] = &[
    "DBG  context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG  context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG Go pools initialized (remote commands: 256)",
    "DBG HTTP server has been already started context=http name=MAIN addr=http://127.0.0.1:PORT1",
    "DBG WebSocket close error context=node sid=SID1",
    "DBG WebSocket session completed sid=SID1",
    "DBG WebSocket session established sid=SID1",
    "DBG WebSocket session established sid=SID2",
    "DBG calls rate context=disconnector rate=10ms workers=4",
    "DBG cannot disconnect session context=node component=hub identifier=\"{\\\"sid\\\":\\\"nobody\\\"}\" reason=\"not found\"",
    "DBG controller authenticate context=node sid=SID1 response.status=success response.transmissions=\"[{\\\"type\\\":\\\"welcome\\\",\\\"sid\\\":\\\"SID1\\\"}]\" response.broadcasts=[] response.identifier=\"{\\\"sid\\\":\\\"SID1\\\"}\" response.disconnect_interest=-1 response.cstate=[] response.istate=[] err=<nil>",
    "DBG controller authenticate context=node sid=SID2 response.status=success response.transmissions=\"[{\\\"type\\\":\\\"welcome\\\",\\\"sid\\\":\\\"SID2\\\"}]\" response.broadcasts=[] response.identifier=\"{\\\"sid\\\":\\\"SID2\\\"}\" response.disconnect_interest=-1 response.cstate=[] response.istate=[] err=<nil>",
    "DBG controller subscribe context=node sid=SID1 response.status=success response.streams=[s1] response.transmissions=\"[{\\\"type\\\":\\\"confirm_subscription\\\",\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"s1\\\\\\\"}\\\"}]\" response.stopped_streams=[] response.stop_all_streams=false response.broadcasts=[] response.disconnect=false response.disconnect_interest=-1 response.cstate=[] response.istate=\"[$p $w]\" err=<nil>",
    "DBG controller subscribe context=node sid=SID2 response.status=success response.streams=[s1] response.transmissions=\"[{\\\"type\\\":\\\"confirm_subscription\\\",\\\"identifier\\\":\\\"{\\\\\\\"channel\\\\\\\":\\\\\\\"$pubsub\\\\\\\",\\\\\\\"stream_name\\\\\\\":\\\\\\\"s1\\\\\\\"}\\\"}]\" response.stopped_streams=[] response.stop_all_streams=false response.broadcasts=[] response.disconnect=false response.disconnect_interest=-1 response.cstate=[] response.istate=\"[$p $w]\" err=<nil>",
    "DBG handle broadcast message context=node payload.stream=nobody payload.data=\"\\\"two\\\"\"",
    "DBG handle broadcast message context=node payload.stream=s1 payload.data=\"{\\\"text\\\":\\\"hello\\\"}\"",
    "DBG handle remote command context=node command.command=disconnect command.payload=\"\\\"garbage\\\"\"",
    "DBG handle remote command context=node command.command=disconnect command.payload=\"{\\\"identifier\\\":\\\"{\\\\\\\"sid\\\\\\\":\\\\\\\"SID1\\\\\\\"}\\\",\\\"reconnect\\\":true}\"",
    "DBG handle remote command context=node command.command=disconnect command.payload=\"{\\\"identifier\\\":\\\"{\\\\\\\"sid\\\\\\\":\\\\\\\"nobody\\\\\\\"}\\\",\\\"reconnect\\\":false}\"",
    "DBG incoming broadcast message context=node payload.stream=nobody payload.data=\"\\\"two\\\"\" payload.offset=1 payload.epoch=EPOCH",
    "DBG incoming broadcast message context=node payload.stream=s1 payload.data=\"{\\\"text\\\":\\\"hello\\\"}\" payload.offset=1 payload.epoch=EPOCH",
    "DBG incoming broadcast message context=node payload.stream=s1 payload.data=\"{\\\"type\\\":\\\"join\\\",\\\"info\\\":{\\\"name\\\":\\\"Sasha\\\"},\\\"id\\\":\\\"42\\\"}\" payload.meta.exclude_socket=\"\"",
    "DBG incoming broadcast message context=node payload.stream=s1 payload.data=\"{\\\"type\\\":\\\"leave\\\",\\\"id\\\":\\\"42\\\"}\" payload.meta.exclude_socket=\"\"",
    "DBG incoming broadcast message context=node payload.stream=s1 payload.data=\"{\\\"x\\\":1}\" payload.meta.exclude_socket=SID1",
    "DBG incoming message context=node sid=SID1 data.command=join data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=leave data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=presence data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=subscribe data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID1 data.command=whisper data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=map[x:1] data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming message context=node sid=SID2 data.command=subscribe data.identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" data.data=<nil> data.history=\"{Since:0 Streams:map[]}\"",
    "DBG incoming pubsub command context=node data.ids=\"{\\\"sid\\\":\\\"SID1\\\"}\" data.reconnect=true",
    "DBG incoming pubsub command context=node data.ids=\"{\\\"sid\\\":\\\"nobody\\\"}\" data.reconnect=false",
    "DBG incoming remote command context=node command.ids=\"{\\\"sid\\\":\\\"SID1\\\"}\" command.reconnect=true",
    "DBG incoming remote command context=node command.ids=\"{\\\"sid\\\":\\\"nobody\\\"}\" command.reconnect=false",
    "DBG no metrics writers, disabling metrics rotation context=metrics",
    "DBG no sessions context=node component=hub gate=14 stream=nobody",
    "DBG presence event context=node sid=SID1 event=join presence=\"&{Type: Info:map[name:Sasha] ID:42}\"",
    "DBG presence event context=node sid=SID1 event=leave presence=<nil>",
    "DBG registered context=node component=hub sid=SID1 ids=\"{\\\"sid\\\":\\\"SID1\\\"}\"",
    "DBG registered context=node component=hub sid=SID2 ids=\"{\\\"sid\\\":\\\"SID2\\\"}\"",
    "DBG schedule broadcast context=node component=hub gate=14 stream=nobody message.stream=nobody message.data=\"\\\"two\\\"\" message.offset=1 message.epoch=EPOCH",
    "DBG schedule broadcast context=node component=hub gate=9 stream=s1 message.stream=s1 message.data=\"{\\\"text\\\":\\\"hello\\\"}\" message.offset=1 message.epoch=EPOCH",
    "DBG schedule broadcast context=node component=hub gate=9 stream=s1 message.stream=s1 message.data=\"{\\\"type\\\":\\\"join\\\",\\\"info\\\":{\\\"name\\\":\\\"Sasha\\\"},\\\"id\\\":\\\"42\\\"}\" message.meta.exclude_socket=\"\"",
    "DBG schedule broadcast context=node component=hub gate=9 stream=s1 message.stream=s1 message.data=\"{\\\"type\\\":\\\"leave\\\",\\\"id\\\":\\\"42\\\"}\" message.meta.exclude_socket=\"\"",
    "DBG schedule broadcast context=node component=hub gate=9 stream=s1 message.stream=s1 message.data=\"{\\\"x\\\":1}\" message.meta.exclude_socket=SID1",
    "DBG starting HTTP server context=http name=\"broadcasting HTTP server\" addr=http://127.0.0.1:PORT2",
    "DBG starting HTTP server context=http name=MAIN addr=http://127.0.0.1:PORT1",
    "DBG subscribed context=node component=hub sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG subscribed context=node component=hub sid=SID2 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG subscribed context=node sid=SID1 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\"",
    "DBG subscribed context=node sid=SID2 identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\"",
    "DBG unregistered context=node component=hub sid=SID1",
    "DBG unregistered context=node component=hub sid=SID2",
    "DBG unsigned context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG unsigned context=streams identifier=\"{\\\"channel\\\":\\\"$pubsub\\\",\\\"stream_name\\\":\\\"s1\\\"}\" stream=s1",
    "DBG unsubscribed from broker streams context=node sid=SID1 channels=1",
    "DBG unsubscribed from broker streams context=node sid=SID2 channels=1",
    "DBG whispered context=node sid=SID1 stream=s1",
    "DBG 🔧 🔧 🔧 Debug mode is on 🔧 🔧 🔧",
    "INF API server is running without authentication",
    "INF Accept broadcast requests at http://127.0.0.1:PORT2/_broadcast (no authorization) context=broadcast provider=http",
    "INF Handle API requests at http://127.0.0.1:PORT1/api (no authorization) context=api",
    "INF Handle WebSocket connections at http://127.0.0.1:PORT1/cable",
    "INF Handle health requests at http://127.0.0.1:PORT1/health",
    "INF Shutting down... (hit Ctrl-C to stop immediately or wait for up to 30s for graceful shutdown)",
    "INF Using channels router: $pubsub",
    "INF Using in-memory broker (epoch: EPOCH, history limit: 100, history ttl: 300s, sessions ttl: 300s, presence ttl: 15s)",
    "INF all active connections closed context=node",
    "INF closing active connections context=node num=1",
    "INF connection authentication is disabled",
    "INF no RPC configured context=rpc impl=null",
    "WRN Server is running in the public mode",
    "WRN failed to parse pubsub message context=node data=\"\\\"not a broadcast\\\"\" error=\"json: cannot unmarshal string into Go value of type common.RemoteCommandMessage\"",
    "WRN failed to parse remote disconnect command context=node data.command=disconnect data.payload=\"\\\"garbage\\\"\" error=\"json: cannot unmarshal string into Go value of type common.RemoteDisconnectMessage\"",
];
