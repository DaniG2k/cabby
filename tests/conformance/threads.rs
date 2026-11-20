//! Resource use of idle connections, observed from outside: the server's OS
//! thread count (`/proc/<pid>/status`) while it holds many idle WebSockets.

use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cabby_testkit::publish::Target;
use cabby_testkit::sut::Sut;
use cabby_testkit::text_ws::TextWs;
use serde_json::json;
use tungstenite::{ClientRequestBuilder, Message, WebSocket};

const IO_DEADLINE: Duration = Duration::from_secs(5);
/// Idle connections opened on top of the baseline.
const CONNECTIONS: usize = 300;
/// Threads the server may add while they connect (pools sized by load, not by
/// connections, may grow by a few workers).
const MAX_EXTRA_THREADS: usize = 16;

/// The `Threads:` line of `/proc/<pid>/status`.
fn threads(pid: u32) -> usize {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_else(|e| panic!("reading /proc/{pid}/status (Linux only): {e}"));
    status
        .lines()
        .find_map(|l| l.strip_prefix("Threads:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no Threads: line in /proc/{pid}/status"))
}

/// Opens an Action Cable connection and waits for its `welcome`.
fn connect(sut: &Sut) -> WebSocket<TcpStream> {
    let stream = TcpStream::connect_timeout(&sut.addr(), IO_DEADLINE).expect("connecting");
    stream.set_read_timeout(Some(IO_DEADLINE)).expect("read timeout");
    stream.set_write_timeout(Some(IO_DEADLINE)).expect("write timeout");
    let uri: tungstenite::http::Uri = format!("ws://{}/cable", sut.addr()).parse().expect("uri");
    let req = ClientRequestBuilder::new(uri).with_sub_protocol("actioncable-v1-json");
    let (mut ws, _) = tungstenite::client(req, stream).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("handshake failed: {e}\nSUT output:\n{}", sut.process.output())
    });
    let deadline = Instant::now() + IO_DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "no welcome within {IO_DEADLINE:?}");
        match ws.read() {
            Ok(Message::Text(t)) if t.contains("\"welcome\"") => return ws,
            Ok(_) => {}
            Err(e) => {
                sut.process.propagate_pending();
                panic!("reading the welcome failed: {e}\nSUT output:\n{}", sut.process.output())
            }
        }
    }
}

/// issue: anycable/anycable#265
/// note: Cabby invariant: no OS thread is tied to a connection. Reads are pushed by each connection's I/O task to a bounded command pool, writes drain on a shared pool, timers share one thread.
/// note: Opens 300 idle Action Cable connections one after another (each waits for its welcome) and checks that the server's thread count grew by at most 16.
#[test]
fn idle_connections_do_not_add_threads() {
    let sut = Sut::start(&["--noauth", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let pid = sut.process.pid();

    // A first connection starts the lazily created workers.
    let mut clients = vec![connect(&sut)];
    let baseline = threads(pid);

    for _ in 0..CONNECTIONS {
        clients.push(connect(&sut));
    }
    let after = threads(pid);
    eprintln!("server threads: {baseline} with one connection, {after} with {} connections", CONNECTIONS + 1);

    assert!(
        after <= baseline + MAX_EXTRA_THREADS,
        "{CONNECTIONS} idle connections grew the server from {baseline} to {after} threads"
    );
    drop(clients);
}

/// Subscribes an Action Cable connection to the public stream `stream`.
fn subscribe(sut: &Sut, ws: &mut WebSocket<TcpStream>, stream: &str) {
    let identifier = json!({"channel": "$pubsub", "stream_name": stream}).to_string();
    let cmd = json!({"command": "subscribe", "identifier": identifier}).to_string();
    ws.send(Message::text(cmd)).expect("sending subscribe");
    let deadline = Instant::now() + IO_DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "no subscription confirmation within {IO_DEADLINE:?}");
        match ws.read() {
            Ok(Message::Text(t)) if t.contains("confirm_subscription") => return,
            Ok(_) => {}
            Err(e) => {
                sut.process.propagate_pending();
                panic!("reading the confirmation failed: {e}\nSUT output:\n{}", sut.process.output())
            }
        }
    }
}

/// issue: anycable/anycable#265
/// note: Cabby invariant: a broadcast fan-out to many connections runs on shared pools, not on a thread per receiving connection.
/// note: 301 connections subscribed to one public stream receive 20 HTTP broadcasts; the server's thread count, sampled meanwhile, grows by at most 16 over the baseline.
#[test]
fn broadcast_fan_out_does_not_add_threads() {
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let pid = sut.process.pid();

    let mut clients = Vec::new();
    for _ in 0..=CONNECTIONS {
        let mut ws = connect(&sut);
        subscribe(&sut, &mut ws, "threads");
        clients.push(ws);
    }
    let baseline = threads(pid);

    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let done = done.clone();
        std::thread::spawn(move || {
            let mut peak = 0;
            while !done.load(Ordering::SeqCst) {
                peak = peak.max(threads(pid));
                std::thread::sleep(Duration::from_millis(2));
            }
            peak
        })
    };
    let target = Target::Http {
        addr: SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port)),
        path: "/_broadcast".into(),
        key: None,
    };
    for i in 0..20 {
        let payload = json!({"stream": "threads", "data": json!({"n": i}).to_string()});
        target.publish(&payload.to_string()).expect("broadcasting");
    }
    // Every client receives the last broadcast.
    for ws in &mut clients {
        let deadline = Instant::now() + IO_DEADLINE;
        loop {
            assert!(Instant::now() < deadline, "the last broadcast did not arrive");
            match ws.read() {
                Ok(Message::Text(t)) if t.contains(r#""n":19"#) => break,
                Ok(_) => {}
                Err(e) => panic!("reading broadcasts failed: {e}\nSUT output:\n{}", sut.process.output()),
            }
        }
    }
    done.store(true, Ordering::SeqCst);
    let peak = sampler.join().expect("the sampler");
    eprintln!("server threads: {baseline} with {} subscribers, at most {peak} while broadcasting", CONNECTIONS + 1);

    assert!(
        peak <= baseline + MAX_EXTRA_THREADS,
        "broadcasting to {} connections grew the server from {baseline} to {peak} threads",
        CONNECTIONS + 1
    );
}

/// Idle Pusher connections opened on top of the baseline.
const PUSHER_CONNECTIONS: usize = 1000;

/// Opens a Pusher connection and waits for `pusher:connection_established`.
fn connect_pusher(sut: &Sut) -> TextWs {
    let mut ws = TextWs::connect(sut.addr(), "/app/threads-key", &[]).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("connecting failed: {e:#}\nSUT output:\n{}", sut.process.output())
    });
    let welcome = ws.recv_text(IO_DEADLINE).expect("connection_established");
    assert!(welcome.contains("pusher:connection_established"), "unexpected first frame {welcome}");
    ws
}

/// issue: anycable/anycable#265
/// note: Cabby invariant: Pusher sessions are served like Action Cable ones (the connection pushes messages to the session's reader), so no OS thread is tied to a Pusher connection either (review 2026-11, M1: each Pusher session had a reader thread).
/// note: Opens 1000 idle Pusher connections one after another (each waits for pusher:connection_established) and checks that the server's thread count grew by at most 16.
#[test]
fn idle_pusher_connections_do_not_add_threads() {
    let sut = Sut::start(&[
        "--pusher_app_id=threads",
        "--pusher_app_key=threads-key",
        "--pusher_secret=threads-secret",
        "--broadcast_adapter=http",
        "--disconnect_mode=never",
    ]);
    let pid = sut.process.pid();

    let mut clients = vec![connect_pusher(&sut)];
    let baseline = threads(pid);

    for _ in 0..PUSHER_CONNECTIONS {
        clients.push(connect_pusher(&sut));
    }
    let after = threads(pid);
    eprintln!("server threads: {baseline} with one Pusher connection, {after} with {}", PUSHER_CONNECTIONS + 1);

    assert!(
        after <= baseline + MAX_EXTRA_THREADS,
        "{PUSHER_CONNECTIONS} idle Pusher connections grew the server from {baseline} to {after} threads"
    );
    drop(clients);
}
