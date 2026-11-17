//! Correctness checks of the `features/*.benchfile` workloads.
//!
//! Upstream runs `websocket-bench broadcast` (anycable/websocket-bench v0.4.0)
//! against the server with the AnyT RPC application and checks *correctness*
//! only: at least 90% of the expected broadcasts arrive (`broadcasts`), and
//! the server does not leak goroutines across two runs (`goroutines`). This
//! module ports those checks with an equivalent in-test workload: per step,
//! `step_size` more clients subscribe to `BenchmarkChannel`, `sample_size`
//! broadcasts (`payload_padding` bytes of padding) are performed by
//! `concurrent` sender connections through the channel's `broadcast` action,
//! and every subscribed client must receive every broadcast within
//! `wait_broadcasts`. Throughput and latency measurements belong to the
//! external Benchi harness (`crates/cabby-bench`), not to these tests.
//!
//! The upstream RPC application uses the gem's default `redis` broadcast
//! adapter (`REDIS_URL` set by CI) and the server its default adapters
//! (`http`, `redis`), so both use the shared Redis service here, with a
//! per-test broadcast channel and stream name prefix.

use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cabby_testkit::anyt_app::{AnytApp, BENCHMARK_CHANNEL};
use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::publish::{self, Target};
use cabby_testkit::services::{self, RedisScope, unique};
use cabby_testkit::sut::Sut;
use serde_json::{Value, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

use crate::support::*;

#[derive(Debug, Clone, Copy)]
struct Workload {
    concurrent: usize,
    sample_size: usize,
    step_size: usize,
    payload_padding: usize,
    total_steps: usize,
    /// `--wait-broadcasts`.
    wait_broadcasts: Duration,
}

fn identifier() -> String {
    format!(r#"{{"channel":"{BENCHMARK_CHANNEL}"}}"#)
}

/// A subscribed client that counts `broadcast` messages on a blocking reader
/// thread (no polling, so hundreds of them stay cheap).
struct Subscriber {
    thread: JoinHandle<()>,
}

fn subscriber(url: &str, received: Arc<AtomicUsize>, stop: Arc<AtomicBool>) -> Result<Subscriber, String> {
    let mut req = url.into_client_request().map_err(|e| e.to_string())?;
    req.headers_mut().insert("Sec-WebSocket-Protocol", "actioncable-v1-json".parse().unwrap());
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], req.uri().port_u16().unwrap_or(80)));
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).map_err(|e| e.to_string())?;
    let (mut ws, _) = tungstenite::client(req, stream).map_err(|e| e.to_string())?;
    ws.send(Message::text(json!({"command": "subscribe", "identifier": identifier()}).to_string()))
        .map_err(|e| e.to_string())?;
    // Wait for the confirmation (ignoring welcome and pings).
    loop {
        let msg = read_json(&mut ws).ok_or("no subscription confirmation")?;
        if msg["type"] == "confirm_subscription" {
            break;
        }
    }
    ws.get_ref().set_read_timeout(Some(Duration::from_millis(250))).map_err(|e| e.to_string())?;
    let thread = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match ws.read() {
                    Ok(Message::Text(t)) => {
                        let v: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
                        if v["message"]["action"] == "broadcast" {
                            received.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Ok(_) => {}
                    Err(tungstenite::Error::Io(e))
                        if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
                    Err(_) => return,
                }
            }
            let _ = ws.close(None);
            let _ = ws.flush();
        })
        .map_err(|e| e.to_string())?;
    Ok(Subscriber { thread })
}

fn read_json(ws: &mut WebSocket<TcpStream>) -> Option<Value> {
    loop {
        match ws.read().ok()? {
            Message::Text(t) => return serde_json::from_str(&t).ok(),
            _ => continue,
        }
    }
}

/// Runs the workload; returns `(expected, received)` per step.
fn run_workload(sut: &Sut, w: Workload) -> Vec<(usize, usize)> {
    let url = ws_url(sut, "/cable");
    let stop = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicUsize::new(0));
    let mut subscribers = Vec::new();
    let mut senders: Vec<CableClient> = (0..w.concurrent)
        .map(|_| {
            let mut c = CableClient::connect(
                &url,
                Options { ignore: vec!["ping".into(), "welcome".into()], ..Default::default() },
            )
            .unwrap_or_else(|e| panic!("sender: {e}"));
            c.send(&json!({"command": "subscribe", "identifier": identifier()}));
            let ack = c.receive().unwrap_or_else(|e| panic!("sender subscription: {e}"));
            assert_eq!(json!("confirm_subscription"), ack["type"], "{ack}");
            c
        })
        .collect();
    let padding = "x".repeat(w.payload_padding);
    let mut results = Vec::new();
    for step in 0..w.total_steps {
        for _ in 0..w.step_size {
            subscribers.push(
                subscriber(&url, Arc::clone(&received), Arc::clone(&stop))
                    .unwrap_or_else(|e| panic!("subscriber: {e}")),
            );
        }
        received.store(0, Ordering::SeqCst);
        // `sample_size` broadcasts, `concurrent` in flight at a time: each sender
        // waits for its `broadcastResult` before sending the next one.
        std::thread::scope(|s| {
            for (i, sender) in senders.iter_mut().enumerate() {
                let count = w.sample_size / w.concurrent + usize::from(i < w.sample_size % w.concurrent);
                let padding = &padding;
                s.spawn(move || {
                    for n in 0..count {
                        let payload = json!({"step": step, "sender": i, "n": n, "padding": padding});
                        let data = json!({"action": "broadcast", "payload": payload}).to_string();
                        sender.send(&json!({"command": "message", "identifier": identifier(), "data": data}));
                        let deadline = Instant::now() + Duration::from_secs(10);
                        loop {
                            let left = deadline.saturating_duration_since(Instant::now());
                            let msg = sender.receive_within(left).unwrap_or_else(|e| panic!("broadcastResult: {e}"));
                            if msg["message"]["action"] == "broadcastResult" {
                                break;
                            }
                        }
                    }
                });
            }
        });
        let expected = w.sample_size * subscribers.len();
        let until = Instant::now() + w.wait_broadcasts;
        while received.load(Ordering::SeqCst) < expected && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        results.push((expected, received.load(Ordering::SeqCst)));
    }
    stop.store(true, Ordering::SeqCst);
    for s in subscribers {
        let _ = s.thread.join();
    }
    drop(senders);
    results
}

/// Host and port of `CABBY_REDIS_URL` (the default user publishes).
fn redis_addr() -> std::net::SocketAddr {
    use std::net::ToSocketAddrs;
    let url = std::env::var("CABBY_REDIS_URL").expect("CABBY_REDIS_URL");
    let rest = url.trim_start_matches("redis://");
    let authority = rest.split('/').next().unwrap_or(rest);
    let hp = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
    hp.to_socket_addrs().ok().and_then(|mut a| a.next()).unwrap_or_else(|| panic!("cannot resolve {url}"))
}

/// The server and the application on the shared Redis: a per-test ACL user,
/// broadcast channel (`--redis_channel`) and stream name prefix.
fn start() -> (RedisScope, AnytApp, Sut) {
    let redis = services::redis().scope("bench");
    let channel = redis.name("anycable");
    let app = rpc_app_with_prefix(&format!("{}:", unique("bench")));
    let rpc = format!("--rpc_host={}", app.grpc_target());
    let channel_flag = format!("--redis_channel={channel}");
    let redis_url = redis.url();
    let sut = launch(&[&rpc, &channel_flag], &[("REDIS_URL", &redis_url)]);
    let until = Instant::now() + READY_DEADLINE;
    while publish::redis_subscribers(redis_addr(), &channel) == 0 {
        assert!(Instant::now() < until, "the server did not subscribe to Redis broadcasts");
        std::thread::sleep(Duration::from_millis(20));
    }
    app.set_broadcaster(Target::RedisPublish { addr: redis_addr(), channel });
    (redis, app, sut)
}

/// upstream: features/broadcasts.benchfile#scenario
/// note: correctness check only, same workload parameters; throughput belongs to the external Benchi harness (crates/cabby-bench); inherent window: --wait-broadcasts 30 s
/// `websocket-bench broadcast --concurrent 40 --sample-size 100 --step-size 200
/// --payload-padding 200 --total-steps 4 --wait-broadcasts 30`; fails if a step
/// receives less than 90% of the expected broadcasts.
#[test]
fn broadcasts() {
    let (_redis, app, sut) = start();
    let workload = Workload {
        concurrent: 40,
        sample_size: 100,
        step_size: 200,
        payload_padding: 200,
        total_steps: 4,
        wait_broadcasts: Duration::from_secs(30),
    };

    for (expected, actual) in run_workload(&sut, workload) {
        // "Missing received broadcasts: expected #{expected}, got #{actual}"
        eprintln!("received broadcasts: {actual} / {expected}");
        assert!(
            actual as f64 / expected as f64 >= 0.9,
            "Received less than 90% of expected broadcasts: {actual} / {expected}"
        );
    }
    assert!(app.errors().is_empty(), "app errors: {:?}", app.errors());
}

/// Open file descriptors and OS threads of a process (procfs).
fn resources(pid: u32) -> (usize, usize) {
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).map(|d| d.count()).unwrap_or(0);
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let threads =
        status.lines().find_map(|l| l.strip_prefix("Threads:")).and_then(|n| n.trim().parse().ok()).unwrap_or(0);
    (fds, threads)
}

/// upstream: features/goroutines.benchfile#scenario
/// note: replacement invariant for the Go-specific gops goroutine checks: server fds return to idle (+10) after load and grow at most 10% (min 2) between runs; OS threads reported only
/// The upstream assertions are Go runtime measurements (`gops stats`: at most
/// 200 goroutines after a 5 s cool-down, and no more than 10% growth between
/// two runs of `websocket-bench broadcast --concurrent 10 --sample-size 100
/// --step-size 200 --payload-padding 200 --total-steps 3`). The documented
/// replacement invariant observes the server process from outside, for any
/// implementation: after each run the clients disconnect and the server's open
/// file descriptors return to their idle level (within 10; bounded wait instead
/// of `sleep 5`), and residual descriptors after the second run exceed those
/// after the first by at most 10% (at least 2). OS threads are reported only:
/// runtimes keep worker threads after load (Go's grew 34 -> 40 here), which is
/// not a leak.
#[test]
fn goroutines() {
    let (_redis, _app, sut) = start();
    let pid = sut.process.pid();
    let workload = Workload {
        concurrent: 10,
        sample_size: 100,
        step_size: 200,
        payload_padding: 200,
        total_steps: 3,
        wait_broadcasts: Duration::from_secs(30),
    };
    let (idle_fds, _) = resources(pid);

    let results: Vec<(usize, usize)> = (0..2)
        .map(|_| {
            run_workload(&sut, workload);
            // Give some time to cool down after the benchmark (upstream: sleep 5).
            let until = Instant::now() + Duration::from_secs(30);
            loop {
                let (fds, threads) = resources(pid);
                if fds <= idle_fds + 10 || Instant::now() >= until {
                    break (fds, threads);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
        .collect();

    eprintln!("idle descriptors: {idle_fds}; after each run (descriptors, threads): {results:?}");
    for (fds, _) in &results {
        assert!(*fds <= idle_fds + 10, "Too many open descriptors after cool-down: {fds} (idle: {idle_fds})");
    }
    let (fds1, fds2) = (results[0].0, results[1].0);
    // 10% growth, with a floor of 2 descriptors for small counts.
    assert!(fds2 <= fds1 + (fds1 / 10).max(2), "Descriptor leak detected: {fds1} -> {fds2}");
}
