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
//! Counting: websocket-bench counts the broadcast messages its clients receive
//! while a step waits (up to `--wait-broadcasts`) and reports "Missing received
//! broadcasts: expected N, got M" for the step. Here every broadcast carries
//! `(step, sender, n)`, each client keeps the ids it has seen, and a delivery
//! is credited to the step it belongs to, once per client: late arrivals can
//! neither count toward the next step nor be double-counted. A step's count is
//! taken when its wait ends.
//!
//! The upstream RPC application uses the gem's default `redis` broadcast
//! adapter (`REDIS_URL` set by CI) and the server its default adapters
//! (`http`, `redis`), so both use the shared Redis service here, with a
//! per-test broadcast channel and stream name prefix.

use std::collections::HashSet;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cabby_testkit::anyt_app::{AnytApp, BENCHMARK_CHANNEL};
use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::publish::{self, Target};
use cabby_testkit::raw_http;
use cabby_testkit::services::{self, RedisScope, unique};
use cabby_testkit::sut::Sut;
use cabby_testkit::{Backend, backend};
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

/// Deliveries per step, each `(client, broadcast id)` counted once.
struct Ledger {
    per_step: Vec<AtomicUsize>,
}

/// A subscribed client that records broadcast ids on a blocking reader thread
/// (no polling, so hundreds of them stay cheap).
struct Subscriber {
    thread: JoinHandle<()>,
}

fn subscriber(url: &str, ledger: Arc<Ledger>, stop: Arc<AtomicBool>) -> Result<Subscriber, String> {
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
            let mut seen = HashSet::new();
            while !stop.load(Ordering::SeqCst) {
                match ws.read() {
                    Ok(Message::Text(t)) => {
                        let v: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
                        let m = &v["message"];
                        if m["action"] != "broadcast" {
                            continue;
                        }
                        let p = &m["payload"];
                        let id = (p["step"].as_u64(), p["sender"].as_u64(), p["n"].as_u64());
                        if let (Some(step), Some(_), Some(_)) = id
                            && seen.insert(id)
                            && let Some(c) = ledger.per_step.get(step as usize)
                        {
                            c.fetch_add(1, Ordering::SeqCst);
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
    let ledger = Arc::new(Ledger { per_step: (0..w.total_steps).map(|_| AtomicUsize::new(0)).collect() });
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
                subscriber(&url, Arc::clone(&ledger), Arc::clone(&stop)).unwrap_or_else(|e| panic!("subscriber: {e}")),
            );
        }
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
        let count = || ledger.per_step[step].load(Ordering::SeqCst);
        while count() < expected && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        results.push((expected, count()));
    }
    stop.store(true, Ordering::SeqCst);
    for s in subscribers {
        let _ = s.thread.join();
    }
    drop(senders);
    results
}

/// Host and port of `CABBY_TEST_REDIS_URL` (the default user publishes).
fn redis_addr() -> std::net::SocketAddr {
    use std::net::ToSocketAddrs;
    let url = std::env::var("CABBY_TEST_REDIS_URL").expect("CABBY_TEST_REDIS_URL");
    let rest = url.trim_start_matches("redis://");
    let authority = rest.split('/').next().unwrap_or(rest);
    let hp = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
    hp.to_socket_addrs().ok().and_then(|mut a| a.next()).unwrap_or_else(|| panic!("cannot resolve {url}"))
}

/// The server and the application on the shared Redis: a per-test ACL user,
/// broadcast channel (`--redis_channel`) and stream name prefix.
fn start(extra: &[&str]) -> (RedisScope, AnytApp, Sut) {
    let redis = services::redis().scope("bench");
    let channel = redis.name("anycable");
    let app = rpc_app_with_prefix(&format!("{}:", unique("bench")));
    let rpc = format!("--rpc_host={}", app.grpc_target());
    let channel_flag = format!("--redis_channel={channel}");
    let redis_url = redis.url();
    let mut args = vec![rpc.as_str(), channel_flag.as_str()];
    args.extend_from_slice(extra);
    let sut = launch(&args, &[("REDIS_URL", &redis_url)]);
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
/// note: deliveries are reconciled per step by broadcast id (step, sender, n), once per client, instead of websocket-bench's per-step message count
/// `websocket-bench broadcast --concurrent 40 --sample-size 100 --step-size 200
/// --payload-padding 200 --total-steps 4 --wait-broadcasts 30`; fails if a step
/// receives less than 90% of the expected broadcasts.
#[test]
fn broadcasts() {
    let (_redis, app, sut) = start(&[]);
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

/// Open file descriptors of a process (procfs).
fn open_fds(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map(|d| d.count()).unwrap_or(0)
}

/// Gauges from the Prometheus endpoint (`--metrics_http=/metrics`):
/// `anycable_go_<name>[{tags}] <value>`.
fn gauges(sut: &Sut) -> std::collections::HashMap<String, u64> {
    let res = raw_http::request(sut.addr(), "GET", "/metrics", &[], None).unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("GET /metrics: {e:#}")
    });
    res.text()
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let (name, value) = l.rsplit_once(' ')?;
            let name = name.split('{').next()?.strip_prefix("anycable_go_")?;
            Some((name.to_owned(), value.trim().parse().ok()?))
        })
        .collect()
}

/// upstream: features/goroutines.benchfile#scenario
/// note: gauges come from --metrics_http with --stats_refresh_interval=1 (upstream reads goroutines with gops), so the 5 s cool-down observes fresh values
/// note: Go backend: upstream's goroutine checks (at most 200 after the cool-down, at most 10% growth between runs) on goroutines_num; every backend: clients_num, clients_uniq_num and broadcast_streams_num back at 0 within the 5 s cool-down, and server fds back near idle (+10, growth at most 10%, min 2) as a runtime-independent leak check
/// `websocket-bench broadcast --concurrent 10 --sample-size 100 --step-size 200
/// --payload-padding 200 --total-steps 3`, run twice, each followed by a
/// cool-down (upstream: `sleep 5`; here bounded at 5 s and ended as soon as
/// the server reports no clients and no streams).
#[test]
fn goroutines() {
    const IDLE_ROUTINES_MAX: u64 = 200;
    const COOL_DOWN: Duration = Duration::from_secs(5);
    let (_redis, _app, sut) = start(&["--metrics_http=/metrics", "--stats_refresh_interval=1"]);
    let pid = sut.process.pid();
    let workload = Workload {
        concurrent: 10,
        sample_size: 100,
        step_size: 200,
        payload_padding: 200,
        total_steps: 3,
        wait_broadcasts: Duration::from_secs(30),
    };
    let idle_fds = open_fds(pid);

    let results: Vec<(usize, Option<u64>)> = (0..2)
        .map(|_| {
            run_workload(&sut, workload);
            // Give some time to cool down after the benchmark
            let until = Instant::now() + COOL_DOWN;
            let idle = |g: &std::collections::HashMap<String, u64>| {
                ["clients_num", "clients_uniq_num", "broadcast_streams_num"].iter().all(|k| g.get(*k) == Some(&0))
            };
            let mut g = gauges(&sut);
            while !idle(&g) && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(100));
                g = gauges(&sut);
            }
            assert!(idle(&g), "not idle {COOL_DOWN:?} after the benchmark: {g:?}");
            (open_fds(pid), g.get("goroutines_num").copied())
        })
        .collect();

    eprintln!("idle descriptors: {idle_fds}; after each run (descriptors, goroutines): {results:?}");
    if backend() == Backend::Go {
        let goroutines: Vec<u64> = results.iter().map(|(_, g)| g.expect("goroutines_num gauge")).collect();
        for num in &goroutines {
            assert!(*num <= IDLE_ROUTINES_MAX, "Too many goroutines: {num}");
        }
        assert!(
            goroutines[1] as f64 / goroutines[0] as f64 <= 1.1,
            "Go routines leak detected: {} -> {}",
            goroutines[0],
            goroutines[1]
        );
    }
    for (fds, _) in &results {
        assert!(*fds <= idle_fds + 10, "Too many open descriptors after cool-down: {fds} (idle: {idle_fds})");
    }
    let (fds1, fds2) = (results[0].0, results[1].0);
    // 10% growth, with a floor of 2 descriptors for small counts.
    assert!(fds2 <= fds1 + (fds1 / 10).max(2), "Descriptor leak detected: {fds1} -> {fds2}");
}
