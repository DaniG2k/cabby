//! Pub/sub and broker behavior when Redis or NATS misbehave (independent review
//! of November 2026, `docs/review/2026-11-fable-review.md`): a stalled server
//! (H2), NATS without JetStream (H3), Redis Cluster (H6, M9). They run against
//! both servers; Go is the reference.
//!
//! Setup shared by the scenarios: a public server (`--public`: no RPC, `$pubsub`
//! streams) taking broadcasts over HTTP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cabby_testkit::cable_client::{CableClient, ClientError, Options};
use cabby_testkit::fake_redis_cluster::FakeRedisCluster;
use cabby_testkit::fault_proxy::FaultProxy;
use cabby_testkit::raw_http;
use cabby_testkit::services::{self, NatsServer};
use cabby_testkit::sut::Sut;
use serde_json::json;

/// How long the broadcasters flood a stalled server.
const FLOOD: Duration = Duration::from_secs(8);
/// Concurrent broadcasters.
const WORKERS: usize = 8;
/// How long one broadcast request may take before the client gives up.
const REQUEST_DEADLINE: Duration = Duration::from_secs(2);

fn public_server(args: &[&str]) -> Sut {
    let mut all = vec!["--public", "--broadcast_adapter=http"];
    all.extend_from_slice(args);
    Sut::start(&all)
}

fn identifier(stream: &str) -> String {
    json!({"channel": "$pubsub", "stream_name": stream}).to_string()
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

fn broadcast_addr(sut: &Sut) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port))
}

/// Posts a broadcast; the status, or `None` if no response came within `within`.
fn broadcast(sut: &Sut, stream: &str, data: &str, within: Duration) -> Option<u16> {
    let body = json!({"stream": stream, "data": data}).to_string();
    let headers = [("Content-Type", "application/json")];
    raw_http::request_until(
        broadcast_addr(sut),
        "POST",
        "/_broadcast",
        &headers,
        Some(body.as_bytes()),
        Instant::now() + within,
    )
    .ok()
    .map(|r| r.status)
}

/// Waits until the client receives a broadcast whose data is `text`, skipping
/// anything else (stale messages, pings).
fn received_within(c: &mut CableClient, text: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        match c.receive_within(Duration::from_millis(250)) {
            Ok(m) if m["message"] == text => return true,
            Ok(_) | Err(ClientError::Timeout) => {}
            Err(e) => panic!("client error while waiting for delivery: {e}"),
        }
    }
    false
}

/// Broadcasts `text` every 500 ms until the client receives it.
fn delivered_within(sut: &Sut, c: &mut CableClient, stream: &str, text: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let _ = broadcast(sut, stream, text, Duration::from_secs(5));
        if received_within(c, text, Duration::from_millis(500)) {
            return true;
        }
    }
    false
}

/// Resident memory of a process (Linux), in KiB.
fn rss_kib(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| l.strip_prefix("VmRSS:").and_then(|v| v.split_whitespace().next()?.parse().ok()))
        })
        .unwrap_or(0)
}

struct Flood {
    /// Broadcasts answered (201) within the request deadline.
    accepted: usize,
    /// Broadcasts without an answer within the deadline.
    held: usize,
}

/// [`WORKERS`] clients broadcast ~4 KB messages to `stream` for [`FLOOD`].
fn flood(sut: &Sut, stream: &str) -> Flood {
    let accepted = Arc::new(AtomicUsize::new(0));
    let held = Arc::new(AtomicUsize::new(0));
    let end = Instant::now() + FLOOD;
    let pad = "x".repeat(4000);
    std::thread::scope(|s| {
        for w in 0..WORKERS {
            let (accepted, held, pad) = (Arc::clone(&accepted), Arc::clone(&held), pad.clone());
            s.spawn(move || {
                let mut i = 0;
                while Instant::now() < end {
                    let data = json!({"w": w, "i": i, "pad": pad}).to_string();
                    match broadcast(sut, stream, &data, REQUEST_DEADLINE) {
                        Some(201) => accepted.fetch_add(1, Ordering::SeqCst),
                        Some(status) => panic!("broadcast rejected: {status}"),
                        None => held.fetch_add(1, Ordering::SeqCst),
                    };
                    i += 1;
                }
            });
        }
    });
    Flood { accepted: accepted.load(Ordering::SeqCst), held: held.load(Ordering::SeqCst) }
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Review finding H2: Go publishes to Redis synchronously, so a stalled Redis holds broadcast requests back; Cabby queued every publication without bound (25 s stall: RSS 14 MB to 1 GB, 26 s stale deliveries).
/// note: Redis is frozen behind a fault proxy (nothing forwarded, sockets open) while 8 clients broadcast ~4 KB messages for 8 s with a 2 s request deadline.
/// note: Asserted on both servers: requests are held back after a bounded number (Go: none; Cabby: its 1024-message queue), memory stays bounded, and delivery resumes once Redis answers again.
#[test]
fn redis_publishing_holds_broadcasts_back_while_redis_stalls() {
    let redis = services::redis().scope("stallredis");
    let proxy = FaultProxy::start(services::redis().addr());
    let sut = public_server(&["--pubsub=redis", &format!("--redis_url={}", redis.url_via(proxy.addr()))]);
    let stream = redis.name("flood");
    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    assert!(delivered_within(&sut, &mut c, &stream, "warm", Duration::from_secs(15)), "no delivery before the stall");

    let rss_before = rss_kib(sut.process.pid());
    proxy.freeze();
    let f = flood(&sut, &stream);
    let rss_growth = rss_kib(sut.process.pid()).saturating_sub(rss_before);
    proxy.thaw();

    assert!(f.accepted <= 3000, "{} broadcasts accepted while Redis stalled (unbounded queue)", f.accepted);
    assert!(f.held >= WORKERS, "broadcast requests were not held back ({} held)", f.held);
    assert!(rss_growth < 100 * 1024, "memory grew by {rss_growth} KiB while Redis stalled");
    assert!(
        delivered_within(&sut, &mut c, &stream, "after", Duration::from_secs(40)),
        "no delivery after Redis recovered\nSUT output:\n{}",
        sut.process.output()
    );
    // Logged once per connection (Cabby used to repeat it every 10 s).
    let connected = sut.process.output().lines().filter(|l| l.contains(" INF connected ")).count();
    assert_eq!(1, connected, "SUT output:\n{}", sut.process.output());
}

/// regression-for: pubsub/nats_test.go#TestNATSCommon/Broadcast
/// note: Review finding H2: Go's NATS Publish blocks once the connection's buffers are full (stalled server); Cabby queued every publication without bound, and subscribe/unsubscribe waited behind that queue.
/// note: NATS is frozen behind a fault proxy while 8 clients broadcast ~4 KB messages for 8 s with a 2 s request deadline.
/// note: Asserted on both servers: requests are held back after a bounded number (Go: about 1000, its buffers; Cabby: its 1024-message queue plus the client's), memory stays bounded, and after recovery a new subscription gets its broadcasts at once.
#[test]
fn nats_publishing_holds_broadcasts_back_while_nats_stalls() {
    let nats = NatsServer::start();
    let proxy = FaultProxy::start(SocketAddr::from(([127, 0, 0, 1], nats.port)));
    let sut = public_server(&["--pubsub=nats", &format!("--nats_servers=nats://{}", proxy.addr())]);
    let stream = services::unique("natsflood");
    let mut c = connect(&sut);
    subscribe(&mut c, &stream);
    assert!(delivered_within(&sut, &mut c, &stream, "warm", Duration::from_secs(15)), "no delivery before the stall");

    let rss_before = rss_kib(sut.process.pid());
    proxy.freeze();
    let f = flood(&sut, &stream);
    let rss_growth = rss_kib(sut.process.pid()).saturating_sub(rss_before);
    proxy.thaw();

    assert!(f.accepted <= 8000, "{} broadcasts accepted while NATS stalled (unbounded queue)", f.accepted);
    assert!(f.held >= WORKERS, "broadcast requests were not held back ({} held)", f.held);
    assert!(rss_growth < 150 * 1024, "memory grew by {rss_growth} KiB while NATS stalled");

    // A subscription made after recovery is not stuck behind the backlog.
    let fresh = services::unique("natsfresh");
    let mut c2 = connect(&sut);
    subscribe(&mut c2, &fresh);
    assert!(
        delivered_within(&sut, &mut c2, &fresh, "fresh", Duration::from_secs(10)),
        "a new subscription got nothing after NATS recovered\nSUT output:\n{}",
        sut.process.output()
    );
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Review finding H3: upstream reports a JetStream initialization that fails for good (after 5 retries) to the runner, which exits 1 with the error; Cabby kept running and kept every later broadcast in a growing backlog.
/// note: The NATS server runs without JetStream; the retries take about 30-60 s (upstream backoff).
#[test]
fn nats_broker_without_jetstream_exits_with_the_error() {
    let nats = NatsServer::start();
    let mut sut = public_server(&["--broker=nats", "--pubsub=nats", &format!("--nats_servers={}", nats.url())]);
    let status = sut.process.wait_exit(Duration::from_secs(120)).unwrap_or_else(|e| {
        panic!("the server did not exit: {e:#}\nSUT output:\n{}", sut.process.output());
    });
    let output = sut.process.output();
    assert_eq!(Some(1), status.code(), "SUT output:\n{output}");
    assert!(output.contains("WRN JetStream initialization failed"), "SUT output:\n{output}");
    assert!(
        output.contains(
            "JetStream is unavailable, cause: failed to connect to JetStream KV, cause: failed to create bucket: _anycable_, cause: nats: API error: code=503 err_code=10076 description=jetstream not enabled"
        ),
        "SUT output:\n{output}"
    );
}

/// Subscribes to `n` streams, broadcasts to each concurrently and returns the
/// streams whose broadcast never arrived.
fn missing_cluster_deliveries(sut: &Sut, prefix: &str, n: usize) -> Vec<String> {
    let streams: Vec<String> = (0..n).map(|i| format!("{prefix}-{i}")).collect();
    let mut c = connect(sut);
    for s in &streams {
        subscribe(&mut c, s);
    }
    // Let the subscriptions reach the node before publishing.
    std::thread::sleep(Duration::from_secs(1));
    std::thread::scope(|scope| {
        for s in &streams {
            scope.spawn(move || {
                assert_eq!(Some(201), broadcast(sut, s, s, Duration::from_secs(10)), "broadcast to {s}");
            });
        }
    });
    let mut missing: std::collections::BTreeSet<String> = streams.iter().cloned().collect();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !missing.is_empty() && Instant::now() < deadline {
        match c.receive_within(Duration::from_millis(250)) {
            Ok(m) => {
                if let Some(text) = m["message"].as_str() {
                    missing.remove(text);
                }
            }
            Err(ClientError::Timeout) => {}
            Err(e) => panic!("client error: {e}"),
        }
    }
    missing.into_iter().collect()
}

/// regression-for: redis/config_test.go#TestCluster
/// note: Review finding H6: Go publishes each broadcast with its own PUBLISH, routed by the channel's slot; Cabby pipelined a batch of channels in one request, which the cluster client rejects across slots (CROSSSLOT), and dropped the batch.
/// note: A fake single-node cluster (all slots) answers each PUBLISH after 100 ms, so that publications queue up and get batched.
#[test]
fn redis_cluster_publishes_every_broadcast() {
    let cluster = FakeRedisCluster::start(Duration::from_millis(100));
    let port = cluster.addr().port();
    let sut =
        public_server(&["--pubsub=redis", &format!("--redis_url=redis://127.0.0.1:{port},redis://localhost:{port}")]);
    let missing = missing_cluster_deliveries(&sut, "room", 20);
    assert!(missing.is_empty(), "not delivered: {missing:?}\nSUT output:\n{}", sut.process.output());
    let published = cluster.published();
    for i in 0..20 {
        assert!(published.contains(&format!("room-{i}")), "room-{i} never published: {published:?}");
    }
}

/// regression-for: redis/config_test.go#TestBasic
/// note: Review finding M9: with a single Redis URL, rueidis probes the server with CLUSTER SLOTS (CLUSTER SHARDS from Redis 7) and uses a cluster client when it is a cluster node; Cabby always used a plain connection.
#[test]
fn redis_cluster_node_given_as_a_single_url_is_probed() {
    let cluster = FakeRedisCluster::start(Duration::ZERO);
    let sut = public_server(&["--pubsub=redis", &format!("--redis_url=redis://{}", cluster.addr())]);
    let missing = missing_cluster_deliveries(&sut, "single", 5);
    assert!(missing.is_empty(), "not delivered: {missing:?}\nSUT output:\n{}", sut.process.output());
    let commands = cluster.commands();
    assert!(commands.iter().any(|c| c == "CLUSTER SLOTS"), "no cluster probe: {commands:?}");
}
