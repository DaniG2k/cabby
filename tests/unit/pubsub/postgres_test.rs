//! Deviation D-070: the Postgres pub/sub subscriber (`--pubsub=postgres`), a
//! Cabby addition, through the shared subscriber subtests plus its own cases
//! (shard filtering, spillover of large payloads, reconnect and re-`LISTEN`).
//!
//! Real PostgreSQL at `CABBY_TEST_POSTGRES_URL` (required). Each test has its
//! own schema and base channel (`cabby_testkit::postgres::PgScope`). AnyCable
//! has no Postgres subscriber, so the Go arm is not observable.
//!
//! Waiting for subscriptions: `subscribe` only queues the shard's `LISTEN`
//! and returns, so the harness waits on each subscriber's
//! [`ListenProbe`] until its shards (or, for `internal`, all its channels)
//! are listened to.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cabby_core::common::StreamMessage;
use cabby_server::postgres::pubsub::ListenProbe;
use cabby_server::postgres::{Metrics, PostgresConfig, SHARDS, shard};
use cabby_testkit::fault_proxy::FaultProxy;
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::reference::pubsub::{Subscriber, TestHandler};

use super::subscriber_test::{self as shared, Harness, ShutdownGuard};

/// Bound for every wait on the subscribers.
const WAIT: Duration = Duration::from_secs(10);

struct Postgres {
    scope: PgScope,
    config: PostgresConfig,
    probes: Mutex<Vec<ListenProbe>>,
}

impl Postgres {
    fn new() -> Self {
        rust_only();
        let scope = PgScope::new("pubsub");
        let config = scope.config();
        Postgres { scope, config, probes: Mutex::new(Vec::new()) }
    }

    /// A subscriber with its own metrics; its probe joins the harness's.
    fn subscriber_with(&self, handler: &TestHandler, config: &PostgresConfig, metrics: Metrics) -> Subscriber {
        let (s, probe) = Subscriber::postgres(handler, config, metrics).expect("PostgresSubscriber::new");
        self.probes.lock().unwrap().push(probe);
        s
    }

    /// Waits until every subscriber subscribed to `stream` listens to its shard.
    fn wait(&self, stream: &str) {
        self.wait_subscriptions(stream, 1).expect("LISTEN");
    }
}

impl Harness for Postgres {
    fn subscriber(&self, handler: &TestHandler) -> Subscriber {
        self.subscriber_with(handler, &self.config, Metrics::none())
    }

    fn stream(&self, name: &str) -> String {
        name.to_owned()
    }

    fn internal(&self) -> String {
        format!("{}_cmd", self.config.channel)
    }

    /// Waits on each subscriber's probe: the commands channel for
    /// `internal`, the stream's shard otherwise (subscribers not subscribed
    /// to it have nothing to wait for; delivery is filtered locally, so an
    /// unsubscribe needs no wait either).
    fn wait_subscriptions(&self, channel: &str, _count: usize) -> Result<(), String> {
        let probes = self.probes.lock().unwrap().clone();
        let ready = if channel == self.internal() {
            probes.iter().all(|p| p.wait_ready(WAIT))
        } else {
            probes.iter().all(|p| p.wait_stream(channel, WAIT))
        };
        if ready { Ok(()) } else { Err(format!("{channel} is not listened to within {WAIT:?}")) }
    }
}

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

mod test_postgres_common {
    use super::*;

    /// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
    /// note: Deviation D-070: the shared subscriber subtests against the Postgres subscriber (not observable on Go: AnyCable has no Postgres pub/sub).
    #[test]
    fn broadcast() {
        shared::broadcast(&Postgres::new());
    }

    /// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast_commands
    #[test]
    fn broadcast_commands() {
        shared::broadcast_commands(&Postgres::new());
    }

    /// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
    /// note: Deviation D-070: two subscribers on one database are two nodes; each delivers only its own streams.
    #[test]
    fn subscribe_broadcast() {
        shared::subscribe_broadcast(&Postgres::new());
    }

    /// regression-for: pubsub/redis_test.go#TestRedisCommon/Re-transmit_commands
    #[test]
    fn retransmit_commands() {
        shared::retransmit_commands(&Postgres::new());
    }

    /// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast_-_Unsubscribe_-_Broadcast
    #[test]
    fn subscribe_broadcast_unsubscribe_broadcast() {
        shared::subscribe_broadcast_unsubscribe_broadcast(&Postgres::new());
    }
}

/// Two streams hashing into the same shard share its channel; a node
/// subscribed to one of them never delivers the other.
fn same_shard_pair() -> (String, String) {
    let first = "shard-a".to_owned();
    let other = (0..10_000)
        .map(|i| format!("shard-b-{i}"))
        .find(|s| shard(s) == shard(&first))
        .expect("a stream in the same shard");
    assert!(shard(&first) < SHARDS);
    (first, other)
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
/// note: Deviation D-070: streams share 16 LISTEN channels; messages for streams in a listened shard that are not subscribed locally are dropped by the receiver.
#[test]
fn deviation_d070_postgres_shards_deliver_only_subscribed_streams() {
    let h = Postgres::new();
    let (handler, other_handler) = (TestHandler::new(), TestHandler::new());
    let s1 = ShutdownGuard(h.subscriber(&handler));
    let s2 = ShutdownGuard(h.subscriber(&other_handler));
    s1.0.start().expect("start");
    s2.0.start().expect("start");

    let (a, b) = same_shard_pair();
    s1.0.subscribe(&a);
    s2.0.subscribe(&b);
    h.wait(&a);
    h.wait(&b);

    s2.0.broadcast(&msg(&b, "for-b"));
    s2.0.broadcast(&msg(&a, "for-a"));

    let got = handler.receive().expect("node 1 gets its stream");
    assert_eq!((a.as_str(), "for-a"), (got.stream.as_str(), got.data.as_str()));
    assert!(handler.receive().is_none(), "node 1 must not deliver {b}");

    let got = other_handler.receive().expect("node 2 gets its stream");
    assert_eq!((b.as_str(), "for-b"), (got.stream.as_str(), got.data.as_str()));
    assert!(other_handler.receive().is_none(), "node 2 must not deliver {a}");
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Deviation D-070: an envelope over 7500 bytes is stored in <prefix>pubsub_overflow and the NOTIFY carries its row ID; both nodes receive the full message and the spillover is counted.
#[test]
fn deviation_d070_postgres_large_payloads_spill_over() {
    let h = Postgres::new();
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let metrics = Metrics::register(registry.clone());
    let (handler, other_handler) = (TestHandler::new(), TestHandler::new());
    let s1 = ShutdownGuard(h.subscriber_with(&handler, &h.config, metrics.clone()));
    let s2 = ShutdownGuard(h.subscriber_with(&other_handler, &h.config, metrics));
    s1.0.start().expect("start");
    s2.0.start().expect("start");
    s1.0.subscribe("big");
    s2.0.subscribe("big");
    h.wait("big");

    let data = format!("{{\"text\":\"{}\"}}", "x".repeat(10 * 1024));
    s1.0.broadcast(&msg("big", &data));

    for handler in [&handler, &other_handler] {
        let got = handler.receive().expect("the spilled message");
        assert_eq!(data, got.data);
    }
    assert_eq!(1, registry.counter("postgres_spillover_total").expect("spillover metric").value());
    let rows = h.scope.scalar(&format!("SELECT count(*) FROM {}", h.scope.table("pubsub_overflow")));
    assert_eq!(Some("1".to_owned()), rows, "the payload went through the overflow table");

    // Small messages still go inline.
    s1.0.broadcast(&msg("big", "small"));
    assert_eq!("small", handler.receive().expect("message").data);
    assert_eq!(1, registry.counter("postgres_spillover_total").unwrap().value());
}

/// regression-for: pubsub/redis_test.go#TestRedisReconnect
/// note: Deviation D-070: the LISTEN connection is terminated on the server (pg_terminate_backend); it reconnects, issues LISTEN again for the commands channel and every shard in use, and messages flow again. The reconnect is counted.
#[test]
fn deviation_d070_postgres_reconnects_and_listens_again() {
    let h = Postgres::new();
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let handler = TestHandler::new();
    let subscriber = ShutdownGuard(h.subscriber_with(&handler, &h.config, Metrics::register(registry.clone())));
    subscriber.0.start().expect("start");
    subscriber.0.subscribe("reconnectos");
    h.wait("reconnectos");

    subscriber.0.broadcast(&msg("reconnectos", "2022"));
    assert_eq!("2022", handler.receive().expect("message").data);

    let app = h.scope.pubsub_listener();
    let old = h.scope.pids(&app);
    assert_eq!(1, old.len(), "one LISTEN connection: {old:?}");
    assert_eq!(1, h.scope.terminate(&app));
    h.scope.wait_listening_again(&app, &old);
    h.wait("reconnectos");

    subscriber.0.broadcast(&msg("reconnectos", "2023"));
    assert_eq!("2023", handler.receive().expect("message after the reconnect").data);

    subscriber.0.broadcast_command(&cabby_core::common::RemoteCommandMessage {
        command: "disconnect".into(),
        payload: "{}".into(),
    });
    assert_eq!("disconnect", handler.receive_command().expect("command after the reconnect").command);
    assert_eq!(1, registry.counter("postgres_reconnects_total").expect("reconnects metric").value());
}

/// regression-for: pubsub/redis_test.go#TestRedisReconnect
/// note: Needs PostgreSQL 14 or newer (idle_session_timeout).
/// note: Deviation D-070: when the server keeps closing the LISTEN connection right after it connects (here a role with idle_session_timeout = 200ms), reconnects back off (250 ms, then about 2 s, 4 s, ...) instead of looping; the backoff resets only after a connection stayed up for 30 s.
#[test]
fn deviation_d070_postgres_listen_reconnects_back_off_when_connections_keep_dropping() {
    let h = Postgres::new();
    h.scope.migrate();
    let mut config = h.config.clone();
    config.url = h.scope.role_url(&[("idle_session_timeout", "200ms")]);
    config.auto_migrate = false;
    config.keepalive_interval = 0;
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let handler = TestHandler::new();
    let subscriber = ShutdownGuard(h.subscriber_with(&handler, &config, Metrics::register(registry.clone())));
    subscriber.0.start().expect("start");

    // An observation window, not a synchronization: count reconnects in 6 s.
    std::thread::sleep(Duration::from_secs(6));
    let reconnects = registry.counter("postgres_reconnects_total").expect("reconnects metric").value();
    assert!((1..=4).contains(&reconnects), "{reconnects} reconnects in 6 s");
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Deviation D-070: a spilled message followed by a small one in the same publish batch arrive in publish order (both statements are prepared before a batch is sent).
#[test]
fn deviation_d070_postgres_spilled_and_inline_messages_keep_their_order() {
    let h = Postgres::new();
    let handler = TestHandler::new();
    let s = ShutdownGuard(h.subscriber(&handler));
    s.0.start().expect("start");
    s.0.subscribe("order");
    h.wait("order");
    s.0.broadcast(&msg("order", "warm-up"));
    assert_eq!("warm-up", handler.receive().expect("message").data);

    let big = format!("{{\"text\":\"{}\"}}", "x".repeat(9000));
    let mut sent = Vec::new();
    for round in 0..10 {
        for i in 0..10 {
            let data = if i == 0 { format!("{big}{round}") } else { format!("small-{round}-{i}") };
            s.0.broadcast(&msg("order", &data));
            sent.push(data);
        }
    }
    let received: Vec<String> = sent.iter().map(|_| handler.receive().expect("message").data).collect();
    assert!(received == sent, "out of order: {:?}", received.iter().map(|d| &d[..d.len().min(12)]).collect::<Vec<_>>());
}

/// Starts a subscriber whose connections go through `proxy`.
fn proxied(h: &Postgres, proxy: &FaultProxy, handler: &TestHandler, metrics: Metrics) -> ShutdownGuard {
    let mut config = h.config.clone();
    config.url = h.scope.url_via(proxy.addr());
    let s = ShutdownGuard(h.subscriber_with(handler, &config, metrics));
    s.0.start().expect("start");
    s
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
/// note: Deviation D-070: subscribe and unsubscribe only queue the LISTEN/UNLISTEN for the LISTEN connection and return, so a stalled connection holds no caller (nor the broker's stream tracker) back; the probe sees the shard listened to once the connection answers again.
#[test]
fn deviation_d070_postgres_subscribe_does_not_wait_for_the_server() {
    let h = Postgres::new();
    let proxy = FaultProxy::start(h.scope.addr());
    let handler = TestHandler::new();
    let s = proxied(&h, &proxy, &handler, Metrics::none());

    proxy.freeze();
    let started = std::time::Instant::now();
    for i in 0..SHARDS * 2 {
        s.0.subscribe(&format!("frozen-{i}"));
    }
    s.0.unsubscribe("frozen-0");
    assert!(started.elapsed() < Duration::from_millis(500), "subscribe waited: {:?}", started.elapsed());
    proxy.thaw();

    h.wait("frozen-1");
    s.0.broadcast(&msg("frozen-1", "live"));
    assert_eq!("live", handler.receive().expect("message").data);
}

/// regression-for: pubsub/redis_test.go#TestRedisReconnect
/// note: Deviation D-070: a LISTEN connection that stops answering (a black-holed TCP connection) fails its SELECT 1 heartbeat within the keepalive interval and is replaced; messages flow again once the network is back.
#[test]
fn deviation_d070_postgres_heartbeat_timeout_replaces_a_silent_connection() {
    let h = Postgres::new();
    let proxy = FaultProxy::start(h.scope.addr());
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let handler = TestHandler::new();
    let s = proxied(&h, &proxy, &handler, Metrics::register(registry.clone()));
    s.0.subscribe("beat");
    h.wait("beat");

    proxy.freeze();
    let errors = || registry.counter("postgres_errors_total").map_or(0, |c| c.value());
    let deadline = std::time::Instant::now() + WAIT;
    while errors() == 0 {
        assert!(std::time::Instant::now() < deadline, "the heartbeat did not time out");
        std::thread::sleep(Duration::from_millis(50));
    }
    proxy.thaw();

    let deadline = std::time::Instant::now() + WAIT * 2;
    while registry.counter("postgres_reconnects_total").map_or(0, |c| c.value()) == 0 {
        assert!(std::time::Instant::now() < deadline, "no reconnect");
        std::thread::sleep(Duration::from_millis(50));
    }
    h.wait("beat");
    s.0.broadcast(&msg("beat", "after"));
    assert_eq!("after", handler.receive().expect("message after the reconnect").data);
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Deviation D-070: a notification whose overflow row is gone (expired after 60 s, or deleted) is skipped with a warning; later messages are delivered.
#[test]
fn deviation_d070_postgres_an_expired_overflow_row_is_skipped() {
    let h = Postgres::new();
    let handler = TestHandler::new();
    let s = ShutdownGuard(h.subscriber(&handler));
    s.0.start().expect("start");
    s.0.subscribe("gone");
    h.wait("gone");

    let channel = format!("{}_{}", h.config.channel, shard("gone"));
    h.scope.notify(&channel, "{\"t\":\"ref\",\"id\":987654321}");
    s.0.broadcast(&msg("gone", "next"));
    assert_eq!("next", handler.receive().expect("the message after the missing row").data);
    assert!(handler.receive().is_none());
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast_-_Unsubscribe_-_Broadcast
/// note: Deviation D-070: of two streams in one shard, the one unsubscribed is no longer delivered while the other still is (the shard stays listened to).
#[test]
fn deviation_d070_postgres_unsubscribing_one_stream_of_a_shard_keeps_the_other() {
    let h = Postgres::new();
    let handler = TestHandler::new();
    let s = ShutdownGuard(h.subscriber(&handler));
    s.0.start().expect("start");
    let (a, b) = same_shard_pair();
    s.0.subscribe(&a);
    s.0.subscribe(&b);
    h.wait(&a);
    s.0.unsubscribe(&b);

    s.0.broadcast(&msg(&b, "dropped"));
    s.0.broadcast(&msg(&a, "kept"));
    assert_eq!("kept", handler.receive().expect("message").data);
    assert!(handler.receive().is_none(), "the unsubscribed stream is not delivered");
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
/// note: Deviation D-070: a burst of 20000 notifications arriving while a new shard is listened to doesn't stall the LISTEN connection: the LISTEN completes, there is no reconnect, and every notification is delivered or counted in postgres_dropped_total.
#[test]
fn deviation_d070_postgres_a_notification_burst_does_not_stall_listen() {
    let h = Postgres::new();
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let handler = TestHandler::new();
    let s = ShutdownGuard(h.subscriber_with(&handler, &h.config, Metrics::register(registry.clone())));
    s.0.start().expect("start");
    s.0.subscribe("s");
    h.wait("s");
    let other = (0..1000).map(|i| format!("o{i}")).find(|n| shard(n) != shard("s")).unwrap();
    let channel = format!("{}_{}", h.config.channel, shard("s"));

    h.scope.execute(&format!(
        "SELECT count(pg_notify('{channel}', '{{\"t\":\"s\",\"m\":{{\"stream\":\"s\",\"data\":\"' || i || '\"}}}}')) FROM generate_series(1, 20000) i"
    ));
    let started = std::time::Instant::now();
    s.0.subscribe(&other);
    h.wait(&other);
    let took = started.elapsed();

    // Every notification ends up delivered or counted as dropped; a pause in
    // deliveries (a busy machine) doesn't end the count, only the deadline does.
    let count = |name: &str| registry.counter(name).map_or(0, |c| c.value());
    let mut delivered = 0u64;
    let deadline = std::time::Instant::now() + WAIT * 3;
    while delivered + count("postgres_dropped_total") < 20000 && std::time::Instant::now() < deadline {
        if handler.receive().is_some() {
            delivered += 1;
        }
    }
    assert!(took < Duration::from_secs(5), "the LISTEN took {took:?}");
    assert_eq!(0, count("postgres_reconnects_total"), "no reconnect");
    assert_eq!(20000, delivered + count("postgres_dropped_total"), "delivered {delivered}");
    assert!(handler.receive().is_none(), "nothing beyond the 20000");
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Broadcast
/// note: Deviation D-070: start opens the publish connection (and prepares its statements) before it returns, as it does the LISTEN connection, so the first broadcast after start doesn't wait for a connection.
#[test]
fn deviation_d070_postgres_start_opens_the_publish_connection() {
    let h = Postgres::new();
    let handler = TestHandler::new();
    let s = ShutdownGuard(h.subscriber(&handler));
    s.0.start().expect("start");
    let publishers = h.scope.pids(&format!("cabby pubsub {}", h.config.channel));
    assert_eq!(1, publishers.len(), "one publish connection open when start returns: {publishers:?}");
}
