//! Deviation D-070: the Postgres pub/sub subscriber (`--pubsub=postgres`), a
//! Cabby addition, through the shared subscriber subtests plus its own cases
//! (shard filtering, spillover of large payloads, reconnect and re-`LISTEN`).
//!
//! Real PostgreSQL at `CABBY_TEST_POSTGRES_URL` (required). Each test has its
//! own schema and base channel (`cabby_testkit::postgres::PgScope`). AnyCable
//! has no Postgres subscriber, so the Go arm is not observable.
//!
//! Waiting for subscriptions: `start` returns once the `LISTEN` connection
//! listens to the commands channel, and `subscribe` once the stream's shard
//! is listened to, so there is nothing left to wait for on the server.

use std::sync::Arc;

use cabby_core::common::StreamMessage;
use cabby_server::postgres::{Metrics, PostgresConfig, SHARDS, shard};
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::reference::pubsub::{Subscriber, TestHandler};

use super::subscriber_test::{self as shared, Harness, ShutdownGuard};

struct Postgres {
    scope: PgScope,
    config: PostgresConfig,
}

impl Postgres {
    fn new() -> Self {
        rust_only();
        let scope = PgScope::new("pubsub");
        let config = scope.config();
        Postgres { scope, config }
    }
}

impl Harness for Postgres {
    fn subscriber(&self, handler: &TestHandler) -> Subscriber {
        Subscriber::postgres(handler, &self.config, Metrics::none()).expect("PostgresSubscriber::new")
    }

    fn stream(&self, name: &str) -> String {
        name.to_owned()
    }

    fn internal(&self) -> String {
        format!("{}_cmd", self.config.channel)
    }

    fn wait_subscriptions(&self, _channel: &str, _count: usize) -> Result<(), String> {
        Ok(())
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
    let s1 = ShutdownGuard(Subscriber::postgres(&handler, &h.config, metrics.clone()).expect("new"));
    let s2 = ShutdownGuard(Subscriber::postgres(&other_handler, &h.config, metrics).expect("new"));
    s1.0.start().expect("start");
    s2.0.start().expect("start");
    s1.0.subscribe("big");
    s2.0.subscribe("big");

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
    let subscriber =
        ShutdownGuard(Subscriber::postgres(&handler, &h.config, Metrics::register(registry.clone())).expect("new"));
    subscriber.0.start().expect("start");
    subscriber.0.subscribe("reconnectos");

    subscriber.0.broadcast(&msg("reconnectos", "2022"));
    assert_eq!("2022", handler.receive().expect("message").data);

    let app = h.scope.pubsub_listener();
    let old = h.scope.pids(&app);
    assert_eq!(1, old.len(), "one LISTEN connection: {old:?}");
    assert_eq!(1, h.scope.terminate(&app));
    h.scope.wait_listening_again(&app, &old);

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
    let subscriber =
        ShutdownGuard(Subscriber::postgres(&handler, &config, Metrics::register(registry.clone())).expect("new"));
    subscriber.0.start().expect("start");

    std::thread::sleep(std::time::Duration::from_secs(6));
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
