//! Deviation D-070: the Postgres broadcaster (`--broadcast_adapter=postgres`),
//! a Cabby addition: broadcasts inserted into the `<prefix>broadcasts` outbox
//! (usually inside the application's transaction) or sent with a direct
//! `NOTIFY`.
//!
//! Real PostgreSQL at `CABBY_TEST_POSTGRES_URL` (required); each test has its
//! own schema and channel. AnyCable has no Postgres broadcaster, so the Go arm
//! is not observable.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cabby_server::postgres::Metrics;
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::reference::broadcast::{Broadcaster, HandlerCall, MockHandler};

/// Stop collecting after this long without a call.
const DRAIN_IDLE: Duration = Duration::from_secs(1);

struct ShutdownGuard(Broadcaster);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

fn scope() -> PgScope {
    rust_only();
    PgScope::new("broadcast")
}

fn start(scope: &PgScope, handler: &MockHandler, metrics: Metrics) -> ShutdownGuard {
    let b = Broadcaster::postgres(handler, &scope.config(), metrics).expect("PostgresBroadcaster::new");
    b.start().expect("start");
    ShutdownGuard(b)
}

fn payload(stream: &str, data: &str) -> String {
    format!("{{\"stream\":\"{stream}\",\"data\":\"{data}\"}}")
}

fn kinds(calls: &[HandlerCall]) -> Vec<&str> {
    calls.iter().map(|c| c.kind.as_str()).collect()
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: a row inserted into <prefix>broadcasts is picked up through the AFTER INSERT trigger's NOTIFY, handed to the node once (HandleBroadcast) and deleted; the broadcaster is not a fan-out one.
#[test]
fn deviation_d070_postgres_outbox_rows_are_broadcast() {
    let scope = scope();
    let handler = MockHandler::new();
    let b = start(&scope, &handler, Metrics::none());
    assert!(!b.0.is_fanout());

    scope.insert_broadcast(&payload("any_test", "123_test"));

    let calls = handler.drain(DRAIN_IDLE);
    assert_eq!(vec!["HandleBroadcast"], kinds(&calls));
    assert_eq!(payload("any_test", "123_test"), calls[0].payload);
    assert_eq!(Some("0".to_owned()), scope.scalar(&format!("SELECT count(*) FROM {}", scope.table("broadcasts"))));
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/With_multiple_subscribers
/// note: Deviation D-070: three nodes claim rows with SELECT ... FOR UPDATE SKIP LOCKED under the claim lock; 300 rows inserted in one statement and 100 more one by one are each handled exactly once and in id order (one node drains at a time, under the claim lock), and the claimed counter adds up to 400.
#[test]
fn deviation_d070_postgres_nodes_claim_each_row_once() {
    let scope = scope();
    let registry = Arc::new(cabby_server::metrics::Metrics::new(Vec::new(), 0));
    let metrics = Metrics::register(registry.clone());
    let handler = MockHandler::new();
    let _nodes: Vec<ShutdownGuard> = (0..3).map(|_| start(&scope, &handler, metrics.clone())).collect();

    scope.execute(&format!(
        "INSERT INTO {} (payload) SELECT '{{\"stream\":\"s\",\"data\":\"' || i || '\"}}' FROM generate_series(1, 300) i",
        scope.table("broadcasts")
    ));
    for i in 301..=400 {
        scope.insert_broadcast(&payload("s", &i.to_string()));
    }

    let calls = handler.drain(DRAIN_IDLE * 2);
    assert!(calls.iter().all(|c| c.kind == "HandleBroadcast"), "{calls:?}");
    let unique: HashSet<&str> = calls.iter().map(|c| c.payload.as_str()).collect();
    assert_eq!(400, calls.len(), "every row handled once");
    assert_eq!(400, unique.len(), "no duplicates");
    assert_eq!(400, registry.counter("postgres_claimed_broadcasts_total").expect("claimed metric").value());
    let order: Vec<String> = calls.iter().map(|c| c.payload.clone()).collect();
    let expected: Vec<String> = (1..=400).map(|i| payload("s", &i.to_string())).collect();
    assert_eq!(expected, order, "handled in id order across nodes (the claim lock)");
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: an insert inside a transaction that rolls back broadcasts nothing (no row and no notification); the committed insert after it is delivered alone.
#[test]
fn deviation_d070_postgres_rolled_back_inserts_broadcast_nothing() {
    let scope = scope();
    let handler = MockHandler::new();
    let _b = start(&scope, &handler, Metrics::none());

    scope.execute(&format!(
        "BEGIN; INSERT INTO {} (payload) VALUES ('{}'); ROLLBACK;",
        scope.table("broadcasts"),
        payload("s", "rolled-back")
    ));
    scope.execute(&format!(
        "BEGIN; INSERT INTO {} (payload) VALUES ('{}'); COMMIT;",
        scope.table("broadcasts"),
        payload("s", "committed")
    ));

    let calls = handler.drain(DRAIN_IDLE);
    assert_eq!(vec![payload("s", "committed")], calls.into_iter().map(|c| c.payload).collect::<Vec<_>>());
}

/// regression-for: broadcast/legacy_redis_test.go#TestLegacyRedisBroadcaster/Handles_pubsub_broadcasts
/// note: Deviation D-070: a JSON payload sent with NOTIFY <channel>_in reaches every node, and each one delivers it to its own clients (HandlePubSub, no history), so it is not duplicated through the broker.
#[test]
fn deviation_d070_postgres_direct_notify_is_delivered_locally_on_every_node() {
    let scope = scope();
    let (h1, h2) = (MockHandler::new(), MockHandler::new());
    let _b1 = start(&scope, &h1, Metrics::none());
    let _b2 = start(&scope, &h2, Metrics::none());

    scope.notify(&format!("{}_in", scope.channel), &payload("direct", "1"));

    for h in [&h1, &h2] {
        let calls = h.drain(DRAIN_IDLE);
        assert_eq!(vec!["HandlePubSub"], kinds(&calls));
        assert_eq!(payload("direct", "1"), calls[0].payload);
    }
}

/// regression-for: broadcast/legacy_redis_test.go#TestLegacyRedisBroadcasterReconnect
/// note: Deviation D-070: rows inserted before a broadcaster starts and while its LISTEN connection is down are claimed when it (re)connects; delivery continues after the reconnect.
#[test]
fn deviation_d070_postgres_rows_missed_while_disconnected_are_claimed() {
    let scope = scope();
    // Create the tables first (another node would have), then insert before this one starts.
    scope.migrate();
    scope.insert_broadcast(&payload("s", "before-start"));

    let handler = MockHandler::new();
    let _b = start(&scope, &handler, Metrics::none());
    let calls = handler.drain(DRAIN_IDLE);
    assert_eq!(vec![payload("s", "before-start")], calls.into_iter().map(|c| c.payload).collect::<Vec<_>>());

    let app = scope.broadcast_listener();
    let old = scope.pids(&app);
    assert_eq!(1, old.len(), "{old:?}");
    assert_eq!(1, scope.terminate(&app));
    scope.insert_broadcast(&payload("s", "while-down"));
    scope.wait_listening_again(&app, &old);
    scope.insert_broadcast(&payload("s", "after"));

    let got: Vec<String> = handler.drain(DRAIN_IDLE).into_iter().map(|c| c.payload).collect();
    assert_eq!(vec![payload("s", "while-down"), payload("s", "after")], got);
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: with --postgres_auto_migrate=false and no outbox table, start fails naming the missing table; nothing is created.
#[test]
fn deviation_d070_postgres_missing_tables_fail_the_start_without_auto_migrate() {
    let scope = scope();
    let mut config = scope.config();
    config.auto_migrate = false;
    let handler = MockHandler::new();
    let b = Broadcaster::postgres(&handler, &config, Metrics::none()).expect("new");
    let err = b.start().expect_err("start must fail");
    assert!(
        err.contains(&format!("table {}.cabby_broadcasts is missing", scope.schema))
            && err.contains("docs/postgres.md"),
        "{err}"
    );
    assert_eq!(None, scope.scalar(&format!("SELECT to_regclass('{}')::text", scope.table("broadcasts"))));
}

fn payloads(calls: &[HandlerCall], kind: &str) -> Vec<String> {
    calls.iter().filter(|c| c.kind == kind).map(|c| c.payload.clone()).collect()
}

fn outbox_rows(scope: &PgScope) -> Option<String> {
    scope.scalar(&format!("SELECT count(*) FROM {}", scope.table("broadcasts")))
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: while another session holds the claim lock, a node skips its round instead of waiting on the server (no stalled claim, no extra backends); once the lock is released, the rows are delivered.
#[test]
fn deviation_d070_postgres_a_held_claim_lock_does_not_stall_the_claimer() {
    let scope = scope();
    scope.migrate();
    let names = cabby_server::postgres::Names::from_config(&scope.config()).unwrap();
    let key = cabby_server::postgres::broadcast::claim_lock_key(&names);
    let holder = scope.other_session();
    holder.execute(&format!("BEGIN; SELECT pg_advisory_xact_lock({key});"));

    let handler = MockHandler::new();
    let _b = start(&scope, &handler, Metrics::none());
    scope.insert_broadcast(&payload("s", "held"));
    assert!(handler.drain(Duration::from_secs(2)).is_empty(), "nothing is claimed while the lock is held");
    let app = format!("cabby broadcast {}", scope.channel);
    assert!(scope.pids(&app).len() <= 1, "one claim connection at most: {:?}", scope.pids(&app));
    let waiting = scope.scalar(&format!(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = '{app}' AND wait_event_type = 'Lock'"
    ));
    assert_eq!(Some("0".to_owned()), waiting, "the claimer does not wait on the lock");

    holder.execute("ROLLBACK;");
    let calls = handler.drain(Duration::from_secs(3));
    assert_eq!(vec![payload("s", "held")], payloads(&calls, "HandleBroadcast"));
    assert!(scope.pids(&app).len() <= 1, "no backend pile-up: {:?}", scope.pids(&app));
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: a row that is not JSON is deleted and logged without reaching the node; the rows after it are delivered.
#[test]
fn deviation_d070_postgres_invalid_rows_are_dropped() {
    let scope = scope();
    let handler = MockHandler::new();
    let _b = start(&scope, &handler, Metrics::none());
    scope.insert_broadcast("not json");
    scope.insert_broadcast(&payload("s", "after"));

    let calls = handler.drain(DRAIN_IDLE);
    assert_eq!(vec![payload("s", "after")], payloads(&calls, "HandleBroadcast"));
    assert_eq!(1, calls.len(), "{calls:?}");
    assert_eq!(Some("0".to_owned()), outbox_rows(&scope));
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: when the node fails to handle a row, the rows handled before it are deleted, it and the rows after it stay in the outbox, and the claim is retried with backoff; nothing is lost.
#[test]
fn deviation_d070_postgres_handler_errors_keep_the_remaining_rows() {
    let scope = scope();
    scope.migrate();
    let handler = MockHandler::new();
    handler.fail_broadcasts_containing("\"bad\"", Some(1));
    scope.execute(&format!(
        "INSERT INTO {} (payload) VALUES ('{}'), ('{}'), ('{}')",
        scope.table("broadcasts"),
        payload("s", "a"),
        payload("s", "bad"),
        payload("s", "c")
    ));
    let _b = start(&scope, &handler, Metrics::none());

    let calls = handler.drain(Duration::from_secs(2));
    assert_eq!(vec![payload("s", "bad")], payloads(&calls, "HandleBroadcastFailed"));
    assert_eq!(
        vec![payload("s", "a"), payload("s", "bad"), payload("s", "c")],
        payloads(&calls, "HandleBroadcast"),
        "a once, then bad and c after the retry"
    );
    assert_eq!(Some("0".to_owned()), outbox_rows(&scope));
}

/// regression-for: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
/// note: Deviation D-070: a row the node fails to handle 5 times in a row is deleted with an error log, so it cannot block the outbox; the rows after it are delivered.
#[test]
fn deviation_d070_postgres_a_row_failing_five_times_is_dropped() {
    let scope = scope();
    scope.migrate();
    let handler = MockHandler::new();
    handler.fail_broadcasts_containing("\"bad\"", None);
    scope.insert_broadcast(&payload("s", "bad"));
    scope.insert_broadcast(&payload("s", "c"));
    let _b = start(&scope, &handler, Metrics::none());

    let calls = handler.drain(Duration::from_secs(3));
    assert_eq!(5, payloads(&calls, "HandleBroadcastFailed").len(), "{calls:?}");
    assert_eq!(vec![payload("s", "c")], payloads(&calls, "HandleBroadcast"));
    assert_eq!(Some("0".to_owned()), outbox_rows(&scope));
}
