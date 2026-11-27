//! Deviation D-071: the Postgres broker (`--broker=postgres`), a Cabby
//! addition, through the memory and NATS broker cases (history by offset and
//! by time, limit, TTL, gaps, peak, sessions, shared epoch) plus its own
//! multi-node cases.
//!
//! Real PostgreSQL at `CABBY_TEST_POSTGRES_URL` (required); each test has its
//! own schema. TTLs are enforced on the database clock, which a test cannot
//! control, so the waits that let them elapse are real (the NATS tests'
//! durations); they wait for expiry, not for synchronization. AnyCable has no
//! Postgres broker, so the Go arm is not observable.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use cabby_core::broker::{Broadcaster, Broker, Config};
use cabby_core::common::{RemoteCommandMessage, StreamMessage, StreamMessageMetadata};
use cabby_server::postgres::{Metrics, PostgresBroadcaster, PostgresBroker, PostgresConfig, PostgresSubscriber};
use cabby_testkit::jwt::unix_from_now;
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::reference::broker::{TestCacheable, new_config};

/// Lets time pass on the database clock.
fn let_server_ttl_elapse(d: Duration) {
    thread::sleep(d);
}

#[derive(Default)]
struct Recorder(Mutex<Vec<StreamMessage>>);

impl Recorder {
    fn messages(&self) -> Vec<StreamMessage> {
        self.0.lock().unwrap().clone()
    }
}

impl Broadcaster for Recorder {
    fn broadcast(&self, msg: &StreamMessage) {
        self.0.lock().unwrap().push(msg.clone());
    }
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
    fn subscribe(&self, _stream: &str) {}
    fn unsubscribe(&self, _stream: &str) {}
}

fn scope() -> PgScope {
    rust_only();
    PgScope::new("broker")
}

fn broker(scope: &PgScope, config: &Config, rec: Option<Arc<Recorder>>) -> PostgresBroker {
    let rec = rec.map(|r| r as Arc<dyn Broadcaster>);
    let broker = PostgresBroker::new(rec, config, &scope.config(), Metrics::none()).expect("PostgresBroker::new");
    broker.start().expect("start");
    broker
}

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
/// regression-for: broker/memory_test.go#TestMemory_Expire
/// note: Deviation D-071: the NATS/memory expiry case against the Postgres broker; entries older than history_ttl (database clock) are no longer returned.
#[test]
fn deviation_d071_postgres_history_since_expiration() {
    let scope = scope();
    let mut config = new_config();
    config.history_ttl = 1;
    let broker = broker(&scope, &config, None);

    let start = unix_from_now(-10);

    broker.handle_broadcast(&msg("test", "a")).unwrap();
    broker.handle_broadcast(&msg("test", "b")).unwrap();

    // Stream must be expired after 1 second
    let_server_ttl_elapse(Duration::from_secs(2));

    broker.handle_broadcast(&msg("test", "c")).unwrap();
    broker.handle_broadcast(&msg("test", "d")).unwrap();

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();
    assert_eq!(2, history.len(), "{history:?}");
    assert_eq!((3, "c"), (history[0].offset, history[0].data.as_str()));
    assert_eq!((4, "d"), (history[1].offset, history[1].data.as_str()));

    let_server_ttl_elapse(Duration::from_secs(3));

    let history = broker.history_since("test", start).expect("history");
    assert!(history.unwrap_or_default().is_empty());

    // Pruning only reclaims space: the answers stay the same.
    broker.prune().expect("prune");
    let history = broker.history_since("test", start).expect("history");
    assert!(history.unwrap_or_default().is_empty());
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_with_limit
/// regression-for: broker/memory_test.go#TestMemory_Limit
#[test]
fn deviation_d071_postgres_history_since_with_limit() {
    let scope = scope();
    let mut config = new_config();
    config.history_limit = 2;
    let broker = broker(&scope, &config, None);

    let start = unix_from_now(-10);
    for data in ["a", "b", "c"] {
        broker.handle_broadcast(&msg("test", data)).unwrap();
    }

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();
    assert_eq!(2, history.len(), "{history:?}");
    assert_eq!((3, "c"), (history[1].offset, history[1].data.as_str()));

    assert_eq!(None, broker.history_since("unknown", start).expect("history"), "no stream: nothing");
}

mod test_postgres_broker_history_from {
    use super::*;

    struct Fixture {
        broker: PostgresBroker,
        _scope: PgScope,
    }

    fn fixture() -> Fixture {
        let scope = scope();
        let broker = broker(&scope, &new_config(), None);
        for data in ["y", "z", "a", "b", "c", "d", "e"] {
            broker.handle_broadcast(&msg("test", data)).unwrap();
        }
        Fixture { broker, _scope: scope }
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_current_epoch
    /// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_current_epoch
    #[test]
    fn with_current_epoch() {
        let f = fixture();
        let history = f.broker.history_from("test", &f.broker.epoch(), 4).expect("history");
        assert_eq!(3, history.len(), "{history:?}");
        assert_eq!((5, "c"), (history[0].offset, history[0].data.as_str()));
        assert_eq!((6, "d"), (history[1].offset, history[1].data.as_str()));
        assert_eq!((7, "e"), (history[2].offset, history[2].data.as_str()));
        assert!(history.iter().all(|m| m.epoch == f.broker.epoch() && m.stream == "test"));
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_new_messages
    #[test]
    fn when_no_new_messages() {
        let f = fixture();
        let history = f.broker.history_from("test", &f.broker.epoch(), 7).expect("history");
        assert_eq!(0, history.len());
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_stream
    #[test]
    fn when_no_stream() {
        let f = fixture();
        let history = f.broker.history_from("unknown", &f.broker.epoch(), 4);
        assert_eq!("stream not found", history.unwrap_err().message);
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_unknown_epoch
    /// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
    #[test]
    fn with_unknown_epoch() {
        let f = fixture();
        let err = f.broker.history_from("test", "unknown", 4).unwrap_err();
        assert_eq!(format!("unknown epoch: unknown, current: {}", f.broker.epoch()), err.message);
    }

    /// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
    /// note: Deviation D-071: an offset past the latest one is an error, with the memory broker's text.
    #[test]
    fn with_an_offset_past_the_latest() {
        let f = fixture();
        let err = f.broker.history_from("test", &f.broker.epoch(), 10).unwrap_err();
        assert_eq!("requested offset couldn't be found: 10, latest: 7", err.message);
    }
}

/// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
/// note: Deviation D-071: gaps and expiry match the memory broker: an offset older than the oldest retained one (history_limit) is an error naming the lowest offset, and a stream whose entries expired (history_ttl) is empty while it still exists.
#[test]
fn deviation_d071_postgres_history_from_reports_gaps_and_expiry() {
    let scope = scope();
    let mut config = new_config();
    config.history_limit = 3;
    config.history_ttl = 1;
    let broker = broker(&scope, &config, None);
    for data in ["1", "2", "3", "4", "5"] {
        broker.handle_broadcast(&msg("gap", data)).unwrap();
    }
    let epoch = broker.epoch();

    let err = broker.history_from("gap", &epoch, 1).unwrap_err();
    assert_eq!("requested offset couldn't be found: 1, lowest: 3", err.message);
    let err = broker.history_from("gap", &epoch, 2).unwrap_err();
    assert_eq!("requested offset couldn't be found: 2, lowest: 3", err.message, "offset 2 < lowest 3, as memory");
    let history = broker.history_from("gap", &epoch, 3).expect("history");
    assert_eq!(vec![4, 5], history.iter().map(|m| m.offset).collect::<Vec<_>>());

    let_server_ttl_elapse(Duration::from_secs(2));
    let err = broker.history_from("gap", &epoch, 3).unwrap_err();
    assert_eq!("stream is empty", err.message);
    assert_eq!(None, broker.peak("gap").expect("peak"), "no retained entries: no position, as memory");

    // The stream keeps its offsets while it exists (10 x history_ttl).
    broker.handle_broadcast(&msg("gap", "6")).unwrap();
    assert_eq!(Some(6), broker.peak("gap").expect("peak").map(|m| m.offset));
}

/// regression-for: broker/memory_test.go#TestMemory_Peak
#[test]
fn deviation_d071_postgres_peak_reports_latest_position() {
    let scope = scope();
    let broker = broker(&scope, &new_config(), None);

    assert_eq!(None, broker.peak("test").expect("peak"));

    broker.handle_broadcast(&msg("test", "a")).unwrap();
    broker.handle_broadcast(&msg("test", "b")).unwrap();

    let peak = broker.peak("test").expect("peak").expect("position");
    assert_eq!(2, peak.offset);
    assert_eq!(broker.epoch(), peak.epoch);

    assert_eq!(None, broker.peak("unknown").expect("peak"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// regression-for: broker/memory_test.go#TestMemory_RestoreSession
/// note: Deviation D-071: sessions are shared by every node and expire after sessions_ttl on the database clock (3 s here, expiry observed by polling); touch extends them.
#[test]
fn deviation_d071_postgres_sessions() {
    let scope = scope();
    let mut config = new_config();
    config.sessions_ttl = 3;

    let broker = broker(&scope, &config, None);
    broker.commit_session("test123", &TestCacheable("cache-me".into())).expect("commit");

    let another_broker = self::broker(&scope, &config, None);
    let restored = another_broker.restore_session("test123").expect("restore");
    assert_eq!(Some(b"cache-me".to_vec()), restored, "Expected to restore session data");

    // Expiry, observed by polling (the database clock decides).
    wait_until_restored_is_none(&broker, "test123");

    broker.commit_session("test345", &TestCacheable("cache-me-again".into())).expect("commit");
    assert_eq!(Some(b"cache-me-again".to_vec()), another_broker.restore_session("test345").expect("restore"));

    // Touched 1 s in: it expires 3 s after the touch, so 1 s after its original
    // expiry it is still there.
    let_server_ttl_elapse(Duration::from_secs(1));
    broker.touch_session("test345").expect("touch");
    let_server_ttl_elapse(Duration::from_secs(2));
    assert_eq!(
        Some(b"cache-me-again".to_vec()),
        another_broker.restore_session("test345").expect("restore"),
        "the touch extended it"
    );
    wait_until_restored_is_none(&another_broker, "test345");

    // Touching an expired or unknown session is not an error and revives nothing.
    broker.touch_session("test345").expect("touch");
    broker.touch_session("missing").expect("touch");
    assert_eq!(None, another_broker.restore_session("test345").expect("restore"));
}

/// Polls until `sid` no longer restores (at most 10 s).
fn wait_until_restored_is_none(broker: &PostgresBroker, sid: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while broker.restore_session(sid).expect("restore").is_some() {
        assert!(std::time::Instant::now() < deadline, "{sid} did not expire");
        thread::sleep(Duration::from_millis(100));
    }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-071: nodes starting at the same time agree on one epoch (inserted once into <prefix>meta).
#[test]
fn deviation_d071_postgres_nodes_share_the_epoch() {
    let scope = scope();
    scope.migrate();
    let config = new_config();
    let pg = scope.config();
    let brokers: Vec<PostgresBroker> = thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                s.spawn(|| {
                    let b = PostgresBroker::new(None, &config, &pg, Metrics::none()).expect("new");
                    b.start().expect("start");
                    b
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let epochs: BTreeSet<String> = brokers.iter().map(PostgresBroker::epoch).collect();
    assert_eq!(1, epochs.len(), "{epochs:?}");
    assert_eq!(4, brokers[0].epoch().len());
    assert_eq!(
        scope.scalar(&format!("SELECT value FROM {} WHERE key = 'epoch'", scope.table("meta"))),
        epochs.into_iter().next()
    );
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-071: when the meta and history tables are emptied (as PostgreSQL does to UNLOGGED tables after a crash), the next write or read creates a new epoch; old offsets are then reported as an unknown epoch and the other node picks the new epoch up.
#[test]
fn deviation_d071_postgres_epoch_rotates_when_tables_are_emptied() {
    let scope = scope();
    let rec = Arc::new(Recorder::default());
    let broker = broker(&scope, &new_config(), Some(rec.clone()));
    let other = self::broker(&scope, &new_config(), None);
    broker.handle_broadcast(&msg("s", "before")).unwrap();
    let old = broker.epoch();

    scope.execute(&format!(
        "TRUNCATE {}, {}, {}, {}",
        scope.table("meta"),
        scope.table("streams"),
        scope.table("history"),
        scope.table("sessions")
    ));

    broker.handle_broadcast(&msg("s", "after")).unwrap();
    let new = broker.epoch();
    assert_ne!(old, new, "a new epoch");
    let after = rec.messages().pop().unwrap();
    assert_eq!((1, new.as_str()), (after.offset, after.epoch.as_str()), "offsets restart with the new epoch");

    let err = other.history_from("s", &old, 1).unwrap_err();
    assert_eq!(format!("unknown epoch: {old}, current: {new}"), err.message);
    assert_eq!(new, other.refresh_epoch().unwrap());
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_current_epoch
/// note: Deviation D-071: two nodes broadcasting to one stream concurrently get distinct, gap-free offsets, and every broadcast is fanned out with its offset and the shared epoch.
#[test]
fn deviation_d071_postgres_offsets_are_monotonic_across_nodes() {
    let scope = scope();
    let (r1, r2) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let b1 = broker(&scope, &new_config(), Some(r1.clone()));
    let b2 = broker(&scope, &new_config(), Some(r2.clone()));

    thread::scope(|s| {
        for (b, tag) in [(&b1, "x"), (&b2, "y")] {
            s.spawn(move || {
                for i in 0..50 {
                    b.handle_broadcast(&msg("shared", &format!("{tag}{i}"))).unwrap();
                }
            });
        }
    });

    let mut offsets: Vec<u64> = r1.messages().iter().chain(r2.messages().iter()).map(|m| m.offset).collect();
    offsets.sort_unstable();
    assert_eq!((1..=100).collect::<Vec<u64>>(), offsets);
    for (r, tag) in [(&r1, "x"), (&r2, "y")] {
        let mine: Vec<u64> = r.messages().iter().filter(|m| m.data.starts_with(tag)).map(|m| m.offset).collect();
        assert!(mine.windows(2).all(|w| w[0] < w[1]), "a node's offsets increase: {mine:?}");
        assert!(r.messages().iter().all(|m| m.epoch == b1.epoch()));
    }
    let history = b2.history_from("shared", &b2.epoch(), 0).unwrap_err();
    assert_eq!("requested offset couldn't be found: 0, lowest: 1", history.message, "as memory");
    assert_eq!(99, b2.history_from("shared", &b2.epoch(), 1).unwrap().len());
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_current_epoch
/// note: Deviation D-071: transient messages are fanned out without an offset and not stored; presence is not supported (as with the NATS broker).
#[test]
fn deviation_d071_postgres_transient_messages_and_presence() {
    let scope = scope();
    let rec = Arc::new(Recorder::default());
    let broker = broker(&scope, &new_config(), Some(rec.clone()));
    let transient = StreamMessage {
        meta: Some(StreamMessageMetadata { transient: true, ..Default::default() }),
        ..msg("t", "typing")
    };
    broker.handle_broadcast(&transient).unwrap();
    assert_eq!(0, rec.messages()[0].offset);
    assert_eq!(None, broker.peak("t").unwrap());

    assert!(!broker.supports_presence());
    assert_eq!("presence not supported", broker.presence_info("t", None).unwrap_err().message);
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-071: database errors are broker errors (as NATS transport errors): restoring a session from an unreachable server fails with a decorated error that carries no credentials, and nothing panics.
#[test]
fn deviation_d071_postgres_database_errors_are_broker_errors() {
    rust_only();
    let port = cabby_testkit::ports::free_port();
    let mut pg = cabby_server::postgres::PostgresConfig::new();
    pg.url = format!("postgres://cabby:s3cret@127.0.0.1:{port}/cabby?sslmode=disable");
    let rec = Arc::new(Recorder::default());
    let broker = PostgresBroker::new(Some(rec.clone() as Arc<dyn Broadcaster>), &new_config(), &pg, Metrics::none())
        .expect("new");

    let start = broker.start().unwrap_err().message;
    assert!(start.starts_with("failed to start Postgres broker, cause: "), "{start}");
    let restore = broker.restore_session("sid").unwrap_err().message;
    assert!(restore.starts_with("failed to restore session from Postgres, cause: "), "{restore}");
    let write = broker.handle_broadcast(&msg("s", "x")).unwrap_err().message;
    assert!(write.starts_with("failed to add message to Postgres history: stream=s, cause: "), "{write}");
    assert!(rec.messages().is_empty(), "a failed write is not fanned out");
    for e in [start, restore, write] {
        assert!(!e.contains("s3cret"), "{e}");
    }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-071: with --postgres_auto_migrate=false, a missing table stops the start with a message naming it.
#[test]
fn deviation_d071_postgres_missing_tables_fail_the_start_without_auto_migrate() {
    let scope = scope();
    let mut pg = scope.config();
    pg.auto_migrate = false;
    let broker = PostgresBroker::new(None, &new_config(), &pg, Metrics::none()).expect("new");
    let err = broker.start().unwrap_err();
    assert!(err.message.contains(&format!("table {}.cabby_meta is missing", scope.schema)), "{}", err.message);

    scope.migrate();
    broker.start().expect("start once the tables exist");
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-071: two nodes that find the epoch row missing at the same moment both create-or-read it in one statement and agree on the result (repeated many times).
#[test]
fn deviation_d071_postgres_concurrent_epoch_creation_agrees() {
    let scope = scope();
    let a = broker(&scope, &new_config(), None);
    let b = self::broker(&scope, &new_config(), None);
    let delete = format!("DELETE FROM {} WHERE key = 'epoch'", scope.table("meta"));
    for i in 0..50 {
        scope.execute(&delete);
        let barrier = std::sync::Barrier::new(2);
        let (ea, eb) = thread::scope(|s| {
            let ha = s.spawn(|| {
                barrier.wait();
                a.refresh_epoch()
            });
            let hb = s.spawn(|| {
                barrier.wait();
                b.refresh_epoch()
            });
            (ha.join().unwrap(), hb.join().unwrap())
        });
        let (ea, eb) =
            (ea.unwrap_or_else(|e| panic!("round {i}: {e}")), eb.unwrap_or_else(|e| panic!("round {i}: {e}")));
        assert_eq!(ea, eb, "round {i}");
    }
}

/// A server that accepts connections and never answers (a black-holed
/// database); returns its port.
fn black_hole() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        let mut held = Vec::new();
        for s in listener.incoming().flatten() {
            held.push(s);
        }
    });
    port
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-071: with a database that accepts connections but never answers, 8 concurrent calls all fail within one 10 s deadline (connect included, not one deadline per queued caller), and calls right after a failed connect fail fast.
#[test]
fn deviation_d071_postgres_calls_to_an_unresponsive_database_are_bounded() {
    rust_only();
    let mut pg = cabby_server::postgres::PostgresConfig::new();
    pg.url = format!("postgres://cabby:x@127.0.0.1:{}/cabby?sslmode=disable", black_hole());
    let broker = PostgresBroker::new(None, &new_config(), &pg, Metrics::none()).expect("new");

    let started = std::time::Instant::now();
    let errors: Vec<String> = thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|i| {
                s.spawn({
                    let broker = &broker;
                    move || broker.restore_session(&format!("sid{i}")).unwrap_err().message
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let took = started.elapsed();
    assert!(took < Duration::from_secs(13), "8 concurrent calls took {took:?}: {errors:?}");
    assert!(errors.iter().all(|e| e.starts_with("failed to restore session from Postgres, cause: ")), "{errors:?}");

    let started = std::time::Instant::now();
    let err = broker.restore_session("again").unwrap_err().message;
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "no fail-fast after a failed connect: {:?} {err}",
        started.elapsed()
    );
}

/// regression-for: broker/memory_test.go#TestMemory_Expire
/// note: Deviation D-071: pruning deletes in batches of 10000 rows, each in its own transaction with a server-side statement timeout, so a large backlog of expired history, streams and sessions is cleared instead of timing out as a whole.
#[test]
fn deviation_d071_postgres_pruning_clears_a_large_backlog_in_batches() {
    let scope = scope();
    let mut config = new_config();
    config.history_ttl = 1;
    let broker = broker(&scope, &config, None);
    scope.execute(&format!(
        "INSERT INTO {streams} (stream, max_seq, deadline_sec, last_ts_sec) SELECT 'old' || i, 1, 0, 0 FROM generate_series(1, 12000) i; \
         INSERT INTO {history} (stream, seq, ts_sec, data) SELECT 'bulk', i, 0, 'x' FROM generate_series(1, 25000) i; \
         INSERT INTO {sessions} (sid, data, expires_at_ms) SELECT 's' || i, '\\x00', 0 FROM generate_series(1, 25000) i;",
        streams = scope.table("streams"),
        history = scope.table("history"),
        sessions = scope.table("sessions"),
    ));
    assert!(broker.prune().expect("prune"), "this node held the pruning lock");
    for table in ["streams", "history", "sessions"] {
        assert_eq!(
            Some("0".to_owned()),
            scope.scalar(&format!("SELECT count(*) FROM {}", scope.table(table))),
            "{table}"
        );
    }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-071: when the server refuses new connections (here a role at its CONNECTION LIMIT) but one pool connection is open, calls keep succeeding on it; only a pool with no open connection fails fast.
#[test]
fn deviation_d071_postgres_a_slot_that_cannot_connect_does_not_fail_the_pool() {
    let scope = scope();
    scope.migrate();
    let mut pg = scope.config();
    pg.url = scope.role_url(&[]);
    pg.auto_migrate = false;
    let role = format!("r_{}", scope.channel);
    let broker = PostgresBroker::new(None, &new_config(), &pg, Metrics::none()).expect("new");
    broker.start().expect("start");
    broker.commit_session("sid", &TestCacheable("kept".into())).expect("commit");
    // The broker's other connections are gone; no new one is allowed.
    scope.execute(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{role}' AND application_name NOT LIKE 'cabby broker c%'; \
         ALTER ROLE \"{role}\" CONNECTION LIMIT 1;"
    ));

    let mut failures = Vec::new();
    for _ in 0..40 {
        if let Err(e) = broker.restore_session("sid") {
            failures.push(e.message);
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(failures.is_empty(), "{} of 40 calls failed: {:?}", failures.len(), failures.first());
}

/// regression-for: broker/memory_test.go#TestMemory_Expire
/// note: Deviation D-071: a stream that is written (its deadline extended) while pruning is about to delete it as expired is kept: the delete checks the deadline again on the row it locks.
#[test]
fn deviation_d071_postgres_pruning_keeps_a_stream_written_meanwhile() {
    let scope = scope();
    let broker = broker(&scope, &new_config(), None);
    let streams = scope.table("streams");
    scope.execute(&format!(
        "INSERT INTO {streams} (stream, max_seq, deadline_sec, last_ts_sec) VALUES ('live', 1, 0, 0)"
    ));

    // A broadcast in progress on another node: it holds the row while extending the deadline.
    let writer = scope.other_session();
    writer.execute(&format!(
        "BEGIN; UPDATE {streams} SET max_seq = max_seq + 1, deadline_sec = EXTRACT(EPOCH FROM now())::bigint + 3000 WHERE stream = 'live';"
    ));
    thread::scope(|s| {
        let pruning = s.spawn(|| broker.prune());
        let app = format!("cabby broker-maintenance {}", scope.channel);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while scope.scalar(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = '{app}' AND wait_event_type = 'Lock'"
        )) != Some("1".to_owned())
        {
            assert!(std::time::Instant::now() < deadline, "pruning did not reach the locked row");
            thread::sleep(Duration::from_millis(20));
        }
        writer.execute("COMMIT;");
        assert!(pruning.join().unwrap().expect("prune"));
    });
    assert_eq!(
        Some("2".to_owned()),
        scope.scalar(&format!("SELECT max_seq FROM {streams} WHERE stream = 'live'")),
        "the stream written meanwhile is kept"
    );
}

/// An adapter starting on its own thread.
struct Starting(std::sync::mpsc::Receiver<(Result<(), String>, Duration)>);

impl Starting {
    fn new(start: impl FnOnce() -> Result<(), String> + Send + 'static) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let started = std::time::Instant::now();
        thread::spawn(move || {
            let result = start();
            let _ = tx.send((result, started.elapsed()));
        });
        Self(rx)
    }

    fn broker(config: PostgresConfig) -> Self {
        Self::new(move || {
            PostgresBroker::new(None, &new_config(), &config, Metrics::none())
                .and_then(|b| b.start())
                .map_err(|e| e.message)
        })
    }

    /// The outcome and the time it took.
    fn wait(self, within: Duration) -> (Result<(), String>, Duration) {
        self.0.recv_timeout(within).expect("the start hangs")
    }
}

/// Waits until a migration in this scope's schema waits for a lock.
fn wait_migration_blocked(scope: &PgScope) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while scope.scalar(&format!(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'cabby migrate' AND wait_event_type = 'Lock' AND query LIKE '%{}%'",
        scope.schema
    )) == Some("0".to_owned())
    {
        assert!(std::time::Instant::now() < deadline, "the migration did not wait for the lock");
        thread::sleep(Duration::from_millis(20));
    }
}

fn plain_broker(scope: &PgScope) -> PostgresBroker {
    let broker = PostgresBroker::new(None, &new_config(), &scope.plain_config(), Metrics::none()).expect("new");
    broker.start().expect("start");
    broker
}

struct NoNode;

impl cabby_core::pubsub::Handler for NoNode {
    fn broadcast(&self, _msg: &StreamMessage) {}
    fn execute_remote_command(&self, _cmd: &RemoteCommandMessage) {}
}

impl cabby_core::broadcast::Handler for NoNode {
    fn handle_broadcast(&self, _json: &[u8]) -> Result<(), cabby_core::broadcast::BroadcastError> {
        Ok(())
    }
    fn handle_pubsub(&self, _json: &[u8]) {}
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviations D-070 and D-071: nodes starting with existing tables (three nodes with all three adapters at once, migrating or only checking) don't wait for sessions writing to them, nor for a VACUUM, and the broadcasts of running nodes go through meanwhile.
#[test]
fn deviation_d071_postgres_starts_do_not_wait_for_locks_on_existing_tables() {
    use cabby_core::broadcast::Broadcaster as _;
    use cabby_core::pubsub::Subscriber as _;

    let scope = scope();
    let running = plain_broker(&scope);
    running.handle_broadcast(&msg("s", "before")).unwrap();
    let holder = scope.other_session();
    holder.execute("BEGIN");
    for table in ["history", "sessions", "pubsub_overflow", "streams", "meta"] {
        let table = scope.table(table);
        // A writer's lock and VACUUM's.
        holder.execute(&format!(
            "LOCK TABLE {table} IN ROW EXCLUSIVE MODE; LOCK TABLE {table} IN SHARE UPDATE EXCLUSIVE MODE;"
        ));
    }

    for auto_migrate in [true, false] {
        let mut config = scope.plain_config();
        config.auto_migrate = auto_migrate;
        let mut starting = Vec::new();
        for _ in 0..3 {
            let (c1, c2) = (config.clone(), config.clone());
            starting.push(Starting::broker(config.clone()));
            starting.push(Starting::new(move || {
                let s = PostgresSubscriber::new(Arc::new(NoNode), &c1, Metrics::none()).map_err(|e| e.0)?;
                s.start(std::sync::mpsc::channel().0).map_err(|e| e.0)
            }));
            starting.push(Starting::new(move || {
                let b = PostgresBroadcaster::new(Arc::new(NoNode), &c2, Metrics::none()).map_err(|e| e.0)?;
                b.start(std::sync::mpsc::channel().0).map_err(|e| e.0)
            }));
        }
        running.handle_broadcast(&msg("s", "while starting")).expect("a running node broadcasts");
        for start in starting {
            let (result, _) = start.wait(Duration::from_secs(20));
            assert_eq!(Ok(()), result, "auto_migrate={auto_migrate}");
        }
        running.handle_broadcast(&msg("s", "after")).expect("a running node broadcasts");
    }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-071: a start that needs a lock another session holds (here: a missing index) fails after 5 s, naming the statement; the broadcasts of running nodes that queue behind it are delayed, not failed. Once the lock is gone the next start creates the index.
#[test]
fn deviation_d071_postgres_start_fails_when_a_lock_it_needs_is_held() {
    let scope = scope();
    let running = plain_broker(&scope);
    running.handle_broadcast(&msg("s", "before")).unwrap();
    scope.execute(&format!("DROP INDEX \"{}\".cabby_history_ts_sec", scope.schema));
    let holder = scope.other_session();
    holder.execute(&format!("BEGIN; LOCK TABLE {} IN ROW EXCLUSIVE MODE;", scope.table("history")));

    let starting = Starting::broker(scope.plain_config());
    wait_migration_blocked(&scope);
    running.handle_broadcast(&msg("s", "delayed")).expect("a running node's broadcast is only delayed");
    let (result, took) = starting.wait(Duration::from_secs(30));
    let error = result.expect_err("the start needs the lock");
    assert_eq!(
        format!(
            "failed to start Postgres broker, cause: timed out after 5s waiting for a lock to run CREATE INDEX IF NOT \
             EXISTS \"cabby_history_ts_sec\" ON \"{}\".\"cabby_history\": another session holds a conflicting lock \
             (see pg_locks and pg_stat_activity)",
            scope.schema
        ),
        error
    );
    assert!(took >= Duration::from_secs(5), "{took:?}");

    holder.execute("ROLLBACK");
    plain_broker(&scope);
    let index = format!("SELECT to_regclass('\"{}\".cabby_history_ts_sec') IS NOT NULL", scope.schema);
    assert_eq!(Some("t".to_owned()), scope.scalar(&index), "the next start creates the index");
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-071: a start waits at most 15 s for another node's migration, and one that only checks the tables (--postgres_auto_migrate=false) at most 5 s for a table another session has locked exclusively; both then fail with an error.
#[test]
fn deviation_d071_postgres_start_does_not_wait_forever_for_a_migration_or_a_table() {
    let scope = scope();
    plain_broker(&scope);
    let key = format!("cabby:{}.cabby_", scope.schema)
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, b| (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3));
    let holder = scope.other_session();
    holder.execute(&format!("BEGIN; SELECT pg_advisory_xact_lock({});", key.cast_signed()));
    let expected = |cause: &str| {
        format!(
            "failed to start Postgres broker, cause: {cause}: another session holds a conflicting lock (see pg_locks and \
             pg_stat_activity)"
        )
    };
    let (result, took) = Starting::broker(scope.plain_config()).wait(Duration::from_secs(40));
    assert_eq!(
        Err(expected("timed out after 15s waiting for a lock to run the migration (one node at a time)")),
        result
    );
    assert!(took >= Duration::from_secs(15), "{took:?}");
    holder.execute("ROLLBACK");

    holder.execute(&format!("BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE;", scope.table("meta")));
    let mut config = scope.plain_config();
    config.auto_migrate = false;
    let (result, _) = Starting::broker(config).wait(Duration::from_secs(40));
    assert_eq!(Err(expected("timed out after 5s waiting for a lock to read the schema version")), result);
}

/// regression-for: broker/memory_test.go#TestMemory_Expire
/// note: Deviation D-071: a pruning step that fails (here: the three steps deleting history, while another session holds a SHARE lock on the table) doesn't stop the others: each gives up after its lock wait, the expired sessions are deleted all the same, and the failed steps are reported by name; the next pass, once the lock is gone, deletes the history and the expired streams. The lock wait is 2 s; here 200 ms (PostgresBroker::with_prune_lock_timeout), against the 10 s statement timeout a step waited for before.
#[test]
fn deviation_d071_postgres_a_failing_pruning_step_does_not_stop_the_others() {
    let scope = scope();
    let mut config = new_config();
    config.history_limit = 1;
    let broker = PostgresBroker::new(None, &config, &scope.config(), Metrics::none())
        .expect("PostgresBroker::new")
        .with_prune_lock_timeout(Duration::from_millis(200));
    broker.start().expect("start");
    let (streams, history, sessions) = (scope.table("streams"), scope.table("history"), scope.table("sessions"));
    scope.execute(&format!(
        "INSERT INTO {streams} (stream, max_seq, deadline_sec, last_ts_sec) VALUES ('kept', 100, EXTRACT(EPOCH FROM now())::bigint + 3000, EXTRACT(EPOCH FROM now())::bigint); \
         INSERT INTO {streams} (stream, max_seq, deadline_sec, last_ts_sec) SELECT 'old' || i, 1, 0, 0 FROM generate_series(1, 10) i; \
         INSERT INTO {history} (stream, seq, ts_sec, data) SELECT 'kept', i, EXTRACT(EPOCH FROM now())::bigint, 'x' FROM generate_series(1, 100) i; \
         INSERT INTO {sessions} (sid, data, expires_at_ms) SELECT 's' || i, '\\x00', 0 FROM generate_series(1, 10) i;"
    ));
    let count = |table: &str| scope.scalar(&format!("SELECT count(*) FROM {table}"));
    let holder = scope.other_session();
    holder.execute(&format!("BEGIN; LOCK TABLE {history} IN SHARE MODE;"));

    let started = std::time::Instant::now();
    let err = broker.prune().expect_err("the history steps fail");
    let took = started.elapsed();
    assert!(err.message.contains("history beyond history_limit"), "{}", err.message);
    assert_eq!(Some("0".to_owned()), count(&sessions), "expired sessions are deleted all the same");
    assert_eq!(Some("11".to_owned()), count(&streams), "expired streams go with their history: kept");
    assert_eq!(Some("100".to_owned()), count(&history));
    assert!(took < Duration::from_secs(3), "pruning took {took:?}");

    holder.execute("ROLLBACK;");
    assert!(broker.prune().expect("prune"), "this node held the pruning lock");
    assert_eq!(Some("1".to_owned()), count(&history), "history beyond the limit, on the next pass");
    assert_eq!(Some("1".to_owned()), count(&streams), "and the expired streams");
}
