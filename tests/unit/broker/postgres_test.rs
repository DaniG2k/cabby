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
use cabby_server::postgres::{Metrics, PostgresBroker};
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
