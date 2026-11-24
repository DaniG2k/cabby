//! Deviation D-069: the Redis broker (`--broker=redis`), a Cabby addition.
//!
//! The memory and NATS broker tests of this package are the contract: the
//! same history, session and epoch scenarios run here against a real Redis
//! (`CABBY_TEST_REDIS_URL`, required: no silent skip), plus what is specific
//! to sharing one Redis between nodes (per-stream epochs that rotate when a
//! stream's state is lost or its Redis instance changes, key TTLs, failures).
//! Each test has its own Redis scope: an ACL user and a key prefix.
//!
//! History and session TTLs are enforced on the Redis server's clock, which a
//! test cannot control: TTL tests use 2 s TTLs and wait for the expiry by
//! polling, with a deadline. AnyCable rejects `--broker=redis`, so these cases
//! are not observable on Go.

use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use cabby_core::broker::{Broker, Config};
use cabby_core::common::{StreamMessage, StreamMessageMetadata};
use cabby_testkit::clock::ManualClock;
use cabby_testkit::jwt::unix_from_now;
use cabby_testkit::redis_broker::{self as rb, Recorder, RedisBroker};
use cabby_testkit::reference::broker::{TestCacheable, new_memory_broker};
use cabby_testkit::services::{self, RedisScope};

/// The longest a test waits for Redis to expire a key.
const EXPIRY_DEADLINE: Duration = Duration::from_secs(10);

fn scope() -> RedisScope {
    services::redis().scope("rbroker")
}

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

fn started(scope: &RedisScope, config: &Config) -> RedisBroker {
    let broker = rb::new_broker(scope, config, None);
    broker.start().expect("start");
    broker
}

/// A started broker that records what it fans out.
fn recorded(scope: &RedisScope, config: &Config) -> (RedisBroker, Arc<Recorder>) {
    let recorder = Recorder::new();
    let broker = rb::new_broker(scope, config, Some(recorder.clone()));
    broker.start().expect("start");
    (broker, recorder)
}

/// Broadcasts `data` and returns the message as it was fanned out.
fn send(broker: &RedisBroker, recorder: &Recorder, stream: &str, data: &str) -> StreamMessage {
    broker.handle_broadcast(&msg(stream, data)).expect("broadcast");
    recorder.messages().pop().expect("fanned out")
}

/// Waits (polling, with a deadline) until `done` holds.
fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + EXPIRY_DEADLINE;
    while !done() {
        assert!(Instant::now() < deadline, "{what} within {EXPIRY_DEADLINE:?}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn offsets_and_data(history: &[StreamMessage]) -> Vec<(u64, &str)> {
    history.iter().map(|m| (m.offset, m.data.as_str())).collect()
}

fn ttl(scope: &RedisScope, key: &str) -> i64 {
    scope.command(&["TTL", key]).expect("TTL").as_int().expect("TTL reply")
}

fn hist_key(scope: &RedisScope, stream: &str) -> String {
    format!("{}:{{s:{stream}}}:hist", scope.prefix)
}

fn meta_key(scope: &RedisScope, stream: &str) -> String {
    format!("{}:{{s:{stream}}}:meta", scope.prefix)
}

fn meta_field(scope: &RedisScope, stream: &str, field: &str) -> Option<String> {
    scope.command(&["HGET", &meta_key(scope, stream), field]).expect("HGET").as_text()
}

/// The stream's current epoch, as `peak` reports it.
fn epoch_of(broker: &RedisBroker, stream: &str) -> String {
    broker.peak(stream).expect("peak").expect("position").epoch
}

mod history_from {
    use super::*;

    /// Upstream's NATS fixture: y z a b c d e on stream `test`.
    fn fixture(scope: &RedisScope) -> (RedisBroker, String) {
        let (broker, recorder) = recorded(scope, &Config::new());
        for data in ["y", "z", "a", "b", "c", "d", "e"] {
            broker.handle_broadcast(&msg("test", data)).expect("broadcast");
        }
        let sent = recorder.messages();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7], sent.iter().map(|m| m.offset).collect::<Vec<_>>());
        let epoch = sent[0].epoch.clone();
        assert!(sent.iter().all(|m| m.epoch == epoch), "one epoch: {sent:?}");
        assert_eq!(epoch, epoch_of(&broker, "test"));
        (broker, epoch)
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_current_epoch
    /// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_current_epoch
    /// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
    #[test]
    fn with_current_epoch() {
        rb::rust_only();
        let scope = scope();
        let (broker, epoch) = fixture(&scope);

        let history = broker.history_from("test", &epoch, 4).expect("history");

        assert_eq!(vec![(5, "c"), (6, "d"), (7, "e")], offsets_and_data(&history));
        assert!(history.iter().all(|m| m.epoch == epoch && m.stream == "test" && m.meta.is_none()));
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_new_messages
    /// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
    #[test]
    fn when_no_new_messages() {
        rb::rust_only();
        let scope = scope();
        let (broker, epoch) = fixture(&scope);
        assert_eq!(Ok(vec![]), broker.history_from("test", &epoch, 7));
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_stream
    /// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
    #[test]
    fn when_no_stream() {
        rb::rust_only();
        let scope = scope();
        let (broker, epoch) = fixture(&scope);
        assert_eq!("stream not found", broker.history_from("unknown", &epoch, 4).unwrap_err().message);
    }

    /// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_unknown_epoch
    /// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
    /// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
    #[test]
    fn with_unknown_epoch() {
        rb::rust_only();
        let scope = scope();
        let (broker, epoch) = fixture(&scope);
        let e = broker.history_from("test", "unknown", 4).unwrap_err();
        assert_eq!(format!("unknown epoch: unknown, current: {epoch}"), e.message);
    }

    /// regression-for: broker/memory_test.go#TestMemory_Limit
    /// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
    /// note: Deviation D-069: an offset trimmed out of the history (or not reached yet) is unavailable, with the memory broker's errors; not_observable on Go: AnyCable rejects --broker=redis.
    #[test]
    fn trimmed_and_future_offsets_are_unavailable() {
        rb::rust_only();
        let scope = scope();
        let mut config = Config::new();
        config.history_limit = 2;
        let broker = started(&scope, &config);
        for data in ["a", "b", "c", "d", "e"] {
            broker.handle_broadcast(&msg("test", data)).expect("broadcast");
        }
        let epoch = epoch_of(&broker, "test");

        let error = |offset| broker.history_from("test", &epoch, offset).unwrap_err().message;
        assert_eq!("requested offset couldn't be found: 2, lowest: 4", error(2));
        assert_eq!("requested offset couldn't be found: 3, lowest: 4", error(3));
        assert_eq!(vec![(5, "e")], offsets_and_data(&broker.history_from("test", &epoch, 4).expect("history")));
        assert_eq!("requested offset couldn't be found: 6, latest: 5", error(6));
    }
}

/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_current_epoch
/// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
/// note: Deviation D-069: for every history limit and offset, the Redis broker answers history_from as the memory broker does (the same messages or the same error); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn history_from_matches_the_memory_broker() {
    rb::rust_only();
    let scope = scope();
    for limit in [1usize, 2, 3, 10] {
        let mut config = Config::new();
        config.history_limit = limit;
        let memory = new_memory_broker(None, &config, Arc::new(ManualClock::new()));
        let redis = started(&scope, &config);
        let stream = format!("limit{limit}");
        for data in ["a", "b", "c", "d", "e"] {
            memory.add(&stream, data);
            redis.handle_broadcast(&msg(&stream, data)).expect("broadcast");
        }
        let epoch = epoch_of(&redis, &stream);
        for offset in 0..8 {
            let simplify = |h: Vec<StreamMessage>| h.into_iter().map(|m| (m.offset, m.data)).collect::<Vec<_>>();
            let expected = memory.history_from(&stream, &memory.epoch(), offset).map(simplify);
            let actual = redis.history_from(&stream, &epoch, offset).map(simplify);
            assert_eq!(expected, actual, "limit {limit}, offset {offset}");
        }
    }
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_with_limit
/// regression-for: broker/memory_test.go#TestMemory_Limit
/// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn history_since_with_limit() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_limit = 2;
    let broker = started(&scope, &config);

    let start = unix_from_now(-10);
    for data in ["a", "b", "c"] {
        broker.handle_broadcast(&msg("test", data)).expect("broadcast");
    }

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();
    assert_eq!(vec![(2, "b"), (3, "c")], offsets_and_data(&history));
    assert_eq!(Ok(None), broker.history_since("unknown", start), "no stream: nothing, as the memory broker");
}

/// regression-for: broker/memory_test.go#TestMemory_Limit
/// note: Deviation D-069: `MAXLEN ~` trims approximately (whole stream nodes); reads still return exactly history_limit messages; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn history_limit_is_exact_despite_approximate_trimming() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_limit = 2;
    let broker = started(&scope, &config);
    for i in 1..=300 {
        broker.handle_broadcast(&msg("test", &format!("m{i}"))).expect("broadcast");
    }

    let xlen = scope.command(&["XLEN", &hist_key(&scope, "test")]).expect("XLEN").as_int().unwrap_or(0);
    assert!(xlen > 2, "Redis kept {xlen} entries");
    let history = broker.history_since("test", unix_from_now(-10)).expect("history").unwrap_or_default();
    assert_eq!(vec![(299, "m299"), (300, "m300")], offsets_and_data(&history));
    let epoch = epoch_of(&broker, "test");
    assert_eq!(
        "requested offset couldn't be found: 290, lowest: 299",
        broker.history_from("test", &epoch, 290).unwrap_err().message
    );
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
/// regression-for: broker/memory_test.go#TestMemory_Expire
/// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
/// note: Deviation D-069: entries expire after history_ttl on the Redis server's clock; the stream's offsets and epoch outlive them (10 x history_ttl), as in the memory broker; 2 s TTLs, expiry awaited by polling; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn history_since_expiration() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_ttl = 2;
    let broker = started(&scope, &config);

    let start = unix_from_now(-10);
    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "b")).expect("broadcast");
    let epoch = epoch_of(&broker, "test");

    eventually("a and b expire", || broker.history_since("test", start) == Ok(Some(vec![])));

    broker.handle_broadcast(&msg("test", "c")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "d")).expect("broadcast");

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();
    assert_eq!(vec![(3, "c"), (4, "d")], offsets_and_data(&history));
    assert_eq!(epoch, epoch_of(&broker, "test"), "the epoch outlives the entries");
    assert_eq!(
        "requested offset couldn't be found: 1, lowest: 3",
        broker.history_from("test", &epoch, 1).unwrap_err().message
    );

    eventually("c and d expire", || broker.history_since("test", start) == Ok(Some(vec![])));

    // The entries are gone, the stream (its offsets and epoch) is not.
    assert_eq!("stream is empty", broker.history_from("test", &epoch, 4).unwrap_err().message);
    assert_eq!(Ok(None), broker.peak("test"));
    broker.handle_broadcast(&msg("test", "e")).expect("broadcast");
    let peak = broker.peak("test").expect("peak").expect("position");
    assert_eq!((5, epoch), (peak.offset, peak.epoch), "offsets continue in the same epoch");
}

/// regression-for: broker/memory_test.go#TestMemory_Peak
/// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn peak_reports_the_latest_position() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent) = recorded(&scope, &Config::new());

    assert_eq!(Ok(None), broker.peak("test"));

    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "b")).expect("broadcast");

    let peak = broker.peak("test").expect("peak").expect("position");
    assert_eq!(2, peak.offset);
    assert_eq!(sent.messages()[1].epoch, peak.epoch);

    assert_eq!(Ok(None), broker.peak("unknown"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// regression-for: broker/memory_test.go#TestMemory_RestoreSession
/// note: Deviation D-069: sessions are shared through Redis and expire after sessions_ttl on its clock (2 s here, expiry awaited by polling); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn sessions() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.sessions_ttl = 2;

    let broker = started(&scope, &config);
    broker.commit_session("test123", &TestCacheable("cache-me".into())).expect("commit");

    let another_broker = started(&scope, &config);
    let restored = another_broker.restore_session("test123").expect("restore");
    assert_eq!(Some(b"cache-me".to_vec()), restored, "Expected to restore session data");

    eventually("the session expires", || broker.restore_session("test123") == Ok(None));

    broker.commit_session("test345", &TestCacheable("cache-me-again".into())).expect("commit");
    assert_eq!(Ok(Some(b"cache-me-again".to_vec())), another_broker.restore_session("test345"));

    // Touched after 1 s, the session outlives its first 2 s.
    let committed = Instant::now();
    thread::sleep(Duration::from_secs(1));
    broker.touch_session("test345").expect("touch");
    thread::sleep(Duration::from_secs(2).saturating_sub(committed.elapsed()) + Duration::from_millis(300));
    assert_eq!(Ok(Some(b"cache-me-again".to_vec())), another_broker.restore_session("test345"));

    eventually("the touched session expires", || another_broker.restore_session("test345") == Ok(None));

    // Unknown sessions: nothing to restore, nothing to touch.
    assert_eq!(Ok(None), broker.restore_session("unknown"));
    assert_eq!(Ok(()), broker.touch_session("unknown"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
/// note: Deviation D-069: the key layout and its TTLs (history_ttl for the history, 10 x history_ttl for the stream's offset and epoch, sessions_ttl); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn keys_and_their_ttls() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_ttl = 60;
    config.sessions_ttl = 30;
    let (broker, sent) = recorded(&scope, &config);

    let m = send(&broker, &sent, "chat/1", "a");
    broker.commit_session("sid1", &TestCacheable("state".into())).expect("commit");

    let hist = ttl(&scope, &hist_key(&scope, "chat/1"));
    assert!((55..=60).contains(&hist), "history TTL {hist}");
    let meta = ttl(&scope, &meta_key(&scope, "chat/1"));
    assert!((595..=600).contains(&meta), "offset and epoch TTL {meta}");
    let sess = ttl(&scope, &format!("{}:sess:sid1", scope.prefix));
    assert!((25..=30).contains(&sess), "session TTL {sess}");
    assert_eq!(Some("1".to_owned()), meta_field(&scope, "chat/1", "offset"));
    assert_eq!(Some(m.epoch.clone()), meta_field(&scope, "chat/1", "epoch"));
    assert_eq!(6, m.epoch.len());
    let instance = meta_field(&scope, "chat/1", "instance").expect("instance");
    let (run_id, replid) = instance.split_once(':').expect("run_id:master_replid");
    assert_eq!((40, 40), (run_id.len(), replid.len()), "{instance}");
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-069: two brokers on one Redis share each stream's epoch and offsets, the history and the sessions; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn two_brokers_share_one_redis() {
    rb::rust_only();
    let scope = scope();
    let config = Config::new();
    let (a, sent) = recorded(&scope, &config);
    let b = started(&scope, &config);

    for data in ["a", "b", "c"] {
        a.handle_broadcast(&msg("test", data)).expect("broadcast");
    }
    let epoch = sent.messages()[0].epoch.clone();

    assert_eq!(vec![(2, "b"), (3, "c")], offsets_and_data(&b.history_from("test", &epoch, 1).expect("history")));
    let since = b.history_since("test", unix_from_now(-10)).expect("history").unwrap_or_default();
    assert_eq!(3, since.len());
    assert_eq!(Some((3, epoch.clone())), b.peak("test").expect("peak").map(|m| (m.offset, m.epoch)));

    b.handle_broadcast(&msg("test", "d")).expect("broadcast");
    assert_eq!(vec![(4, "d")], offsets_and_data(&a.history_from("test", &epoch, 3).expect("history")));

    a.commit_session("s1", &TestCacheable("from-a".into())).expect("commit");
    assert_eq!(Ok(Some(b"from-a".to_vec())), b.restore_session("s1"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom
/// note: Deviation D-069: concurrent broadcasts from two brokers to one stream get distinct, gap-free offsets in one epoch; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn concurrent_broadcasts_from_two_brokers_get_distinct_offsets() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_limit = 1000;
    let (a, sent_a) = recorded(&scope, &config);
    let (b, sent_b) = recorded(&scope, &config);

    thread::scope(|s| {
        for (broker, tag) in [(&a, "a"), (&b, "b")] {
            s.spawn(move || {
                for i in 0..100 {
                    broker.handle_broadcast(&msg("test", &format!("{tag}{i}"))).expect("broadcast");
                }
            });
        }
    });

    let mut all: Vec<StreamMessage> = sent_a.messages().into_iter().chain(sent_b.messages()).collect();
    all.sort_by_key(|m| m.offset);
    assert_eq!((1..=200).collect::<Vec<u64>>(), all.iter().map(|m| m.offset).collect::<Vec<_>>());
    assert!(all.iter().all(|m| m.epoch == all[0].epoch), "one epoch");
    let history = a.history_since("test", 0).expect("history").unwrap_or_default();
    let stored: Vec<(u64, String)> = history.into_iter().map(|m| (m.offset, m.data)).collect();
    assert_eq!(all.into_iter().map(|m| (m.offset, m.data)).collect::<Vec<_>>(), stored);
}

/// A stream after node A broadcast a, b, c on it, and the fields of its state.
fn three_messages(scope: &RedisScope) -> (RedisBroker, Arc<Recorder>, String) {
    let (broker, sent) = recorded(scope, &Config::new());
    for data in ["a", "b", "c"] {
        broker.handle_broadcast(&msg("test", data)).expect("broadcast");
    }
    let epoch = sent.messages()[2].epoch.clone();
    (broker, sent, epoch)
}

/// Rewrites the Redis instance stored with the stream, as if it had been
/// written by another Redis process (`run_id`) or replication history
/// (`master_replid`).
fn change_instance(scope: &RedisScope, stream: &str, run_id: bool) {
    let instance = meta_field(scope, stream, "instance").expect("instance");
    let (r, replid) = instance.split_once(':').expect("run_id:master_replid");
    let other = |s: &str| s.chars().map(|c| if c == '0' { '1' } else { '0' }).collect::<String>();
    let changed = if run_id { format!("{}:{replid}", other(r)) } else { format!("{r}:{}", other(replid)) };
    scope.command(&["HSET", &meta_key(scope, stream), "instance", &changed]).expect("HSET");
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
/// note: Deviation D-069: after Redis restarts from a snapshot (a new run_id, the offset counter back at the snapshot), the stream's next broadcast gets a new epoch, so reissued offsets never pair with the old epoch; clients holding the old epoch get unknown epoch and resync; the entries kept from before are not served under the new epoch.
/// note: The restart is simulated on the shared test Redis: the stream's keys are copied after b and copied back after c (COPY ... REPLACE), and the stored instance gets another run_id; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_restart_from_a_snapshot_rotates_the_stream_epoch() {
    rb::rust_only();
    let scope = scope();
    let metrics = rb::metrics();
    let sent = Recorder::new();
    let broker = rb::try_new_broker(
        &scope.url(),
        &Config::new(),
        &rb::options(&scope),
        Some(sent.clone()),
        Some(metrics.clone()),
    )
    .expect("broker");
    broker.start().expect("start");
    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "b")).expect("broadcast");
    // A snapshot of the stream's keys after b.
    let (meta, hist) = (meta_key(&scope, "test"), hist_key(&scope, "test"));
    let (snap_meta, snap_hist) = (format!("{}:snapshot:meta", scope.prefix), format!("{}:snapshot:hist", scope.prefix));
    scope.command(&["COPY", &meta, &snap_meta]).expect("COPY");
    scope.command(&["COPY", &hist, &snap_hist]).expect("COPY");
    broker.handle_broadcast(&msg("test", "c")).expect("broadcast");
    let old = sent.messages()[2].epoch.clone();

    // Redis restarts from the snapshot: c is gone, the counter is back at 2,
    // and the process (run_id) is new.
    scope.command(&["COPY", &snap_meta, &meta, "REPLACE"]).expect("COPY");
    scope.command(&["COPY", &snap_hist, &hist, "REPLACE"]).expect("COPY");
    change_instance(&scope, "test", true);

    let d = send(&broker, &sent, "test", "d");
    assert_eq!(3, d.offset, "the counter continues from the snapshot");
    assert_ne!(old, d.epoch, "a new epoch for the reissued offset");
    assert_eq!(1, metrics.counter("redis_broker_epoch_rotations_total").unwrap().value());

    let e = broker.history_from("test", &old, 2).unwrap_err();
    assert_eq!(format!("unknown epoch: {old}, current: {}", d.epoch), e.message);
    let since = broker.history_since("test", 0).expect("history").unwrap_or_default();
    assert_eq!(vec![(3, "d")], offsets_and_data(&since), "only entries of the new epoch");
    assert_eq!(Ok(vec![]), broker.history_from("test", &d.epoch, 3));

    let e2 = send(&broker, &sent, "test", "e");
    assert_eq!((4, d.epoch.clone()), (e2.offset, e2.epoch), "the new epoch stays");
    assert_eq!(1, metrics.counter("redis_broker_epoch_rotations_total").unwrap().value());
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-069: after a failover (a new master_replid), the stream's next broadcast gets a new epoch; simulated by changing the stored replication ID; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_failover_rotates_the_stream_epoch() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent, old) = three_messages(&scope);
    change_instance(&scope, "test", false);

    let d = send(&broker, &sent, "test", "d");
    assert_eq!(4, d.offset);
    assert_ne!(old, d.epoch);
    assert!(broker.history_from("test", &old, 3).unwrap_err().message.starts_with("unknown epoch"));
    assert_eq!(
        vec![(4, "d")],
        offsets_and_data(&broker.history_since("test", 0).expect("history").unwrap_or_default())
    );
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// regression-for: broker/memory_test.go#TestMemory_Store
/// note: Deviation D-069: a stream whose offset and epoch were lost while its history stayed (an eviction) gets a new epoch and continues after the newest offset Redis generated; only entries of the new epoch are served; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_lost_stream_state_gets_a_new_epoch() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent, old) = three_messages(&scope);
    scope.command(&["DEL", &meta_key(&scope, "test")]).expect("DEL");

    assert_eq!(Ok(None), broker.history_since("test", 0), "no state: no stream");
    let d = send(&broker, &sent, "test", "d");
    assert_eq!(4, d.offset);
    assert_ne!(old, d.epoch);
    assert_eq!(
        vec![(4, "d")],
        offsets_and_data(&broker.history_since("test", 0).expect("history").unwrap_or_default())
    );
    assert!(broker.history_from("test", &old, 3).unwrap_err().message.starts_with("unknown epoch"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-069: a stream lost entirely (offset, epoch and history, as after an eviction or a restart without persistence) starts again at offset 1 under a new epoch, so old offsets never match; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_stream_lost_entirely_starts_over_in_a_new_epoch() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent, old) = three_messages(&scope);
    for key in [meta_key(&scope, "test"), hist_key(&scope, "test")] {
        scope.command(&["DEL", &key]).expect("DEL");
    }

    let d = send(&broker, &sent, "test", "d");
    assert_eq!(1, d.offset);
    assert_ne!(old, d.epoch);
    assert!(broker.history_from("test", &old, 3).unwrap_err().message.starts_with("unknown epoch"));
}

/// regression-for: broker/memory_test.go#TestMemory_Limit
/// note: Deviation D-069: with history_limit 0 nothing is stored and broadcasts still get offsets; a counter lost next to an emptied history continues after the newest offset Redis generated; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn an_empty_history_never_fails_a_broadcast() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_limit = 0;
    let (none, sent) = recorded(&scope, &config);
    assert_eq!(1, send(&none, &sent, "nothing", "a").offset);
    assert_eq!(2, send(&none, &sent, "nothing", "b").offset);
    assert_eq!(Ok(None), none.peak("nothing"));
    assert_eq!(Ok(Some(vec![])), none.history_since("nothing", 0));

    let (broker, sent, _) = three_messages(&scope);
    scope.command(&["XTRIM", &hist_key(&scope, "test"), "MAXLEN", "0"]).expect("XTRIM");
    scope.command(&["DEL", &meta_key(&scope, "test")]).expect("DEL");
    assert_eq!(4, send(&broker, &sent, "test", "d").offset);
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom
/// note: Deviation D-069: after Redis forgets its scripts (SCRIPT FLUSH, a restart), operations send the script body again (NOSCRIPT fallback); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_flushed_script_cache_is_refilled() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent, epoch) = three_messages(&scope);
    scope.command(&["SCRIPT", "FLUSH"]).expect("SCRIPT FLUSH");

    assert_eq!(vec![(3, "c")], offsets_and_data(&broker.history_from("test", &epoch, 2).expect("history")));
    assert_eq!(4, send(&broker, &sent, "test", "d").offset);
    assert_eq!(5, send(&broker, &sent, "test", "e").offset);
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistoryFrom
/// note: Deviation D-069: broadcasts fan out with their offset and epoch, transient ones without being stored; streams are (un)subscribed on the broadcaster on the first and last reference; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn fan_out_transient_messages_and_subscriptions() {
    rb::rust_only();
    let scope = scope();
    let (broker, recorder) = recorded(&scope, &Config::new());

    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    let transient = StreamMessage {
        meta: Some(StreamMessageMetadata { transient: true, ..Default::default() }),
        ..msg("whispers", "w")
    };
    broker.handle_broadcast(&transient).expect("broadcast");

    let sent = recorder.messages();
    assert_eq!((1, epoch_of(&broker, "test")), (sent[0].offset, sent[0].epoch.clone()));
    assert_eq!(transient, sent[1], "fanned out as is");
    assert_eq!(Ok(None), broker.history_since("whispers", 0), "not stored");

    assert_eq!("test", broker.subscribe("test"));
    broker.subscribe("test");
    broker.unsubscribe("test");
    assert!(recorder.unsubscribed.lock().unwrap().is_empty());
    broker.unsubscribe("test");
    assert_eq!(vec!["test".to_owned()], *recorder.subscribed.lock().unwrap());
    assert_eq!(vec!["test".to_owned()], *recorder.unsubscribed.lock().unwrap());

    broker.shutdown().expect("shutdown");
    assert_eq!("Redis broker is shut down", broker.handle_broadcast(&msg("test", "b")).unwrap_err().message);
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-069: a Redis user limited to the broker's keys (~<prefix>:*) and the documented commands runs every broker operation; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn the_documented_acl_is_enough() {
    rb::rust_only();
    let scope = scope();
    let user = format!("{}_acl", scope.prefix);
    let pattern = format!("~{}:*", scope.prefix);
    let mut acl = vec!["ACL", "SETUSER", &user, "reset", "on", ">pw", &pattern, "resetchannels"];
    acl.extend(rb::ACL_COMMANDS.split(' '));
    scope.command(&acl).expect("ACL SETUSER");
    let url = format!("redis://{user}:pw@{}/", services::redis().addr());

    let sent = Recorder::new();
    let broker =
        rb::try_new_broker(&url, &Config::new(), &rb::options(&scope), Some(sent.clone()), None).expect("broker");
    let checked = (|| {
        broker.start()?;
        broker.handle_broadcast(&msg("test", "a"))?;
        broker.handle_broadcast(&msg("test", "b"))?;
        let epoch = sent.messages()[0].epoch.clone();
        assert_eq!(1, broker.history_from("test", &epoch, 1)?.len());
        assert_eq!(2, broker.history_since("test", 0)?.unwrap_or_default().len());
        assert!(broker.peak("test")?.is_some());
        broker.commit_session("s1", &TestCacheable("x".into()))?;
        broker.touch_session("s1")?;
        assert_eq!(Some(b"x".to_vec()), broker.restore_session("s1")?);
        Ok::<_, cabby_core::broker::BrokerError>(())
    })();
    let _ = scope.command(&["CLIENT", "KILL", "USER", &user]);
    let _ = scope.command(&["ACL", "DELUSER", &user]);
    checked.expect("broker operations with the documented ACL");
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-069: with Redis unreachable, start fails and every operation returns an error (counted in redis_broker_errors_total) instead of panicking; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn an_unreachable_redis_fails_operations() {
    rb::rust_only();
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr").port()
    };
    let metrics = rb::metrics();
    let options = rb::RedisBrokerConfig { key_prefix: "unreachable".into(), timeout: 1 };
    let url = format!("redis://127.0.0.1:{port}");
    let broker = rb::try_new_broker(&url, &Config::new(), &options, None, Some(metrics.clone())).expect("broker");

    let e = broker.start().unwrap_err();
    assert!(e.message.starts_with("failed to start the Redis broker, cause: "), "{e}");
    let e = broker.handle_broadcast(&msg("test", "a")).unwrap_err();
    assert!(e.message.starts_with("failed to add message to Redis stream: stream=test, cause: "), "{e}");
    assert!(broker.history_from("test", "e", 1).is_err());
    assert!(broker.restore_session("s1").unwrap_err().message.starts_with("failed to restore session from Redis"));
    assert!(broker.commit_session("s1", &TestCacheable("x".into())).is_err());
    assert_eq!(5, metrics.counter("redis_broker_errors_total").unwrap().value());
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-069: an operation on a Redis that accepts connections and never answers fails after the broker timeout (--redis_broker_timeout), counted in redis_broker_timeouts_total; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_stalled_redis_times_out() {
    rb::rust_only();
    // Accepts connections and never answers.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("redis://{}", listener.local_addr().expect("addr"));
    let metrics = rb::metrics();
    let options = rb::RedisBrokerConfig { key_prefix: "stalled".into(), timeout: 1 };
    let broker = rb::try_new_broker(&url, &Config::new(), &options, None, Some(metrics.clone())).expect("broker");

    let started = Instant::now();
    let e = broker.handle_broadcast(&msg("test", "a")).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(4), "took {:?}", started.elapsed());
    assert!(e.message.ends_with("cause: operation timed out after 1s"), "{e}");
    assert_eq!(1, metrics.counter("redis_broker_timeouts_total").unwrap().value());
    assert_eq!(1, metrics.counter("redis_broker_errors_total").unwrap().value());
    drop(listener);
}
