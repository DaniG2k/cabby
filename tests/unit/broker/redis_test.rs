//! Deviation D-069: the Redis broker (`--broker=redis`), a Cabby addition.
//!
//! The memory and NATS broker tests of this package are the contract: the
//! same history, session and epoch scenarios run here against a real Redis
//! (`CABBY_TEST_REDIS_URL`, required: no silent skip), plus what is specific
//! to sharing one Redis between nodes (one epoch, a lost epoch, key TTLs).
//! Each test has its own Redis scope: an ACL user and a key prefix.
//!
//! History and session TTLs are enforced on the Redis server's clock, which a
//! test cannot control, so the TTL waits are real (as in the NATS tests).
//! AnyCable rejects `--broker=redis`, so these cases are not observable on Go.

use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use cabby_core::broker::{Broker, Config};
use cabby_core::common::{StreamMessage, StreamMessageMetadata};
use cabby_testkit::jwt::unix_from_now;
use cabby_testkit::redis_broker::{self as rb, Recorder, RedisBroker};
use cabby_testkit::reference::broker::TestCacheable;
use cabby_testkit::services::{self, RedisScope};

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

/// Lets time pass on the Redis server's clock.
fn let_server_ttl_elapse(d: Duration) {
    thread::sleep(d);
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

fn seq_key(scope: &RedisScope, stream: &str) -> String {
    format!("{}:{{s:{stream}}}:seq", scope.prefix)
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
        assert_eq!(epoch, broker.epoch());
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
        let epoch = broker.epoch();

        let error = |offset| broker.history_from("test", &epoch, offset).unwrap_err().message;
        assert_eq!("requested offset couldn't be found: 2, lowest: 4", error(2));
        assert_eq!("requested offset couldn't be found: 3, lowest: 4", error(3));
        assert_eq!(vec![(5, "e")], offsets_and_data(&broker.history_from("test", &epoch, 4).expect("history")));
        assert_eq!("requested offset couldn't be found: 6, latest: 5", error(6));
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
    let epoch = broker.epoch();
    assert_eq!(
        "requested offset couldn't be found: 290, lowest: 299",
        broker.history_from("test", &epoch, 290).unwrap_err().message
    );
}

/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
/// regression-for: broker/memory_test.go#TestMemory_Expire
/// regression-for: broker/memory_test.go#TestMemstream_filterByOffset
/// note: Deviation D-069: entries expire after history_ttl on the Redis server's clock; the stream's offsets outlive them (10 x history_ttl), as in the memory broker; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn history_since_expiration() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_ttl = 1;
    let broker = started(&scope, &config);

    let start = unix_from_now(-10);
    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "b")).expect("broadcast");

    // Stream must be expired after 1 second
    let_server_ttl_elapse(Duration::from_secs(2));

    broker.handle_broadcast(&msg("test", "c")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "d")).expect("broadcast");

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();
    assert_eq!(vec![(3, "c"), (4, "d")], offsets_and_data(&history));
    let epoch = broker.epoch();
    assert_eq!(
        "requested offset couldn't be found: 1, lowest: 3",
        broker.history_from("test", &epoch, 1).unwrap_err().message
    );

    let_server_ttl_elapse(Duration::from_secs(3));

    // The entries are gone, the stream (and its offsets) is not.
    assert_eq!(Ok(Some(vec![])), broker.history_since("test", start));
    assert_eq!("stream is empty", broker.history_from("test", &epoch, 4).unwrap_err().message);
    assert_eq!(Ok(None), broker.peak("test"));
    broker.handle_broadcast(&msg("test", "e")).expect("broadcast");
    assert_eq!(Some(5), broker.peak("test").expect("peak").map(|m| m.offset), "offsets continue");
}

/// regression-for: broker/memory_test.go#TestMemory_Peak
/// note: Deviation D-069: the Redis broker on a real Redis (Cabby only); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn peak_reports_the_latest_position() {
    rb::rust_only();
    let scope = scope();
    let broker = started(&scope, &Config::new());

    assert_eq!(Ok(None), broker.peak("test"));

    broker.handle_broadcast(&msg("test", "a")).expect("broadcast");
    broker.handle_broadcast(&msg("test", "b")).expect("broadcast");

    let peak = broker.peak("test").expect("peak").expect("position");
    assert_eq!(2, peak.offset);
    assert_eq!(broker.epoch(), peak.epoch);

    assert_eq!(Ok(None), broker.peak("unknown"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// regression-for: broker/memory_test.go#TestMemory_RestoreSession
/// note: Deviation D-069: sessions are shared through Redis and expire after sessions_ttl on its clock; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn sessions() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.sessions_ttl = 1;

    let broker = started(&scope, &config);
    broker.commit_session("test123", &TestCacheable("cache-me".into())).expect("commit");

    let another_broker = started(&scope, &config);
    let restored = another_broker.restore_session("test123").expect("restore");
    assert_eq!(Some(b"cache-me".to_vec()), restored, "Expected to restore session data");

    // Expiration
    let_server_ttl_elapse(Duration::from_secs(2));

    assert_eq!(Ok(None), broker.restore_session("test123"));

    broker.commit_session("test345", &TestCacheable("cache-me-again".into())).expect("commit");
    assert_eq!(Ok(Some(b"cache-me-again".to_vec())), another_broker.restore_session("test345"));

    let_server_ttl_elapse(Duration::from_millis(500));
    broker.touch_session("test345").expect("touch");
    let_server_ttl_elapse(Duration::from_millis(500));

    assert_eq!(Ok(Some(b"cache-me-again".to_vec())), another_broker.restore_session("test345"));

    // Expiration
    let_server_ttl_elapse(Duration::from_secs(1));

    assert_eq!(Ok(None), another_broker.restore_session("test345"));

    // Unknown sessions: nothing to restore, nothing to touch.
    assert_eq!(Ok(None), broker.restore_session("unknown"));
    assert_eq!(Ok(()), broker.touch_session("unknown"));
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// regression-for: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
/// note: Deviation D-069: the key layout and its TTLs (history_ttl, 10 x history_ttl for the offset counter, sessions_ttl); not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn keys_and_their_ttls() {
    rb::rust_only();
    let scope = scope();
    let mut config = Config::new();
    config.history_ttl = 60;
    config.sessions_ttl = 30;
    let broker = started(&scope, &config);

    broker.handle_broadcast(&msg("chat/1", "a")).expect("broadcast");
    broker.commit_session("sid1", &TestCacheable("state".into())).expect("commit");

    let hist = ttl(&scope, &hist_key(&scope, "chat/1"));
    assert!((55..=60).contains(&hist), "history TTL {hist}");
    let seq = ttl(&scope, &seq_key(&scope, "chat/1"));
    assert!((595..=600).contains(&seq), "offset counter TTL {seq}");
    let sess = ttl(&scope, &format!("{}:sess:sid1", scope.prefix));
    assert!((25..=30).contains(&sess), "session TTL {sess}");
    assert_eq!(-1, ttl(&scope, &format!("{}:epoch", scope.prefix)), "the epoch does not expire");
    let epoch = scope.command(&["GET", &format!("{}:epoch", scope.prefix)]).expect("GET").as_text();
    assert_eq!(Some(broker.epoch()), epoch);
}

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// regression-for: broker/nats_test.go#TestNATSBroker_Sessions
/// note: Deviation D-069: two brokers on one Redis share the epoch, the offsets, the history and the sessions; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn two_brokers_share_one_redis() {
    rb::rust_only();
    let scope = scope();
    let config = Config::new();
    let (a, sent) = recorded(&scope, &config);
    let b = started(&scope, &config);
    assert_eq!(a.epoch(), b.epoch());
    assert_eq!(6, a.epoch().len());

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

/// regression-for: broker/nats_test.go#TestNATSBroker_Epoch
/// note: Deviation D-069: when Redis loses the epoch key (or everything, as after a restart without persistence), the next operation creates a new epoch; every node uses it, old offsets fail with unknown epoch, so clients resync; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_lost_epoch_is_replaced_and_every_node_follows() {
    rb::rust_only();
    let scope = scope();
    let config = Config::new();
    let metrics = rb::metrics();
    let sent = Recorder::new();
    let a = rb::try_new_broker(&scope.url(), &config, &rb::options(&scope), Some(sent.clone()), Some(metrics.clone()))
        .expect("broker");
    a.start().expect("start");
    let b = started(&scope, &config);
    let first = a.epoch();

    a.handle_broadcast(&msg("test", "a")).expect("broadcast");
    scope.command(&["DEL", &format!("{}:epoch", scope.prefix)]).expect("DEL");

    a.handle_broadcast(&msg("test", "b")).expect("broadcast");
    let second = sent.messages()[1].epoch.clone();
    assert_ne!(first, second, "a new epoch");
    assert_eq!(2, sent.messages()[1].offset, "the stream itself was kept");
    assert_eq!(second, a.epoch());
    assert_eq!(1, metrics.counter("redis_broker_epoch_rotations_total").unwrap().value());

    // B still caches the first epoch; reads check the shared one.
    let e = b.history_from("test", &first, 1).unwrap_err();
    assert_eq!(format!("unknown epoch: {first}, current: {second}"), e.message);
    assert_eq!(vec![(2, "b")], offsets_and_data(&b.history_from("test", &second, 1).expect("history")));
    assert_eq!(second, b.epoch());

    // Everything lost: offsets start over, under yet another epoch.
    for key in [format!("{}:epoch", scope.prefix), seq_key(&scope, "test"), hist_key(&scope, "test")] {
        scope.command(&["DEL", &key]).expect("DEL");
    }
    b.handle_broadcast(&msg("test", "c")).expect("broadcast");
    let third = b.epoch();
    assert_ne!(second, third);
    assert!(b.history_from("test", &second, 2).unwrap_err().message.starts_with("unknown epoch"));
    let peak = a.peak("test").expect("peak").expect("position");
    assert_eq!((1, third), (peak.offset, peak.epoch));
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
    assert_eq!((1, broker.epoch()), (sent[0].offset, sent[0].epoch.clone()));
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
/// note: Deviation D-069: an operation on a Redis that doesn't answer fails after the broker timeout (--redis_broker_timeout), counted in redis_broker_timeouts_total; not_observable on Go: AnyCable rejects --broker=redis.
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

/// regression-for: broker/memory_test.go#TestMemory_Store
/// note: Deviation D-069: an offset counter lost while its history stayed (an eviction) continues after the newest entry, so offsets never go back; not_observable on Go: AnyCable rejects --broker=redis.
#[test]
fn a_lost_offset_counter_continues_after_the_newest_entry() {
    rb::rust_only();
    let scope = scope();
    let (broker, sent) = recorded(&scope, &Config::new());
    for data in ["a", "b", "c"] {
        broker.handle_broadcast(&msg("test", data)).expect("broadcast");
    }
    scope.command(&["DEL", &seq_key(&scope, "test")]).expect("DEL");

    broker.handle_broadcast(&msg("test", "d")).expect("broadcast");
    assert_eq!(4, sent.messages()[3].offset);
    let epoch = broker.epoch();
    assert_eq!(vec![(3, "c"), (4, "d")], offsets_and_data(&broker.history_from("test", &epoch, 2).expect("history")));
    broker.handle_broadcast(&msg("test", "e")).expect("broadcast");
    assert_eq!(5, sent.messages()[4].offset);
}
