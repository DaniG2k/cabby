//! upstream file: broker/nats_test.go
//!
//! Integration tests against a real NATS server with JetStream
//! (`cabby_testkit::nats::NatsServer`; upstream starts the embedded `enats`
//! service on fixed ports, here every test gets its own port and storage).
//!
//! History and session TTLs are enforced by the NATS server on its own wall
//! clock, which a test cannot control. The upstream `time.Sleep` calls that let
//! those TTLs elapse are therefore kept as real waits ([`let_server_ttl_elapse`],
//! original durations); they wait for expiry, not for synchronization.
//! Because those waits are timing-sensitive, the NATS tests run one at a time
//! ([`serial`]): concurrent tests would delay each other's bridge calls.

use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use cabby_core::broker::Config;
use cabby_core::common::StreamMessage;
use cabby_testkit::jwt::unix_from_now;
use cabby_testkit::nats::NatsServer;
use cabby_testkit::reference::broker::*;

/// Upstream `jetstreamReadyTimeout` (unexported, broker/nats.go).
const JETSTREAM_READY_TIMEOUT: Duration = Duration::from_secs(1);

/// Held for the whole test: one NATS test at a time.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Lets time pass on the NATS server's clock (upstream `time.Sleep(d)`).
fn let_server_ttl_elapse(d: Duration) {
    thread::sleep(d);
}

fn nats_config(server: &NatsServer) -> cabby_core::broker::NatsConfig {
    let mut nconfig = new_nats_config();
    nconfig.servers = server.url();
    nconfig
}

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

/// Starts a broker as upstream's history tests do: legacy subscriber, `Start`,
/// `Reset` ("Ensure no stream exists"), and a subscription to `test` ("We must
/// subscribe to receive messages from the stream").
fn history_broker(server: &NatsServer, config: &Config) -> NatsBroker {
    let broker = new_nats_broker(NatsBroadcaster::FakeLegacySubscriber, config, &nats_config(server));

    broker.start().expect("start");

    broker.reset().expect("reset");

    broker.subscribe("test");
    broker
}

/// upstream: broker/nats_test.go#TestNATSBroker_HistorySince_expiration
#[test]
fn test_nats_broker_history_since_expiration() {
    let _serial = serial();
    let server = NatsServer::start();

    let mut config = new_config();
    config.history_ttl = 1;

    let broker = history_broker(&server, &config);

    let start = unix_from_now(-10);

    let _ = broker.handle_broadcast(&msg("test", "a"));
    let _ = broker.handle_broadcast(&msg("test", "b"));

    // Stream must be expired after 1 second
    let_server_ttl_elapse(Duration::from_secs(2));

    let _ = broker.handle_broadcast(&msg("test", "c"));
    let _ = broker.handle_broadcast(&msg("test", "d"));

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();

    assert_eq!(2, history.len(), "{history:?}");
    assert_eq!(3, history[0].offset);
    assert_eq!("c", history[0].data);
    assert_eq!(4, history[1].offset);
    assert_eq!("d", history[1].data);

    // Stream must be expired after 2 seconds
    let_server_ttl_elapse(Duration::from_secs(3));

    let history = broker.history_since("test", start).expect("history");
    assert!(history.unwrap_or_default().is_empty());

    broker.unsubscribe("test");
    broker.shutdown().expect("shutdown");
}

/// upstream: broker/nats_test.go#TestNATSBroker_HistorySince_with_limit
#[test]
fn test_nats_broker_history_since_with_limit() {
    let _serial = serial();
    let server = NatsServer::start();

    let mut config = new_config();
    config.history_limit = 2;

    let broker = history_broker(&server, &config);

    let start = unix_from_now(-10);

    for data in ["a", "b", "c"] {
        let _ = broker.handle_broadcast(&msg("test", data));
    }

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();

    assert_eq!(2, history.len(), "{history:?}");
    assert_eq!(3, history[1].offset);
    assert_eq!("c", history[1].data);

    broker.unsubscribe("test");
    broker.shutdown().expect("shutdown");
}

mod test_nats_broker_history_from {
    //! The parent test's own assertions (start, reset, reading the consumer
    //! sequence) run in every subtest's setup, so each subtest also maps to it.

    use super::*;

    struct Fixture {
        broker: NatsBroker,
        offsets: Vec<u64>,
        _server: NatsServer,
        _serial: MutexGuard<'static, ()>,
    }

    fn fixture() -> Fixture {
        let serial = serial();
        let server = NatsServer::start();
        let config = new_config();

        let broker = history_broker(&server, &config);

        for data in ["y", "z", "a", "b", "c", "d", "e"] {
            let _ = broker.handle_broadcast(&msg("test", data));
        }

        // We obtain sequence numbers with some offset to ensure that we use sequence numbers
        // as stream offsets
        let offsets = consumer_sequence(&server.url(), "test", 3, 5).expect("consumer sequence");

        Fixture { broker, offsets, _server: server, _serial: serial }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if !thread::panicking() {
                self.broker.unsubscribe("test");
                self.broker.shutdown().expect("shutdown");
            }
        }
    }

    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_current_epoch
    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom
    #[test]
    fn with_current_epoch() {
        let f = fixture();
        assert_eq!(4, f.offsets[1]);

        let history = f.broker.history_from("test", &f.broker.epoch(), f.offsets[1]).expect("history");

        assert_eq!(3, history.len(), "{history:?}");
        assert_eq!(5, history[0].offset);
        assert_eq!("c", history[0].data);
        assert_eq!(6, history[1].offset);
        assert_eq!("d", history[1].data);
        assert_eq!(7, history[2].offset);
        assert_eq!("e", history[2].data);
    }

    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_new_messages
    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom
    #[test]
    fn when_no_new_messages() {
        let f = fixture();
        let history = f.broker.history_from("test", &f.broker.epoch(), f.offsets[4]).expect("history");
        assert_eq!(0, history.len());
    }

    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom/When_no_stream
    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom
    #[test]
    fn when_no_stream() {
        let f = fixture();
        // Upstream also requires a nil history; an `Err` carries none.
        let history = f.broker.history_from("unknown", &f.broker.epoch(), f.offsets[1]);
        assert!(history.is_err(), "{history:?}");
    }

    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom/With_unknown_epoch
    /// upstream: broker/nats_test.go#TestNATSBroker_HistoryFrom
    #[test]
    fn with_unknown_epoch() {
        let f = fixture();
        let history = f.broker.history_from("test", "unknown", f.offsets[1]);
        assert!(history.is_err(), "{history:?}");
    }
}

/// upstream: broker/nats_test.go#TestNATSBroker_Sessions
#[test]
fn test_nats_broker_sessions() {
    let _serial = serial();
    let server = NatsServer::start();

    let mut config = new_config();
    config.sessions_ttl = 1;

    let nconfig = nats_config(&server);

    let broker = new_nats_broker(NatsBroadcaster::None, &config, &nconfig);

    broker.start().expect("start");

    broker.commit_session("test123", &TestCacheable("cache-me".into())).expect("commit");

    let another_broker = new_nats_broker(NatsBroadcaster::None, &config, &nconfig);
    another_broker.start().expect("start");

    another_broker.ready(JETSTREAM_READY_TIMEOUT).expect("ready");

    let restored = another_broker.restore_session("test123").expect("restore");

    assert_eq!(Some(b"cache-me".to_vec()), restored, "Expected to restore session data");

    // Expiration
    let_server_ttl_elapse(Duration::from_secs(2));

    let expired = broker.restore_session("test123").expect("restore");
    assert_eq!(None, expired);

    broker.commit_session("test345", &TestCacheable("cache-me-again".into())).expect("commit");

    let committed = another_broker.restore_session("test345").expect("restore");

    assert_eq!(Some(b"cache-me-again".to_vec()), committed);

    // Expiration
    let_server_ttl_elapse(Duration::from_millis(500));

    broker.touch_session("test345").expect("touch");

    let_server_ttl_elapse(Duration::from_millis(500));

    let finished = another_broker.restore_session("test345").expect("restore");

    assert_eq!(Some(b"cache-me-again".to_vec()), finished);

    // Expiration
    let_server_ttl_elapse(Duration::from_secs(1));

    let finished_stale = another_broker.restore_session("test345").expect("restore");
    assert_eq!(None, finished_stale);

    another_broker.shutdown().expect("shutdown");
    broker.shutdown().expect("shutdown");
}

/// upstream: broker/nats_test.go#TestNATSBroker_SessionsTTLChange
#[test]
fn test_nats_broker_sessions_ttl_change() {
    let _serial = serial();
    let server = NatsServer::start();

    let mut config = new_config();
    config.sessions_ttl = 1;

    let nconfig = nats_config(&server);

    let broker = new_nats_broker(NatsBroadcaster::None, &config, &nconfig);

    broker.start().expect("start");

    broker.ready(JETSTREAM_READY_TIMEOUT).expect("ready");

    broker.commit_session("test123", &TestCacheable("cache-me".into())).expect("commit");

    let mut a_config = new_config();
    a_config.sessions_ttl = 3;

    let another_broker = new_nats_broker(NatsBroadcaster::None, &a_config, &nconfig);
    another_broker.start().expect("start");

    another_broker.ready(JETSTREAM_READY_TIMEOUT).expect("ready");

    // The session must be missing since we recreated the bucket due to TTL change
    let missing = another_broker.restore_session("test123").expect("restore");

    assert_eq!(None, missing);

    another_broker.commit_session("test234", &TestCacheable("cache-me-again".into())).expect("commit");

    let_server_ttl_elapse(Duration::from_secs(1));

    // Shouldn't fail and catch up a new bucket
    let restored = broker.restore_session("test234").expect("restore");
    assert_eq!(Some(b"cache-me-again".to_vec()), restored, "Expected to restore session data");

    // Touch session
    another_broker.touch_session("test234").expect("touch");

    let_server_ttl_elapse(Duration::from_secs(2));

    let restored_again = broker.restore_session("test234").expect("restore");
    assert!(restored_again.is_some());

    let_server_ttl_elapse(Duration::from_secs(2));

    let expired = broker.restore_session("test234").expect("restore");
    assert_eq!(None, expired);

    another_broker.shutdown().expect("shutdown");
    broker.shutdown().expect("shutdown");
}

/// Upstream's epoch test up to `anotherBroker.SetEpoch`: two brokers on one
/// server share the epoch.
fn epoch_brokers(server: &NatsServer) -> (NatsBroker, NatsBroker, String) {
    let config = new_config();

    let nconfig = nats_config(server);

    let broker = new_nats_broker(NatsBroadcaster::None, &config, &nconfig);

    broker.start().expect("start");

    broker.ready(JETSTREAM_READY_TIMEOUT).expect("ready");
    let _ = broker.reset();

    let epoch = broker.epoch();

    let another_broker = new_nats_broker(NatsBroadcaster::None, &config, &nconfig);
    another_broker.start().expect("start");

    another_broker.ready(JETSTREAM_READY_TIMEOUT).expect("ready");

    (broker, another_broker, epoch)
}

/// upstream: broker/nats_test.go#TestNATSBroker_Epoch
/// Every assertion except the node-local epoch (see `test_nats_broker_epoch_local_broker`).
#[test]
fn test_nats_broker_epoch() {
    let _serial = serial();
    let server = NatsServer::start();
    let (broker, another_broker, epoch) = epoch_brokers(&server);

    assert_eq!(epoch, another_broker.epoch());

    // Now let's test that epoch changes are picked up
    another_broker.set_epoch("new-epoch").expect("set epoch");

    assert_eq!("new-epoch", another_broker.epoch());

    // Upstream polls every 100ms for up to 2 seconds.
    let deadline = Instant::now() + Duration::from_secs(2);
    while broker.epoch() != "new-epoch" {
        assert!(Instant::now() < deadline, "Epoch change wasn't picked up");
        thread::sleep(Duration::from_millis(100));
    }

    another_broker.shutdown().expect("shutdown");
    broker.shutdown().expect("shutdown");
}

/// upstream: broker/nats_test.go#TestNATSBroker_Epoch
/// The node-local broker follows `SetEpoch` (upstream reads the unexported
/// `anotherBroker.local`).
#[test]
fn test_nats_broker_epoch_local_broker() {
    let _serial = serial();
    let server = NatsServer::start();
    let (broker, another_broker, _) = epoch_brokers(&server);

    another_broker.set_epoch("new-epoch").expect("set epoch");

    assert_eq!("new-epoch", another_broker.local_epoch());

    another_broker.shutdown().expect("shutdown");
    broker.shutdown().expect("shutdown");
}
