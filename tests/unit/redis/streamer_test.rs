//! upstream file: redis/streamer_test.go
//!
//! Real Redis at `CABBY_TEST_REDIS_URL` (required; upstream skips without Redis).
//! Upstream uses the fixed stream `__test__` (destroying group `__tg__` in
//! `init`); here the stream is unique per test, so no cleanup of earlier runs is
//! needed. Test-side publishing and consumer counting use the testkit's own
//! Redis connection, and consumers are counted per ACL user (upstream counts
//! `cmd=xreadgroup` across all clients of the server).

use std::time::Duration;

use cabby_testkit::reference::redis::{Config, StreamCollector, Streamer};
use cabby_testkit::services::{self, RedisScope};
use cabby_testkit::{Backend, backend};

const GROUP: &str = "__tg__";
/// Upstream `<-streamer.Ready()` has no deadline.
const READY: Duration = Duration::from_secs(10);
/// Upstream `drainStream(..., 2*time.Second)`.
const DRAIN: Duration = Duration::from_secs(2);
/// Upstream `require.Eventuallyf(..., 3*time.Second, 200*time.Millisecond)`.
const CONSUMERS: Duration = Duration::from_secs(3);

/// `defer streamer.Shutdown(...)`; skipped while a Rust stub is unwinding.
struct ShutdownGuard<'a>(&'a Streamer);

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        if backend() == Backend::Go || !std::thread::panicking() {
            let _ = self.0.shutdown();
        }
    }
}

fn config_for(url: String) -> cabby_server::redis::RedisConfig {
    let mut rconfig = Config::new();
    rconfig.url = url;
    rconfig.config()
}

fn setup() -> (RedisScope, String, cabby_server::redis::RedisConfig) {
    let mut scope = services::redis().scope("streamer");
    // Each XADD returns once a running consumer has read the entry: upstream
    // starts a second streamer right after adding entries for the first one,
    // and the second could otherwise read an entry first.
    scope.await_stream_delivery(DRAIN);
    let stream = scope.name("__test__");
    let rconfig = config_for(scope.url());
    (scope, stream, rconfig)
}

mod test_streamer {
    use super::*;

    const BLOCK_MS: i64 = 500;

    /// upstream: redis/streamer_test.go#TestStreamer/Handles_incoming_messages
    /// upstream: redis/streamer_test.go#TestStreamer
    #[test]
    fn handles_incoming_messages() {
        let (scope, stream, rconfig) = setup();
        let received = StreamCollector::new();

        let streamer = Streamer::new(&stream, GROUP, &rconfig, &received, BLOCK_MS);

        streamer.start().expect("Start");
        let _guard = ShutdownGuard(&streamer);

        assert!(streamer.wait_ready(READY), "streamer not ready within {READY:?}");

        scope.xadd(&stream, "payload", "testo").expect("XADD");

        let messages = received.drain(1, DRAIN);
        assert_eq!(1, messages.len(), "haven't received 1 messages on time");

        let msg = &messages[0];

        assert_eq!(Some("testo"), msg.get("payload").map(String::as_str));
    }

    /// upstream: redis/streamer_test.go#TestStreamer/With_multiple_subscribers
    /// upstream: redis/streamer_test.go#TestStreamer
    #[test]
    fn with_multiple_subscribers() {
        let (scope, stream, rconfig) = setup();
        let received = StreamCollector::new();

        let streamer = Streamer::new(&stream, GROUP, &rconfig, &received, BLOCK_MS);
        streamer.start().expect("Start");
        let _guard = ShutdownGuard(&streamer);

        let streamer2 = Streamer::new(&stream, GROUP, &rconfig, &received, BLOCK_MS);
        streamer2.start().expect("Start");
        let _guard2 = ShutdownGuard(&streamer2);

        assert!(streamer.wait_ready(READY), "streamer not ready within {READY:?}");
        assert!(streamer2.wait_ready(READY), "streamer2 not ready within {READY:?}");

        scope.xadd(&stream, "payload", "123_test").expect("XADD");
        scope.xadd(&stream, "payload", "124_test").expect("XADD");
        scope.xadd(&stream, "payload", "125_test").expect("XADD");

        let messages = received.drain(3, DRAIN);
        assert_eq!(3, messages.len(), "haven't received 3 messages on time");
    }
}

/// upstream: redis/streamer_test.go#TestStreamerAcksClaims
///
/// The handler closes the first streamer's client when it gets payload "2", so
/// that entry is never acknowledged and the second streamer must claim it.
/// Each streamer connects as its own ACL user so consumers can be counted per
/// streamer.
/// note: upstream closes the unexported `streamer.client` and sets `reconnectAttempt = MaxReconnectAttempts + 1`; here the handler calls the exported `Client().Close()` and streamer 1's config has `MaxReconnectAttempts = 0`, which makes `maybeReconnect` give up the same way. Upstream's `initClient()` before waiting is implied by the reader starting.
#[test]
fn test_streamer_acks_claims() {
    const BLOCK_MS: i64 = 100;

    let (mut scope, stream, mut rconfig) = setup();
    // Upstream: `streamer.reconnectAttempt = rconfig.MaxReconnectAttempts + 1`.
    rconfig.max_reconnect_attempts = 0;
    let (user2, url2) = scope.extra_user("second");
    let rconfig2 = config_for(url2);

    let received = StreamCollector::new();

    let streamer = Streamer::new(&stream, GROUP, &rconfig, &received, BLOCK_MS);
    received.close_client_on_payload(&streamer, "2");

    streamer.start().expect("Start");
    let _guard = ShutdownGuard(&streamer);

    scope.wait_clients_with_cmd(scope.user(), "xreadgroup", 1, CONSUMERS).expect("No stream consumer were created");

    scope.xadd(&stream, "payload", "1").expect("XADD");
    scope.xadd(&stream, "payload", "2").expect("XADD");

    let streamer2 = Streamer::new(&stream, GROUP, &rconfig2, &received, BLOCK_MS);
    streamer2.start().expect("Start");
    let _guard2 = ShutdownGuard(&streamer2);

    scope.wait_clients_with_cmd(&user2, "xreadgroup", 1, CONSUMERS).expect("No stream consumer were created");

    let messages = received.drain(3, DRAIN);
    assert_eq!(3, messages.len(), "haven't received 3 messages on time");

    let payload = |i: usize| messages[i].get("payload").cloned().unwrap_or_default();
    assert_eq!("1", payload(0));
    assert_eq!("2", payload(1));
    // We haven't acked the last message within the first streamer,
    // so the second one must have picked it up
    assert_eq!("2", payload(2));
}

/// Entries added to `stream` for `GROUP` before any streamer runs (the group
/// is created at `$` first), with payloads `"0"`, `"1"`, ...
fn backlog(scope: &RedisScope, stream: &str, entries: usize) {
    scope.command(&["XGROUP", "CREATE", stream, GROUP, "$", "MKSTREAM"]).expect("XGROUP CREATE");
    for i in 0..entries {
        scope.xadd(stream, "payload", &i.to_string()).expect("XADD");
    }
}

/// `XPENDING stream GROUP`: the number of entries delivered and not acknowledged.
fn pending(scope: &RedisScope, stream: &str) -> i64 {
    let summary = scope.command(&["XPENDING", stream, GROUP]).expect("XPENDING");
    summary.items().first().and_then(|n| n.as_int()).unwrap_or(-1)
}

/// regression-for: redis/streamer_test.go#TestStreamerAcksClaims
/// note: Deviation D-109: a backlog is read 1000 entries at a time. The handler closes the client on the first entry, so the streamer stops after its first read and the pending list holds what that read took (upstream: the whole backlog).
#[test]
fn deviation_d109_a_backlog_is_read_in_batches() {
    const BACKLOG: usize = 2500;
    let scope = services::redis().scope("streamer");
    let stream = scope.name("__test__");
    let mut rconfig = config_for(scope.url());
    rconfig.max_reconnect_attempts = 0;
    backlog(&scope, &stream, BACKLOG);

    let received = StreamCollector::new();
    let streamer = Streamer::new(&stream, GROUP, &rconfig, &received, 100);
    received.close_client_on_payload(&streamer, "0");
    streamer.start().expect("Start");
    let _guard = ShutdownGuard(&streamer);

    let first = received.drain(1, DRAIN);
    assert_eq!(1, first.len(), "the backlog was not read");
    let taken = pending(&scope, &stream);
    // The rest of what the first read took (Go's handler keeps 10 at a time).
    let rest = received.drain(BACKLOG, Duration::from_millis(500));

    assert_eq!(1000, taken, "entries the first read took");
    assert_eq!(999, rest.len(), "entries handled after the first");
}
