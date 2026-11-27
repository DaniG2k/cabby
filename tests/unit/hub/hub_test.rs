//! upstream file: hub/hub_test.go
//!
//! `MockSession` is in `cabby_testkit::reference::hub` (reads wait up to 100 ms,
//! as upstream). Upstream subtests share one hub and session; here each subtest
//! builds its own and repeats the setup of the subtests before it where that
//! state matters.

use std::collections::HashMap;
use std::time::Duration;

use cabby_core::common::{RemoteDisconnectMessage, StreamMessage, StreamMessageMetadata};
use cabby_testkit::reference::hub::{Hub, MockSession, build_message_json, drain_sessions};

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

/// upstream: hub/hub_test.go#TestUnsubscribeRaceConditions
/// note: the two racing threads are joined before session2 is read (upstream reads concurrently); on Go a blocking bridge read would otherwise hold back the broadcasts it waits for.
#[test]
fn test_unsubscribe_race_conditions() {
    let _load = crate::shared_load();
    let hub = Hub::start(2);

    let session = MockSession::new("123");
    let session2 = MockSession::new("321");
    let session3 = MockSession::new("213");

    hub.add_session(&session);
    hub.subscribe_session(&session, "test", "test_channel");

    hub.add_session(&session2);
    hub.subscribe_session(&session2, "test", "test_channel");

    hub.add_session(&session3);
    hub.subscribe_session(&session3, "test", "test_channel");

    hub.broadcast("test", "hello");

    assert!(session.read().is_ok());
    assert!(session2.read().is_ok());
    assert!(session3.read().is_ok());

    // Upstream's message says 2 while asserting 3; kept as written.
    assert_eq!(3, hub.size(), "Connections size must be equal 2");

    // Upstream reads session2 while the two goroutines run. Here the two racing
    // threads are joined first: on the Go backend every call goes through one
    // serialized bridge, so a read blocking there for its 100 ms window would hold
    // back the very broadcasts it waits for. The race under test (broadcasts vs.
    // RemoveSession from two threads) is unchanged.
    std::thread::scope(|s| {
        s.spawn(|| {
            hub.broadcast("test", "pong");
            hub.remove_session(&session);
            hub.broadcast("test", "ping");
        });

        s.spawn(|| {
            hub.broadcast("test", "bye-bye");
            hub.remove_session(&session3);
            hub.broadcast("test", "meow-meow");
        });
    });

    for _ in 1..5 {
        assert!(session2.read().is_ok());
    }

    assert!(session2.read().is_err());

    assert_eq!(1, hub.size(), "Connections size must be equal 1");
}

/// upstream: hub/hub_test.go#TestUnsubscribeSession
/// note: on Go the unexported `unsubscribeSessionFromAllChannels` step is driven through the exported synchronous `RemoveSession`, which calls it and then only drops the sessions/identifiers entries that gate delivery never consults.
#[test]
fn test_unsubscribe_session() {
    let _load = crate::shared_load();
    let hub = Hub::start(2);

    let session = MockSession::new("123");
    hub.add_session(&session);

    hub.subscribe_session(&session, "test", "test_channel");
    hub.subscribe_session(&session, "test2", "test_channel");

    hub.broadcast("test", "\"hello\"");

    let msg = session.read();
    assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hello\"}".to_owned()), msg);

    hub.unsubscribe_session(&session, "test", "test_channel");

    hub.broadcast("test", "\"goodbye\"");

    assert!(session.read().is_err());

    hub.broadcast("test2", "\"bye\"");

    let msg = session.read();
    assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"bye\"}".to_owned()), msg);

    hub.unsubscribe_session_from_all_channels(&session);

    hub.broadcast("test2", "\"goodbye\"");

    assert!(session.read().is_err());
}

/// upstream: hub/hub_test.go#TestUnsubscribeSessionFromChannel
#[test]
fn test_unsubscribe_session_from_channel() {
    let _load = crate::shared_load();
    let hub = Hub::start(2);

    let session = MockSession::new("123");
    hub.add_session(&session);

    hub.subscribe_session(&session, "test1", "test_channel");
    hub.subscribe_session(&session, "test2", "test_channel");
    hub.subscribe_session(&session, "test3", "other_channel");

    hub.broadcast("test1", "\"hello1\"");
    assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hello1\"}".to_owned()), session.read());

    hub.broadcast("test2", "\"hello2\"");
    assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hello2\"}".to_owned()), session.read());

    hub.broadcast("test3", "\"hello3\"");
    assert_eq!(Ok("{\"identifier\":\"other_channel\",\"message\":\"hello3\"}".to_owned()), session.read());

    hub.unsubscribe_session_from_channel(&session, "test_channel");

    hub.broadcast("test1", "\"goodbye1\"");
    assert!(session.read().is_err(), "Should not receive message from test1 after unsubscribing from test_channel");

    hub.broadcast("test2", "\"goodbye2\"");
    assert!(session.read().is_err(), "Should not receive message from test2 after unsubscribing from test_channel");

    hub.broadcast("test3", "\"still_here\"");
    assert_eq!(Ok("{\"identifier\":\"other_channel\",\"message\":\"still_here\"}".to_owned()), session.read());
}

mod test_subscribe_session {
    use super::*;

    /// upstream: hub/hub_test.go#TestSubscribeSession/Subscribe_to_a_single_channel
    #[test]
    fn subscribe_to_a_single_channel() {
        let _load = crate::shared_load();
        let hub = Hub::start(2);
        let session = MockSession::new("123");
        hub.add_session(&session);

        hub.subscribe_session(&session, "test", "test_channel");

        hub.broadcast("test", "\"hello\"");

        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hello\"}".to_owned()), session.read());
    }

    /// upstream: hub/hub_test.go#TestSubscribeSession/Successful_to_the_same_stream_from_multiple_channels
    /// note: upstream shares the hub and session with the previous subtest; its steps are replayed first.
    #[test]
    fn successful_to_the_same_stream_from_multiple_channels() {
        let _load = crate::shared_load();
        let hub = Hub::start(2);
        let session = MockSession::new("123");
        hub.add_session(&session);

        // Replay of subtest "Subscribe to a single channel".
        hub.subscribe_session(&session, "test", "test_channel");
        hub.broadcast("test", "\"hello\"");
        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hello\"}".to_owned()), session.read());

        hub.subscribe_session(&session, "test", "test_channel");
        hub.subscribe_session(&session, "test", "test_channel2");

        hub.broadcast("test", "\"hello twice\"");

        let mut received = Vec::new();

        let msg = session.read();
        assert!(msg.is_ok());
        received.push(msg.unwrap_or_default());

        let msg = session.read();
        assert!(msg.is_ok());
        received.push(msg.unwrap_or_default());

        assert!(received.contains(&"{\"identifier\":\"test_channel\",\"message\":\"hello twice\"}".to_owned()));
        assert!(received.contains(&"{\"identifier\":\"test_channel2\",\"message\":\"hello twice\"}".to_owned()));
    }
}

mod test_remote_disconnect {
    use super::*;

    /// upstream: hub/hub_test.go#TestRemoteDisconnect/Disconnect_session
    #[test]
    fn disconnect_session() {
        let _load = crate::shared_load();
        let hub = Hub::start(2);
        let session = MockSession::new("123");
        hub.add_session(&session);

        hub.remote_disconnect(RemoteDisconnectMessage { identifier: "123".into(), reconnect: false });

        assert_eq!(
            Ok("{\"type\":\"disconnect\",\"reason\":\"remote\",\"reconnect\":false}".to_owned()),
            session.read()
        );

        assert!(session.closed());
    }
}

mod test_broadcast_message {
    use super::*;

    fn setup() -> (Hub, MockSession) {
        let hub = Hub::start(2);
        let session = MockSession::new("123");
        hub.add_session(&session);
        hub.subscribe_session(&session, "test", "test_channel");
        (hub, session)
    }

    /// upstream: hub/hub_test.go#TestBroadcastMessage/Broadcast_without_stream_data
    #[test]
    fn broadcast_without_stream_data() {
        let _load = crate::shared_load();
        let (hub, session) = setup();

        hub.broadcast_message(msg("test", "\"ciao\""));

        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"ciao\"}".to_owned()), session.read());
    }

    /// upstream: hub/hub_test.go#TestBroadcastMessage/Broadcast_with_stream_data
    #[test]
    fn broadcast_with_stream_data() {
        let _load = crate::shared_load();
        let (hub, session) = setup();

        hub.broadcast_message(StreamMessage { epoch: "xyz".into(), offset: 2022, ..msg("test", "\"ciao\"") });

        assert_eq!(
            Ok("{\"identifier\":\"test_channel\",\"message\":\"ciao\",\"stream_id\":\"test\",\"epoch\":\"xyz\",\"offset\":2022}"
                .to_owned()),
            session.read()
        );
    }

    /// upstream: hub/hub_test.go#TestBroadcastMessage/Broadcast_with_exclude_socket
    #[test]
    fn broadcast_with_exclude_socket() {
        let _load = crate::shared_load();
        let (hub, session) = setup();

        let session2 = MockSession::new("234");
        hub.add_session(&session2);
        hub.subscribe_session(&session2, "test", "test_channel");

        hub.broadcast_message(msg("test", "\"ciao\""));

        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"ciao\"}".to_owned()), session.read());
        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"ciao\"}".to_owned()), session2.read());

        hub.broadcast_message(StreamMessage {
            meta: Some(StreamMessageMetadata { exclude_socket: "234".into(), ..Default::default() }),
            ..msg("test", "\"hoi!\"")
        });

        assert_eq!(Ok("{\"identifier\":\"test_channel\",\"message\":\"hoi!\"}".to_owned()), session.read());

        let msg = session2.read();
        // Upstream: assert.Nil(msg); require.Error(err); assert.Contains(err, "hasn't received any messages").
        let err = msg.expect_err("session2 must not receive the excluded broadcast");
        assert!(err.contains("hasn't received any messages"), "{err}");
    }
}

/// upstream: hub/hub_test.go#TestBroadcastOrder
#[test]
fn test_broadcast_order() {
    let _load = crate::shared_load();
    let hub = Hub::start(10);

    let session = MockSession::new("123");
    hub.add_session(&session);
    hub.subscribe_session(&session, "test", "test_channel");

    let session2 = MockSession::new("321");
    hub.add_session(&session2);
    hub.subscribe_session(&session2, "test2", "test2_channel");

    // Upstream: two goroutines and a WaitGroup.
    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 1..=5 {
                hub.broadcast_message(msg("test", &i.to_string()));
            }
        });

        s.spawn(|| {
            for i in 1..=5 {
                hub.broadcast_message(msg("test2", &i.to_string()));
            }
        });
    });

    for i in 1..=5 {
        assert_eq!(Ok(format!("{{\"identifier\":\"test_channel\",\"message\":{i}}}")), session.read());
    }

    for i in 1..=5 {
        assert_eq!(Ok(format!("{{\"identifier\":\"test2_channel\",\"message\":{i}}}")), session2.read());
    }
}

/// upstream: hub/hub_test.go#TestBuildMessageJSON
/// note: on Go, buildMessage (unexported) is observed as the EncodedMessage a gate passes to HubSession.Send for a session subscribed to ("", "chat").
#[test]
fn test_build_message_json() {
    let _load = crate::shared_load();
    let expected = b"{\"identifier\":\"chat\",\"message\":{\"text\":\"hello!\"}}".to_vec();
    let actual = build_message_json(&msg("", "{\"text\":\"hello!\"}"), "chat");
    assert_eq!(expected, actual);
}

/// upstream: hub/hub_test.go#TestBuildMessageString
/// note: on Go, buildMessage (unexported) is observed as the EncodedMessage a gate passes to HubSession.Send for a session subscribed to ("", "chat").
#[test]
fn test_build_message_string() {
    let _load = crate::shared_load();
    let expected = b"{\"identifier\":\"chat\",\"message\":\"plain string\"}".to_vec();
    let actual = build_message_json(&msg("", "\"plain string\""), "chat");
    assert_eq!(expected, actual);
}

/// regression-for: hub/hub_test.go#TestBuildMessageJSON
/// `buildMessage` is unexported upstream; the same frame observed through the
/// exported hub API (a subscriber of channel `chat`).
#[test]
fn build_message_json_through_hub() {
    let _load = crate::shared_load();
    let hub = Hub::start(2);
    let session = MockSession::new("123");
    hub.add_session(&session);
    hub.subscribe_session(&session, "s", "chat");

    hub.broadcast("s", "{\"text\":\"hello!\"}");

    assert_eq!(Ok("{\"identifier\":\"chat\",\"message\":{\"text\":\"hello!\"}}".to_owned()), session.read());
}

/// regression-for: hub/hub_test.go#TestBuildMessageString
/// Observable equivalent of the unexported `buildMessage` assertion.
#[test]
fn build_message_string_through_hub() {
    let _load = crate::shared_load();
    let hub = Hub::start(2);
    let session = MockSession::new("123");
    hub.add_session(&session);
    hub.subscribe_session(&session, "s", "chat");

    hub.broadcast("s", "\"plain string\"");

    assert_eq!(Ok("{\"identifier\":\"chat\",\"message\":\"plain string\"}".to_owned()), session.read());
}

/// Upstream `BenchmarkBroadcast` as a correctness test: the same
/// configurations, asserting that every session receives exactly the expected
/// frames. Throughput is measured by the external benchmark harness, not here.
///
/// Upstream derives `broadcastsPerStream = b.N / totalStreams` from the
/// benchmark's iteration count and assigns streams with `math/rand`; here
/// `b.N` is fixed ([`BENCH_N`], at least one broadcast per stream) and streams
/// come from a seeded generator, so the expected deliveries are known exactly.
/// Upstream waits for the readers with a `sync.WaitGroup`; here each session's
/// frames are collected with a bounded deadline.
mod benchmark_broadcast {
    use super::*;

    const BENCH_N: usize = 100;
    const PAYLOAD: &str = "\"A quick brow fox bla-bla-bla\"";
    const POOL_SIZES: [usize; 4] = [128, 16, 2, 1];
    const SESSIONS_NUM: [usize; 2] = [1000, 10000];
    const DELIVERY_DEADLINE: Duration = Duration::from_secs(20);

    struct Config {
        hub_pool_size: usize,
        total_streams: usize,
        total_sessions: usize,
        streams_per_session: usize,
    }

    impl Config {
        /// Upstream `benchmarkConfig.Inspect()` (the payload is a JSON string, so
        /// its decoded length is reported).
        fn inspect(&self) -> String {
            let decoded: String = serde_json::from_str(PAYLOAD).expect("payload is a JSON string");
            format!(
                "pool={},streams={},sessions={},streams-per-session={},payload={}B",
                self.hub_pool_size,
                self.total_streams,
                self.total_sessions,
                self.streams_per_session,
                decoded.len()
            )
        }
    }

    /// xorshift64*: deterministic stand-in for `rand.Intn`.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
        }
    }

    fn run(config: &Config) {
        let name = config.inspect();
        let broadcasts_per_stream = (BENCH_N / config.total_streams).max(1);
        let messages_per_session = config.streams_per_session * broadcasts_per_stream;

        let hub = Hub::start(config.hub_pool_size);

        let streams: Vec<String> = (0..config.total_streams).map(|i| format!("stream_{i}")).collect();

        let sids: Vec<String> = (0..config.total_sessions).map(|i| i.to_string()).collect();
        let sessions = MockSession::new_many(&sids);
        hub.add_sessions(&sessions);

        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (config.total_sessions * 31 + config.hub_pool_size) as u64);
        let mut subs = Vec::new();
        let mut expected: Vec<HashMap<String, usize>> = Vec::new();
        for i in 0..config.total_sessions {
            let mut frames = HashMap::new();
            for j in 0..config.streams_per_session {
                let channel = format!("test_channel{j}");
                let stream = streams[rng.below(streams.len())].clone();
                frames.insert(
                    format!("{{\"identifier\":\"{channel}\",\"message\":\"A quick brow fox bla-bla-bla\"}}"),
                    broadcasts_per_stream,
                );
                subs.push((i, stream, channel));
            }
            expected.push(frames);
        }
        hub.subscribe_sessions(&sessions, &subs);

        let mut broadcasts = Vec::new();
        for stream in &streams {
            for _ in 0..broadcasts_per_stream {
                broadcasts.push((stream.clone(), PAYLOAD.to_owned()));
            }
        }
        hub.broadcast_many(&broadcasts);

        let received = drain_sessions(&sessions, &vec![messages_per_session; sessions.len()], DELIVERY_DEADLINE);

        for (i, (got, want)) in received.iter().zip(&expected).enumerate() {
            assert_eq!(want, got, "{name}: session {i} must receive exactly {messages_per_session} frames");
        }
    }

    fn run_streams(total_streams: usize, streams_per_session: usize) {
        let _load = crate::exclusive_load();
        for hub_pool_size in POOL_SIZES {
            for total_sessions in SESSIONS_NUM {
                run(&Config { hub_pool_size, total_streams, total_sessions, streams_per_session });
            }
        }
    }

    /// upstream: hub/hub_test.go#BenchmarkBroadcast/<config.Inspect()>
    /// note: correctness only (exact per-session deliveries) with fixed b.N=100 and seeded stream assignment; throughput is measured by the external harness.
    /// Configurations with streams=1000, streams-per-session=10.
    #[test]
    fn streams_1000_per_session_10() {
        run_streams(1000, 10);
    }

    /// upstream: hub/hub_test.go#BenchmarkBroadcast/<config.Inspect()>
    /// note: correctness only (exact per-session deliveries) with fixed b.N=100 and seeded stream assignment; throughput is measured by the external harness.
    /// Configurations with streams=100, streams-per-session=10.
    #[test]
    fn streams_100_per_session_10() {
        run_streams(100, 10);
    }

    /// upstream: hub/hub_test.go#BenchmarkBroadcast/<config.Inspect()>
    /// note: correctness only (exact per-session deliveries) with fixed b.N=100 and seeded stream assignment; throughput is measured by the external harness.
    /// Configurations with streams=10, streams-per-session=3.
    #[test]
    fn streams_10_per_session_3() {
        run_streams(10, 3);
    }

    /// upstream: hub/hub_test.go#BenchmarkBroadcast/<config.Inspect()>
    /// note: correctness only (exact per-session deliveries) with fixed b.N=100 and seeded stream assignment; throughput is measured by the external harness.
    /// Configurations with streams=1, streams-per-session=1.
    #[test]
    fn streams_1_per_session_1() {
        run_streams(1, 1);
    }
}
