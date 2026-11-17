//! upstream file: ds/integration_test.go
//!
//! The upstream client is `durable-streams/packages/client-go`; its read path is
//! ported as `cabby_testkit::ds_client`. Upstream sleeps after broadcasting
//! (the memory broker stores synchronously, so they are dropped) and before
//! broadcasting to a waiting live request (replaced by waiting until the session is
//! subscribed to the stream).

use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

use cabby_core::common::{CommandResult, ConnectResult, Status};
use cabby_testkit::ds_client::{Client, Error, LiveMode, START_OFFSET};
use cabby_testkit::reference::ds::{Handler, Harness, Request, Server, StreamsKind, new_config};
use serde_json::{Map, Value};

/// Bound for reads upstream runs with `context.Background()`.
const READ_DEADLINE: Duration = Duration::from_secs(10);
/// Bound for barriers and session cleanup.
const WAIT: Duration = Duration::from_secs(5);

struct IntegrationServer {
    // Dropped in this order: the server first (`defer ts.Close()`), the node last.
    ts: Server,
    _handler: Handler,
    n: Harness,
}

impl IntegrationServer {
    fn client(&self) -> Client {
        Client::new(self.ts.url())
    }

    fn assert_no_unexpected_calls(&self) {
        assert_eq!(self.n.unexpected_calls(), Vec::<String>::new(), "unexpected mock calls");
    }
}

/// Upstream `setupIntegrationServer`.
fn setup_integration_server() -> IntegrationServer {
    let n = Harness::memory("test-epoch", 60, 100);
    n.on_authenticate(
        None,
        ConnectResult {
            identifier: "ds2026".into(),
            status: Status::Success,
            transmissions: vec![r#"{"type":"welcome"}"#.into()],
            ..Default::default()
        },
    );

    let mut ds_config = new_config();
    ds_config.path = "/ds".into();
    ds_config.poll_interval = 3;

    let handler = n.handler(&ds_config, StreamsKind::RejectNoauth).expect("DSHandler");
    let ts = handler.listen();
    IntegrationServer { ts, _handler: handler, n }
}

fn messages(data: &[u8]) -> Vec<Map<String, Value>> {
    serde_json::from_slice(data).unwrap_or_else(|e| panic!("not a JSON array of objects ({e}): {data:?}"))
}

fn subscribe_result(stream: &str) -> CommandResult {
    CommandResult {
        status: Status::Success,
        transmissions: vec![r#"{"type":"confirm","identifier":"chat_1"}"#.into()],
        streams: vec![stream.into()],
        ..Default::default()
    }
}

/// upstream: ds/integration_test.go#TestDSIntegration_Head
#[test]
fn test_ds_integration_head() {
    let s = setup_integration_server();

    let stream = s.client().stream("/ds/test-stream");

    let meta = stream.head(&[]).expect("HEAD");

    assert_eq!(meta.content_type, "application/json");
    assert!(!meta.next_offset.is_empty());
    s.assert_no_unexpected_calls();
}

mod test_ds_integration_catchup_read {
    use super::*;

    /// upstream: ds/integration_test.go#TestDSIntegration_CatchupRead/with_empty_stream
    #[test]
    fn with_empty_stream() {
        let s = setup_integration_server();
        let stream = s.client().stream("/ds/empty-stream");

        let mut it = stream.read(START_OFFSET, LiveMode::None, READ_DEADLINE);

        let chunk = match it.next() {
            Err(Error::Done) => panic!("expected data but got Done"),
            r => r.expect("read"),
        };

        assert_eq!(String::from_utf8_lossy(&chunk.data), "[]");
        assert!(chunk.up_to_date);
        it.close();
        s.assert_no_unexpected_calls();
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_CatchupRead/with_non-empty_stream
    #[test]
    fn with_non_empty_stream() {
        let s = setup_integration_server();
        let stream_name = "test-stream-data";

        s.n.broker_subscribe(stream_name);
        s.n.broadcast(stream_name, r#"{"id":1,"msg":"hello"}"#).expect("broadcast");
        s.n.broadcast(stream_name, r#"{"id":2,"msg":"world"}"#).expect("broadcast");

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        let mut it = stream.read(START_OFFSET, LiveMode::None, READ_DEADLINE);

        let chunk = match it.next() {
            Err(Error::Done) => panic!("expected data but got Done"),
            r => r.expect("read"),
        };

        // Parse the JSON array
        let messages = messages(&chunk.data);

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["id"], 1.0);
        assert_eq!(messages[0]["msg"], "hello");
        assert_eq!(messages[1]["id"], 2.0);
        assert_eq!(messages[1]["msg"], "world");

        assert!(chunk.up_to_date);
        assert!(!chunk.next_offset.is_empty());
        it.close();
        s.assert_no_unexpected_calls();
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_CatchupRead/with_offset
    #[test]
    fn with_offset() {
        let s = setup_integration_server();
        let stream_name = "test-stream-offset";
        s.n.broker_subscribe(stream_name);

        let _ = s.n.broadcast(stream_name, r#"{"id":1,"msg":"first"}"#);
        let _ = s.n.broadcast(stream_name, r#"{"id":2,"msg":"second"}"#);
        let _ = s.n.broadcast(stream_name, r#"{"id":3,"msg":"third"}"#);

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        let mut it = stream.read(START_OFFSET, LiveMode::None, READ_DEADLINE);
        let chunk = it.next().expect("read");
        it.close();

        let first_offset = chunk.next_offset;

        let _ = s.n.broadcast(stream_name, r#"{"id":4,"msg":"fourth"}"#);

        let mut it2 = stream.read(&first_offset, LiveMode::None, READ_DEADLINE);

        let chunk2 = match it2.next() {
            Err(Error::Done) => panic!("No new messages - offset may be at end of stream"),
            r => r.expect("read"),
        };

        let messages = messages(&chunk2.data);

        assert!(!messages.is_empty());
        let last_msg = &messages[messages.len() - 1];
        assert_eq!(last_msg["id"], 4.0);
        assert_eq!(last_msg["msg"], "fourth");
        it2.close();
        s.assert_no_unexpected_calls();
    }
}

/// upstream: ds/integration_test.go#TestDSIntegration_Authenticate
#[test]
fn test_ds_integration_authenticate() {
    let s = setup_integration_server();

    // Reset mocks
    s.n.reset_controller();
    s.n.on_shutdown();
    s.n.on_authenticate(
        Some("sid-fail"),
        ConnectResult {
            status: Status::Failure,
            transmissions: vec![r#"{"type":"disconnect"}"#.into()],
            ..Default::default()
        },
    );
    s.n.on_authenticate(
        Some("sid-pass"),
        ConnectResult {
            identifier: "ds2026".into(),
            status: Status::Success,
            transmissions: vec![r#"{"type":"welcome"}"#.into()],
            ..Default::default()
        },
    );

    let stream = s.client().stream("/ds/test-stream");

    let err = stream.head(&[("x-request-id", "sid-fail")]).expect_err("rejected");
    assert!(err.to_string().contains("401"), "{err}");

    let stream = s.client().stream("/ds/test-stream");
    stream.head(&[("x-request-id", "sid-pass")]).expect("HEAD");
    s.assert_no_unexpected_calls();
}

/// upstream: ds/integration_test.go#TestDSIntegration_StreamAuthorization
/// note: upstream sleeps 100ms before asserting `n.Size() == 0`; here the size is awaited (bounded), then asserted.
#[test]
fn test_ds_integration_stream_authorization() {
    let s = setup_integration_server();

    let stream = s.client().stream("/ds/please-noauth");

    let err = stream.head(&[]).expect_err("unauthorized");
    assert!(err.to_string().contains("401"), "{err}");

    let mut it = stream.read(START_OFFSET, LiveMode::Sse, READ_DEADLINE);
    let err = it.next().expect_err("unauthorized");
    assert!(err.to_string().contains("401"), "{err}");

    s.n.wait_size(0, WAIT);

    assert_eq!(s.n.size(), 0);
    s.assert_no_unexpected_calls();
}

mod test_ds_integration_long_poll {
    use super::*;

    /// The parent test's controller expectation, shared by both subtests.
    fn setup() -> IntegrationServer {
        let s = setup_integration_server();
        s.n.on_subscribe(None, subscribe_result("longpoll-wait-test"), 0, false);
        s
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_LongPoll/should_wait_for_new_data_with_long-poll
    /// note: upstream sleeps 500ms "for the long-poll to be active" before asserting `n.Size() == 1` and broadcasting; here it waits (bounded) until the session is subscribed to the stream.
    #[test]
    fn should_wait_for_new_data_with_long_poll() {
        let s = setup();
        let stream_name = "longpoll-wait-test";
        s.n.broker_subscribe(stream_name);

        // Add data first
        s.n.broadcast(stream_name, r#"{"msg":"existing data"}"#).expect("broadcast");

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        // Get current offset
        let meta = stream.head(&[]).expect("HEAD");

        let offset = meta.next_offset;

        let (done_tx, done) = channel();

        // Start reading in long-poll mode (a detached thread: a hung read must not
        // block the test's failure).
        std::thread::spawn(move || {
            let mut it = stream.read(&offset, LiveMode::LongPoll, Duration::from_secs(2));
            let mut received: Option<Vec<Map<String, Value>>> = None;
            let mut read_err = None;

            // Client may perform a non-live catch-up request first (fetch-then-live),
            // so skip empty chunks until we receive live data
            loop {
                match it.next() {
                    Err(Error::Done) => break,
                    Err(e) => {
                        read_err = Some(e);
                        break;
                    }
                    Ok(chunk) => {
                        if chunk.data.is_empty() || chunk.data == b"[]" {
                            continue;
                        }
                        received = serde_json::from_slice(&chunk.data).ok();
                        break;
                    }
                }
            }
            it.close();
            let _ = done_tx.send((received, read_err));
        });

        assert!(s.n.wait_subscribed(stream_name, WAIT), "the long-poll request did not subscribe");

        assert_eq!(s.n.size(), 1);

        // Append data while long-poll is waiting
        s.n.broadcast(stream_name, r#"{"msg":"new data"}"#).expect("broadcast");

        let (received, read_err) = done.recv_timeout(Duration::from_secs(2)).expect("timeout waiting for long-poll");

        assert_eq!(read_err, None);
        let received = received.unwrap_or_default();
        assert!(!received.is_empty(), "should have received data via long-poll");
        assert_eq!(received[0]["msg"], "new data");

        // Ensure the session is removed from the hub
        assert_eq!(s.n.size(), 0);
        s.assert_no_unexpected_calls();
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_LongPoll/should_return_immediately_if_data_already_exists
    #[test]
    fn should_return_immediately_if_data_already_exists() {
        let s = setup();
        let stream_name = "longpoll-immediate-test";
        s.n.broker_subscribe(stream_name);

        // Add data first
        s.n.broadcast(stream_name, r#"{"msg":"existing data"}"#).expect("broadcast");

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        // Read should return existing data immediately (no live mode)
        let start_time = Instant::now();

        let mut it = stream.read(START_OFFSET, LiveMode::LongPoll, READ_DEADLINE);

        let chunk = it.next().expect("read");

        let elapsed = start_time.elapsed();

        // Should return quickly (not waiting for timeout)
        assert!(elapsed < Duration::from_secs(1), "should return immediately without waiting ({elapsed:?})");

        let messages = messages(&chunk.data);

        assert!(!messages.is_empty());
        assert_eq!(messages[0]["msg"], "existing data");

        // Ensure the session is removed from the hub
        assert_eq!(s.n.size(), 0);
        it.close();
        s.assert_no_unexpected_calls();
    }
}

/// upstream: ds/integration_test.go#TestDSIntegration_SSE_OffsetRequired
#[test]
fn test_ds_integration_sse_offset_required() {
    let s = setup_integration_server();

    let stream_name = "sse-no-offset-test";
    s.n.broker_subscribe(stream_name);

    let resp = s.ts.request(&Request::new("GET", &format!("/ds/{stream_name}?live=sse")));

    assert_eq!(resp.status, 400);
    s.assert_no_unexpected_calls();
}

mod test_ds_integration_sse {
    use super::*;

    /// "should return catchup data via SSE with correct metadata"'s expectation,
    /// scoped to its stream.
    fn program_catchup(s: &IntegrationServer) {
        let stream_name = "sse-catchup-test";
        s.n.on_subscribe(Some(stream_name), subscribe_result(stream_name), 0, true);
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_SSE/should_return_catchup_data_via_SSE_with_correct_metadata
    #[test]
    fn should_return_catchup_data_via_sse_with_correct_metadata() {
        let s = setup_integration_server();
        let stream_name = "sse-catchup-test";
        s.n.broker_subscribe(stream_name);

        // Clients using fetch-then-live do not subscribe during catch-up,
        // so scope the expectation to this stream to avoid leaking it into other tests
        program_catchup(&s);

        s.n.broadcast(stream_name, r#"{"id":1,"msg":"hello"}"#).expect("broadcast");
        s.n.broadcast(stream_name, r#"{"id":2,"msg":"world"}"#).expect("broadcast");

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        let mut it = stream.read(START_OFFSET, LiveMode::Sse, READ_DEADLINE);

        let chunk = it.next().expect("read");

        let messages = messages(&chunk.data);

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["id"], 1.0);
        assert_eq!(messages[0]["msg"], "hello");
        assert_eq!(messages[1]["id"], 2.0);
        assert_eq!(messages[1]["msg"], "world");

        assert!(chunk.up_to_date, "should be up to date after catchup");
        assert!(!chunk.next_offset.is_empty(), "should have next offset");
        assert!(!chunk.cursor.is_empty(), "should have cursor for CDN collapsing");
        it.close();
        s.assert_no_unexpected_calls();
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_SSE/should_stream_live_data_via_SSE
    /// note: upstream sleeps 500ms before asserting `n.Size() == 1` and broadcasting (here: waits, bounded, until the session is subscribed) and 100ms before asserting `n.Size() == 0` (here: the size is awaited, bounded, then asserted). The catch-up subtest's stream-scoped expectation is replayed.
    #[test]
    fn should_stream_live_data_via_sse() {
        let s = setup_integration_server();
        program_catchup(&s);
        let stream_name = "sse-live-test";
        s.n.broker_subscribe(stream_name);

        s.n.on_subscribe(None, subscribe_result(stream_name), 1, false);

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        let (done_tx, done) = channel();

        std::thread::spawn(move || {
            let mut it = stream.read(START_OFFSET, LiveMode::Sse, Duration::from_secs(5));
            let result = (|| {
                // First chunk is empty catchup
                let mut chunk = match it.next() {
                    Err(Error::Done) => return (None, None),
                    Err(e) => return (None, Some(e)),
                    Ok(c) => c,
                };

                // If first chunk is empty, wait for the next one (live data)
                if chunk.data.is_empty() || chunk.data == b"[]" {
                    chunk = match it.next() {
                        Err(Error::Done) => return (None, None),
                        Err(e) => return (None, Some(e)),
                        Ok(c) => c,
                    };
                }

                let received: Option<Vec<Map<String, Value>>> =
                    if chunk.data.is_empty() { None } else { serde_json::from_slice(&chunk.data).ok() };
                (received, None)
            })();
            it.close();
            let _ = done_tx.send(result);
        });

        assert!(s.n.wait_subscribed(stream_name, WAIT), "the SSE request did not subscribe");

        assert_eq!(s.n.size(), 1);

        s.n.broadcast(stream_name, r#"{"msg":"live data"}"#).expect("broadcast");

        let (received, read_err) = done.recv_timeout(Duration::from_secs(10)).expect("the SSE reader did not finish");

        assert_eq!(read_err, None);
        let received = received.unwrap_or_default();
        assert!(!received.is_empty(), "should have received data via SSE");
        assert_eq!(received[0]["msg"], "live data");

        s.n.wait_size(0, WAIT);

        assert_eq!(s.n.size(), 0);
        s.assert_no_unexpected_calls();
    }

    /// upstream: ds/integration_test.go#TestDSIntegration_SSE/should_support_reconnection_with_last_known_offset
    /// note: replays the earlier subtests' controller expectations that can still match: the catch-up one (scoped to its stream); the live subtest's `Once` expectation was used up upstream, and a used-up testify expectation never matches again, so it is left out.
    #[test]
    fn should_support_reconnection_with_last_known_offset() {
        let s = setup_integration_server();
        program_catchup(&s);
        let stream_name = "sse-reconnect-test";
        s.n.broker_subscribe(stream_name);

        // Called twice: initial connection and reconnection
        s.n.on_subscribe(None, subscribe_result(stream_name), 2, false);

        s.n.broadcast(stream_name, r#"{"id":1,"msg":"message 1"}"#).expect("broadcast");
        s.n.broadcast(stream_name, r#"{"id":2,"msg":"message 2"}"#).expect("broadcast");

        let stream = s.client().stream(&format!("/ds/{stream_name}"));

        let mut it = stream.read(START_OFFSET, LiveMode::Sse, READ_DEADLINE);

        let chunk = it.next().expect("read");
        it.close();

        let last_offset = chunk.next_offset;
        assert!(!last_offset.is_empty());

        s.n.broadcast(stream_name, r#"{"id":3,"msg":"message 3"}"#).expect("broadcast");

        let mut it2 = stream.read(&last_offset, LiveMode::Sse, READ_DEADLINE);

        let chunk2 = it2.next().expect("read");

        let messages = messages(&chunk2.data);

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["id"], 3.0);
        assert_eq!(messages[0]["msg"], "message 3");
        it2.close();
        s.assert_no_unexpected_calls();
    }
}
