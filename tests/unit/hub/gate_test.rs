//! upstream file: hub/gate_test.go

use cabby_core::common::StreamMessage;
use cabby_testkit::reference::hub::{Gate, MockSession};

fn msg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

/// upstream: hub/gate_test.go#TestUnsubscribe
/// note: upstream's `len(gate.streams)` is exactly `Gate.Size()`, asserted here; the `sessionsStreams` check is in test_unsubscribe_sessions_streams.
#[test]
fn test_unsubscribe() {
    let _load = crate::shared_load();
    let gate = Gate::new();

    let session = MockSession::new("123");

    gate.subscribe(&session, "test", "test_channel");

    assert_eq!(1, gate.size());

    gate.unsubscribe(&session, "test", "test_channel");

    assert_eq!(0, gate.size());

    // Upstream: assert.Equal(t, 0, len(gate.streams)) (= Size()).
    assert_eq!(0, gate.size());
}

/// upstream: hub/gate_test.go#TestUnsubscribe
/// note: not_observable on Go: `len(gate.sessionsStreams)` reads an unexported map (which the Go implementation never writes, so upstream's assertion is vacuous there).
#[test]
fn test_unsubscribe_sessions_streams() {
    let _load = crate::shared_load();
    let gate = Gate::new();

    let session = MockSession::new("123");

    gate.subscribe(&session, "test", "test_channel");
    gate.unsubscribe(&session, "test", "test_channel");

    assert_eq!(0, gate.sessions_streams_size());
}

/// upstream: hub/gate_test.go#TestShutdown
///
/// Upstream builds the gate with `context.WithTimeout(100ms)`, broadcasts "1",
/// waits for `<-ctx.Done()`, broadcasts "2", ignores the first read and requires
/// the second read to fail. Here the context is cancelled explicitly instead of
/// by a timer, and the (ignored) first read happens before cancelling: it
/// synchronizes on "1" having been handled, so the cancelled broadcast loop is
/// idle and cannot pick up "2" (a Go `select` with both cases ready chooses at
/// random).
/// note: context cancelled explicitly instead of a 100 ms timeout; the ignored first read moved before the cancel so the cancelled loop cannot pick up "2".
#[test]
fn test_shutdown() {
    let _load = crate::shared_load();
    let gate = Gate::new();

    let session = MockSession::new("123");

    gate.subscribe(&session, "test", "test_channel");

    gate.broadcast(msg("test", "1"));

    // Ignore first message if any, we want to make sure the second one is not received
    let _ = session.read();

    gate.cancel();

    gate.broadcast(msg("test", "2"));

    let msg = session.read();

    assert!(msg.is_err(), "expected not to receive messages when context is done, but received: {msg:?}");
}
