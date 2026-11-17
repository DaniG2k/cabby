//! upstream file: node/session_test.go

use std::collections::HashMap;

use cabby_core::common::{SessionEnv, StringMap};
use cabby_testkit::reference::common::{get_channel_state_field, get_connection_state_field};
use cabby_testkit::reference::node::*;
use serde_json::json;

use super::support::{DEADLINE, map};

/// upstream: node/session_test.go#TestSendRaceConditions
/// note: the scaffolding (10 sessions, endless readers, four nested concurrent sendFrame calls each, wg.Wait) runs in the facade on detached threads, bounded by a deadline; on Go it runs as goroutines in the bridge, with sendFrame(TextFrame) as the exported SendJSONTransmission (the same frame with the JSON encoder).
#[test]
fn test_send_race_conditions() {
    let node = new_mock_node();

    assert!(send_race_conditions(&node, DEADLINE), "concurrent sends did not complete within {DEADLINE:?}");
}

/// upstream: node/session_test.go#TestSessionSend
/// note: upstream sends from a goroutine while reading; here the ten sends come first (the mock connection buffers ten frames, so no send blocks) because Go reads go through the serialized bridge. sendFrame(TextFrame) is the exported SendJSONTransmission on Go.
#[test]
fn test_session_send() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);

    for _ in 1..=10 {
        session.send_text_frame("bye");
    }

    for _ in 1..=10 {
        let r = session.read();
        assert!(r.is_ok(), "{r:?}");
    }
}

/// upstream: node/session_test.go#TestSessionDisconnect
/// note: session.connected.Store(true) is node.Authenticated(s, s.GetIdentifiers()) on Go (also registers the session in the hub); sendFrame(TextFrame) is SendJSONTransmission; the send and Disconnect run before the reads (upstream: in a goroutine) because Go reads go through the serialized bridge.
#[test]
fn test_session_disconnect() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);
    // Upstream: session.closed = false; session.connected.Store(true)
    session.mark_connected(&node);

    session.send_text_frame("bye");
    session.disconnect("test", 1042);

    // Message frame
    let r = session.read();
    assert!(r.is_ok(), "{r:?}");

    // Close frame
    let r = session.read();
    assert!(r.is_ok(), "{r:?}");
}

fn merge_env_fixture() -> (SessionEnv, SessionEnv, SessionEnv) {
    let istate: HashMap<String, _> = [("test_channel".to_owned(), map(&[("foo", "bar"), ("a", "z")]))].into();
    let cstate = map(&[("_s_", "id=42")]);
    let orig_env = SessionEnv { channel_states: Some(istate), connection_state: Some(cstate), ..Default::default() };

    let istate2: HashMap<String, _> = [
        ("test_channel".to_owned(), map(&[("foo", "baz")])),
        ("another_channel".to_owned(), map(&[("wasting", "time")])),
    ]
    .into();

    let env = SessionEnv { channel_states: Some(istate2), ..Default::default() };

    let cstate2 = map(&[("red", "end of silence")]);

    let env2 = SessionEnv { connection_state: Some(cstate2), ..Default::default() };

    (orig_env, env, env2)
}

/// upstream: node/session_test.go#TestMergeEnv
/// note: the two concurrent MergeEnv calls run on threads (Go: goroutines in the bridge). Upstream asserts on origEnv, which the Go session holds by pointer and MergeEnv mutates; the Rust session owns its env, so its snapshot stands for origEnv.
#[test]
fn test_merge_env() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);

    let (orig_env, env, env2) = merge_env_fixture();

    session.set_env(&orig_env);

    assert!(merge_env_concurrently(&session, &[env, env2], DEADLINE), "MergeEnv calls did not return");

    // Upstream: assert.Equal(t, &origEnv, session.GetEnv()). In Go the session
    // keeps the very `origEnv` pointer and MergeEnv mutates it in place, so the
    // comparison is with itself and the assertions below read `origEnv`. The
    // parts MergeEnv must not touch are still those of `origEnv`.
    let orig_env = session.get_env();
    assert_eq!("", orig_env.url);
    assert_eq!(None, orig_env.headers);
    assert_eq!("", orig_env.identifiers);

    assert_eq!("id=42", get_connection_state_field(&orig_env, "_s_"));
    assert_eq!("end of silence", get_connection_state_field(&orig_env, "red"));

    assert_eq!("baz", get_channel_state_field(&orig_env, "test_channel", "foo"));
    assert_eq!("z", get_channel_state_field(&orig_env, "test_channel", "a"));
    assert_eq!("time", get_channel_state_field(&orig_env, "another_channel", "wasting"));
}

/// Upstream's first assertion compares a pointer with itself and the others
/// read single fields. The merged env is exactly the union: no key lost, none
/// added, untouched channel keys kept.
///
/// regression-for: node/session_test.go#TestMergeEnv
#[test]
fn merge_env_produces_the_exact_union() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);

    let (orig_env, env, env2) = merge_env_fixture();
    session.set_env(&orig_env);

    assert!(merge_env_concurrently(&session, &[env, env2], DEADLINE), "MergeEnv calls did not return");

    let merged = session.get_env();
    assert_eq!(Some(map(&[("_s_", "id=42"), ("red", "end of silence")])), merged.connection_state);
    let expected: HashMap<String, StringMap> = [
        ("test_channel".to_owned(), map(&[("foo", "baz"), ("a", "z")])),
        ("another_channel".to_owned(), map(&[("wasting", "time")])),
    ]
    .into();
    assert_eq!(Some(expected), merged.channel_states);
}

/// upstream: node/session_test.go#TestCacheEntry
/// note: on Go, the bare sessions are node.BuildSession; subscriptions.AddChannel* re-apply a cache entry through the exported RestoreFromCache, subscriptions are read from ToCacheEntry's subs, and env merges are called on GetEnv().
#[test]
fn test_cache_entry() {
    let session = bare_session();

    session.add_channel("chat_1");
    session.add_channel("presence_1");

    session.add_channel_stream("chat_1", "a");
    session.add_channel_stream("chat_1", "b");
    session.add_channel_stream("presence_1", "z");

    // session.env = common.NewSessionEnv("/cable", nil) (bare_session)
    session.set_identifiers("plastilin");
    session.merge_connection_state(&map(&[("tenant", "x"), ("locale", "it")]));
    session.merge_channel_state("chat_1", &map(&[("presence", "on")]));

    session.mark_disconnectable(true);

    let cached = session.to_cache_entry().expect("cache entry");

    let new_session = bare_session();

    new_session.restore_from_cache(&cached).expect("restore");

    assert_eq!("plastilin", new_session.get_identifiers());

    let channels = new_session.channels();
    assert!(channels.contains(&"chat_1".to_owned()));
    assert!(channels.contains(&"presence_1".to_owned()));
    let streams = |id| new_session.streams_for(id).unwrap_or_default();
    assert!(streams("chat_1").contains(&"a".to_owned()));
    assert!(streams("chat_1").contains(&"b".to_owned()));
    assert!(streams("presence_1").contains(&"z".to_owned()));

    let env = new_session.get_env();
    assert_eq!("x", get_connection_state_field(&env, "tenant"));
    assert_eq!("it", get_connection_state_field(&env, "locale"));
    assert_eq!("on", get_channel_state_field(&env, "chat_1", "presence"));

    assert!(new_session.is_disconnectable());
}

/// upstream: node/session_test.go#TestCacheEntryEmptySession
/// note: on Go, the bare sessions are node.BuildSession.
#[test]
fn test_cache_entry_empty_session() {
    let session = bare_session();

    let cached = session.to_cache_entry().expect("cache entry");

    let new_session = bare_session();

    new_session.restore_from_cache(&cached).expect("restore");
}

/// upstream: node/session_test.go#TestMarkDisconnectable
/// note: on Go, the bare session is node.BuildSession.
#[test]
fn test_mark_disconnectable() {
    let session = bare_session();

    session.mark_disconnectable(false);

    assert!(!session.is_disconnectable());

    session.mark_disconnectable(true);

    assert!(session.is_disconnectable());

    session.mark_disconnectable(false);

    assert!(session.is_disconnectable());
}

mod test_send_max_pending_size {
    use super::*;

    /// Upstream `sendTestMessages`. Upstream locks the session's write mutex
    /// (`session.wmu.Lock()`) and sleeps 100ms so the writer routine picks up
    /// the first message and blocks writing it; here the mock connection's
    /// writes are gated and the test waits until that write is held.
    fn send_test_messages(session: &TestSession) {
        // Lock writing to populate the queue
        // (the session was created with SessionOpt::GatedWrites)

        // This message is picked up by the writer routine, so it's not part of the queue
        session.send_reply("message", json!("test A"));
        session.await_writes_held(1); // upstream: time.Sleep(100 * time.Millisecond)

        // This message stays in the queue, size is 37 bytes (31 boilerplate + 6 content)
        session.send_reply("message", json!("test B"));

        // This message tests the maxPendingSize check
        session.send_reply("message", json!("test C"));
        session.release_writes(); // session.wmu.Unlock()

        // Pop initial non-queued message
        let msg = session.read_string();
        assert_eq!(Ok(r#"{"type":"message","message":"test A"}"#.to_owned()), msg);
    }

    /// Upstream sets `session.maxPendingSize.Store(n)` right after
    /// `NewMockSession`, before any send; the option sets the same value at
    /// creation.
    fn gated_session(node: &TestNode, max_pending: u64) -> TestSession {
        new_mock_session("123", node, vec![SessionOpt::GatedWrites, SessionOpt::MaxPendingSize(max_pending)])
    }

    /// upstream: node/session_test.go#TestSend__maxPendingSize/Disconnect_when_queue_size_at_least_maxPendingSize
    /// upstream: node/session_test.go#TestSend__maxPendingSize
    /// note: session.wmu.Lock() plus a 100ms sleep is a gated mock connection (the writer blocks writing the first frame) and a wait for that write; maxPendingSize.Store(n) after creation is WithMaxPendingSize(n) at creation.
    #[test]
    fn disconnect_when_queue_size_at_least_max_pending_size() {
        let node = new_mock_node();
        let session = gated_session(&node, 37);

        send_test_messages(&session);

        // Receive slow disconnect notice instead of queued messages
        let msg = session.read_string();
        assert_eq!(Ok(r#"{"type":"disconnect","reason":"too_slow","reconnect":true}"#.to_owned()), msg);
    }

    /// upstream: node/session_test.go#TestSend__maxPendingSize/Allow_when_queue_size_less_than_maxPendingSize
    /// upstream: node/session_test.go#TestSend__maxPendingSize
    /// note: session.wmu.Lock() plus a 100ms sleep is a gated mock connection and a wait for the held write; maxPendingSize.Store(n) is WithMaxPendingSize(n) at creation.
    #[test]
    fn allow_when_queue_size_less_than_max_pending_size() {
        let node = new_mock_node();
        let session = gated_session(&node, 38);

        send_test_messages(&session);

        // Receive queued messages
        assert_eq!(Ok(r#"{"type":"message","message":"test B"}"#.to_owned()), session.read_string());
        assert_eq!(Ok(r#"{"type":"message","message":"test C"}"#.to_owned()), session.read_string());
    }

    /// upstream: node/session_test.go#TestSend__maxPendingSize/Allow_when_maxPendingSize_is_0_(unlimited)
    /// upstream: node/session_test.go#TestSend__maxPendingSize
    /// note: session.wmu.Lock() plus a 100ms sleep is a gated mock connection and a wait for the held write; maxPendingSize.Store(n) is WithMaxPendingSize(n) at creation.
    #[test]
    fn allow_when_max_pending_size_is_0_unlimited() {
        let node = new_mock_node();
        let session = gated_session(&node, 0);

        send_test_messages(&session);

        // Receive queued messages
        assert_eq!(Ok(r#"{"type":"message","message":"test B"}"#.to_owned()), session.read_string());
        assert_eq!(Ok(r#"{"type":"message","message":"test C"}"#.to_owned()), session.read_string());
    }
}
