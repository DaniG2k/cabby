//! upstream file: node/session_test.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use cabby_core::common::SessionEnv;
use cabby_core::encoders::{FrameType, SentFrame};
use cabby_server::node::Connection;
use cabby_testkit::reference::common::{get_channel_state_field, get_connection_state_field};
use cabby_testkit::reference::node::*;
use serde_json::json;

use super::support::{DEADLINE, map};

fn text(payload: &str) -> SentFrame {
    SentFrame { frame_type: FrameType::Text, payload: payload.as_bytes().to_vec(), ..Default::default() }
}

/// upstream: node/session_test.go#TestSendRaceConditions
#[test]
fn test_send_race_conditions() {
    let node = new_mock_node();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let stop_readers = AtomicBool::new(false);
    let mut expected = 0;

    std::thread::scope(|sc| {
        for _ in 1..=10 {
            let session = new_mock_session("123", &node, vec![]);
            let conn = session.conn().clone();
            let session = session.rust().clone();

            // Upstream reads `session.conn` in an endless goroutine; these
            // readers stop when the sends are done.
            let stop = &stop_readers;
            sc.spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let _ = conn.read();
                }
            });

            expected += 4;
            for pair in [["hi!", "bye"], ["bye", "why"]] {
                let session = session.clone();
                let done_tx = done_tx.clone();
                sc.spawn(move || {
                    std::thread::scope(|inner| {
                        for payload in pair {
                            let (session, done_tx) = (session.clone(), done_tx.clone());
                            inner.spawn(move || {
                                session.send_frame(text(payload));
                                done_tx.send(()).expect("done channel");
                            });
                        }
                    });
                });
            }
        }

        // wg.Wait(): every concurrent send completes (bounded here).
        let result = (0..expected).try_for_each(|i| done_rx.recv_timeout(DEADLINE).map_err(|_| i));
        stop_readers.store(true, Ordering::Release);
        if let Err(i) = result {
            panic!("only {i} of {expected} sends completed within {DEADLINE:?}");
        }
    });
}

/// upstream: node/session_test.go#TestSessionSend
#[test]
fn test_session_send() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);

    std::thread::scope(|sc| {
        let s = session.rust().clone();
        sc.spawn(move || {
            for _ in 1..=10 {
                s.send_frame(text("bye"));
            }
        });

        for _ in 1..=10 {
            let r = session.conn().read();
            assert!(r.is_ok(), "{r:?}");
        }
    });
}

/// upstream: node/session_test.go#TestSessionDisconnect
#[test]
fn test_session_disconnect() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);
    // Upstream: session.closed = false; session.connected.Store(true)
    session.rust().set_connected(true);

    std::thread::scope(|sc| {
        let s = session.rust().clone();
        sc.spawn(move || {
            s.send_frame(text("bye"));
            s.disconnect("test", 1042);
        });

        // Message frame
        let r = session.conn().read();
        assert!(r.is_ok(), "{r:?}");

        // Close frame
        let r = session.conn().read();
        assert!(r.is_ok(), "{r:?}");
    });
}

/// upstream: node/session_test.go#TestMergeEnv
#[test]
fn test_merge_env() {
    let node = new_mock_node();
    let session = new_mock_session("123", &node, vec![]);

    let istate: HashMap<String, _> = [("test_channel".to_owned(), map(&[("foo", "bar"), ("a", "z")]))].into();
    let cstate = map(&[("_s_", "id=42")]);
    let orig_env = SessionEnv { channel_states: Some(istate), connection_state: Some(cstate), ..Default::default() };

    session.set_env(&orig_env);

    let istate2: HashMap<String, _> = [
        ("test_channel".to_owned(), map(&[("foo", "baz")])),
        ("another_channel".to_owned(), map(&[("wasting", "time")])),
    ]
    .into();

    let env = SessionEnv { channel_states: Some(istate2), ..Default::default() };

    let cstate2 = map(&[("red", "end of silence")]);

    let env2 = SessionEnv { connection_state: Some(cstate2), ..Default::default() };

    std::thread::scope(|sc| {
        sc.spawn(|| session.merge_env(&env));
        sc.spawn(|| session.merge_env(&env2));
    });

    // Upstream: assert.Equal(t, &origEnv, session.GetEnv()). In Go the session
    // keeps the very `origEnv` pointer and MergeEnv mutates it in place, so the
    // assertions below read `origEnv`. A Rust session owns its env; its
    // snapshot stands for `origEnv`, and the parts MergeEnv must not touch are
    // still those of `origEnv`.
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

/// upstream: node/session_test.go#TestCacheEntry
#[test]
fn test_cache_entry() {
    let session = bare_session();

    session.subscriptions().add_channel("chat_1");
    session.subscriptions().add_channel("presence_1");

    session.subscriptions().add_channel_stream("chat_1", "a");
    session.subscriptions().add_channel_stream("chat_1", "b");
    session.subscriptions().add_channel_stream("presence_1", "z");

    // session.env = common.NewSessionEnv("/cable", nil) (bare_session)
    session.set_identifiers("plastilin");
    session.rust().update_env(&mut |e| {
        e.merge_connection_state(&map(&[("tenant", "x"), ("locale", "it")]));
        e.merge_channel_state("chat_1", &map(&[("presence", "on")]));
    });

    session.mark_disconnectable(true);

    let cached = session.to_cache_entry().expect("cache entry");

    let new_session = bare_session();

    new_session.restore_from_cache(&cached).expect("restore");

    assert_eq!("plastilin", new_session.get_identifiers());

    let channels = new_session.subscriptions().channels();
    assert!(channels.contains(&"chat_1".to_owned()));
    assert!(channels.contains(&"presence_1".to_owned()));
    let streams = |id| new_session.subscriptions().streams_for(id).unwrap_or_default();
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
#[test]
fn test_cache_entry_empty_session() {
    let session = bare_session();

    let cached = session.to_cache_entry().expect("cache entry");

    let new_session = bare_session();

    new_session.restore_from_cache(&cached).expect("restore");
}

/// upstream: node/session_test.go#TestMarkDisconnectable
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
