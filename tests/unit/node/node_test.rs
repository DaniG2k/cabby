//! upstream file: node/node_test.go
//!
//! Upstream subtests share their parent's node and sessions. Each Rust test
//! rebuilds that setup and replays the earlier subtests' steps its own steps
//! depend on (`TestWhisper/When_whispering_stream_is_not_configured` first
//! configures the whisper stream it removes; `TestPerform/With_channel_state`
//! first runs the earlier performs on `test_channel`). Accumulated testify
//! expectations only matter when their arguments match and are replicated
//! where they do.
//!
//! `assert.Nil(msg); assert.Error(err)` after a read means "no frame within
//! the mock connection's 100ms read timeout".
//!
//! Upstream arranges hub state directly (`node.hub.AddSession`,
//! `node.hub.SubscribeSession`) and session subscriptions directly
//! (`session.subscriptions.AddChannel*`). Against Go the facade reaches the
//! same state through exported API (see each method's documentation in
//! `cabby_testkit::reference::node`); the tests note where that applies.
//! `hub.AddSession` also marks the session connected on both backends (Go has
//! no hub-only call and uses `node.Authenticated`), so both exercise the same
//! disconnect path.

use std::time::Duration;

use cabby_core::common::{
    CommandResult, ConnectResult, HistoryPosition, HistoryRequest, MessageData, PRESENCE_JOIN_TYPE,
    PRESENCE_STREAM_STATE, PresenceEvent, Status, StreamMessage, WHISPER_STREAM_STATE,
};
use cabby_testkit::reference::common::{get_channel_state_field, get_connection_state_field};
use cabby_testkit::reference::node::*;
use serde_json::json;

use super::support::*;

fn sm(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

// ------------------------------------------------------------ TestAuthenticate

mod test_authenticate {
    use super::*;

    /// upstream: node/node_test.go#TestAuthenticate/Successful_authentication
    /// upstream: node/node_test.go#TestAuthenticate
    #[test]
    fn successful_authentication() {
        let node = new_mock_node();
        let _hub = node.run_hub();

        let session = new_mock_session_with_env("1", &node, "/cable", Some(map(&[("id", "test_id")])), vec![]);
        let res = node.authenticate(&session);
        let _remove = remove_on_drop(&node, &session);

        assert!(res.is_ok(), "Error must be nil: {res:?}");
        assert!(session.is_connected(), "Session must be marked as connected");
        assert_eq!("test_id", session.get_identifiers(), "Identifiers must be equal to test_id");

        let msg = session.read();
        assert!(msg.is_ok(), "{msg:?}");

        assert_eq!(b"welcome".to_vec(), msg.unwrap(), "Sent message is invalid");

        assert_eq!(1, node.size());
    }

    /// upstream: node/node_test.go#TestAuthenticate/Failed_authentication
    /// upstream: node/node_test.go#TestAuthenticate
    #[test]
    fn failed_authentication() {
        let node = new_mock_node();
        let _hub = node.run_hub();

        let session = new_mock_session_with_env("1", &node, "/failure", Some(map(&[("id", "test_id")])), vec![]);

        let res = node.authenticate(&session);

        assert!(res.is_ok(), "Error must be nil: {res:?}");

        let msg = session.read();
        assert!(msg.is_ok(), "{msg:?}");

        assert_eq!(b"unauthorized".to_vec(), msg.unwrap(), "Sent message is invalid");
        assert_eq!(0, node.size());
    }

    /// upstream: node/node_test.go#TestAuthenticate/Error_during_authentication
    /// upstream: node/node_test.go#TestAuthenticate
    #[test]
    fn error_during_authentication() {
        let node = new_mock_node();
        let _hub = node.run_hub();

        let session = new_mock_session_with_env("1", &node, "/error", Some(map(&[("id", "test_id")])), vec![]);

        let res = node.authenticate(&session);

        assert!(res.is_err(), "Error must not be nil");
        assert_eq!(0, node.size());
    }

    /// upstream: node/node_test.go#TestAuthenticate/With_connection_state
    /// upstream: node/node_test.go#TestAuthenticate
    #[test]
    fn with_connection_state() {
        let node = new_mock_node();
        let _hub = node.run_hub();

        let session = new_mock_session_with_env(
            "1",
            &node,
            "/cable",
            Some(map(&[("x-session-test", "my_session"), ("id", "session_id")])),
            vec![],
        );
        let _remove = remove_on_drop(&node, &session);

        let res = node.authenticate(&session);

        assert!(res.is_ok(), "Error must be nil: {res:?}");
        assert!(session.is_connected(), "Session must be marked as connected");

        let cstate = session.get_env().connection_state.expect("connection state");
        assert_eq!(1, cstate.len());
        assert_eq!(Some(&"my_session".to_owned()), cstate.get("_s_"));

        assert_eq!(1, node.size());
    }
}

// ------------------------------------------- TestAuthenticatedRacesWithDisconnect

/// Reproduces a hub leak: a session that closes while Authenticated runs
/// can stay registered in the hub forever.
///
/// upstream: node/node_test.go#TestAuthenticatedRacesWithDisconnect
/// note: the racing callers (1000 sessions, Authenticated and Disconnect released together, wg.Wait) run in the facade; on Go they are goroutines in the bridge.
/// note: on Go, hub.FindByIdentifier is read through the exported node.LookupSession, which returns exactly it.
#[test]
fn test_authenticated_races_with_disconnect() {
    let node = new_mock_node();
    let _hub = node.run_hub();

    const N: usize = 1000;
    let sessions = authenticated_races_with_disconnect(&node, N, DEADLINE);

    let leaked =
        || sessions.iter().filter(|s| s.is_closed() && node.hub_find_by_identifier(&s.get_id()).is_some()).count();

    // Upstream: assert.EventuallyWithT(..., time.Second, 10*time.Millisecond)
    eventually("closed sessions removed from the hub", Duration::from_secs(1), Duration::from_millis(10), || {
        leaked() == 0 && node.size() == 0
    });
    assert_eq!(0, leaked(), "closed sessions left in the hub");
    assert_eq!(0, node.size());
}

// ---------------------------------------------- TestDisconnectNowClosesBeforeEnqueue

/// A full disconnect queue blocks Enqueue; in-flight commands must see
/// the session as closed meanwhile (otherwise they could re-add it to the hub).
///
/// upstream: node/node_test.go#TestDisconnectNowClosesBeforeEnqueue
/// note: blockingDisconnector is built in the facade (Go: in the bridge) and installed with the exported SetDisconnector; it counts Enqueue calls (upstream's second close would panic) and the case asserts one.
/// note: on Go, session.closeCtx (unexported) is read by reflection and its Err() checked, as upstream.
#[test]
fn test_disconnect_now_closes_before_enqueue() {
    let node = new_mock_node();
    let _hub = node.run_hub();

    let disconnector = node.set_blocking_disconnector();

    let session = new_mock_session("1", &node, vec![]);
    session.mark_disconnectable(true);
    node.authenticated(&session, "ids");

    session.disconnect_now_in_background("Read failed", CLOSE_ABNORMAL_CLOSURE);

    // Upstream: select on `enqueued` with a 1s timeout.
    if !disconnector.await_enqueued(Duration::from_secs(1)) {
        disconnector.release();
        panic!("session was not enqueued for disconnect");
    }

    assert!(session.is_closed());
    assert!(session.close_context_cancelled(&node), "closeCtx.Err() must be non-nil");
    // Upstream closes `enqueued` in Enqueue, so a second call would panic.
    assert_eq!(1, disconnector.enqueues(), "Enqueue must be called once");

    disconnector.release();
}

// ---------------------------------------------------------------- TestSubscribe

mod test_subscribe {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode, &TestSession)) {
        let node = new_mock_node();
        let session = new_mock_session("14", &node, vec![]);

        node.hub_add_session(&session);
        let _remove = remove_on_drop(&node, &session);

        let _hub = node.run_hub();

        f(&node, &session);
    }

    /// upstream: node/node_test.go#TestSubscribe/Successful_subscription
    /// upstream: node/node_test.go#TestSubscribe
    /// note: on Go, hub.AddSession is node.Authenticated(s, s.GetIdentifiers()) (both backends mark the session connected), and subscriptions are read from ToCacheEntry's subs.
    #[test]
    fn successful_subscription() {
        setup(|node, session| {
            let res = node.subscribe(session, &msg("test_channel"));
            assert!(res.is_ok(), "Error must be nil: {res:?}");

            // Adds subscription to session
            assert!(session.has_channel("test_channel"), "Session subscription must be set");

            let msg = session.read();
            assert!(msg.is_ok(), "{msg:?}");

            assert_eq!(b"14".to_vec(), msg.unwrap(), "Sent message is invalid");
        });
    }

    /// upstream: node/node_test.go#TestSubscribe/Subscription_with_a_stream
    /// upstream: node/node_test.go#TestSubscribe
    /// note: on Go, hub.AddSession is node.Authenticated(s, s.GetIdentifiers()) (both backends mark the session connected), hub.BroadcastMessage is node.Broadcast, and subscriptions are read from ToCacheEntry's subs.
    #[test]
    fn subscription_with_a_stream() {
        setup(|node, session| {
            let res = node.subscribe(session, &msg("with_stream"));
            assert!(res.is_ok(), "Error must be nil: {res:?}");

            // Adds subscription and stream to session
            assert!(session.has_channel("with_stream"), "Session subsription must be set");
            assert_eq!(Some(strings(&["stream"])), session.streams_for("with_stream"));

            let msg = session.read_string();
            assert_eq!(Ok("14".to_owned()), msg, "Sent message is invalid");

            // Make sure session is subscribed
            node.hub_broadcast_message(&sm("stream", "41"));

            let msg = session.read_string();
            assert_eq!(
                Ok("{\"identifier\":\"with_stream\",\"message\":41}".to_owned()),
                msg,
                "Broadcasted message is invalid"
            );
        });
    }

    /// upstream: node/node_test.go#TestSubscribe/Error_during_subscription
    /// upstream: node/node_test.go#TestSubscribe
    /// note: on Go, hub.AddSession is node.Authenticated(s, s.GetIdentifiers()) (both backends mark the session connected).
    #[test]
    fn error_during_subscription() {
        setup(|node, session| {
            let res = node.subscribe(session, &msg("error"));
            assert!(res.is_err());
        });
    }

    /// upstream: node/node_test.go#TestSubscribe/Rejected_subscription
    /// upstream: node/node_test.go#TestSubscribe
    /// note: on Go, hub.AddSession is node.Authenticated(s, s.GetIdentifiers()) (both backends mark the session connected), and subscriptions are read from ToCacheEntry's subs.
    #[test]
    fn rejected_subscription() {
        setup(|node, _| {
            let session = new_mock_session("15", node, vec![]);

            node.hub_add_session(&session);
            let _remove = remove_on_drop(node, &session);

            let res = node.subscribe(&session, &msg("failure"));

            let res = res.expect("no error");
            assert_eq!(Status::Failure, res.expect("a result").status);
            assert_eq!(0, session.channels().len());
        });
    }
}

// -------------------------------------------------------------- TestUnsubscribe

mod test_unsubscribe {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode, &TestSession)) {
        let node = new_mock_node();
        let session = new_mock_session("14", &node, vec![]);

        node.hub_add_session(&session);
        let _remove = remove_on_drop(&node, &session);

        let _hub = node.run_hub();

        f(&node, &session);
    }

    /// upstream: node/node_test.go#TestUnsubscribe/Successful_unsubscribe
    /// upstream: node/node_test.go#TestUnsubscribe
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel re-applies a cache entry (RestoreFromCache); hub.SubscribeSession is the upstream call on node.hub (read by reflection); hub.Broadcast is node.Broadcast.
    #[test]
    fn successful_unsubscribe() {
        setup(|node, session| {
            session.add_channel("test_channel");
            node.hub_subscribe_session(session, "streamo", "test_channel");

            node.hub_broadcast("streamo", r#""before""#);
            let msg = session.read_string().expect("a broadcast");

            assert_eq!(r#"{"identifier":"test_channel","message":"before"}"#, msg, "Broadcasted message is invalid");

            let res = node.unsubscribe(session, &msg_ident("test_channel"));
            assert!(res.is_ok(), "Error must be nil: {res:?}");

            // Removes subscription from session
            assert!(!session.has_channel("test_channel"), "Shouldn't contain test_channel");

            node.hub_broadcast_message(&sm("streamo", "41"));

            assert_no_message(session, "Session hasn't received any messages");
        });
    }

    /// upstream: node/node_test.go#TestUnsubscribe/Error_during_unsubscription
    /// upstream: node/node_test.go#TestUnsubscribe
    /// note: on Go, subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn error_during_unsubscription() {
        setup(|node, session| {
            session.add_channel("failure");

            let res = node.unsubscribe(session, &msg_ident("failure"));
            assert!(res.is_err());
        });
    }

    fn msg_ident(id: &str) -> cabby_core::common::Message {
        msg(id)
    }
}

// ------------------------------------------------------------------ TestPerform

mod test_perform {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode, &TestSession)) {
        let node = new_mock_node();
        let session = new_mock_session("14", &node, vec![]);

        node.hub_add_session(&session);
        let _remove = remove_on_drop(&node, &session);

        session.add_channel("test_channel");

        let _hub = node.run_hub();

        f(&node, &session);
    }

    /// upstream: node/node_test.go#TestPerform/Successful_perform
    /// upstream: node/node_test.go#TestPerform
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected) and subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn successful_perform() {
        setup(|node, session| {
            let res = node.perform(session, &msg_data("test_channel", "action"));
            assert!(res.is_ok(), "{res:?}");

            let msg = session.read();
            assert!(msg.is_ok(), "{msg:?}");

            assert_eq!(b"action".to_vec(), msg.unwrap(), "Sent message is invalid");
        });
    }

    /// upstream: node/node_test.go#TestPerform/With_connection_state
    /// upstream: node/node_test.go#TestPerform
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected) and subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn with_connection_state() {
        setup(|node, session| {
            let res = node.perform(session, &msg_data("test_channel", "session"));
            assert!(res.is_ok(), "{res:?}");

            let r = session.read();
            assert!(r.is_ok(), "{r:?}");

            let cstate = session.get_env().connection_state.expect("connection state");
            assert_eq!(1, cstate.len());
            assert_eq!(Some(&"performed".to_owned()), cstate.get("_s_"));
        });
    }

    /// upstream: node/node_test.go#TestPerform/Error_during_perform
    /// upstream: node/node_test.go#TestPerform
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected) and subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn error_during_perform() {
        setup(|node, session| {
            session.add_channel("failure");

            let res = node.perform(session, &msg_data("failure", "test"));
            assert!(res.is_err(), "Error must not be nil");
        });
    }

    /// upstream: node/node_test.go#TestPerform/With_stopped_streams
    /// upstream: node/node_test.go#TestPerform
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache); hub.SubscribeSession is the upstream call on node.hub (read by reflection); hub.BroadcastMessage is node.Broadcast.
    #[test]
    fn with_stopped_streams() {
        setup(|node, session| {
            session.add_channel_stream("test_channel", "stop_stream");
            node.hub_subscribe_session(session, "stop_stream", "test_channel");

            node.hub_broadcast_message(&sm("stop_stream", "40"));

            let msg = session.read();
            assert!(msg.is_ok(), "a broadcast must arrive: {msg:?}");

            let res = node.perform(session, &msg_data("test_channel", "stop_stream"));
            assert!(res.is_ok(), "{res:?}");

            assert!(session.streams_for("test_channel").unwrap_or_default().is_empty());

            let res = node.perform(session, &msg_data("test_channel", "stop_stream"));
            assert!(res.is_ok(), "{res:?}");

            node.hub_broadcast_message(&sm("stop_stream", "41"));

            assert_no_message(session, "Session hasn't received any messages");
        });
    }

    /// upstream: node/node_test.go#TestPerform/With_channel_state
    /// upstream: node/node_test.go#TestPerform
    /// note: replays the earlier subtests' performs on test_channel before the opening empty-channel-state assertion, which guards that they leave none.
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected) and subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn with_channel_state() {
        setup(|node, session| {
            // The earlier subtests' performs on test_channel ("action",
            // "session", "stop_stream" twice), which must leave no channel state.
            for data in ["action", "session"] {
                node.perform(session, &msg_data("test_channel", data)).expect("perform");
                let r = session.read();
                assert!(r.is_ok(), "{r:?}");
            }
            for _ in 0..2 {
                node.perform(session, &msg_data("test_channel", "stop_stream")).expect("perform");
            }

            assert_eq!(0, session.get_env().channel_states.expect("channel states").len());

            let res = node.perform(session, &msg_data("test_channel", "channel_state"));
            assert!(res.is_ok(), "{res:?}");

            let r = session.read();
            assert!(r.is_ok(), "{r:?}");

            let states = session.get_env().channel_states.expect("channel states");
            assert_eq!(1, states.len());
            assert_eq!(1, states["test_channel"].len());
            assert_eq!("performed", states["test_channel"]["_c_"]);
        });
    }
}

// ------------------------------------------------------------------ TestWhisper

mod test_whisper {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode, &TestSession, &TestSession)) {
        let node = new_mock_node();
        let session = new_mock_session("14", &node, vec![]);
        let session2 = new_mock_session("15", &node, vec![]);

        // Subscribe using different identifiers to make sure whisper is working
        // per stream name, not per identifier
        let _r1 = subscribe_session_to_stream(&session, &node, "test_channel", "test_whisper");
        let _r2 = subscribe_session_to_stream(&session2, &node, "test_channel_2", "test_whisper");

        let _hub = node.run_hub();

        f(&node, &session, &session2);
    }

    fn set_whisper_stream(session: &TestSession) {
        session.merge_channel_state("test_channel", &map(&[(WHISPER_STREAM_STATE, "test_whisper")]));
    }

    /// upstream: node/node_test.go#TestWhisper/When_whispering_stream_is_configured_for_sending_subscription
    /// upstream: node/node_test.go#TestWhisper
    /// note: on Go, subscribeSessionToStream uses node.Authenticated (hub.AddSession; both backends mark the session connected), RestoreFromCache (subscriptions), hub.SubscribeSession is the upstream call on node.hub (read by reflection), and the node's legacy broker kept by the bridge (node.broker.Subscribe).
    #[test]
    fn when_whispering_stream_is_configured_for_sending_subscription() {
        setup(|node, session, session2| {
            set_whisper_stream(session);

            let res = node.whisper(session, &msg_data("test_channel", "tshh... it's a secret"));
            assert!(res.is_ok(), "{res:?}");

            let expected = r#"{"identifier":"test_channel_2","message":"tshh... it's a secret"}"#;

            let msg = session2.read_string();
            assert_eq!(Ok(expected.to_owned()), msg);

            // Sender do not receive the message
            assert_no_message(session, "Session hasn't received any messages");
        });
    }

    /// upstream: node/node_test.go#TestWhisper/When_whispering_stream_is_not_configured
    /// upstream: node/node_test.go#TestWhisper
    /// note: configures the whisper stream first, as the preceding upstream subtest leaves it, so RemoveChannelState removes a real stream.
    /// note: on Go, subscribeSessionToStream uses node.Authenticated, RestoreFromCache, hub.SubscribeSession is the upstream call on node.hub (read by reflection), and the node's legacy broker kept by the bridge.
    #[test]
    fn when_whispering_stream_is_not_configured() {
        setup(|node, session, session2| {
            // Upstream runs after the "configured" subtest: the whisper stream exists.
            set_whisper_stream(session);

            session.remove_channel_state("test_channel");

            let res = node.whisper(session, &msg_data("test_channel", "tshh... it's a secret"));
            assert!(res.is_ok(), "{res:?}");

            assert_no_message(session2, "the receiver gets nothing");

            // Sender do not receive the message
            assert_no_message(session, "Session hasn't received any messages");
        });
    }

    /// upstream: node/node_test.go#TestWhisper/whispering_data_as_object
    /// upstream: node/node_test.go#TestWhisper
    /// note: on Go, subscribeSessionToStream uses node.Authenticated, RestoreFromCache, hub.SubscribeSession is the upstream call on node.hub (read by reflection), and the node's legacy broker kept by the bridge.
    #[test]
    fn whispering_data_as_object() {
        setup(|node, session, session2| {
            set_whisper_stream(session);

            let mut m = msg("test_channel");
            m.data = Some(MessageData::Json(json!({"msg": "test"})));
            let res = node.whisper(session, &m);
            assert!(res.is_ok(), "{res:?}");

            let expected = r#"{"identifier":"test_channel_2","message":{"msg":"test"}}"#;

            let msg = session2.read_string();
            assert_eq!(Ok(expected.to_owned()), msg);

            // Sender do not receive the message
            assert_no_message(session, "Session hasn't received any messages");
        });
    }
}

// ---------------------------------------------- TestStreamSubscriptionRaceConditions

mod test_stream_subscription_race_conditions {
    use super::*;

    /// upstream: node/node_test.go#TestStreamSubscriptionRaceConditions/stop_and_start_streams_race_conditions
    /// upstream: node/node_test.go#TestStreamSubscriptionRaceConditions
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected), subscriptions.AddChannel re-applies a cache entry (RestoreFromCache), and hub.Broadcast is node.Broadcast.
    #[test]
    fn stop_and_start_streams_race_conditions() {
        let node = new_mock_node();
        let session = new_mock_session("14", &node, vec![]);

        node.hub_add_session(&session);
        session.add_channel("test_channel");

        // We need a real hub here to catch a race condition
        let _hub = node.run_hub();

        let res = node.perform(&session, &msg_data("test_channel", "stop_and_start_streams"));
        assert!(res.is_ok(), "{res:?}");

        // Make sure session is subscribed to the stream
        node.hub_broadcast("all", "2022");

        let msg = session.read_string().expect("a broadcast");

        assert_eq!("{\"identifier\":\"test_channel\",\"message\":2022}", msg, "Broadcasted message is invalid");
    }
}

// --------------------------------------------------------------- TestDisconnect

mod test_disconnect {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode)) {
        let node = new_mock_node();
        let _hub = node.run_hub();
        f(&node);
    }

    /// upstream: node/node_test.go#TestDisconnect/Disconnectable_session
    /// upstream: node/node_test.go#TestDisconnect
    /// note: q.disconnect.Remove() is unexported; the queued item is observed by draining the queue with the exported Shutdown, which invokes node.DisconnectNow, against a testify controller expecting Disconnect for sid "1" with identifiers "test_id" (installed with WithController). With one session this identifies it (by sid/ids, not pointer identity).
    #[test]
    fn disconnectable_session() {
        setup(|node| {
            let session = new_mock_session_with_env("1", node, "/cable", Some(map(&[("id", "test_id")])), vec![]);
            // Authenticate via controller marks session as disconnectable automatically
            node.authenticate(&session).expect("authenticate");

            assert!(session.is_disconnectable());

            assert!(node.disconnect(&session).is_ok());

            assert_eq!(1, node.mock_queue().size(), "Expected disconnect to have 1 task in a queue");

            // Upstream: item, ok := q.disconnect.Remove(); assert.Equal(session, item.Data)
            let controller = new_mock_controller();
            controller.on(Expect::new("Disconnect", vec![eq("1"), any(), eq("test_id"), any()], Ret::Unit));
            node.set_controller(&controller);

            node.mock_queue().shutdown().expect("shutdown");

            let tasks: Vec<_> = controller
                .calls()
                .into_iter()
                .filter(|c| c.method == "Disconnect" && c.args[0] == json!("1") && c.args[2] == json!("test_id"))
                .collect();
            assert_eq!(1, tasks.len(), "Expected to disconnect session");
            controller.assert_number_of_calls("Disconnect", 1);
        });
    }

    /// upstream: node/node_test.go#TestDisconnect/Non-disconnectable_session
    /// upstream: node/node_test.go#TestDisconnect
    /// note: upstream authenticates with the mock controller and then resets the unexported session.disconnectInterest (MarkDisconnectable can only raise it). Here the session is authenticated under a testify controller answering the mock controller's result with DisconnectInterest -1, so the "auto" disconnect mode never raises the flag: the same end state (authenticated, in the hub, not disconnectable).
    #[test]
    fn non_disconnectable_session() {
        setup(|node| {
            let session = new_mock_session_with_env("1", node, "/cable", Some(map(&[("id", "test_id")])), vec![]);
            // Upstream: Authenticate via the mock controller, then
            // session.disconnectInterest = false. Arranged as one Authenticate
            // whose result declares no Disconnect interest.
            let controller = new_mock_controller();
            controller.on(Expect::new(
                "Authenticate",
                vec![eq("1"), any()],
                ret_connect(&ConnectResult {
                    identifier: "test_id".into(),
                    transmissions: vec!["welcome".into()],
                    disconnect_interest: -1,
                    ..Default::default()
                }),
            ));
            node.set_controller(&controller);
            node.authenticate(&session).expect("authenticate");
            assert!(!session.is_disconnectable(), "arrangement: no Disconnect interest");

            assert!(node.disconnect(&session).is_ok());

            assert_eq!(0, node.mock_queue().size(), "Expected disconnect to have 0 tasks in a queue");
        });
    }

    /// upstream: node/node_test.go#TestDisconnect/Non-resumeable_session_unsubscribes_from_broker_streams
    /// upstream: node/node_test.go#TestDisconnect
    /// note: on Go, the testify mocks.Broker is built in the bridge from the same expectations; hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache, no broker call); session.resumeInterval = 0 is the exported option WithKeepaliveIntervals(0, 0) applied to the session.
    #[test]
    fn non_resumeable_session_unsubscribes_from_broker_streams() {
        setup(|node| {
            let mock_broker = new_mock_broker();
            node.set_mock_broker(&mock_broker);

            let session = new_mock_session("2", node, vec![]);
            node.hub_add_session(&session);

            session.add_channel("test_channel_1");
            session.add_channel_stream("test_channel_1", "stream1");
            session.add_channel_stream("test_channel_1", "stream2");

            session.add_channel("test_channel_2");
            session.add_channel_stream("test_channel_2", "stream3");

            session.set_resume_interval(Duration::ZERO);

            mock_broker.on(Expect::new("TouchPresence", vec![any()], Ret::Unit));
            for s in ["stream1", "stream2", "stream3"] {
                mock_broker.on(Expect::new("Unsubscribe", vec![eq(s)], ret_string(s)));
            }

            node.disconnect(&session).expect("disconnect");

            for s in ["stream1", "stream2", "stream3"] {
                mock_broker.assert_called("Unsubscribe", &[json!(s)]);
            }
            mock_broker.assert_number_of_calls("Unsubscribe", 3);
        });
    }
}

// ------------------------------------------------------------------ TestHistory

mod test_history {
    use super::*;

    fn stream() -> Vec<StreamMessage> {
        vec![
            StreamMessage {
                stream: "streamo".into(),
                data: "ciao".into(),
                offset: 22,
                epoch: "test".into(),
                meta: None,
            },
            StreamMessage {
                stream: "streamo".into(),
                data: "buona sera".into(),
                offset: 23,
                epoch: "test".into(),
                meta: None,
            },
        ]
    }

    fn setup(f: impl FnOnce(&TestNode, &TestMock, &TestSession)) {
        let node = new_mock_node();

        let broker = new_mock_broker();
        node.set_mock_broker(&broker);

        broker.on(Expect::new("CommitSession", vec![any(), any()], Ret::Unit));

        let _hub = node.run_hub();

        let session = new_mock_session("14", &node, vec![]);

        session.add_channel("test_channel");
        session.add_channel_stream("test_channel", "streamo");
        session.add_channel_stream("test_channel", "emptissimo");

        f(&node, &broker, &session);
    }

    fn history(since: i64, streams: &[(&str, &str, u64)]) -> HistoryRequest {
        HistoryRequest {
            since,
            streams: streams
                .iter()
                .map(|(s, e, o)| ((*s).to_owned(), HistoryPosition { epoch: (*e).to_owned(), offset: *o }))
                .collect(),
        }
    }

    fn history_msg(identifier: &str, h: HistoryRequest) -> cabby_core::common::Message {
        let mut m = msg(identifier);
        m.history = h;
        m
    }

    /// upstream: node/node_test.go#TestHistory/Successful_history_with_only_Since
    /// upstream: node/node_test.go#TestHistory
    /// note: on Go, the testify mocks.Broker is built in the bridge from the same expectations, and subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache).
    #[test]
    fn successful_history_with_only_since() {
        setup(|node, broker, session| {
            let ts: i64 = 100200;

            broker.on(Expect::new("HistorySince", vec![eq("streamo"), eq(ts)], ret_messages(&stream())));
            broker.on(Expect::new("HistorySince", vec![eq("emptissimo"), eq(ts)], ret_nil()));

            node.history(session, &history_msg("test_channel", history(ts, &[]))).expect("history");

            let history = [
                "{\"identifier\":\"test_channel\",\"message\":\"ciao\",\"stream_id\":\"streamo\",\"epoch\":\"test\",\"offset\":22}",
                "{\"identifier\":\"test_channel\",\"message\":\"buona sera\",\"stream_id\":\"streamo\",\"epoch\":\"test\",\"offset\":23}",
            ];

            for msg in history {
                let received = session.read_string().expect("a history message");
                assert_eq!(msg, received, "Sent message is invalid");
            }

            let ack = session.read_string().expect("an ack");

            assert_eq!(r#"{"type":"confirm_history","identifier":"test_channel"}"#, ack);

            assert!(session.read().is_err());
        });
    }

    /// upstream: node/node_test.go#TestHistory/Successful_history_with_Since_and_Offset
    /// upstream: node/node_test.go#TestHistory
    /// note: on Go, the testify mocks.Broker is built in the bridge from the same expectations, and subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache).
    #[test]
    fn successful_history_with_since_and_offset() {
        setup(|node, broker, session| {
            let ts: i64 = 100300;

            broker.on(Expect::new("HistoryFrom", vec![eq("streamo"), eq("test"), eq(20u64)], ret_messages(&stream())));
            broker.on(Expect::new(
                "HistorySince",
                vec![eq("emptissimo"), eq(ts)],
                ret_messages(&[StreamMessage {
                    stream: "emptissimo".into(),
                    data: "zer0".into(),
                    offset: 2,
                    epoch: "test_zero".into(),
                    meta: None,
                }]),
            ));

            node.history(session, &history_msg("test_channel", history(ts, &[("streamo", "test", 20)])))
                .expect("history");

            let history = [
                "{\"identifier\":\"test_channel\",\"message\":\"ciao\",\"stream_id\":\"streamo\",\"epoch\":\"test\",\"offset\":22}",
                "{\"identifier\":\"test_channel\",\"message\":\"buona sera\",\"stream_id\":\"streamo\",\"epoch\":\"test\",\"offset\":23}",
                "{\"identifier\":\"test_channel\",\"message\":\"zer0\",\"stream_id\":\"emptissimo\",\"epoch\":\"test_zero\",\"offset\":2}",
            ];

            // The order of streams is non-deterministic, so
            // we're collecting messages first and checking for inclusion later
            let mut received = Vec::new();

            for _ in history {
                received.push(session.read_string().expect("a history message"));
            }

            for msg in history {
                assert!(received.iter().any(|r| r == msg), "{received:?} does not contain {msg}");
            }

            let ack = session.read_string().expect("an ack");

            assert_eq!(r#"{"type":"confirm_history","identifier":"test_channel"}"#, ack);

            assert!(session.read().is_err());
        });
    }

    /// upstream: node/node_test.go#TestHistory/Fetching_history_with_Subscribe
    /// upstream: node/node_test.go#TestHistory
    /// note: on Go, the testify mocks.Broker is built in the bridge from the same expectations, and subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache).
    #[test]
    fn fetching_history_with_subscribe() {
        setup(|node, broker, session| {
            let ts: i64 = 100400;

            broker.on(Expect::new("HistoryFrom", vec![eq("streamo"), eq("test"), eq(21u64)], ret_messages(&stream())));
            broker.on(Expect::new(
                "HistorySince",
                vec![eq("s1"), eq(ts)],
                ret_messages(&[StreamMessage {
                    stream: "s1".into(),
                    data: "{\"foo\":\"bar\"}".into(),
                    offset: 10,
                    epoch: "test".into(),
                    meta: None,
                }]),
            ));
            broker.on(Expect::new("Subscribe", vec![eq("stream")], ret_string("s1")));

            node.subscribe(session, &history_msg("with_stream", history(ts, &[("streamo", "test", 21)])))
                .expect("subscribe");

            let msg = session.read_string().expect("a confirmation");

            assert_eq!("14", msg, "Sent message is invalid");

            let history = [
                "{\"identifier\":\"with_stream\",\"message\":{\"foo\":\"bar\"},\"stream_id\":\"s1\",\"epoch\":\"test\",\"offset\":10}",
            ];

            for msg in history {
                let received = session.read_string().expect("a history message");
                assert_eq!(msg, received, "Sent message is invalid");
            }

            let ack = session.read_string().expect("an ack");

            assert_eq!(r#"{"type":"confirm_history","identifier":"with_stream"}"#, ack);

            assert!(session.read().is_err());
        });
    }

    /// upstream: node/node_test.go#TestHistory/Error_retrieving_history
    /// upstream: node/node_test.go#TestHistory
    /// note: on Go, the testify mocks.Broker is built in the bridge from the same expectations, and subscriptions.AddChannel* re-apply a cache entry (RestoreFromCache).
    #[test]
    fn error_retrieving_history() {
        setup(|node, broker, session| {
            let ts: i64 = 200100;

            broker.on(Expect::new(
                "HistorySince",
                vec![eq("streamo"), eq(ts)],
                Ret::Err("Couldn't restore history".into()),
            ));
            broker.on(Expect::new("HistorySince", vec![eq("emptissimo"), eq(ts)], ret_messages(&stream())));

            let err = node.history(session, &history_msg("test_channel", history(ts, &[])));

            // assert.Error(t, err, "Couldn't restore history") (the text is the failure message)
            assert!(err.is_err(), "Couldn't restore history");

            let ack = session.read_string().expect("a rejection");

            assert_eq!(r#"{"type":"reject_history","identifier":"test_channel"}"#, ack);
        });
    }
}

// ----------------------------------------------------------------- TestPresence

mod test_presence {
    use super::*;

    fn join_event(id: &str, info: &str) -> PresenceEvent {
        PresenceEvent { kind: PRESENCE_JOIN_TYPE.into(), id: id.into(), info: Some(json!(info)) }
    }

    fn setup(f: impl FnOnce(&TestNode, &TestMock, &TestMock)) {
        let node = new_mock_node();

        let controller = new_mock_controller();
        node.set_controller(&controller);

        let broker = new_mock_broker();
        node.set_mock_broker(&broker);

        broker.on(Expect::new("CommitSession", vec![any(), any()], Ret::Unit));

        broker.on(Expect::new("Subscribe", vec![any()], Ret::EchoFirst));

        broker.on(Expect::new("HandleBroadcast", vec![any()], Ret::Unit).run(Run::Broadcast));

        let _hub = node.run_hub();

        f(&node, &controller, &broker);
    }

    /// `broker.On("PresenceAdd", "test_presence", "24", "user_24", "J. Bond")`
    /// running `node.HandleJoin(...)`.
    fn expect_join_24(broker: &TestMock) {
        broker.on(Expect::new(
            "PresenceAdd",
            vec![eq("test_presence"), eq("24"), eq("user_24"), eq("J. Bond")],
            ret_presence(&join_event("user_24", "J. Bond")),
        )
        .run(Run::HandleJoin("test_presence".into(), join_event("user_24", "J. Bond"))));
    }

    /// upstream: node/node_test.go#TestPresence/join_on_subscribe
    /// upstream: node/node_test.go#TestPresence
    /// note: on Go, the testify mocks are built in the bridge from the same expectations (Run callbacks: node.HandleJoin, node.Broadcast); node.controller = c is the exported option WithController(c) applied to the node; hub.AddSession is node.Authenticated (both backends mark the session connected).
    #[test]
    fn join_on_subscribe() {
        setup(|node, controller, broker| {
            let session = new_mock_session("24", node, vec![]);

            node.hub_add_session(&session);
            let _remove = remove_on_drop(node, &session);

            controller.on(Expect::new(
                "Subscribe",
                vec![eq("24"), any(), eq("24"), eq("test_channel")],
                ret_command(&CommandResult {
                    status: Status::Success,
                    transmissions: vec![r#"{"type":"confirm","identifier":"test_channel"}"#.into()],
                    streams: vec!["test_presence".into()],
                    istate: Some(map(&[(PRESENCE_STREAM_STATE, "test_presence")])),
                    presence: Some(join_event("user_24", "J. Bond")),
                    ..Default::default()
                }),
            ));

            expect_join_24(broker);

            node.subscribe(&session, &msg("test_channel")).expect("subscribe");

            assert_eq!(
                "test_presence",
                get_channel_state_field(&session.get_env(), "test_channel", PRESENCE_STREAM_STATE)
            );

            assert_receive(&session, r#"{"type":"confirm","identifier":"test_channel"}"#);

            assert_receive(
                &session,
                r#"{"type":"presence","identifier":"test_channel","message":{"type":"join","id":"user_24","info":"J. Bond"}}"#,
            );
        });
    }

    /// upstream: node/node_test.go#TestPresence/join_on_perform
    /// upstream: node/node_test.go#TestPresence
    /// note: on Go, the testify mocks are built in the bridge from the same expectations (Run callbacks: node.HandleJoin, node.Broadcast); node.controller = c is WithController(c) applied to the node; hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel re-applies a cache entry (RestoreFromCache).
    #[test]
    fn join_on_perform() {
        setup(|node, controller, broker| {
            let session = new_mock_session("24", node, vec![]);

            node.hub_add_session(&session);
            let _remove = remove_on_drop(node, &session);

            session.add_channel("subscribed_channel");

            controller.on(Expect::new(
                "Perform",
                vec![eq("24"), any(), eq("24"), eq("subscribed_channel"), eq(r#"{"action":"follow"}"#)],
                ret_command(&CommandResult {
                    status: Status::Success,
                    streams: vec!["test_presence".into()],
                    istate: Some(map(&[(PRESENCE_STREAM_STATE, "test_presence")])),
                    presence: Some(join_event("user_24", "J. Bond")),
                    ..Default::default()
                }),
            ));

            expect_join_24(broker);

            node.perform(&session, &msg_data("subscribed_channel", r#"{"action":"follow"}"#)).expect("perform");

            assert_eq!(
                "test_presence",
                get_channel_state_field(&session.get_env(), "subscribed_channel", PRESENCE_STREAM_STATE)
            );

            assert_receive(
                &session,
                r#"{"type":"presence","identifier":"subscribed_channel","message":{"type":"join","id":"user_24","info":"J. Bond"}}"#,
            );
        });
    }

    /// upstream: node/node_test.go#TestPresence/no_presence_stream_configured
    /// upstream: node/node_test.go#TestPresence
    /// note: on Go, the testify mocks are built in the bridge from the same expectations; the forbidden PresenceAdd (upstream t.Error in Run) is recorded and asserted at the end; node.controller = c is WithController(c); hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel re-applies a cache entry.
    #[test]
    fn no_presence_stream_configured() {
        setup(|node, controller, broker| {
            let session = new_mock_session("25", node, vec![]);

            node.hub_add_session(&session);
            let _remove = remove_on_drop(node, &session);

            session.add_channel("subscribed_channel");

            controller.on(Expect::new(
                "Perform",
                vec![eq("25"), any(), eq("25"), eq("subscribed_channel"), eq(r#"{"action":"follow"}"#)],
                ret_command(&CommandResult {
                    status: Status::Success,
                    streams: vec!["test_presence".into()],
                    presence: Some(PresenceEvent {
                        kind: PRESENCE_JOIN_TYPE.into(),
                        id: "user_25".into(),
                        info: Some(json!("Fantomas")),
                    }),
                    ..Default::default()
                }),
            ));

            // Earlier subtests registered PresenceAdd for sid "24" only.
            expect_join_24(broker);
            broker.on(Expect::new("PresenceAdd", vec![any(), any(), any(), any()], ret_nil())
                .run(Run::Forbidden("PresenceAdd should not be called".into())));

            node.perform(&session, &msg_data("subscribed_channel", r#"{"action":"follow"}"#)).expect("perform");

            assert_eq!("", get_channel_state_field(&session.get_env(), "subscribed_channel", PRESENCE_STREAM_STATE));

            assert!(session.read().is_err());
            broker.assert_no_forbidden_calls();
        });
    }

    /// upstream: node/node_test.go#TestPresence/leave_on_unsubscribe
    /// upstream: node/node_test.go#TestPresence
    /// note: on Go, the testify mocks are built in the bridge (Run callbacks: node.HandleLeave, node.Broadcast); node.controller = c is WithController(c); hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel re-applies a cache entry; hub.SubscribeSession is the upstream call on node.hub (read by reflection); env.MergeChannelState is called on GetEnv().
    #[test]
    fn leave_on_unsubscribe() {
        setup(|node, controller, broker| {
            let session = new_mock_session("25", node, vec![]);
            let session2 = new_mock_session("251", node, vec![]);

            node.hub_add_session(&session);
            let _remove = remove_on_drop(node, &session);

            node.hub_add_session(&session2);
            let _remove2 = remove_on_drop(node, &session2);

            // Register subscription and stream
            session.add_channel("test_channel");
            node.hub_subscribe_session(&session, "test_presence", "test_channel");
            node.hub_subscribe_session(&session2, "test_presence", "test_channel");

            // Make sure presence stream is defined for the session
            session.merge_channel_state("test_channel", &map(&[(PRESENCE_STREAM_STATE, "test_presence")]));

            controller.on(Expect::new(
                "Unsubscribe",
                vec![eq("25"), any(), eq("25"), eq("test_channel")],
                ret_command(&CommandResult { status: Status::Success, ..Default::default() }),
            ));

            let leave = PresenceEvent { kind: "leave".into(), id: "user_25".into(), info: None };
            broker.on(Expect::new("PresenceRemove", vec![eq("test_presence"), eq("25")], ret_presence(&leave))
                .run(Run::HandleLeave("test_presence".into(), leave.clone())));

            node.unsubscribe(&session, &msg("test_channel")).expect("unsubscribe");

            assert_eq!("", get_channel_state_field(&session.get_env(), "test_channel", PRESENCE_STREAM_STATE));

            assert_receive(
                &session2,
                r#"{"type":"presence","identifier":"test_channel","message":{"type":"leave","id":"user_25"}}"#,
            );
        });
    }
}

// ----------------------------------------------------------- TestRestoreSession

mod test_restore_session {
    use super::*;

    const KEEPALIVE: Duration = Duration::from_millis(500);

    /// Upstream's shared setup. Returns the session that restores from "114".
    fn setup(f: impl FnOnce(&TestNode, &TestMock, TestSession)) {
        let node = new_mock_node();

        let broker = new_mock_broker();
        node.set_mock_broker(&broker);

        let _hub = node.run_hub();

        let prev_session = new_mock_session("114", &node, vec![SessionOpt::KeepaliveIntervals(KEEPALIVE, KEEPALIVE)]);
        prev_session.add_channel("fruits_channel");
        prev_session.add_channel_stream("fruits_channel", "arancia");
        prev_session.add_channel_stream("fruits_channel", "limoni");
        prev_session.merge_connection_state(&map(&[("tenant_id", "71")]));
        prev_session.merge_channel_state("fruits_channel", &map(&[("gardini", "ischia")]));

        let cached = prev_session.to_cache_entry().expect("cache entry");

        broker.on(Expect::new("RestoreSession", vec![eq("114")], ret_cache(&cached)));
        broker.on(Expect::new("CommitSession", vec![any(), any()], Ret::Unit));
        broker.on(Expect::new("Subscribe", vec![any()], Ret::EchoFirst));

        let session = new_mock_session(
            "214",
            &node,
            vec![SessionOpt::KeepaliveIntervals(KEEPALIVE, KEEPALIVE), SessionOpt::PrevSid("114".into())],
        );

        f(&node, &broker, session);
    }

    /// upstream: node/node_test.go#TestRestoreSession/Successful_restore_via_header
    /// upstream: node/node_test.go#TestRestoreSession
    /// note: on Go, mock sessions follow NewMockSession with exported API (BuildSession, WithExecutor, SetID, Log, SetIdentifiers, options, go SendMessages; timers not started); the testify mocks.Broker is built in the bridge; subscriptions are arranged and read through the cache entry (RestoreFromCache / ToCacheEntry); env merges are called on GetEnv(); hub.Broadcast is node.Broadcast.
    #[test]
    fn successful_restore_via_header() {
        setup(|node, _, session| {
            let res = node.authenticate(&session).expect("authenticate");
            assert_eq!(Status::Success, res.expect("a result").status);

            let streams = session.streams_for("fruits_channel").unwrap_or_default();
            assert!(streams.contains(&"arancia".to_owned()));
            assert!(streams.contains(&"limoni".to_owned()));

            let env = session.get_env();
            assert_eq!("71", get_connection_state_field(&env, "tenant_id"));
            assert_eq!("ischia", get_channel_state_field(&env, "fruits_channel", "gardini"));

            let welcome = session.read_string().expect("welcome");

            assert_eq!(
                r#"{"type":"welcome","sid":"214","restored":true,"restored_ids":["fruits_channel"]}"#, welcome,
                "Sent message is invalid"
            );

            node.hub_broadcast("arancia", "delissimo");

            let msg = session.read_string().expect("a broadcast");

            assert_eq!(r#"{"identifier":"fruits_channel","message":"delissimo"}"#, msg, "Sent message is invalid");

            node.hub_broadcast("limoni", "acido");

            let msg = session.read_string().expect("a broadcast");

            assert_eq!(r#"{"identifier":"fruits_channel","message":"acido"}"#, msg, "Sent message is invalid");
        });
    }

    /// Upstream registers `RestoreSession("114") -> (nil, nil)` after the
    /// earlier `-> (cached, nil)` expectation (testify answers with the first
    /// match), and authenticates a session *without* a previous sid or
    /// keepalive, so the broker is never consulted: the case only checks the
    /// plain controller welcome. Kept as written; see
    /// `restore_falls_back_to_authenticate_when_cache_is_missing`.
    ///
    /// upstream: node/node_test.go#TestRestoreSession/Failed_to_restore
    /// upstream: node/node_test.go#TestRestoreSession
    /// note: suspicious upstream case: it never exercises a failed restore (see the doc comment); a regression-for case covers it.
    /// note: on Go, mock sessions follow NewMockSession with exported API and the testify mocks.Broker is built in the bridge.
    #[test]
    fn failed_to_restore() {
        setup(|node, broker, _| {
            broker.on(Expect::new("RestoreSession", vec![eq("114")], ret_nil()));

            let session = new_mock_session("154", node, vec![]);

            let res = node.authenticate(&session).expect("authenticate");
            assert_eq!(Status::Success, res.expect("a result").status);

            let welcome = session.read_string().expect("welcome");

            assert_eq!("welcome", welcome, "Sent message is invalid");
        });
    }

    /// A resumeable session whose previous sid has no cache entry is
    /// authenticated by the controller instead: plain welcome, nothing restored.
    ///
    /// regression-for: node/node_test.go#TestRestoreSession/Failed_to_restore
    #[test]
    fn restore_falls_back_to_authenticate_when_cache_is_missing() {
        let node = new_mock_node();
        let broker = new_mock_broker();
        node.set_mock_broker(&broker);
        let _hub = node.run_hub();

        broker.on(Expect::new("RestoreSession", vec![eq("114")], ret_nil()));
        broker.on(Expect::new("CommitSession", vec![any(), any()], Ret::Unit));

        let session = new_mock_session(
            "154",
            &node,
            vec![SessionOpt::KeepaliveIntervals(KEEPALIVE, KEEPALIVE), SessionOpt::PrevSid("114".into())],
        );

        let res = node.authenticate(&session).expect("authenticate");
        assert_eq!(Status::Success, res.expect("a result").status);

        broker.assert_called("RestoreSession", &[json!("114")]);
        assert_eq!("welcome", session.read_string().expect("welcome"));
        assert!(session.channels().is_empty());
    }
}

// --------------------------------------------------------------- TestBroadcasting

/// upstream: node/node_test.go#TestRestoreSessionAfterRemoteDisconnect
/// note: upstream assigns the unexported `session.broker` of mock sessions; the sessions here are built by the node (`NewSession`, pings off, keepalive timers on), which binds the node's broker on both backends. `require.Eventually(prev.IsClosed, 2s, 10ms)` is a bounded probe. The Rust memory broker runs on the node's controlled clock (no keepalive fires).
#[test]
fn test_restore_session_after_remote_disconnect() {
    let node = new_mock_node();

    let bconf = cabby_testkit::reference::broker::new_config();
    let br = new_memory_broker_for(&node, &bconf);
    node.set_broker(&br);
    br.start().expect("require.NoError");

    let _hub = node.run_hub();

    let keepalive = SessionOpt::KeepaliveIntervals(Duration::from_secs(60), Duration::from_secs(60));
    let prev =
        new_session_with_timers("114", &node, "/cable", Some(map(&[("id", "user-114")])), vec![keepalive.clone()]);

    let res = node.authenticate(&prev).expect("require.NoError");
    assert_eq!(Status::Success, res.expect("a result").status);

    let cached = br.restore_session("114").expect("require.NoError");
    assert!(cached.is_some(), "require.NotNil");

    node.remote_disconnect("user-114", true);

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !prev.is_closed() {
        assert!(std::time::Instant::now() < deadline, "require.Eventually: the session is closed");
        std::thread::sleep(Duration::from_millis(10));
    }

    let cached = br.restore_session("114").expect("require.NoError");
    assert_eq!(None, cached);

    let session = new_session_with_timers(
        "214",
        &node,
        "/cable",
        Some(map(&[("id", "user-114")])),
        vec![keepalive, SessionOpt::PrevSid("114".into())],
    );

    let res = node.authenticate(&session).expect("require.NoError");
    assert_eq!(Status::Success, res.expect("a result").status);

    let welcome = session.read_string().expect("require.NoError");

    assert_eq!("welcome", welcome, "Session must not be restored after remote disconnect: {welcome}");

    let _ = br.shutdown();
}

mod test_broadcasting {
    use super::*;

    fn setup(f: impl FnOnce(&TestNode, &TestSession, &TestSession)) {
        let node = new_mock_node();

        let _hub = node.run_hub();

        let session = new_mock_session("14", &node, vec![]);
        let session2 = new_mock_session("15", &node, vec![]);

        node.hub_add_session(&session);
        node.hub_subscribe_session(&session, "test", "test_channel");
        node.hub_subscribe_session(&session, "staind_2023", "music_channel");

        node.hub_add_session(&session2);
        node.hub_subscribe_session(&session2, "test", "test_channel");
        node.hub_subscribe_session(&session2, "staind_2023", "music_channel");

        node.broker_subscribe("test");
        node.broker_subscribe("staind_2023");

        f(&node, &session, &session2);
    }

    /// upstream: node/node_test.go#TestBroadcasting/HandlePubSub
    /// upstream: node/node_test.go#TestBroadcasting
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected), and hub.SubscribeSession is the upstream call on node.hub (read by reflection); node.broker is the legacy broker the bridge built.
    #[test]
    fn handle_pub_sub() {
        setup(|node, session, session2| {
            node.handle_pub_sub(b"{\"stream\":\"test\",\"data\":\"\\\"abc123\\\"\"}");

            let expected = r#"{"identifier":"test_channel","message":"abc123"}"#;

            assert_eq!(Ok(expected.to_owned()), session.read_string());
            assert_eq!(Ok(expected.to_owned()), session2.read_string());
        });
    }

    /// upstream: node/node_test.go#TestBroadcasting/HandlePubSub_Batch
    /// upstream: node/node_test.go#TestBroadcasting
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected), and hub.SubscribeSession is the upstream call on node.hub (read by reflection); node.broker is the legacy broker the bridge built.
    #[test]
    fn handle_pub_sub_batch() {
        setup(|node, session, session2| {
            node.handle_pub_sub(
                b"[{\"stream\":\"test\",\"data\":\"\\\"follow me\\\"\"},{\"stream\":\"untest\",\"data\":\"\\\"missing\\\"\"},{\"stream\":\"staind_2023\",\"data\":\"{\\\"num\\\":7,\\\"title\\\":\\\"The Fray\\\"}\"}]",
            );

            let first = r#"{"identifier":"test_channel","message":"follow me"}"#.to_owned();
            let second = r#"{"identifier":"music_channel","message":{"num":7,"title":"The Fray"}}"#.to_owned();

            let msgs = read_messages(session, 2).expect("2 messages");
            assert!(msgs.contains(&first));
            assert!(msgs.contains(&second));

            let msgs2 = read_messages(session2, 2).expect("2 messages");
            assert!(msgs2.contains(&first));
            assert!(msgs2.contains(&second));
        });
    }

    /// upstream: node/node_test.go#TestBroadcasting/HandleBroadcast
    /// upstream: node/node_test.go#TestBroadcasting
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected), and hub.SubscribeSession is the upstream call on node.hub (read by reflection); node.broker is the legacy broker the bridge built.
    #[test]
    fn handle_broadcast() {
        setup(|node, session, session2| {
            let _ =
                node.handle_broadcast(br#"{"stream":"staind_2023","data":"{\"num\":5,\"title\":\"Out of time\"}"}"#);

            let expected = r#"{"identifier":"music_channel","message":{"num":5,"title":"Out of time"}}"#;

            assert_eq!(Ok(expected.to_owned()), session.read_string());
            assert_eq!(Ok(expected.to_owned()), session2.read_string());
        });
    }

    /// upstream: node/node_test.go#TestBroadcasting/HandleBroadcast_Batch
    /// upstream: node/node_test.go#TestBroadcasting
    /// note: on Go, hub.AddSession is node.Authenticated (both backends mark the session connected), and hub.SubscribeSession is the upstream call on node.hub (read by reflection); node.broker is the legacy broker the bridge built.
    #[test]
    fn handle_broadcast_batch() {
        setup(|node, session, session2| {
            let _ = node.handle_broadcast(
                br#"[{"stream":"staind_2023","data":"{\"num\":9,\"title\":\"Hate me too\"}"},{"stream":"untest","data":"\"missing\""},{"stream":"staind_2023","data":"{\"num\":10,\"title\":\"Confessions of Fallen\"}"}]"#,
            );

            let first = r#"{"identifier":"music_channel","message":{"num":9,"title":"Hate me too"}}"#.to_owned();
            let second =
                r#"{"identifier":"music_channel","message":{"num":10,"title":"Confessions of Fallen"}}"#.to_owned();

            let msgs = read_messages(session, 2).expect("2 messages");
            assert!(msgs.contains(&first));
            assert!(msgs.contains(&second));

            let msgs2 = read_messages(session2, 2).expect("2 messages");
            assert!(msgs2.contains(&first));
            assert!(msgs2.contains(&second));
        });
    }
}

// ------------------------------------------------------ TestHandlePubSubWithCommand

/// upstream: node/node_test.go#TestHandlePubSubWithCommand
/// note: hub.AddSession also marks the session connected on both backends (Go: node.Authenticated(s, s.GetIdentifiers())), so the remote disconnect also detaches it from the node (no Disconnect call is enqueued: the session is not disconnectable).
#[test]
fn test_handle_pub_sub_with_command() {
    let node = new_mock_node();

    let _hub = node.run_hub();

    let session = new_mock_session("14", &node, vec![]);
    node.hub_add_session(&session);

    node.handle_pub_sub(b"{\"command\":\"disconnect\",\"payload\":{\"identifier\":\"14\",\"reconnect\":false}}");

    // toJSON(common.NewDisconnectMessage("remote", false))
    let expected = r#"{"type":"disconnect","reason":"remote","reconnect":false}"#;

    assert_eq!(Ok(expected.to_owned()), session.read_string());
    assert!(session.is_closed());
}

// -------------------------------------------------------------- TestLookupSession

/// upstream: node/node_test.go#TestLookupSession
/// note: on Go, hub.AddSession is node.Authenticated(s, s.GetIdentifiers()) (both backends mark the session connected) (identifiers set just before); sessions are compared by identity (bridge handle).
#[test]
fn test_lookup_session() {
    let node = new_mock_node();

    let _hub = node.run_hub();

    assert!(node.lookup_session("{\"foo\":\"bar\"}").is_none());

    let session = new_mock_session("14", &node, vec![]);
    session.set_identifiers("{\"foo\":\"bar\"}");
    node.hub_add_session(&session);

    assert_eq!(Some(session.id_ref()), node.lookup_session("{\"foo\":\"bar\"}"));
}

// ---------------------------------------------- TestUnsubscribeReleasesBrokerStreams

/// upstream: node/node_test.go#TestUnsubscribeReleasesBrokerStreams
/// note: on Go, the testify mocks.Broker is built in the bridge; hub.AddSession is node.Authenticated (both backends mark the session connected); subscriptions.AddChannel* re-apply a cache entry; hub.SubscribeSession is the upstream call on node.hub (read by reflection).
#[test]
fn test_unsubscribe_releases_broker_streams() {
    let node = new_mock_node();
    let broker = new_mock_broker();
    node.set_mock_broker(&broker);
    let session = new_mock_session("14", &node, vec![]);

    node.hub_add_session(&session);
    let _remove = remove_on_drop(&node, &session);

    let _hub = node.run_hub();

    session.add_channel("test_channel");
    session.add_channel_stream("test_channel", "streamo");
    node.hub_subscribe_session(&session, "streamo", "test_channel");

    broker.on(Expect::new("Unsubscribe", vec![eq("streamo")], ret_string("streamo")));

    node.unsubscribe(&session, &msg("test_channel")).expect("unsubscribe");

    broker.assert_called("Unsubscribe", &[json!("streamo")]);
    assert!(!session.has_channel("test_channel"));
}
