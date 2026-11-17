//! upstream file: common/common_test.go

use cabby_core::common::{HistoryPosition, PubSubMessage, StreamMessage, StringMap};
use cabby_testkit::reference::common::*;
use serde_json::{Value, json};

fn map(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

mod test_env_merge_connection_state {
    use super::*;

    /// upstream: common/common_test.go#TestEnvMergeConnectionState/when_adding_and_rewriting
    #[test]
    fn when_adding_and_rewriting() {
        let mut env = new_session_env("", None);
        env.connection_state.as_mut().expect("connection state").insert("a".into(), "old".into());

        merge_connection_state(&mut env, &map(&[("a", "new"), ("b", "super new")]));

        let state = env.connection_state.as_ref().expect("connection state");
        assert_eq!(state.len(), 2);
        assert_eq!("new", state["a"]);
        assert_eq!("super new", state["b"]);
    }
}

mod test_env_merge_channel_state {
    use super::*;

    fn setup() -> cabby_core::common::SessionEnv {
        let mut env = new_session_env("", None);
        env.channel_states.as_mut().expect("channel states").insert("id".into(), map(&[("a", "old")]));
        env
    }

    /// upstream: common/common_test.go#TestEnvMergeChannelState/when_adding_and_rewriting
    #[test]
    fn when_adding_and_rewriting() {
        let mut env = setup();

        merge_channel_state(&mut env, "id", &map(&[("a", "new"), ("b", "super new")]));

        let states = env.channel_states.as_ref().expect("channel states");
        assert_eq!(states["id"].len(), 2);
        assert_eq!("new", states["id"]["a"]);
        assert_eq!("super new", states["id"]["b"]);
    }

    /// upstream: common/common_test.go#TestEnvMergeChannelState/when_new_channel
    #[test]
    fn when_new_channel() {
        // Upstream runs this after the previous subtest on the same env; the
        // assertions only concern "id2", so starting from the shared setup is equivalent.
        let mut env = setup();

        merge_channel_state(&mut env, "id2", &map(&[("a", "new")]));

        let states = env.channel_states.as_ref().expect("channel states");
        assert_eq!(states["id2"].len(), 1);
        assert_eq!("new", states["id2"]["a"]);
    }
}

/// upstream: common/common_test.go#TestEnvGetChannelStateField
#[test]
fn test_env_get_channel_state_field() {
    let mut env = new_session_env("", None);
    env.channel_states.as_mut().expect("channel states").insert("id".into(), map(&[("a", "old")]));

    assert_eq!("old", get_channel_state_field(&env, "id", "a"));
    assert_eq!("", get_channel_state_field(&env, "no_id", "a"));
    assert_eq!("", get_channel_state_field(&env, "id", "no_field"));
}

/// upstream: common/common_test.go#TestEnvGetConnectionStateField
#[test]
fn test_env_get_connection_state_field() {
    let mut env = new_session_env("", None);
    env.connection_state.as_mut().expect("connection state").insert("a".into(), "old".into());

    assert_eq!("old", get_connection_state_field(&env, "a"));
    assert_eq!("", get_connection_state_field(&env, "no_a"));
}

mod test_pub_sub_message_from_json {
    use super::*;

    /// upstream: common/common_test.go#TestPubSubMessageFromJSON/Remote_disconnect_message
    #[test]
    fn remote_disconnect_message() {
        let msg = b"{\"command\":\"disconnect\",\"payload\":{\"identifier\":\"14\",\"reconnect\":false}}";

        let result = pub_sub_message_from_json(msg);
        assert!(result.is_ok(), "{result:?}");

        let PubSubMessage::RemoteCommand(casted) = result.unwrap() else {
            panic!("expected a RemoteCommandMessage");
        };

        assert_eq!("disconnect", casted.command);

        // Upstream ignores the error and dereferences the result.
        let dmsg = to_remote_disconnect_message(&casted).expect("disconnect payload");

        assert_eq!("14", dmsg.identifier);
        assert!(!dmsg.reconnect);
    }

    /// upstream: common/common_test.go#TestPubSubMessageFromJSON/Broadcast_message
    #[test]
    fn broadcast_message() {
        let msg = b"{\"stream\":\"bread-test\",\"data\":\"test\"}";

        let result = pub_sub_message_from_json(msg);
        assert!(result.is_ok(), "{result:?}");

        let PubSubMessage::Stream(casted) = result.unwrap() else {
            panic!("expected a StreamMessage");
        };
        assert_eq!("bread-test", casted.stream);
        assert_eq!("test", casted.data);
    }
}

/// upstream: common/common_test.go#TestConfirmationMessage
#[test]
fn test_confirmation_message() {
    assert_eq!(
        "{\"type\":\"confirm_subscription\",\"identifier\":\"test_channel\"}",
        confirmation_message("test_channel")
    );
}

/// upstream: common/common_test.go#TestRejectionMessage
#[test]
fn test_rejection_message() {
    assert_eq!("{\"type\":\"reject_subscription\",\"identifier\":\"test_channel\"}", rejection_message("test_channel"));
}

mod test_stream_message_to_reply_for {
    use super::*;

    /// upstream: common/common_test.go#TestStreamMessageToReplyFor/preserves_large_integers_precision
    #[test]
    fn preserves_large_integers_precision() {
        let sm = StreamMessage {
            stream: "s".into(),
            data: r#"{"sender_id":2675291807195071852,"name":"test"}"#.into(),
            ..StreamMessage::default()
        };

        let reply = to_reply_for(&sm, "test_channel");

        // assert.JSONEq: semantic JSON equality. serde_json keeps 2675291807195071852 as an
        // exact integer, so a lossy (float) decoding would not compare equal.
        let expected: Value = serde_json::from_str(r#"{"sender_id":2675291807195071852,"name":"test"}"#).unwrap();
        assert_eq!(Some(expected), reply.message);
    }

    /// upstream: common/common_test.go#TestStreamMessageToReplyFor/with_trailing_garbage
    #[test]
    fn with_trailing_garbage() {
        let sm = StreamMessage { stream: "s".into(), data: "123abc".into(), ..StreamMessage::default() };

        let reply = to_reply_for(&sm, "test_channel");

        assert_eq!(Some(json!("123abc")), reply.message);
    }
}

/// upstream: common/common_test.go#TestMessageJSONSerialization
#[test]
fn test_message_json_serialization() {
    let command = r#"{
		"command": "subscribe",
		"identifier": "test_channel",
		"data": {"foo": "bar"},
		"history": {
			"since": 20202020,
			"streams": {
				"1": {
					"epoch": "test",
					"offset": 14
				},
				"2": {
					"epoch": "test",
					"offset": 42
				}
			}
		}
	}"#;

    let msg = message_from_json(command);
    assert!(msg.is_ok(), "{msg:?}");
    let msg = msg.unwrap();

    assert_eq!(msg.command, "subscribe");
    assert_eq!(msg.identifier, "test_channel");
    assert_eq!(msg.data, Some(json!({"foo": "bar"})));
    assert_eq!(msg.history.since, 20202020);
    assert_eq!(msg.history.streams["1"], HistoryPosition { epoch: "test".into(), offset: 14 });
    assert_eq!(msg.history.streams["2"], HistoryPosition { epoch: "test".into(), offset: 42 });
}
