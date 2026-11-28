//! upstream file: pusher/controller_test.go
//!
//! The upstream subtests share one `env`; the private and presence subtests merge
//! channel state into it for their own channels, which no other subtest reads. Each
//! Rust test builds its own env. `pusher_verified` is upstream's unexported
//! `verifiedStateField`.
//! `mocks.Broker` with `On("PresenceInfo", mock.Anything)` is [`StubBroker`].
//! Go distinguishes `[]string{}` from nil in `Unsubscribe`'s result; the facade
//! reports Go's nil slices separately (`CommandResult` has none).

use std::collections::HashMap;

use cabby_core::common::{
    PRESENCE_INFO_TYPE, PRESENCE_JOIN_TYPE, PRESENCE_STREAM_STATE, PresenceEvent, PresenceInfo, SessionEnv, Status,
    WHISPER_STREAM_STATE,
};
use cabby_testkit::reference::pusher::{Controller, StubBroker, new_config};
use serde_json::json;

fn build_env() -> SessionEnv {
    // common.NewSessionEnv(url, &headers): empty connection and channel states
    SessionEnv {
        url: "ws://demo.anycable.io/pusher/app-id".into(),
        headers: Some(HashMap::from([("cookie".to_owned(), "val=1;".to_owned())])),
        connection_state: Some(HashMap::new()),
        channel_states: Some(HashMap::new()),
        ..Default::default()
    }
}

fn build_controller() -> Controller {
    let conf = new_config();
    let bro = StubBroker {
        presence_info: Some(PresenceInfo {
            kind: PRESENCE_INFO_TYPE.into(),
            total: 1,
            records: vec![PresenceEvent {
                id: "a2".into(),
                info: Some(json!({"name": "alice"})),
                ..Default::default()
            }],
        }),
    };
    Controller::new(bro, &conf)
}

/// Upstream's unexported `verifiedStateField`.
const VERIFIED_STATE_FIELD: &str = "pusher_verified";

/// `common.ConfirmationMessage(identifier)`: `utils.ToJSON(Reply{Identifier, Type})`
/// with the upstream field order.
fn confirmation_message(identifier: &str) -> String {
    format!(r#"{{"type":"confirm_subscription","identifier":{}}}"#, json!(identifier))
}

/// `common.RejectionMessage(identifier)`.
fn rejection_message(identifier: &str) -> String {
    format!(r#"{{"type":"reject_subscription","identifier":{}}}"#, json!(identifier))
}

/// `res.IState[key]`: Go yields "" for a nil map or missing key.
fn istate<'a>(res: &'a cabby_core::common::CommandResult, key: &str) -> &'a str {
    res.istate.as_ref().and_then(|m| m.get(key)).map_or("", String::as_str)
}

mod test_pusher_controller {
    use super::*;

    /// upstream: pusher/controller_test.go#TestPusherController/Subscribe_private_(success)
    #[test]
    fn subscribe_private_success() {
        let mut env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"private-party"}"#;

        // env.MergeChannelState(channel, &map[string]string{verifiedStateField: "1"})
        env.channel_states = Some(HashMap::from([(
            channel.to_owned(),
            HashMap::from([(VERIFIED_STATE_FIELD.to_owned(), "1".to_owned())]),
        )]));

        let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation_message(channel)], res.transmissions);
        assert_eq!(vec!["private-party".to_owned()], res.streams);
        assert_eq!("private-party", istate(&res, WHISPER_STREAM_STATE));
        assert_eq!("", istate(&res, PRESENCE_STREAM_STATE));
        assert_eq!(-1, res.disconnect_interest);
    }

    /// upstream: pusher/controller_test.go#TestPusherController/Subscribe_presence_(success)
    #[test]
    fn subscribe_presence_success() {
        let mut env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"presence-room"}"#;

        // env.MergeChannelState(channel, ...) on an env without channel state.
        env.channel_states = Some(HashMap::from([(
            channel.to_owned(),
            HashMap::from([
                ("channel_data".to_owned(), r#"{"user_id":"b4","user_info":{"name":"bob"}}"#.to_owned()),
                (VERIFIED_STATE_FIELD.to_owned(), "1".to_owned()),
            ]),
        )]));

        let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

        // utils.ToJSON(&common.Reply{Type, Identifier, Message})
        let confirmation = format!(
            r#"{{"type":"confirm_subscription","identifier":{},"message":{}}}"#,
            json!(channel),
            json!(r#"{"presence":{"count":2,"ids":["a2","b4"],"hash":{"a2":{"name":"alice"},"b4":{"name":"bob"}}}}"#)
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation], res.transmissions);
        assert_eq!(vec!["presence-room".to_owned()], res.streams);
        assert_eq!("presence-room", istate(&res, WHISPER_STREAM_STATE));
        assert_eq!("presence-room", istate(&res, PRESENCE_STREAM_STATE));
        assert_eq!(-1, res.disconnect_interest);
        let presence = res.presence.expect("presence event");
        assert_eq!("b4", presence.id);
        assert_eq!(PRESENCE_JOIN_TYPE, presence.kind);
        assert_eq!(Some(json!({"name": "bob"})), presence.info);
    }

    /// upstream: pusher/controller_test.go#TestPusherController/Subscribe_private/presence_(unverified)
    /// note: `assert.Nil(res.Streams)` is an empty `streams` (Rust slices are never nil).
    #[test]
    fn subscribe_private_presence_unverified() {
        let env = build_env();
        let ctrl = build_controller();

        for channel in [
            r#"{"channel":"$pusher","stream":"private-secret"}"#,
            r#"{"channel":"$pusher","stream":"presence-secret"}"#,
        ] {
            let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

            assert!(out.error.is_none(), "{:?}", out.error);
            let res = out.result.expect("require.NotNil");
            assert_eq!(Status::Failure, res.status);
            assert_eq!(vec![rejection_message(channel)], res.transmissions);
            assert!(res.streams.is_empty(), "{:?}", res.streams);
            assert!(res.istate.is_none(), "{:?}", res.istate);
        }
    }

    /// regression-for: pusher/controller_test.go#TestPusherController/Subscribe_presence_(success)
    /// note: presence data without a user id (or `null`) fails the subscription with an error.
    #[test]
    fn presence_data_must_contain_a_user_id() {
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"presence-room"}"#;

        for data in [r#"{"user_info":{"name":"bob"}}"#, r#"{"user_id":""}"#, "null"] {
            let mut env = build_env();
            env.channel_states = Some(HashMap::from([(
                channel.to_owned(),
                HashMap::from([
                    ("channel_data".to_owned(), data.to_owned()),
                    (VERIFIED_STATE_FIELD.to_owned(), "1".to_owned()),
                ]),
            )]));

            let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

            assert_eq!(Some("presence data must contain user_id".to_owned()), out.error.map(|e| e.message), "{data}");
            assert_eq!(Status::Error, out.result.expect("a result").status, "{data}");
        }
    }

    /// upstream: pusher/controller_test.go#TestPusherController/Subscribe_private_(failure)
    #[test]
    fn subscribe_private_failure() {
        let env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":""}"#;

        let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![rejection_message(channel)], res.transmissions);
    }

    /// upstream: pusher/controller_test.go#TestPusherController/Subscribe_public_(success)
    #[test]
    fn subscribe_public_success() {
        let env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"all-chat"}"#;

        let out = ctrl.subscribe("1234.1234", &env, "name=jack", channel);

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation_message(channel)], res.transmissions);
        assert_eq!(vec!["all-chat".to_owned()], res.streams);
        assert!(res.istate.is_none());
        assert_eq!(-1, res.disconnect_interest);
    }

    /// upstream: pusher/controller_test.go#TestPusherController/Unsubscribe
    /// note: `assert.Equal([]string{}, ...)` also requires non-nil slices; checked through the facade's Go nil flags (Rust slices are never nil).
    #[test]
    fn unsubscribe() {
        let env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"private-party"}"#;

        let out = ctrl.unsubscribe("1234.1234", &env, "name=jack", channel);

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(Vec::<String>::new(), res.transmissions);
        assert!(!out.nil_transmissions, "Transmissions is nil, upstream expects []string{{}}");
        assert_eq!(Vec::<String>::new(), res.streams);
        assert!(!out.nil_streams, "Streams is nil, upstream expects []string{{}}");
        assert!(res.stop_all_streams);
    }
}
