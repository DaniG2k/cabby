//! upstream file: pusher/controller_test.go
//!
//! The upstream subtests share one `env`; the presence subtest merges channel state
//! into it, which no other subtest reads. Each Rust test builds its own env.
//! `mocks.Broker` with `On("PresenceInfo", mock.Anything)` is [`StubBroker`].
//! Go distinguishes `[]string{}` from nil in `Unsubscribe`'s result; Cabby's
//! `CommandResult` has no nil slices, so only emptiness is compared.

use std::collections::HashMap;

use cabby_core::common::{
    PRESENCE_INFO_TYPE, PRESENCE_JOIN_TYPE, PRESENCE_STREAM_STATE, PresenceEvent, PresenceInfo, SessionEnv, Status,
    WHISPER_STREAM_STATE,
};
use cabby_server::pusher::Config;
use cabby_testkit::reference::pusher::{Controller, StubBroker};
use serde_json::json;

fn build_env() -> SessionEnv {
    // common.NewSessionEnv(url, &headers)
    SessionEnv {
        url: "ws://demo.anycable.io/pusher/app-id".into(),
        headers: Some(HashMap::from([("cookie".to_owned(), "val=1;".to_owned())])),
        ..Default::default()
    }
}

fn build_controller() -> Controller {
    let conf = Config::new();
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
        let env = build_env();
        let ctrl = build_controller();
        let channel = r#"{"channel":"$pusher","stream":"private-party"}"#;

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
            HashMap::from([("channel_data".to_owned(), r#"{"user_id":"b4","user_info":{"name":"bob"}}"#.to_owned())]),
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
        assert_eq!(Vec::<String>::new(), res.streams);
        assert!(res.stop_all_streams);
    }
}
