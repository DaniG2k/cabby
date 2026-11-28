//! upstream file: pusher/encoder_test.go

use cabby_core::common::{
    CONFIRMED_TYPE, DisconnectMessage, EncodedMessage, PING_TYPE, PONG_TYPE, PRESENCE_JOIN_TYPE, PRESENCE_LEAVE_TYPE,
    PRESENCE_TYPE, PingMessage, REJECTED_TYPE, Reply, SentFrame, WELCOME_TYPE,
};
use cabby_server::pusher::{PusherClientEvent, PusherSubscriptionData};
use cabby_testkit::reference::pusher::{decode, encode, encode_transmission, new_encoder};
use serde_json::json;

const IDENTIFIER: &str = r#"{"channel":"$pusher","stream":"pucha"}"#;
const PRIVATE_IDENTIFIER: &str = r#"{"channel":"$pusher","stream":"private-party"}"#;
const PRESENCE_IDENTIFIER: &str = r#"{"channel":"$pusher","stream":"presence-chat"}"#;

/// `string(actual.Payload)` after `require.NoError`.
fn payload(res: Result<Option<SentFrame>, String>) -> String {
    let frame = res.expect("no error").expect("a frame");
    String::from_utf8(frame.payload.to_vec()).expect("UTF-8 payload")
}

fn reply(r: Reply) -> EncodedMessage {
    EncodedMessage::Reply(r)
}

/// `utils.ToJSON(common.Reply{...})` for the transmission tests: upstream JSON tags,
/// zero values omitted.
fn reply_json(kind: &str, identifier: &str, message: Option<serde_json::Value>) -> String {
    let mut v = serde_json::Map::new();
    if !kind.is_empty() {
        v.insert("type".into(), json!(kind));
    }
    if !identifier.is_empty() {
        v.insert("identifier".into(), json!(identifier));
    }
    if let Some(m) = message {
        v.insert("message".into(), m);
    }
    serde_json::Value::Object(v).to_string()
}

mod test_pusher_encode {
    use super::*;

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Reply_with_event_and_data
    #[test]
    fn reply_with_event_and_data() {
        let coder = new_encoder();
        let message = json!({"event": "custom-event", "data": "custom-data"});
        let msg = reply(Reply { identifier: IDENTIFIER.into(), message: Some(message), ..Default::default() });

        let expected = r#"{"event":"custom-event","data":"custom-data","channel":"pucha"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Reply_w/o_event/data
    #[test]
    fn reply_without_event_data() {
        let coder = new_encoder();
        let msg = reply(Reply { identifier: IDENTIFIER.into(), message: Some(json!("hello")), ..Default::default() });

        let actual = encode(&coder, &msg).expect("no error");

        assert!(actual.is_none());
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Reply_with_private_channel
    #[test]
    fn reply_with_private_channel() {
        let coder = new_encoder();
        let message = json!({"event": "custom-event", "data": "custom-data"});
        let msg = reply(Reply { identifier: PRIVATE_IDENTIFIER.into(), message: Some(message), ..Default::default() });

        let expected = r#"{"event":"custom-event","data":"custom-data","channel":"private-party"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Ping
    #[test]
    fn ping() {
        let coder = new_encoder();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let msg = EncodedMessage::Ping(PingMessage { kind: PING_TYPE.into(), message: Some(json!(now)) });

        let expected = r#"{"event":"pusher:ping","data":{}}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Pong
    #[test]
    fn pong() {
        let coder = new_encoder();
        let msg = reply(Reply { kind: PONG_TYPE.into(), ..Default::default() });

        let expected = r#"{"event":"pusher:pong"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Disconnect_with_reconnect
    #[test]
    fn disconnect_with_reconnect() {
        let coder = new_encoder();
        let msg = EncodedMessage::Disconnect(DisconnectMessage {
            kind: "disconnect".into(),
            reason: "server error".into(),
            reconnect: true,
        });

        let expected = r#"{"event":"pusher:error","data":{"message":"server error","code":4200}}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Disconnect_without_reconnect
    #[test]
    fn disconnect_without_reconnect() {
        let coder = new_encoder();
        let msg = EncodedMessage::Disconnect(DisconnectMessage {
            kind: "disconnect".into(),
            reason: "unauthorized".into(),
            reconnect: false,
        });

        let expected = r#"{"event":"pusher:error","data":{"message":"unauthorized","code":4009}}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Welcome
    #[test]
    fn welcome() {
        let coder = new_encoder();
        let msg = reply(Reply { kind: WELCOME_TYPE.into(), sid: "lu2007".into(), ..Default::default() });

        let expected =
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"lu2007\",\"activity_timeout\":30}"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Confirmed
    #[test]
    fn confirmed() {
        let coder = new_encoder();
        let msg = reply(Reply { kind: CONFIRMED_TYPE.into(), identifier: IDENTIFIER.into(), ..Default::default() });

        let expected = r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"pucha"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Rejected
    #[test]
    fn rejected() {
        let coder = new_encoder();
        let msg = reply(Reply { kind: REJECTED_TYPE.into(), identifier: IDENTIFIER.into(), ..Default::default() });

        let expected = r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Presence_join
    #[test]
    fn presence_join() {
        let coder = new_encoder();
        let msg = reply(Reply {
            kind: PRESENCE_TYPE.into(),
            identifier: PRESENCE_IDENTIFIER.into(),
            message: Some(json!({"type": PRESENCE_JOIN_TYPE, "id": "42", "info": {"name": "Vova"}})),
            ..Default::default()
        });

        let expected = r#"{"event":"pusher_internal:member_added","data":"{\"user_id\":\"42\",\"user_info\":{\"name\":\"Vova\"}}","channel":"presence-chat"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncode/Presence_leave
    #[test]
    fn presence_leave() {
        let coder = new_encoder();
        let msg = reply(Reply {
            kind: PRESENCE_TYPE.into(),
            identifier: PRESENCE_IDENTIFIER.into(),
            message: Some(json!({"type": PRESENCE_LEAVE_TYPE, "id": "42"})),
            ..Default::default()
        });

        let expected =
            r#"{"event":"pusher_internal:member_removed","data":"{\"user_id\":\"42\"}","channel":"presence-chat"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    mod malformed_presence_events {
        use super::*;

        /// The table row body: encoding the presence reply returns an error
        /// (`require.NotPanics` holds when the call returns).
        fn check(message: serde_json::Value) {
            let coder = new_encoder();
            let msg = reply(Reply {
                kind: PRESENCE_TYPE.into(),
                identifier: PRESENCE_IDENTIFIER.into(),
                message: Some(message),
                ..Default::default()
            });

            let res = encode(&coder, &msg);
            assert!(res.is_err(), "{res:?}");
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/empty
        #[test]
        fn empty() {
            check(json!({}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/missing_id
        #[test]
        fn missing_id() {
            check(json!({"type": PRESENCE_JOIN_TYPE}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/numeric_id
        /// note: upstream's `json.Number("42")` is the JSON number 42 (Go decodes it from the bridge's JSON).
        #[test]
        fn numeric_id() {
            check(json!({"type": PRESENCE_JOIN_TYPE, "id": 42}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/missing_type
        #[test]
        fn missing_type() {
            check(json!({"id": "42"}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/unknown_type
        #[test]
        fn unknown_type() {
            check(json!({"type": "update", "id": "42"}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/numeric_type
        #[test]
        fn numeric_type() {
            check(json!({"type": 1, "id": "42"}));
        }

        /// upstream: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/non-map
        #[test]
        fn non_map() {
            check(json!("join"));
        }

        /// regression-for: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/empty
        /// regression-for: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/missing_type
        /// regression-for: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/unknown_type
        /// regression-for: pusher/encoder_test.go#TestPusherEncode/Malformed_presence_events/non-map
        /// note: the error texts (logged with `failed to encode message`) name the value and its Go type.
        #[test]
        fn errors_name_the_value_and_its_type() {
            let coder = new_encoder();
            for (message, expected) in [
                (json!({}), "invalid presence event id: <nil> (<nil>)"),
                (json!({"id": "42"}), "invalid presence event type: <nil> (<nil>)"),
                (json!({"type": "update", "id": "42"}), "invalid presence event type: update (string)"),
                (json!("join"), "failed to encode presence event: join"),
            ] {
                let msg = reply(Reply {
                    kind: PRESENCE_TYPE.into(),
                    identifier: PRESENCE_IDENTIFIER.into(),
                    message: Some(message),
                    ..Default::default()
                });
                assert_eq!(Err(expected.to_owned()), encode(&coder, &msg).map(|_| ()));
            }
        }
    }
}

mod test_pusher_encode_transmission {
    use super::*;

    /// upstream: pusher/encoder_test.go#TestPusherEncodeTransmission/welcome
    #[test]
    fn welcome() {
        let coder = new_encoder();
        let msg = r#"{"type":"welcome","sid":"lu2007"}"#;
        let expected =
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"lu2007\",\"activity_timeout\":30}"}"#;

        assert_eq!(expected, payload(encode_transmission(&coder, msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncodeTransmission/confirm_subscription
    #[test]
    fn confirm_subscription() {
        let coder = new_encoder();
        let msg = reply_json("confirm_subscription", IDENTIFIER, None);
        let expected = r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"pucha"}"#;

        assert_eq!(expected, payload(encode_transmission(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncodeTransmission/confirm_subscription_+_presence
    #[test]
    fn confirm_subscription_presence() {
        let coder = new_encoder();
        let msg = reply_json(
            "confirm_subscription",
            IDENTIFIER,
            Some(json!(r#"{"presence":{"count":2,"hash":{"xyz":{"name":"jack"}}}}"#)),
        );
        let expected = r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":2,\"hash\":{\"xyz\":{\"name\":\"jack\"}}}}","channel":"pucha"}"#;

        assert_eq!(expected, payload(encode_transmission(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncodeTransmission/reject_subscription
    #[test]
    fn reject_subscription() {
        let coder = new_encoder();
        let msg = reply_json("reject_subscription", IDENTIFIER, None);
        let expected = r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#;

        assert_eq!(expected, payload(encode_transmission(&coder, &msg)));
    }

    /// upstream: pusher/encoder_test.go#TestPusherEncodeTransmission/message_with_event
    #[test]
    fn message_with_event() {
        let coder = new_encoder();
        let message = json!({"event": "user-joined", "data": {"name": "John"}});
        let msg = reply_json("", IDENTIFIER, Some(message));
        let expected = r#"{"event":"user-joined","data":{"name":"John"},"channel":"pucha"}"#;

        assert_eq!(expected, payload(encode_transmission(&coder, &msg)));
    }
}

mod test_pusher_decode {
    use super::*;

    /// upstream: pusher/encoder_test.go#TestPusherDecode/subscribe
    #[test]
    fn subscribe() {
        let coder = new_encoder();
        let msg = br#"{"event":"pusher:subscribe","data":{"auth":"","channel":"pucha"}}"#;

        let actual = decode(&coder, msg).expect("no error");

        assert_eq!("subscribe", actual.command);
        assert_eq!(IDENTIFIER, actual.identifier);

        let data = actual.data.as_ref().and_then(|d| d.downcast_ref::<PusherSubscriptionData>());
        assert!(data.is_some(), "data is *PusherSubscriptionData: {:?}", actual.data);
        assert_eq!("", data.unwrap().auth);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/subscribe_to_private
    #[test]
    fn subscribe_to_private() {
        let coder = new_encoder();
        let msg = br#"{"event":"pusher:subscribe","data":{"auth":"signed-private-party","channel":"private-party"}}"#;

        let actual = decode(&coder, msg).expect("no error");

        assert_eq!("subscribe", actual.command);
        assert_eq!(PRIVATE_IDENTIFIER, actual.identifier);

        let data = actual.data.as_ref().and_then(|d| d.downcast_ref::<PusherSubscriptionData>());
        assert!(data.is_some(), "data is *PusherSubscriptionData: {:?}", actual.data);
        assert_eq!("signed-private-party", data.unwrap().auth);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/unsubscribe
    #[test]
    fn unsubscribe() {
        let coder = new_encoder();
        let msg = br#"{"event":"pusher:unsubscribe","data":{"channel":"pucha"}}"#;

        let actual = decode(&coder, msg).expect("no error");

        assert_eq!("unsubscribe", actual.command);
        assert_eq!(IDENTIFIER, actual.identifier);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/ping
    #[test]
    fn ping() {
        let coder = new_encoder();
        let msg = br#"{"event":"pusher:ping","data":{}}"#;

        let actual = decode(&coder, msg).expect("no error");

        assert_eq!("ping", actual.command);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/pong
    #[test]
    fn pong() {
        let coder = new_encoder();
        let msg = br#"{"event":"pusher:pong"}"#;

        let actual = decode(&coder, msg).expect("no error");

        assert_eq!("pong", actual.command);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/client_event
    #[test]
    fn client_event() {
        let coder = new_encoder();
        let msg = br#"{"event":"client-message","data":{"text":"hello"},"channel":"private-party"}"#;
        let actual = decode(&coder, msg);

        let payload = PusherClientEvent {
            event: "client-message".into(),
            channel: "private-party".into(),
            data: Some(json!({"text": "hello"})),
        };

        assert!(actual.is_ok(), "{actual:?}");
        let actual = actual.unwrap();
        assert_eq!("whisper", actual.command);
        assert_eq!(PRIVATE_IDENTIFIER, actual.identifier);
        let data = actual.data.as_ref().and_then(|d| d.downcast_ref::<PusherClientEvent>());
        assert_eq!(Some(&payload), data, "data is *PusherClientEvent: {:?}", actual.data);
    }

    /// upstream: pusher/encoder_test.go#TestPusherDecode/custom_event
    #[test]
    fn custom_event() {
        let coder = new_encoder();
        let msg = br#"{"event":"custom-message","data":{"channel":"test-channel","text":"hello"}}"#;

        let actual = decode(&coder, msg);

        // `assert.Error(err)` and `assert.Nil(actual)`: an Err carries no message.
        assert!(actual.is_err(), "{actual:?}");
    }
}
