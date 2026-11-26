//! upstream file: pusher/executor_test.go
//!
//! The upstream scaffolding (`MockExecutor` running the pusher `Controller`,
//! `NewMockNode`, `buildSession` over `mocks.NewMockConnection()`) lives in the
//! facade [`Executor`]: against Go it is the upstream code in the bridge, against
//! Rust a double with the same session id, URL, identifiers and `Encoder{}`. Each
//! call builds a fresh session; upstream shares one node and executor across the
//! subtests, which keep no state between them.

use std::sync::Arc;

use cabby_core::common::{Message, MessageData, PRESENCE_INFO_TYPE, PresenceInfo};
use cabby_server::pusher::PusherSubscriptionData;
use cabby_testkit::reference::pusher::Executor;

// From https://pusher.com/docs/channels/library_auth_reference/auth-signatures/
const APP_ID: &str = "278d425bdf160c739803";
const KEY: &str = "7ad3773142a6692b25b8";
const STREAM: &str = "private-foobar";
const SIGNATURE: &str = "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";
const PRESENCE_STREAM: &str = "presence-foobar";
const PRESENCE_SIGNATURE: &str =
    "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800";
const PRESENCE_DATA: &str = "{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}";

/// `TestHandleCommand` setup; the broker's `PresenceInfo` returns no records.
fn build_executor() -> Executor {
    Executor::new(APP_ID, KEY, PresenceInfo { kind: PRESENCE_INFO_TYPE.into(), total: 0, records: vec![] })
}

fn identifier(stream: &str) -> String {
    format!(r#"{{"channel":"$pusher","stream":"{stream}"}}"#)
}

fn subscription(data: PusherSubscriptionData) -> Option<MessageData> {
    Some(MessageData::Typed(Arc::new(data)))
}

/// `require.NoError(HandleCommand)`, then `res, err := conn.Read()` with
/// `require.NoError(err)`.
fn handle_and_read(msg: &Message) -> String {
    let run = build_executor().handle_command(msg, 1);
    assert_eq!(None, run.error);
    let res = run.reads.into_iter().next().expect("one read").expect("a frame");
    String::from_utf8(res).expect("UTF-8 frame")
}

mod test_handle_command {
    use super::*;

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(public)
    #[test]
    fn subscribe_public() {
        let msg = Message { command: "subscribe".into(), identifier: identifier("all-chat"), ..Default::default() };

        let res = handle_and_read(&msg);

        assert_eq!(r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"all-chat"}"#, res);
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(private)
    #[test]
    fn subscribe_private() {
        let msg = Message {
            command: "subscribe".into(),
            identifier: identifier(STREAM),
            data: subscription(PusherSubscriptionData {
                auth: SIGNATURE.into(),
                channel: STREAM.into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let res = handle_and_read(&msg);

        assert_eq!(r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-foobar"}"#, res);
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
    #[test]
    fn subscribe_private_failure_bad_signature() {
        let msg = Message {
            command: "subscribe".into(),
            identifier: identifier(STREAM),
            data: subscription(PusherSubscriptionData { auth: "bla-bla".into(), ..Default::default() }),
            ..Default::default()
        };

        let res = handle_and_read(&msg);

        assert_eq!(r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#, res);
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(presence)
    #[test]
    fn subscribe_presence() {
        let msg = Message {
            command: "subscribe".into(),
            identifier: identifier(PRESENCE_STREAM),
            data: subscription(PusherSubscriptionData {
                auth: PRESENCE_SIGNATURE.into(),
                channel: PRESENCE_STREAM.into(),
                channel_data: PRESENCE_DATA.into(),
            }),
            ..Default::default()
        };

        let res = handle_and_read(&msg);

        assert_eq!(
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":1,\"ids\":[\"10\"],\"hash\":{\"10\":{\"name\":\"Mr. Channels\"}}}}","channel":"presence-foobar"}"#,
            res
        );
    }
}

/// Cabby-only checks next to `TestHandleCommand`: upstream rejects only a bad
/// `private-` signature. A `presence-` subscription with an invalid signature,
/// with `channel_data` other than the signed one, or a `private-`/`presence-`
/// subscription without auth data must be rejected the same way.
mod rejected_subscriptions {
    use super::*;

    const REJECTED: &str = r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#;

    /// regression-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
    #[test]
    fn a_presence_subscription_with_a_bad_signature_is_rejected() {
        // The signature of the private channel, a truncated one, values that are
        // not signatures.
        for auth in [SIGNATURE, "278d425bdf160c739803:00", "bla-bla", ""] {
            let msg = Message {
                command: "subscribe".into(),
                identifier: identifier(PRESENCE_STREAM),
                data: subscription(PusherSubscriptionData {
                    auth: auth.into(),
                    channel: PRESENCE_STREAM.into(),
                    channel_data: PRESENCE_DATA.into(),
                }),
                ..Default::default()
            };

            assert_eq!(REJECTED, handle_and_read(&msg), "auth {auth:?}");
        }
    }

    /// regression-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
    #[test]
    fn a_presence_subscription_with_other_channel_data_is_rejected() {
        let msg = Message {
            command: "subscribe".into(),
            identifier: identifier(PRESENCE_STREAM),
            data: subscription(PusherSubscriptionData {
                auth: PRESENCE_SIGNATURE.into(),
                channel: PRESENCE_STREAM.into(),
                channel_data: "{\"user_id\":\"11\",\"user_info\":{\"name\":\"Mr. Channels\"}}".into(),
            }),
            ..Default::default()
        };

        assert_eq!(REJECTED, handle_and_read(&msg));
    }

    /// regression-for: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
    #[test]
    fn a_private_or_presence_subscription_without_auth_data_is_rejected() {
        for stream in [STREAM, PRESENCE_STREAM] {
            let msg = Message { command: "subscribe".into(), identifier: identifier(stream), ..Default::default() };

            assert_eq!(REJECTED, handle_and_read(&msg), "{stream}");
        }
    }
}
