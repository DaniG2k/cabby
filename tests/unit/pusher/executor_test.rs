//! upstream file: pusher/executor_test.go
//!
//! Upstream builds a real `node.Session` (pusher `Encoder{}`, `mocks.Connection`) on a
//! mock node, and an application executor (`MockExecutor`) that runs the pusher
//! `Controller` and sends its transmissions through the session. Here the session is
//! [`TestSession`] (same id, URL and identifiers; transmissions are encoded with
//! `Encoder{}` and queued like `mocks.Connection`), and [`MockExecutor`] mirrors the
//! upstream one. Not observable through the Go bridge; the external equivalent is
//! `tests/conformance/pusher.rs`.

use std::collections::VecDeque;
use std::sync::Arc;

use cabby_core::common::{Message, MessageData, PRESENCE_INFO_TYPE, PresenceInfo, SessionEnv, StringMap};
use cabby_server::pusher::{Config, Encoder, PusherSubscriptionData};
use cabby_server::pusher_sse_deps::{Error, Executor as NodeExecutor, ExecutorSession};
use cabby_testkit::reference::pusher::{Controller, Executor, StubBroker, encode_transmission};

// From https://pusher.com/docs/channels/library_auth_reference/auth-signatures/
const APP_ID: &str = "278d425bdf160c739803";
const KEY: &str = "7ad3773142a6692b25b8";
const STREAM: &str = "private-foobar";
const SIGNATURE: &str = "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";
const PRESENCE_STREAM: &str = "presence-foobar";
const PRESENCE_SIGNATURE: &str =
    "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800";
const PRESENCE_DATA: &str = "{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}";

/// The upstream test's `MockExecutor`: runs `Subscribe` on the controller and sends
/// the transmissions to the session; anything else is "not implemented".
struct MockExecutor {
    ctrl: Controller,
}

impl NodeExecutor for MockExecutor {
    fn handle_command(&self, s: &mut dyn ExecutorSession, msg: &Message) -> Result<(), Error> {
        let mut res = None;
        let mut err = None;

        if msg.command == "subscribe" {
            let out = self.ctrl.subscribe(&s.id(), &s.env(), &s.identifiers(), &msg.identifier);
            res = out.result;
            err = out.error;
        }

        if let Some(r) = &res {
            for t in &r.transmissions {
                s.send_json_transmission(t);
            }
        }

        if res.is_none() {
            return Err(Error("not implemeented".into()));
        }

        err.map_or(Ok(()), Err)
    }

    fn disconnect(&self, _s: &mut dyn ExecutorSession) -> Result<(), Error> {
        Ok(())
    }
}

/// `buildSession(conn, n, executor)`: sid "1234.1234", URL
/// "ws://anycable.io/pusher/app-id", no headers, authenticated with identifiers "".
struct TestSession {
    env: SessionEnv,
    /// `mocks.Connection`: written frames, read in order.
    conn: VecDeque<Vec<u8>>,
}

impl TestSession {
    fn new() -> Self {
        Self {
            env: SessionEnv { url: "ws://anycable.io/pusher/app-id".into(), ..Default::default() },
            conn: VecDeque::new(),
        }
    }

    /// `conn.Read()`.
    fn read(&mut self) -> Result<Vec<u8>, String> {
        self.conn.pop_front().ok_or_else(|| "connection hasn't received any messages".to_owned())
    }
}

impl ExecutorSession for TestSession {
    fn id(&self) -> String {
        "1234.1234".into()
    }

    fn identifiers(&self) -> String {
        String::new()
    }

    fn env(&self) -> SessionEnv {
        self.env.clone()
    }

    fn merge_channel_state(&mut self, identifier: &str, state: &StringMap) {
        let channels = self.env.channel_states.get_or_insert_with(Default::default);
        channels.entry(identifier.to_owned()).or_default().extend(state.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    fn send_json_transmission(&mut self, msg: &str) {
        // node.WithEncoder(&Encoder{}): zero activity timeout.
        if let Ok(Some(frame)) = encode_transmission(&Encoder::default(), msg) {
            self.conn.push_back(frame.payload);
        }
    }
}

fn build_executor() -> Executor {
    let mut conf = Config::new();
    conf.app_key = APP_ID.into();
    conf.secret = KEY.into();
    let bro =
        StubBroker { presence_info: Some(PresenceInfo { kind: PRESENCE_INFO_TYPE.into(), total: 0, records: vec![] }) };
    let app = Arc::new(MockExecutor { ctrl: Controller::new(bro, &conf) });
    Executor::new(app, APP_ID, KEY)
}

fn identifier(stream: &str) -> String {
    format!(r#"{{"channel":"$pusher","stream":"{stream}"}}"#)
}

fn subscription(data: PusherSubscriptionData) -> Option<MessageData> {
    Some(MessageData::Typed(Arc::new(data)))
}

mod test_handle_command {
    use super::*;

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(public)
    #[test]
    fn subscribe_public() {
        let executor = build_executor();
        let mut session = TestSession::new();

        let msg = Message { command: "subscribe".into(), identifier: identifier("all-chat"), ..Default::default() };

        let err = executor.handle_command(&mut session, &msg);
        assert!(err.is_ok(), "{err:?}");

        let res = session.read().expect("a frame");

        assert_eq!(
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"all-chat"}"#,
            String::from_utf8(res).unwrap()
        );
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(private)
    #[test]
    fn subscribe_private() {
        let executor = build_executor();
        let mut session = TestSession::new();

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

        let err = executor.handle_command(&mut session, &msg);
        assert!(err.is_ok(), "{err:?}");

        let res = session.read().expect("a frame");

        assert_eq!(
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-foobar"}"#,
            String::from_utf8(res).unwrap()
        );
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(private_+_failure_bad_signature)
    #[test]
    fn subscribe_private_failure_bad_signature() {
        let executor = build_executor();
        let mut session = TestSession::new();

        let msg = Message {
            command: "subscribe".into(),
            identifier: identifier(STREAM),
            data: subscription(PusherSubscriptionData { auth: "bla-bla".into(), ..Default::default() }),
            ..Default::default()
        };

        let err = executor.handle_command(&mut session, &msg);
        assert!(err.is_ok(), "{err:?}");

        let res = session.read().expect("a frame");

        assert_eq!(
            r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#,
            String::from_utf8(res).unwrap()
        );
    }

    /// upstream: pusher/executor_test.go#TestHandleCommand/Subscribe_(presence)
    #[test]
    fn subscribe_presence() {
        let executor = build_executor();
        let mut session = TestSession::new();

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

        let err = executor.handle_command(&mut session, &msg);
        assert!(err.is_ok(), "{err:?}");

        let res = session.read().expect("a frame");

        assert_eq!(
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":1,\"ids\":[\"10\"],\"hash\":{\"10\":{\"name\":\"Mr. Channels\"}}}}","channel":"presence-foobar"}"#,
            String::from_utf8(res).unwrap()
        );
    }
}
