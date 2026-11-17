//! upstream file: rpc/rpc_test.go
//!
//! Upstream injects a mock `pb.RPCClient` into the controller. Here the
//! controller gets a real gRPC client to a [`FakeRpc`] (`Dial::TestClient`): mock
//! expectations become scripted replies matched on the exact request, and
//! `AssertCalled` / `AssertNotCalled` become checks on the fake's call log.
//! Subtests that shared one controller and mock upstream get their own here.
//!
//! Nil vs empty: upstream asserts `assert.Nil(t, res.Transmissions)` (and
//! `Streams`, `StoppedStreams`) on results parsed from the response. Here those
//! are `is_empty()`. The Rust result types use `Vec`, and the distinction cannot
//! be observed anyway: the responses arrive over the wire (protobuf, or JSON with
//! `omitempty`), where an absent and an empty repeated field encode identically,
//! so the Go result is nil exactly when the list is empty.

use std::time::Duration;

use cabby_core::common::{SessionEnv, Status};
use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply, Request};
use cabby_testkit::reference::metrics::Metrics;
use cabby_testkit::reference::rpc::*;

use super::helpers::{DEADLINE, elapse, eventually, map, strings};

/// Upstream `NewTestController`: default config, a barrier of 5, an always-ready
/// client state, and a client talking to the fake.
struct TestController {
    fake: FakeRpc,
    controller: Controller,
}

fn new_test_controller() -> TestController {
    new_test_controller_with(|fake| Dial::TestClient(fake.grpc_target()))
}

/// [`new_test_controller`] with another injected client.
fn new_test_controller_with(dial: impl FnOnce(&FakeRpc) -> Dial) -> TestController {
    let mut config = new_config();
    config.concurrency = 5;
    let fake = FakeRpc::start();
    let metrics = Metrics::new(0);
    let controller = Controller::new(&metrics, &config, dial(&fake)).expect("controller");
    controller.start().expect("start");
    TestController { fake, controller }
}

fn env(url: &str, headers: &[(&str, &str)]) -> protos::Env {
    protos::Env { url: url.into(), headers: map(headers), ..Default::default() }
}

fn session(url: &str, headers: &[(&str, &str)]) -> SessionEnv {
    SessionEnv { url: url.into(), headers: Some(map(headers)), ..Default::default() }
}

fn calls_with(fake: &FakeRpc, request: impl Into<Request>) -> usize {
    let request = request.into();
    fake.calls().iter().filter(|c| c.request == request).count()
}

mod test_authenticate {
    use super::*;

    fn connect_request(url: &str, headers: &[(&str, &str)]) -> protos::ConnectionRequest {
        protos::ConnectionRequest { env: Some(env(url, headers)) }
    }

    /// upstream: rpc/rpc_test.go#TestAuthenticate/Success
    #[test]
    fn success() {
        let t = new_test_controller();
        let url = "/cable-test";
        let headers = [("cookie", "token=secret;")];

        t.fake.on(Method::Connect).with_request(connect_request(url, &headers)).reply(protos::ConnectionResponse {
            identifiers: "user=john".into(),
            transmissions: strings(&["welcome"]),
            status: protos::Status::Success as i32,
            env: Some(protos::EnvResponse { cstate: map(&[("_s_", "test-session")]), ..Default::default() }),
            ..Default::default()
        });

        let out = t.controller.authenticate(&Ctx::background(), "42", &session(url, &headers));
        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["welcome"]), res.transmissions);
        assert_eq!("user=john", res.identifier);
        assert_eq!(Some(map(&[("_s_", "test-session")])), res.cstate);
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestAuthenticate/Failure
    #[test]
    fn failure() {
        let t = new_test_controller();
        let url = "/cable-test";
        let headers = [("cookie", "token=invalid;")];

        t.fake.on(Method::Connect).with_request(connect_request(url, &headers)).reply(protos::ConnectionResponse {
            transmissions: strings(&["unauthorized"]),
            status: protos::Status::Failure as i32,
            env: Some(protos::EnvResponse { cstate: map(&[("_s_", "test-session")]), ..Default::default() }),
            error_msg: "Authentication failed".into(),
            ..Default::default()
        });

        let out = t.controller.authenticate(&Ctx::background(), "42", &session(url, &headers));
        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["unauthorized"]), res.transmissions);
        assert_eq!("", res.identifier);
        assert_eq!(Some(map(&[("_s_", "test-session")])), res.cstate);
        assert!(res.broadcasts.is_empty());
    }

    fn error_case() -> RpcOutcome<cabby_core::common::ConnectResult> {
        let t = new_test_controller();
        let url = "/cable-test";
        let headers = [("cookie", "token=exceptional;")];

        t.fake.on(Method::Connect).with_request(connect_request(url, &headers)).reply(protos::ConnectionResponse {
            status: protos::Status::Error as i32,
            error_msg: "Exception".into(),
            ..Default::default()
        });

        t.controller.authenticate(&Ctx::background(), "42", &session(url, &headers))
    }

    /// upstream: rpc/rpc_test.go#TestAuthenticate/Error
    #[test]
    fn error() {
        let out = error_case();
        assert!(out.error.is_some());
        // Upstream `assert.Error(t, err, "Exception")` only checks that err is
        // non-nil ("Exception" is the failure message); kept as written. The
        // message itself is asserted by `error_message`.
        assert!(out.error.is_some(), "Exception");
        let res = out.result.expect("result");
        assert!(res.transmissions.is_empty());
        assert_eq!("", res.identifier);
        assert_eq!(None, res.cstate);
        assert!(res.broadcasts.is_empty());
    }

    /// regression-for: rpc/rpc_test.go#TestAuthenticate/Error
    /// The intended check: the application's error message is the error.
    #[test]
    fn error_message() {
        let out = error_case();
        assert_eq!(
            Some(RpcError::Application(cabby_core::protocol::ApplicationError { message: "Exception".into() })),
            out.error
        );
    }

    /// upstream: rpc/rpc_test.go#TestAuthenticate/Canceled_before_RPC
    #[test]
    fn canceled_before_rpc() {
        let t = new_test_controller();
        let url = "/cancel-cables";
        let expected = connect_request(url, &[]);
        // Cancelled right away.
        let canceled = Ctx::cancelled();

        t.fake
            .on(Method::Connect)
            .with_request(expected.clone())
            .reply(Reply::status(Code::Cancelled, "context canceled"));

        let out = t.controller.authenticate(&canceled, "42", &session(url, &[]));
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(out.result.is_none());

        assert_eq!(0, calls_with(&t.fake, expected));
    }

    /// upstream: rpc/rpc_test.go#TestAuthenticate/Canceled_during_RPC
    #[test]
    fn canceled_during_rpc() {
        let url = "/cancel-cables-2";
        let expected = connect_request(url, &[]);
        let canceled = Ctx::background();

        // Upstream's mock client cancels the context during the call and returns
        // the bare `canceledCtx.Err()`. `Dial::CancelOnCall` is that client: it
        // passes the call to the fake (which records it; the scripted reply is
        // discarded), cancels `canceled` and returns the bare context error.
        let t = new_test_controller_with(|fake| Dial::cancel_on_call(fake.grpc_target(), &canceled));
        t.fake
            .on(Method::Connect)
            .with_request(expected.clone())
            .reply(protos::ConnectionResponse { status: protos::Status::Success as i32, ..Default::default() });

        let out = t.controller.authenticate(&canceled, "42", &session(url, &[]));
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(out.result.is_none());

        assert!(calls_with(&t.fake, expected) > 0);
    }
}

fn command(data: &str, url: &str, headers: &[(&str, &str)]) -> protos::CommandMessage {
    protos::CommandMessage {
        command: "message".into(),
        connection_identifiers: "ids".into(),
        identifier: "test_channel".into(),
        data: data.into(),
        env: Some(env(url, headers)),
    }
}

mod test_perform {
    use super::*;

    fn with_cstate(mut msg: protos::CommandMessage, cstate: &[(&str, &str)]) -> protos::CommandMessage {
        msg.env.as_mut().unwrap().cstate = map(cstate);
        msg
    }

    fn session_with_cstate(url: &str, headers: &[(&str, &str)], cstate: &[(&str, &str)]) -> SessionEnv {
        SessionEnv { connection_state: Some(map(cstate)), ..session(url, headers) }
    }

    /// upstream: rpc/rpc_test.go#TestPerform/Success
    #[test]
    fn success() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(with_cstate(command("hello", url, &headers), &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                streams: strings(&["chat_42"]),
                stop_streams: true,
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "sentCount=1")]), ..Default::default() }),
                transmissions: strings(&["message_sent"]),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
            "hello",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["message_sent"]), res.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), res.cstate);
        assert!(res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.streams);
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestPerform/Failure
    #[test]
    fn failure() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=invalid;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(with_cstate(command("fail", url, &headers), &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Failure as i32,
                streams: strings(&["chat_42"]),
                stop_streams: true,
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "sentCount=1")]), ..Default::default() }),
                transmissions: strings(&["message_sent"]),
                error_msg: "Forbidden".into(),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
            "fail",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Failure, res.status);
        assert_eq!(strings(&["message_sent"]), res.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), res.cstate);
        assert!(res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.streams);
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestPerform/Error
    #[test]
    fn error() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=invalid;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(with_cstate(command("exception", url, &headers), &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Error as i32,
                stop_streams: true,
                error_msg: "Exception".into(),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
            "exception",
        );

        assert!(out.error.is_some());
        let res = out.result.expect("result");
        assert_eq!(Status::Error, res.status);
        // Upstream `assert.Error(t, err, "Exception")`: non-nil check only (kept).
        assert!(out.error.is_some(), "Exception");
        assert!(res.transmissions.is_empty());
        assert!(res.stop_all_streams);
        assert!(res.streams.is_empty());
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestPerform/With_stopped_streams
    #[test]
    fn with_stopped_streams() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(with_cstate(command("stop_stream", url, &headers), &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                stopped_streams: strings(&["chat_42"]),
                stop_streams: false,
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "sentCount=1")]), ..Default::default() }),
                transmissions: strings(&["message_sent"]),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
            "stop_stream",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["message_sent"]), res.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), res.cstate);
        assert!(!res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.stopped_streams);
        assert!(res.streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    fn channel_session(url: &str, headers: &[(&str, &str)], istate: &[(&str, &str)]) -> SessionEnv {
        let channels = [("test_channel".to_owned(), map(istate))].into_iter().collect();
        SessionEnv { channel_states: Some(channels), ..session(url, headers) }
    }

    fn with_istate(mut msg: protos::CommandMessage, istate: &[(&str, &str)]) -> protos::CommandMessage {
        msg.env.as_mut().unwrap().istate = map(istate);
        msg
    }

    /// upstream: rpc/rpc_test.go#TestPerform/With_channel_state
    #[test]
    fn with_channel_state() {
        let t = new_test_controller();
        let (url, headers, istate) = ("/cable-test", [("cookie", "token=secret;")], [("room", "room:1")]);

        t.fake.on(Method::Command).with_request(with_istate(command("channel_state", url, &headers), &istate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                stopped_streams: strings(&["chat_42"]),
                stop_streams: false,
                env: Some(protos::EnvResponse { istate: map(&[("count", "1")]), ..Default::default() }),
                transmissions: strings(&["message_sent"]),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &channel_session(url, &headers, &istate),
            "ids",
            "test_channel",
            "channel_state",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["message_sent"]), res.transmissions);
        assert_eq!(Some(map(&[("count", "1")])), res.istate);
        assert!(!res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.stopped_streams);
        assert!(res.streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestPerform/With_presence
    #[test]
    fn with_presence() {
        let t = new_test_controller();
        let (url, headers, istate) = ("/cable-present", [("cookie", "token=secret;")], [("room", "room:1")]);

        t.fake.on(Method::Command).with_request(with_istate(command("channel_state", url, &headers), &istate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                stopped_streams: strings(&["chat_42"]),
                stop_streams: false,
                // common.PRESENCE_STREAM_STATE
                env: Some(protos::EnvResponse { istate: map(&[("$p", "presence_42")]), ..Default::default() }),
                transmissions: strings(&["message_sent"]),
                presence: Some(protos::PresenceResponse {
                    r#type: "leave".into(),
                    id: "2024".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        let out = t.controller.perform(
            &Ctx::background(),
            "42",
            &channel_session(url, &headers, &istate),
            "ids",
            "test_channel",
            "channel_state",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["message_sent"]), res.transmissions);
        assert_eq!(Some(map(&[("$p", "presence_42")])), res.istate);
        assert!(!res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.stopped_streams);
        assert!(res.streams.is_empty());
        assert!(res.broadcasts.is_empty());

        let presence = res.presence.expect("presence");
        // common.PresenceLeaveType
        assert_eq!("leave", presence.kind);
        assert_eq!("2024", presence.id);
    }
}

mod test_subscribe {
    use super::*;

    fn subscribe(
        channel: &str,
        url: &str,
        headers: &[(&str, &str)],
        cstate: &[(&str, &str)],
    ) -> protos::CommandMessage {
        let mut msg = command("", url, headers);
        msg.command = "subscribe".into();
        msg.identifier = channel.into();
        msg.env.as_mut().unwrap().cstate = map(cstate);
        msg
    }

    fn session_with_cstate(url: &str, headers: &[(&str, &str)], cstate: &[(&str, &str)]) -> SessionEnv {
        SessionEnv { connection_state: Some(map(cstate)), ..session(url, headers) }
    }

    /// upstream: rpc/rpc_test.go#TestSubscribe/Success
    #[test]
    fn success() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(subscribe("test_channel", url, &headers, &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                streams: strings(&["chat_42"]),
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "sentCount=1")]), ..Default::default() }),
                transmissions: strings(&["confirmed"]),
                ..Default::default()
            },
        );

        let out = t.controller.subscribe(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["confirmed"]), res.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), res.cstate);
        assert!(!res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.streams);
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestSubscribe/Failure
    #[test]
    fn failure() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(subscribe("fail_channel", url, &headers, &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Failure as i32,
                error_msg: "Unauthorized".into(),
                disconnect: true,
                transmissions: strings(&["rejected"]),
                ..Default::default()
            },
        );

        let out = t.controller.subscribe(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "fail_channel",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Failure, res.status);
        assert_eq!(strings(&["rejected"]), res.transmissions);
        assert!(res.disconnect);
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());
    }

    /// upstream: rpc/rpc_test.go#TestSubscribe/Error
    #[test]
    fn error() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(subscribe("error_channel", url, &headers, &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Error as i32,
                error_msg: "Exception".into(),
                ..Default::default()
            },
        );

        let out = t.controller.subscribe(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "error_channel",
        );

        assert!(out.error.is_some());
        assert_eq!(Status::Error, out.result.expect("result").status);
    }

    /// upstream: rpc/rpc_test.go#TestSubscribe/With_presence
    #[test]
    fn with_presence() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-present", [("cookie", "token=secret;")], [("_s_", "id=42")]);

        t.fake.on(Method::Command).with_request(subscribe("test_channel", url, &headers, &cstate)).reply(
            protos::CommandResponse {
                status: protos::Status::Success as i32,
                streams: strings(&["chat_42"]),
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "sentCount=1")]), ..Default::default() }),
                transmissions: strings(&["confirmed"]),
                presence: Some(protos::PresenceResponse {
                    r#type: "join".into(),
                    id: "2024".into(),
                    info: r#"{"name":"Torcha"}"#.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        let out = t.controller.subscribe(
            &Ctx::background(),
            "42",
            &session_with_cstate(url, &headers, &cstate),
            "ids",
            "test_channel",
        );

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["confirmed"]), res.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), res.cstate);
        assert!(!res.stop_all_streams);
        assert_eq!(strings(&["chat_42"]), res.streams);
        assert!(res.stopped_streams.is_empty());
        assert!(res.broadcasts.is_empty());

        let presence = res.presence.expect("presence");
        // common.PresenceJoinType
        assert_eq!("join", presence.kind);
        assert_eq!("2024", presence.id);
        assert_eq!(Some(&serde_json::json!("Torcha")), presence.info.as_ref().and_then(|i| i.get("name")));
    }
}

mod test_disconnect {
    use super::*;

    /// upstream: rpc/rpc_test.go#TestDisconnect/Success
    #[test]
    fn success() {
        let t = new_test_controller();
        let (url, headers, cstate) = ("/cable-test", [("cookie", "token=secret;")], [("_s_", "id=42")]);
        let istate = [("test_channel", "{\"room\":\"room:1\"}")];

        let channels = [("test_channel".to_owned(), map(&[("room", "room:1")]))].into_iter().collect();

        t.fake
            .on(Method::Disconnect)
            .with_request(protos::DisconnectRequest {
                identifiers: "ids".into(),
                subscriptions: strings(&["chat_42"]),
                env: Some(protos::Env {
                    url: url.into(),
                    headers: map(&headers),
                    cstate: map(&cstate),
                    istate: map(&istate),
                }),
            })
            .reply(protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() });

        let err = t.controller.disconnect(
            &Ctx::background(),
            "42",
            &SessionEnv {
                connection_state: Some(map(&cstate)),
                channel_states: Some(channels),
                ..session(url, &headers)
            },
            "ids",
            &["chat_42"],
        );
        assert_eq!(Ok(()), err);
    }
}

mod test_custom_dial_fun {
    use super::*;

    /// upstream: rpc/rpc_test.go#TestCustomDialFun
    /// upstream: rpc/rpc_test.go#TestCustomDialFun/Connect
    #[test]
    fn connect() {
        let config = new_config();
        let service = FakeRpc::start();

        // config.DialFun = NewInprocessServiceDialer(&service, MockState{true, false})
        let controller =
            Controller::new(&Metrics::new(0), &config, Dial::Inprocess(service.grpc_target())).expect("controller");
        controller.start().expect("start");

        let url = "/cable-test";
        let headers = [("cookie", "token=secret;")];

        service.on(Method::Connect).with_request(protos::ConnectionRequest { env: Some(env(url, &headers)) }).reply(
            protos::ConnectionResponse {
                identifiers: "user=john".into(),
                transmissions: strings(&["welcome"]),
                status: protos::Status::Success as i32,
                env: Some(protos::EnvResponse { cstate: map(&[("_s_", "test-session")]), ..Default::default() }),
                ..Default::default()
            },
        );

        let out = controller.authenticate(&Ctx::background(), "42", &session(url, &headers));
        assert!(out.error.is_none(), "Error: {:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(strings(&["welcome"]), res.transmissions);
        assert_eq!("user=john", res.identifier);
        assert_eq!(Some(map(&[("_s_", "test-session")])), res.cstate);
        assert!(res.broadcasts.is_empty());

        let calls = service.calls();
        let call = calls.first().expect("the service received the call");
        assert_eq!(vec!["42"], call.meta_all("sid"));
    }
}

mod test_wait_timeout {
    use super::*;

    /// Upstream `newBusyController`: a 1-slot barrier whose only slot is taken,
    /// and the given wait timeout (ms). The slot is taken by a real call of
    /// `holder` held at the fake (upstream acquires the barrier directly).
    struct Busy {
        fake: FakeRpc,
        gate: cabby_testkit::sync::Gate,
        controller: Controller,
        metrics: Metrics,
        holder: Box<dyn FnOnce()>,
    }

    impl Busy {
        fn counter(&self, name: &str) -> u64 {
            self.metrics.counter(name).expect("registered counter").value()
        }

        fn pending(&self) -> u64 {
            self.metrics.gauge(metric_names::RPC_PENDING).expect("registered gauge").value()
        }

        /// Waits until a call is waiting for the slot (it raised rpc_pending_num).
        fn await_waiting(&self) {
            eventually("a call waiting for a slot", DEADLINE, || self.pending() == 1);
        }

        /// Frees the slot (upstream `controller.barrier.Release()`).
        fn release(self) -> (FakeRpc, Controller, Metrics) {
            self.gate.open();
            (self.holder)();
            (self.fake, self.controller, self.metrics)
        }
    }

    fn new_busy_controller(wait_timeout_ms: i64, holder: Method) -> Busy {
        let mut config = new_config();
        config.wait_timeout = wait_timeout_ms;
        new_busy_controller_with(config, holder)
    }

    /// [`new_busy_controller`] with an explicit config (concurrency is forced to 1).
    fn new_busy_controller_with(mut config: Config, holder: Method) -> Busy {
        config.concurrency = 1;
        let fake = FakeRpc::start();
        let metrics = Metrics::new(0);
        let controller = Controller::new(&metrics, &config, Dial::TestClient(fake.grpc_target())).expect("controller");
        controller.start().expect("start");

        // Take the only slot
        let gate = fake.gate();
        let ctx = Ctx::background();
        let holder: Box<dyn FnOnce()> = match holder {
            Method::Connect => {
                fake.on(Method::Connect).once().held_by(&gate).reply(protos::ConnectionResponse::default());
                let call = controller.spawn_authenticate(&ctx, "holder", &session("", &[]));
                Box::new(move || drop(call.wait()))
            }
            Method::Disconnect => {
                fake.on(Method::Disconnect).once().held_by(&gate).reply(protos::DisconnectResponse::default());
                let call = controller.spawn_disconnect(&ctx, "holder", &session("", &[]), "", &[]);
                Box::new(move || drop(call.wait()))
            }
            Method::Command => unreachable!("Command calls are what the tests observe"),
        };
        gate.await_held(1, DEADLINE);

        Busy { fake, gate, controller, metrics, holder }
    }

    /// How long a Subscribe waits for the only (taken) slot before failing with
    /// ErrWaitTimeout, under `config`.
    ///
    /// Every held call is itself bounded by the command timeout, so the first
    /// holder frees the slot about when the waiter's wait timeout (the same
    /// 1234ms by default) expires. A second holder, a Disconnect (which waits for
    /// a slot without a wait timeout), is queued first and takes the slot over,
    /// so the slot stays taken for the whole measurement.
    fn time_to_wait_timeout(config: Config) -> Duration {
        let busy = new_busy_controller_with(config, Method::Disconnect);
        busy.fake.on(Method::Disconnect).held_by(&busy.gate).reply(protos::DisconnectResponse::default());
        let next_holder =
            busy.controller.spawn_disconnect(&Ctx::background(), "next-holder", &session("", &[]), "", &[]);
        busy.await_waiting();

        let started = std::time::Instant::now();
        let out = busy.controller.subscribe(&Ctx::background(), "42", &session("", &[]), "ids", "test_channel");
        let waited = started.elapsed();
        assert_eq!(Some(RpcError::WaitTimeout), out.error);
        assert_eq!(0, busy.fake.call_count(Method::Command));
        busy.release();
        drop(next_holder.wait());
        waited
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Defaults_to_the_command_timeout
    ///
    /// Upstream reads the unexported `controller.waitTimeout`. Here the effective
    /// wait timeout is observed through the exported API as the time a call
    /// waits for a taken slot before ErrWaitTimeout: never before the timeout,
    /// and (for 500ms) clearly before the 1234ms command timeout. Bounded at
    /// about 2s in total.
    #[test]
    fn defaults_to_the_command_timeout() {
        let mut config = new_config();
        config.command_timeout = 1234;

        let waited = time_to_wait_timeout(config.clone());
        assert!(waited >= Duration::from_millis(1234), "waited {waited:?}");
        assert!(waited < Duration::from_millis(1234 + 2000), "waited {waited:?}");

        config.wait_timeout = 500;

        let waited = time_to_wait_timeout(config);
        assert!(waited >= Duration::from_millis(500), "waited {waited:?}");
        assert!(waited < Duration::from_millis(1234), "waited {waited:?}");
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout
    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Zero_wait_timeout_means_no_limit
    #[test]
    fn zero_wait_timeout_means_no_limit() {
        let busy = new_busy_controller(0, Method::Disconnect);

        busy.fake
            .on(Method::Command)
            .reply(protos::CommandResponse { status: protos::Status::Success as i32, ..Default::default() });

        let headers = [];
        let call =
            busy.controller.spawn_subscribe(&Ctx::background(), "42", &session("", &headers), "ids", "test_channel");
        // Upstream releases the slot after 100ms (past the busy controller's 50ms).
        busy.await_waiting();
        elapse(Duration::from_millis(100));
        let (fake, _controller, _metrics) = busy.release();
        let out = call.wait();

        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(fake.call_count(Method::Command) > 0);
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout
    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Subscribe_times_out_waiting_for_a_slot
    #[test]
    fn subscribe_times_out_waiting_for_a_slot() {
        let busy = new_busy_controller(50, Method::Disconnect);

        let out = busy.controller.subscribe(&Ctx::background(), "42", &SessionEnv::default(), "ids", "test_channel");

        assert!(out.result.is_none());
        assert_eq!(Some(RpcError::WaitTimeout), out.error);
        assert_eq!(0, busy.fake.call_count(Method::Command));
        assert_eq!(1, busy.counter(metric_names::RPC_WAIT_TIMEOUTS));
        assert_eq!(0, busy.counter(metric_names::RPC_CANCELED));
        assert_eq!(0, busy.pending());
        busy.release();
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout
    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Authenticate_times_out_waiting_for_a_slot
    #[test]
    fn authenticate_times_out_waiting_for_a_slot() {
        let busy = new_busy_controller(50, Method::Disconnect);

        let out = busy.controller.authenticate(&Ctx::background(), "42", &SessionEnv::default());

        assert!(out.result.is_none());
        assert_eq!(Some(RpcError::WaitTimeout), out.error);
        assert_eq!(0, busy.fake.call_count(Method::Connect));
        busy.release();
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout
    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Session_closes_while_waiting_for_a_slot
    #[test]
    fn session_closes_while_waiting_for_a_slot() {
        let busy = new_busy_controller(0, Method::Disconnect);

        let ctx = Ctx::background();
        let call = busy.controller.spawn_perform(&ctx, "42", &SessionEnv::default(), "ids", "test_channel", "data");
        // Upstream cancels after 10ms; here once the call is observably waiting.
        busy.await_waiting();
        ctx.cancel();
        let out = call.wait();

        assert!(out.result.is_none());
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(0, busy.fake.call_count(Method::Command));
        assert_eq!(1, busy.counter(metric_names::RPC_CANCELED));
        assert_eq!(0, busy.counter(metric_names::RPC_WAIT_TIMEOUTS));
        busy.release();
    }

    /// upstream: rpc/rpc_test.go#TestWaitTimeout
    /// upstream: rpc/rpc_test.go#TestWaitTimeout/Disconnect_waits_longer_than_the_wait_timeout
    #[test]
    fn disconnect_waits_longer_than_the_wait_timeout() {
        // The slot holder is a Connect call, so every Disconnect the fake sees is
        // the one under test.
        let busy = new_busy_controller(50, Method::Connect);

        busy.fake
            .on(Method::Disconnect)
            .reply(protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() });

        let headers = [];
        let call = busy.controller.spawn_disconnect(&Ctx::background(), "42", &session("", &headers), "ids", &[]);
        // Upstream releases the slot after 2 * waitTimeout.
        busy.await_waiting();
        elapse(Duration::from_millis(2 * 50));
        let (fake, _controller, metrics) = busy.release();
        let err = call.wait();

        assert_eq!(Ok(()), err);
        assert!(fake.call_count(Method::Disconnect) > 0);
        assert_eq!(0, metrics.counter(metric_names::RPC_WAIT_TIMEOUTS).expect("registered counter").value());
    }
}
