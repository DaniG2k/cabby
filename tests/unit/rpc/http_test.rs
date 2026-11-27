//! upstream file: rpc/http_test.go
//!
//! Upstream runs `httptest` servers with inline handlers that `require` on the
//! request; here a [`FakeRpc`] records each request and the test asserts on the
//! record after the call. Reply closures reproduce handlers that echo headers.

use cabby_core::common::SessionEnv;
use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
use cabby_testkit::ports;
use cabby_testkit::reference::protocol::{new_command_message, new_connect_message, new_disconnect_message};
use cabby_testkit::reference::rpc::*;

use super::helpers::{DEADLINE, map, strings};

/// `common.NewSessionEnv(url, &headers)`
fn new_session_env(url: &str, headers: &[(&str, &str)]) -> SessionEnv {
    SessionEnv { url: url.into(), headers: Some(map(headers)), ..Default::default() }
}

fn status_code(err: &RpcError) -> Code {
    match err {
        RpcError::Status(s) => s.code,
        other => panic!("expected a gRPC status error (status.FromError ok), got {other:?}"),
    }
}

/// A URL nothing listens on (upstream: `http://localhost:1234`): its port is
/// reserved until the test process exits, so no other test listens there.
fn unknown_url() -> String {
    format!("http://127.0.0.1:{}", ports::reserve().keep())
}

mod test_http_service_rpc {
    use super::*;

    fn service() -> (FakeRpc, HttpService) {
        let ts = FakeRpc::start();
        let mut conf = new_config();
        conf.host = ts.http_url();
        let service = HttpService::new(&conf).expect("service");
        (ts, service)
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceRPC/Connect
    #[test]
    fn connect() {
        let (ts, service) = service();
        ts.on(Method::Connect).reply(Reply::with(|r| {
            let identifiers = format!(
                "{}-{}",
                r.meta("x-anycable-meta-year").unwrap_or_default(),
                r.meta("x-anycable-meta-album").unwrap_or_default()
            );
            protos::ConnectionResponse {
                transmissions: strings(&["welcome"]),
                identifiers,
                status: protos::Status::Success as i32,
                ..Default::default()
            }
            .into()
        }));

        let ctx = Ctx::with_incoming_metadata(&[("album", "Kamni"), ("year", "2008")]);
        let res = service
            .connect(&ctx, &new_connect_message(&new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")])));

        let calls = ts.calls();
        let r = calls.first().expect("the server received the request");
        assert_eq!("/connect", r.http.as_ref().unwrap().path);
        let req = r.request.connect();
        assert_eq!("ws://anycable.io/cable", req.env.as_ref().unwrap().url);
        assert_eq!(Some(&"foo=bar".to_owned()), req.env.as_ref().unwrap().headers.get("cookie"));

        let res = res.expect("no error");

        assert_eq!(protos::Status::Success as i32, res.status);
        assert_eq!(strings(&["welcome"]), res.transmissions);
        assert_eq!("2008-Kamni", res.identifiers);
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceRPC/Disconnect
    #[test]
    fn disconnect() {
        let (ts, service) = service();
        ts.on(Method::Disconnect).reply(Reply::with(|r| {
            protos::DisconnectResponse {
                status: protos::Status::Error as i32,
                error_msg: r.meta("x-anycable-meta-error").unwrap_or_default().to_owned(),
            }
            .into()
        }));

        let ctx = Ctx::with_incoming_metadata(&[("error", "test error")]);
        let res = service.disconnect(
            &ctx,
            &new_disconnect_message(
                &new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]),
                "test-session",
                &[],
            ),
        );

        let calls = ts.calls();
        let r = calls.first().expect("the server received the request");
        assert_eq!("/disconnect", r.http.as_ref().unwrap().path);
        let req = r.request.disconnect();
        assert_eq!("ws://anycable.io/cable", req.env.as_ref().unwrap().url);
        assert_eq!(Some(&"foo=bar".to_owned()), req.env.as_ref().unwrap().headers.get("cookie"));
        assert_eq!("test-session", req.identifiers);

        let res = res.expect("no error");

        assert_eq!(protos::Status::Error as i32, res.status);
        assert_eq!("test error", res.error_msg);
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceRPC/Command
    #[test]
    fn command() {
        let (ts, service) = service();
        ts.on(Method::Command).reply(Reply::with(|r| {
            let stream = r.meta("x-anycable-meta-track").unwrap_or_default().to_owned();
            protos::CommandResponse {
                transmissions: strings(&["confirmed"]),
                streams: vec![stream],
                status: protos::Status::Success as i32,
                presence: Some(protos::PresenceResponse {
                    r#type: "join".into(),
                    id: "42".into(),
                    info: r#"{"name":"Dexter"}"#.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into()
        }));

        let ctx = Ctx::with_incoming_metadata(&[("track", "easy-way-out")]);
        let res = service.command(
            &ctx,
            &new_command_message(
                &new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]),
                "subscribe",
                "chat_1",
                "test-session",
                "{}",
            ),
        );

        let calls = ts.calls();
        let r = calls.first().expect("the server received the request");
        assert_eq!("/command", r.http.as_ref().unwrap().path);
        let req = r.request.command();
        assert_eq!("chat_1", req.identifier);
        assert_eq!("subscribe", req.command);

        let res = res.expect("no error");

        assert_eq!(protos::Status::Success as i32, res.status);
        assert_eq!(strings(&["confirmed"]), res.transmissions);
        assert_eq!(strings(&["easy-way-out"]), res.streams);
        let presence = res.presence.expect("presence");
        assert_eq!("join", presence.r#type);
        assert_eq!("42", presence.id);
        assert_eq!(r#"{"name":"Dexter"}"#, presence.info);
    }
}

mod test_http_service_authentication {
    use super::*;

    fn server() -> FakeRpc {
        let ts = FakeRpc::start();
        ts.on(Method::Connect)
            .matching(|r| r.meta("authorization") == Some("Bearer secretto"))
            .reply(protos::ConnectionResponse { status: protos::Status::Success as i32, ..Default::default() });
        ts.on(Method::Connect).reply(Reply::http(401, ""));
        ts
    }

    fn request() -> protos::ConnectionRequest {
        new_connect_message(&new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]))
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceAuthentication/Authentication_SUCCESS
    #[test]
    fn authentication_success() {
        let ts = server();
        let mut conf = new_config();
        conf.secret = "secretto".into();
        conf.host = ts.http_url();
        let service = HttpService::new(&conf).expect("service");

        let res = service.connect(&Ctx::background(), &request()).expect("no error");

        assert_eq!(protos::Status::Success as i32, res.status);
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceAuthentication/Authentication_FAILURE
    #[test]
    fn authentication_failure() {
        let ts = server();
        let mut new_conf = new_config();
        new_conf.secret = "not-a-secret".into();
        new_conf.host = ts.http_url();

        let service = HttpService::new(&new_conf).expect("service");

        let err = service.connect(&Ctx::background(), &request()).expect_err("an error");

        assert_eq!(Code::Unauthenticated, status_code(&err));
    }
}

/// upstream: rpc/http_test.go#TestHTTPServiceRequestTimeout
#[test]
fn test_http_service_request_timeout() {
    let ts = FakeRpc::start();
    // Upstream's handler spins until the client call returned ("completed").
    let completed = ts.gate();
    ts.on(Method::Connect)
        .held_by(&completed)
        .reply(protos::ConnectionResponse { status: protos::Status::Success as i32, ..Default::default() });

    let mut conf = new_config();
    conf.host = ts.http_url();

    let service = HttpService::new(&conf).expect("service");
    let request = new_connect_message(&new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]));

    let ctx = Ctx::with_timeout(std::time::Duration::from_millis(50));

    let res = service.connect(&ctx, &request);
    completed.open();

    let err = res.expect_err("an error");

    assert_eq!(Code::DeadlineExceeded, status_code(&err));
}

mod test_http_service_bad_requests {
    use super::*;

    fn request() -> protos::ConnectionRequest {
        new_connect_message(&new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]))
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceBadRequests/unknown_url
    #[test]
    fn unknown_url() {
        let mut new_conf = new_config();
        new_conf.host = super::unknown_url();

        let service = HttpService::new(&new_conf).expect("service");

        let err = service.connect(&Ctx::background(), &request()).expect_err("an error");

        assert_eq!(Code::Unavailable, status_code(&err));
    }

    /// upstream: rpc/http_test.go#TestHTTPServiceBadRequests/bad_request
    #[test]
    fn bad_request() {
        let ts = FakeRpc::start();
        ts.on(Method::Connect).reply(Reply::http(422, ""));
        let mut conf = new_config();
        conf.host = ts.http_url();
        conf.request_timeout = 50;

        let service = HttpService::new(&conf).expect("service");

        let err = service.connect(&Ctx::background(), &request()).expect_err("an error");

        assert_eq!(Code::InvalidArgument, status_code(&err));
    }
}

/// upstream: rpc/http_test.go#TestHTTPClientHelper_READY
#[test]
fn test_http_client_helper_ready() {
    let mut conf = new_config();
    conf.host = unknown_url();

    let service = HttpService::new(&conf).expect("service");

    assert_eq!(Ok(()), service.helper_ready());

    // by default, we open a breaker if there are >20% of errors
    let request = new_connect_message(&new_session_env("ws://anycable.io/cable", &[("cookie", "foo=bar")]));

    for _ in 0..20 {
        assert!(service.connect(&Ctx::background(), &request).is_err());
    }

    // Shouldn't be ready
    assert!(service.helper_ready().is_err());
}

/// upstream: rpc/http_test.go#TestHTTPServiceReusesConnections
///
/// Upstream issues `GET`s through the unexported `service.client`; here the
/// same client is exercised through `Connect`. Upstream overlaps the calls by
/// sleeping 10ms in the handler; here each round's calls are held until all of
/// them are in flight at once.
#[test]
fn test_http_service_reuses_connections() {
    let ts = FakeRpc::start();
    let in_flight = ts.gate();
    ts.on(Method::Connect).held_by(&in_flight).reply(protos::ConnectionResponse::default());

    let mut conf = new_config();
    conf.host = ts.http_url();
    conf.concurrency = 8;
    let service = HttpService::new(&conf).expect("service");
    let request = protos::ConnectionRequest::default();
    let concurrency = conf.concurrency as usize;

    for _round in 0..5 {
        let calls: Vec<_> = (0..concurrency).map(|_| service.spawn_connect(&Ctx::background(), &request)).collect();
        in_flight.await_held(concurrency, DEADLINE);
        in_flight.release(concurrency);
        for c in calls {
            c.wait().expect("no error");
        }
    }

    assert!(ts.http_connections() <= conf.concurrency as u64, "opened {} connections", ts.http_connections());
}

mod http_rpc_wire_format {
    use super::*;
    use cabby_testkit::reference::metrics::Metrics;
    use serde_json::json;

    fn meta_headers(call: &cabby_testkit::fake_rpc::Call) -> Vec<(String, String)> {
        let mut m: Vec<_> = call.metadata.iter().filter(|(k, _)| k.starts_with("x-anycable-meta-")).cloned().collect();
        m.sort();
        m
    }

    /// regression-for: rpc/http_test.go#TestHTTPServiceRPC/Connect
    /// The HTTP RPC wire format of controller calls (impl "http"): request
    /// path under the base URL, method, headers and the exact JSON bodies, plus
    /// decoding of minimal hand-written JSON replies.
    #[test]
    fn controller_requests_and_replies() {
        let ts = FakeRpc::start();
        ts.on(Method::Connect)
            .reply(Reply::http(200, r#"{"status":1,"identifiers":"user=1","transmissions":["welcome"]}"#));
        ts.on(Method::Command).reply(Reply::http(200, r#"{"status":1,"streams":["s1"],"stop_streams":true}"#));
        ts.on(Method::Disconnect).reply(Reply::http(200, r#"{"status":1}"#));

        let mut config = new_config();
        config.host = format!("{}/_anycable", ts.http_url());
        config.secret = "s3cret".into();
        let controller = Controller::new(&Metrics::new(0), &config, Dial::Config).expect("controller");
        controller.start().expect("start");

        let env = SessionEnv {
            url: "ws://example.com/cable".into(),
            headers: Some(map(&[("cookie", "a=b")])),
            connection_state: Some(map(&[("_s_", "x")])),
            ..Default::default()
        };

        let connected = controller.authenticate(&Ctx::background(), "sid-1", &env);
        assert!(connected.error.is_none(), "{:?}", connected.error);
        let connected = connected.result.expect("result");
        assert_eq!("user=1", connected.identifier);
        assert_eq!(strings(&["welcome"]), connected.transmissions);

        let subscribed = controller.subscribe(&Ctx::background(), "sid-1", &env, "user=1", "chat");
        assert!(subscribed.error.is_none(), "{:?}", subscribed.error);
        let subscribed = subscribed.result.expect("result");
        assert_eq!(strings(&["s1"]), subscribed.streams);
        assert!(subscribed.stop_all_streams);

        assert_eq!(Ok(()), controller.disconnect(&Ctx::background(), "sid-1", &env, "user=1", &["chat"]));

        let calls = ts.calls();
        assert_eq!(3, calls.len(), "{calls:?}");
        let expected_meta = vec![
            ("x-anycable-meta-protov".to_owned(), "v1".to_owned()),
            ("x-anycable-meta-sid".to_owned(), "sid-1".to_owned()),
        ];
        let env_json = json!({"url": "ws://example.com/cable", "headers": {"cookie": "a=b"}, "cstate": {"_s_": "x"}});
        let bodies = [
            json!({"env": env_json}),
            json!({"command": "subscribe", "identifier": "chat", "connection_identifiers": "user=1", "env": env_json}),
            json!({"identifiers": "user=1", "subscriptions": ["chat"], "env": env_json}),
        ];
        let paths = ["/_anycable/connect", "/_anycable/command", "/_anycable/disconnect"];
        for ((call, path), body) in calls.iter().zip(paths).zip(bodies) {
            let http = call.http.as_ref().expect("an HTTP call");
            assert_eq!("POST", http.method);
            assert_eq!(path, http.path);
            assert_eq!(Some("application/json"), call.meta("content-type"));
            assert_eq!(Some("Bearer s3cret"), call.meta("authorization"));
            assert_eq!(expected_meta, meta_headers(call));
            let sent: serde_json::Value = serde_json::from_slice(&http.body).expect("JSON body");
            assert_eq!(body, sent, "body of {path}");
        }
        let _ = controller.shutdown();
    }

    /// regression-for: rpc/http_test.go#TestHTTPServiceRPC/Connect
    /// Go's JSON omits zero values, and status ERROR is 0: a reply without
    /// `status` decodes as ERROR; an empty request encodes as `{}`.
    #[test]
    fn missing_status_means_error() {
        let ts = FakeRpc::start();
        ts.on(Method::Connect).reply(Reply::http(200, r#"{"error_msg":"boom"}"#));
        let mut conf = new_config();
        conf.host = ts.http_url();
        let service = HttpService::new(&conf).expect("service");

        let res = service.connect(&Ctx::background(), &protos::ConnectionRequest::default()).expect("no error");

        assert_eq!(protos::Status::Error as i32, res.status);
        assert_eq!("boom", res.error_msg);
        let body: serde_json::Value = serde_json::from_slice(&ts.calls()[0].http.as_ref().unwrap().body).unwrap();
        assert_eq!(json!({}), body);
    }
}
