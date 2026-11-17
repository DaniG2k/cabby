//! upstream file: sse/handler_test.go
//!
//! Upstream shares one node and `mocks.Controller` across subtests (expectations are
//! keyed by session ID); each Rust test builds its own. Requests carry a context
//! with a 2s timeout: every read below is bounded by the same 2s deadline, and the
//! request context is cancelled when the test ends (`defer cancel()`).
//!
//! Streaming subtests read events from a `streamingWriter`, which embeds a *copy*
//! of the `httptest.ResponseRecorder` `w`. Their final `require.Equal(200, w.Code)`
//! therefore inspects the untouched original recorder and always passes; it is kept
//! as written and `streaming_response_status_is_ok` asserts the intended status.
//!
//! Requests rejected before authentication are served through the Go bridge; the
//! rest need the in-process node and RPC mock (not observable through the bridge,
//! covered externally by tests/conformance/sse.rs).

use std::sync::Arc;
use std::time::{Duration, Instant};

use cabby_core::common::{CommandResult, ConnectResult, Status, StreamMessage};
use cabby_server::sse::Config;
use cabby_testkit::reference::sse::{Handler, Node, Recorder, TestRequest, build_node, sse_handler};
use tokio_util::sync::CancellationToken;

use crate::support::MockController;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

struct Fixture {
    node: Node,
    controller: Arc<MockController>,
    conf: Config,
    handler: Handler,
}

/// `TestSSEHandler` setup: `buildNode()`, a disconnect queue with rate 1, the node
/// started, and the handler with a never-cancelled shutdown context.
fn setup() -> Fixture {
    let controller = Arc::new(MockController::default());
    let node = build_node(controller.clone());
    let conf = Config::new();

    node.start();

    let handler = sse_handler(&node, &CancellationToken::new(), &conf);

    Fixture { node, controller, conf, handler }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // defer appNode.Shutdown(...)
        if !std::thread::panicking() {
            self.node.shutdown();
        }
    }
}

/// A context deadline shared by several reads.
struct Deadline(Instant);

impl Deadline {
    fn after(d: Duration) -> Self {
        Self(Instant::now() + d)
    }

    fn left(&self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }
}

fn welcome(identifier: &str) -> ConnectResult {
    ConnectResult {
        identifier: identifier.into(),
        status: Status::Success,
        transmissions: vec![r#"{"type":"welcome"}"#.into()],
        ..Default::default()
    }
}

fn confirmed(transmission: &str, stream: &str) -> CommandResult {
    CommandResult {
        status: Status::Success,
        transmissions: vec![transmission.into()],
        streams: vec![stream.into()],
        ..Default::default()
    }
}

fn broadcast(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

mod test_sse_handler {
    use super::*;

    /// upstream: sse/handler_test.go#TestSSEHandler/headers
    #[test]
    fn headers() {
        let f = setup();
        let req = TestRequest::new("GET", "/");

        let w = f.handler.serve(&req);

        assert_eq!(Some("text/event-stream; charset=utf-8"), w.header("Content-Type"));
        assert_eq!(Some("private, no-cache, no-store, must-revalidate, max-age=0"), w.header("Cache-Control"));
        assert_eq!(Some("no-cache"), w.header("Pragma"));
        assert_eq!(Some("keep-alive"), w.header("Connection"));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/headers_+_CORS
    #[test]
    fn headers_cors() {
        let f = setup();
        let req = TestRequest::new("GET", "/").header("Origin", "http://www.example.com");

        let mut cors_conf = Config::new();
        cors_conf.allowed_origins = "*.example.com".into();

        let cors_handler = sse_handler(&f.node, &CancellationToken::new(), &cors_conf);

        let w = cors_handler.serve(&req);

        assert_eq!(Some("http://www.example.com"), w.header("Access-Control-Allow-Origin"));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/OPTIONS
    #[test]
    fn options() {
        let f = setup();
        let req = TestRequest::new("OPTIONS", "/");

        let w = f.handler.serve(&req);

        assert_eq!(200, w.status);
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/non-GET/OPTIONS/POST
    #[test]
    fn non_get_options_post() {
        let f = setup();
        let req = TestRequest::new("PUT", "/");

        let w = f.handler.serve(&req);

        assert_eq!(405, w.status);
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/when_authentication_fails
    #[test]
    fn when_authentication_fails() {
        let f = setup();

        f.controller.on_authenticate(
            "sid-fail",
            ConnectResult {
                status: Status::Failure,
                transmissions: vec![r#"{"type":"disconnect"}"#.into()],
                ..Default::default()
            },
        );

        let req = TestRequest::new("GET", "/?channel=room_1").header("X-Request-ID", "sid-fail");
        let ctx = CancellationToken::new();

        let w = f.handler.serve_app(&req, &ctx);

        assert_eq!(401, w.status);
        assert!(w.text().is_empty());

        ctx.cancel();
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_identifier
    #[test]
    fn get_request_with_identifier() {
        let f = setup();

        f.controller.on_authenticate("sid-gut", welcome("se2023"));
        f.controller.on_subscribe(
            "sid-gut",
            "se2023",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );

        let req = TestRequest::new("GET", "/?identifier=chat_1").header("X-Request-ID", "sid-gut");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        f.node.broadcast(&broadcast("messages_1", r#"{"content":"hello"}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"content":"hello"}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_turbo_signed_stream_name
    #[test]
    fn get_request_with_turbo_signed_stream_name() {
        let f = setup();

        f.controller.on_authenticate("sid-turbo", welcome("se2023"));

        let turbo_identifier = r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"chat_1"}"#;

        f.controller.on_subscribe(
            "sid-turbo",
            "se2023",
            turbo_identifier,
            confirmed(r#"{"type":"confirm","identifier":"turbo_1"}"#, "chat_1"),
        );

        let req = TestRequest::new("GET", "/?turbo_signed_stream_name=chat_1").header("X-Request-ID", "sid-turbo");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"turbo_1"}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_stream
    #[test]
    fn get_request_with_stream() {
        let f = setup();

        f.controller.on_authenticate("sid-public-stream", welcome("se2024"));

        let identifier = r#"{"channel":"$pubsub","stream_name":"chat_1"}"#;

        f.controller.on_subscribe(
            "sid-public-stream",
            "se2024",
            identifier,
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "chat_1"),
        );

        let req = TestRequest::new("GET", "/?stream=chat_1").header("X-Request-ID", "sid-public-stream");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_signed_stream
    #[test]
    fn get_request_with_signed_stream() {
        let f = setup();

        f.controller.on_authenticate("sid-signed-stream", welcome("se2024"));

        let identifier = r#"{"channel":"$pubsub","signed_stream_name":"secretto"}"#;

        f.controller.on_subscribe(
            "sid-signed-stream",
            "se2024",
            identifier,
            confirmed(r#"{"type":"confirm","identifier":"secret_chat_1"}"#, "chat_1"),
        );

        let req = TestRequest::new("GET", "/?signed_stream=secretto").header("X-Request-ID", "sid-signed-stream");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"secret_chat_1"}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_channel_+_rejected
    #[test]
    fn get_request_with_channel_rejected() {
        let f = setup();

        f.controller.on_authenticate("sid-reject", welcome("se2034"));

        f.controller.on_subscribe(
            "sid-reject",
            "se2034",
            r#"{"channel":"room_1"}"#,
            CommandResult {
                status: Status::Failure,
                transmissions: vec![r#"{"type":"reject","identifier":"room_1"}"#.into()],
                ..Default::default()
            },
        );

        let req = TestRequest::new("GET", "/?channel=room_1").header("X-Request-ID", "sid-reject");
        let ctx = CancellationToken::new();

        let w = f.handler.serve_app(&req, &ctx);

        assert_eq!(400, w.status);
        assert!(w.text().is_empty());

        f.controller.assert_called_subscribe("sid-reject", "se2034", r#"{"channel":"room_1"}"#);

        ctx.cancel();
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_without_channel_or_identifier
    #[test]
    fn get_request_without_channel_or_identifier() {
        let f = setup();
        let req = TestRequest::new("GET", "/");

        let w = f.handler.serve(&req);

        assert_eq!(400, w.status);
        assert!(w.text().is_empty());
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t
    #[test]
    fn get_request_with_raw_t() {
        let f = setup();

        f.controller.on_authenticate("sid-gut-raw", welcome("se2023"));
        f.controller.on_subscribe(
            "sid-gut-raw",
            "se2023",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );

        let req = TestRequest::new("GET", "/?identifier=chat_1&raw=t").header("X-Request-ID", "sid-gut-raw");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        // Upstream sleeps 1s "to make sure that the session is subscribed". The handler
        // subscribes before it writes the status line, so wait for that instead.
        assert_eq!(Some(200), sw.wait_status(deadline.left()), "response not started");

        f.node.broadcast(&broadcast("messages_1", r#"{"content":"hello"}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"content":"hello"}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t_sends_comment_pings
    #[test]
    fn get_request_with_raw_t_sends_comment_pings() {
        let f = setup();

        f.controller.on_authenticate("sid-gut-raw-ping", welcome("se2023"));
        f.controller.on_subscribe(
            "sid-gut-raw-ping",
            "se2023",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );

        let mut ping_conf = Config::new();
        ping_conf.ping_interval = 1;

        let ping_handler = sse_handler(&f.node, &CancellationToken::new(), &ping_conf);

        let req = TestRequest::new("GET", "/?identifier=chat_1&raw=t").header("X-Request-ID", "sid-gut-raw-ping");

        // The node's ping interval is 3s, so the first ping would arrive in 1.5-4.5s if
        // the custom interval wasn't applied.
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = ping_handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(": ping", msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_comment_pings
    #[test]
    fn get_request_with_comment_pings() {
        let f = setup();

        f.controller.on_authenticate("sid-gut-comment-ping", welcome("se2023"));
        f.controller.on_subscribe(
            "sid-gut-comment-ping",
            "se2023",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );

        let mut ping_conf = Config::new();
        ping_conf.comment_pings = true;
        ping_conf.ping_interval = 1;

        let ping_handler = sse_handler(&f.node, &CancellationToken::new(), &ping_conf);

        let req = TestRequest::new("GET", "/?identifier=chat_1").header("X-Request-ID", "sid-gut-comment-ping");

        // The node's ping interval is 3s, so the first ping would arrive in 1.5-4.5s if
        // the custom interval wasn't applied.
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = ping_handler.serve_streaming(&req, &ctx);

        // Protocol events are still sent
        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(": ping", msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/POST_request_without_commands_+_server_shutdown
    #[test]
    fn post_request_without_commands_server_shutdown() {
        let f = setup();

        f.controller.on_authenticate("sid-post-no-op", welcome("se2023-09-06"));

        let req = TestRequest::new("POST", "/").header("X-Request-ID", "sid-post-no-op");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();

        let shutdown_ctx = CancellationToken::new();

        let shutdown_handler = sse_handler(&f.node, &shutdown_ctx, &f.conf);

        let sw = shutdown_handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        shutdown_ctx.cancel();

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(
            "event: disconnect\n".to_owned()
                + r#"data: {"type":"disconnect","reason":"server_restart","reconnect":true}"#,
            msg
        );

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/POST_request_with_multiple_subscriptions
    #[test]
    fn post_request_with_multiple_subscriptions() {
        let f = setup();

        f.controller.on_authenticate("sid-post", welcome("se2023-09-06"));
        f.controller.on_subscribe(
            "sid-post",
            "se2023-09-06",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );
        f.controller.on_subscribe(
            "sid-post",
            "se2023-09-06",
            "presence_1",
            confirmed(r#"{"type":"confirm","identifier":"presence_1"}"#, "presence_1"),
        );

        let req = TestRequest::new("POST", "/").header("X-Request-ID", "sid-post").body(
            "{\"command\":\"subscribe\",\"identifier\":\"chat_1\"}\n{\"command\":\"subscribe\",\"identifier\":\"presence_1\"}",
        );
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let w = Recorder::new();
        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"presence_1"}"#, msg);

        f.node.broadcast(&broadcast("messages_1", r#"{"content":"hello"}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"identifier":"chat_1","message":{"content":"hello"}}"#, msg);

        f.node.broadcast(&broadcast("presence_1", r#"{"type":"join","user_id":1}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"identifier":"presence_1","message":{"type":"join","user_id":1}}"#, msg);

        assert_eq!(200, w.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_identifier
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_turbo_signed_stream_name
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_stream
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_signed_stream
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t_sends_comment_pings
    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_comment_pings
    /// regression-for: sse/handler_test.go#TestSSEHandler/POST_request_without_commands_+_server_shutdown
    /// regression-for: sse/handler_test.go#TestSSEHandler/POST_request_with_multiple_subscriptions
    /// The upstream `w.Code` checks read a recorder the handler never writes to; this
    /// asserts the status the streaming response actually carries.
    #[test]
    fn streaming_response_status_is_ok() {
        let f = setup();

        f.controller.on_authenticate("sid-status", welcome("se2023"));
        f.controller.on_subscribe(
            "sid-status",
            "se2023",
            "chat_1",
            confirmed(r#"{"type":"confirm","identifier":"chat_1"}"#, "messages_1"),
        );

        let req = TestRequest::new("GET", "/?identifier=chat_1").header("X-Request-ID", "sid-status");
        let deadline = Deadline::after(REQUEST_TIMEOUT);
        let ctx = CancellationToken::new();

        let sw = f.handler.serve_streaming(&req, &ctx);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);
        assert_eq!(200, sw.code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }
}
