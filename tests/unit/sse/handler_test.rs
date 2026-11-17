//! upstream file: sse/handler_test.go
//!
//! Upstream shares one node and `mocks.Controller` across subtests (expectations are
//! keyed by session ID); each Rust test builds its own through the facade, which
//! runs the upstream harness in the bridge against Go. Requests carry a context
//! with a 2s timeout (`context.WithTimeout`): every read is bounded by the same 2s
//! deadline and the request context is cancelled when it expires or the test ends.
//!
//! Streaming subtests read events from a `streamingWriter`, which embeds a *copy*
//! of the `httptest.ResponseRecorder` `w`. Their final `require.Equal(200, w.Code)`
//! therefore inspects the untouched original recorder and always passes; it is kept
//! as written (`original_code`), and `streaming_response_status_is_ok` asserts the
//! intended status for one of them.
//!
//! `mocks.Controller` calls without an expectation are forbidden: they are recorded
//! and the fixture asserts there were none.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cabby_core::common::{CommandResult, ConnectResult, Status, StreamMessage};
use cabby_server::sse::Config;
use cabby_testkit::reference::sse::{Handler, MockController, Node, TestRequest, build_node, new_config, sse_handler};

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
    let (node, controller) = build_node();
    let conf = new_config();

    node.start();

    let handler = sse_handler(&node, &conf);

    Fixture { node, controller, conf, handler }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            // testify would have failed the test on any call without expectation.
            assert_eq!(Vec::<String>::new(), self.controller.unexpected_calls(), "forbidden controller calls");
            // defer appNode.Shutdown(...)
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

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

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

        let mut cors_conf = new_config();
        cors_conf.allowed_origins = "*.example.com".into();

        let cors_handler = sse_handler(&f.node, &cors_conf);

        let w = cors_handler.serve(&req, REQUEST_TIMEOUT);

        assert_eq!(Some("http://www.example.com"), w.header("Access-Control-Allow-Origin"));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/OPTIONS
    #[test]
    fn options() {
        let f = setup();
        let req = TestRequest::new("OPTIONS", "/");

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

        assert_eq!(200, w.status);
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/non-GET/OPTIONS/POST
    #[test]
    fn non_get_options_post() {
        let f = setup();
        let req = TestRequest::new("PUT", "/");

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

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

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

        assert_eq!(401, w.status);
        assert!(w.text().is_empty());

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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        f.node.broadcast(&broadcast("messages_1", r#"{"content":"hello"}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"content":"hello"}"#, msg);

        assert_eq!(200, sw.original_code());

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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"turbo_1"}"#, msg);

        assert_eq!(200, sw.original_code());

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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        assert_eq!(200, sw.original_code());

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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"secret_chat_1"}"#, msg);

        assert_eq!(200, sw.original_code());

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

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

        assert_eq!(400, w.status);
        assert!(w.text().is_empty());

        f.controller.assert_called_subscribe("sid-reject", "se2034", r#"{"channel":"room_1"}"#);

        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_without_channel_or_identifier
    #[test]
    fn get_request_without_channel_or_identifier() {
        let f = setup();
        let req = TestRequest::new("GET", "/");

        let w = f.handler.serve(&req, REQUEST_TIMEOUT);

        assert_eq!(400, w.status);
        assert!(w.text().is_empty());
    }

    /// upstream: sse/handler_test.go#TestSSEHandler/GET_request_with_?raw=t
    /// note: upstream's 1s sleep before broadcasting is replaced by waiting for the status line, which the handler writes after subscribing.
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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        // Upstream sleeps 1s "to make sure that the session is subscribed". The handler
        // subscribes before it writes the status line, so wait for that instead.
        assert_eq!(Some(200), sw.wait_status(deadline.left()), "response not started");

        f.node.broadcast(&broadcast("messages_1", r#"{"content":"hello"}"#));

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(r#"data: {"content":"hello"}"#, msg);

        assert_eq!(200, sw.original_code());

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

        let mut ping_conf = new_config();
        ping_conf.ping_interval = 1;

        let ping_handler = sse_handler(&f.node, &ping_conf);

        let req = TestRequest::new("GET", "/?identifier=chat_1&raw=t").header("X-Request-ID", "sid-gut-raw-ping");

        // The node's ping interval is 3s, so the first ping would arrive in 1.5-4.5s if
        // the custom interval wasn't applied.
        let deadline = Deadline::after(REQUEST_TIMEOUT);

        let sw = ping_handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(": ping", msg);

        assert_eq!(200, sw.original_code());

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

        let mut ping_conf = new_config();
        ping_conf.comment_pings = true;
        ping_conf.ping_interval = 1;

        let ping_handler = sse_handler(&f.node, &ping_conf);

        let req = TestRequest::new("GET", "/?identifier=chat_1").header("X-Request-ID", "sid-gut-comment-ping");

        // The node's ping interval is 3s, so the first ping would arrive in 1.5-4.5s if
        // the custom interval wasn't applied.
        let deadline = Deadline::after(REQUEST_TIMEOUT);

        let sw = ping_handler.serve_streaming(&req, REQUEST_TIMEOUT);

        // Protocol events are still sent
        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: confirm\n".to_owned() + r#"data: {"type":"confirm","identifier":"chat_1"}"#, msg);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(": ping", msg);

        assert_eq!(200, sw.original_code());

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

        // shutdownCtx, shutdownFn := context.WithCancel(...): the handler's own context.
        let shutdown_handler = sse_handler(&f.node, &f.conf);

        let sw = shutdown_handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        shutdown_handler.shutdown();

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!(
            "event: disconnect\n".to_owned()
                + r#"data: {"type":"disconnect","reason":"server_restart","reconnect":true}"#,
            msg
        );

        assert_eq!(200, sw.original_code());

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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

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

        assert_eq!(200, sw.original_code());

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        f.node.assert_no_sessions(Duration::from_secs(2));
    }

    /// regression-for: sse/handler_test.go#TestSSEHandler/GET_request_with_identifier
    /// note: upstream's `w.Code` check reads a recorder the handler never writes to (the streaming writer holds a copy); this asserts the status the streaming response carries. The other streaming subtests share the weak check.
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

        let sw = f.handler.serve_streaming(&req, REQUEST_TIMEOUT);

        let msg = sw.read_event(deadline.left()).expect("no error");
        assert_eq!("event: welcome\n".to_owned() + r#"data: {"type":"welcome"}"#, msg);

        assert!(sw.finish(REQUEST_TIMEOUT), "handler did not return after the request was cancelled");
        assert_eq!(200, sw.code());
        f.node.assert_no_sessions(Duration::from_secs(2));
    }
}
