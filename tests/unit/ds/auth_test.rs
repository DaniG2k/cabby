//! upstream file: ds/auth_test.go
//!
//! The subtests share a server but only read from it; each Rust test builds the
//! same setup.

use cabby_testkit::ds_client::query_escape;
use cabby_testkit::reference::ds::{Handler, Harness, Request, Server, StreamsKind, new_config};
use cabby_testkit::reference::utils::verifier_generate;
use serde_json::json;

const SECRET: &str = "s3Krit";

struct Fixture {
    // Dropped in this order: the server first (`defer ts.Close()`), the node last.
    server: Server,
    _handler: Handler,
    h: Harness,
    signed: String,
}

/// The test's setup: a memory broker with two streams, a streams controller with
/// a secret and public streams disabled, `SkipAuth` and a 1s poll interval.
fn setup() -> Fixture {
    let h = Harness::memory("test-epoch", 0, 0);

    h.broadcast("chat:2021", r#"{"msg":"hello"}"#).expect("broadcast");
    h.broadcast("admin/confidential", r#"{"classified":true}"#).expect("broadcast");

    let mut ds_config = new_config();
    ds_config.path = "/ds".into();
    ds_config.skip_auth = true;
    ds_config.poll_interval = 1;

    let handler =
        h.handler(&ds_config, StreamsKind::Signed { secret: SECRET.into(), public: false }).expect("DSHandler");
    let server = handler.listen();

    let signed = verifier_generate(SECRET, &json!("chat:2021")).expect("signed stream name");

    Fixture { server, _handler: handler, h, signed }
}

impl Fixture {
    fn request(&self, method: &str, path: &str, query: &str, header: &str) -> (u16, String) {
        let mut req = Request::new(method, &format!("{path}?{query}"));
        if !header.is_empty() {
            req = req.header("x-signed", header);
        }
        let resp = self.server.request(&req);
        (resp.status, resp.text())
    }

    fn signed_query(&self) -> String {
        format!("signed={}", query_escape(&self.signed))
    }

    fn assert_no_unexpected_calls(&self) {
        assert_eq!(self.h.unexpected_calls(), Vec::<String>::new(), "unexpected mock calls");
    }
}

/// `t.Run(tc.name+": rejects signed stream for a different path", ...)`.
fn rejects_signed_stream_for_a_different_path(method: &str, tc_query: &str) {
    let f = setup();
    let mut query = f.signed_query();
    if !tc_query.is_empty() {
        query = format!("{tc_query}&{query}");
    }

    let (status, body) = f.request(method, "/ds/admin/confidential", &query, "");

    assert_eq!(status, 401);
    assert!(!body.contains("classified"), "{body}");
    f.assert_no_unexpected_calls();
}

/// `t.Run(tc.name+": rejects signed stream header for a different path", ...)`.
fn rejects_signed_stream_header_for_a_different_path(method: &str, tc_query: &str) {
    let f = setup();

    let (status, body) = f.request(method, "/ds/admin/confidential", tc_query, &f.signed);

    assert_eq!(status, 401);
    assert!(!body.contains("classified"), "{body}");
    f.assert_no_unexpected_calls();
}

mod test_dshandler_signed_stream_authorization {
    use super::*;

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization
    #[test]
    fn setup_succeeds() {
        let f = setup();
        assert!(!f.signed.is_empty());
        assert!(f.server.url().starts_with("http://"));
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/HEAD:_rejects_signed_stream_for_a_different_path
    #[test]
    fn head_rejects_signed_stream_for_a_different_path() {
        rejects_signed_stream_for_a_different_path("HEAD", "");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/HEAD:_rejects_signed_stream_header_for_a_different_path
    #[test]
    fn head_rejects_signed_stream_header_for_a_different_path() {
        rejects_signed_stream_header_for_a_different_path("HEAD", "");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/catch-up:_rejects_signed_stream_for_a_different_path
    #[test]
    fn catch_up_rejects_signed_stream_for_a_different_path() {
        rejects_signed_stream_for_a_different_path("GET", "offset=-1");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/catch-up:_rejects_signed_stream_header_for_a_different_path
    #[test]
    fn catch_up_rejects_signed_stream_header_for_a_different_path() {
        rejects_signed_stream_header_for_a_different_path("GET", "offset=-1");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/long-poll:_rejects_signed_stream_for_a_different_path
    #[test]
    fn long_poll_rejects_signed_stream_for_a_different_path() {
        rejects_signed_stream_for_a_different_path("GET", "offset=-1&live=long-poll");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/long-poll:_rejects_signed_stream_header_for_a_different_path
    #[test]
    fn long_poll_rejects_signed_stream_header_for_a_different_path() {
        rejects_signed_stream_header_for_a_different_path("GET", "offset=-1&live=long-poll");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/SSE:_rejects_signed_stream_for_a_different_path
    #[test]
    fn sse_rejects_signed_stream_for_a_different_path() {
        rejects_signed_stream_for_a_different_path("GET", "offset=-1&live=sse");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/SSE:_rejects_signed_stream_header_for_a_different_path
    #[test]
    fn sse_rejects_signed_stream_header_for_a_different_path() {
        rejects_signed_stream_header_for_a_different_path("GET", "offset=-1&live=sse");
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/rejects_unsigned_stream_when_public_streams_are_disabled
    #[test]
    fn rejects_unsigned_stream_when_public_streams_are_disabled() {
        let f = setup();

        let (status, body) = f.request("GET", "/ds/admin/confidential", "offset=-1", "");

        assert_eq!(status, 401);
        assert!(!body.contains("classified"), "{body}");
        f.assert_no_unexpected_calls();
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/allows_signed_stream_matching_the_path
    #[test]
    fn allows_signed_stream_matching_the_path() {
        let f = setup();

        let (status, body) = f.request("GET", "/ds/chat:2021", &format!("offset=-1&{}", f.signed_query()), "");

        assert_eq!(status, 200);
        assert_eq!(body, r#"[{"msg":"hello"}]"#);
        f.assert_no_unexpected_calls();
    }

    /// upstream: ds/auth_test.go#TestDSHandler_SignedStreamAuthorization/allows_signed_stream_header_matching_the_path
    #[test]
    fn allows_signed_stream_header_matching_the_path() {
        let f = setup();

        let (status, body) = f.request("GET", "/ds/chat:2021", "offset=-1", &f.signed);

        assert_eq!(status, 200);
        assert_eq!(body, r#"[{"msg":"hello"}]"#);
        f.assert_no_unexpected_calls();
    }
}
