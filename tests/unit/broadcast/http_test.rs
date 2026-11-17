//! upstream file: broadcast/http_test.go
//!
//! Upstream subtests share the broadcasters started in `TestHttpHandler`; here
//! each subtest starts its own. The mock handler expectation
//! `handler.On("HandleBroadcast", payload).Return(nil)` (a call with any other
//! argument panics) is checked on the recorded calls.

use cabby_testkit::reference::broadcast::{
    Broadcaster, MockHandler, http_config_decode_toml, http_config_to_toml, new_http_config,
};
use cabby_testkit::{Backend, backend};

/// `json.Marshal(map[string]string{"stream": "any_test", "data": "123_test"})`
/// (Go sorts map keys).
const PAYLOAD: &str = "{\"data\":\"123_test\",\"stream\":\"any_test\"}";
const BROADCAST_KEY: &str = "42923a28b760e667fc92f7c6123bb07a282822b329dd2ef48e7aee7830d98485";

struct Setup {
    handler: MockHandler,
    broadcaster: Broadcaster,
    protected_broadcaster: Broadcaster,
}

impl Setup {
    fn new() -> Self {
        let handler = MockHandler::new();
        let config = new_http_config();

        let mut secret_config = new_http_config();
        secret_config.secret_base = "qwerty".into();

        let broadcaster = Broadcaster::http(&handler, &config);
        let protected_broadcaster = Broadcaster::http(&handler, &secret_config);

        broadcaster.start().expect("broadcaster.Start");
        protected_broadcaster.start().expect("protectedBroadcaster.Start");

        Setup { handler, broadcaster, protected_broadcaster }
    }

    /// Every call must match the mock expectation `HandleBroadcast(payload)`.
    fn assert_expected_calls(&self) {
        for call in self.handler.calls() {
            assert_eq!(
                ("HandleBroadcast", PAYLOAD),
                (call.kind.as_str(), call.payload.as_str()),
                "unexpected mock call"
            );
        }
    }
}

impl Drop for Setup {
    fn drop(&mut self) {
        if backend() == Backend::Go || !std::thread::panicking() {
            let _ = self.broadcaster.shutdown();
            let _ = self.protected_broadcaster.shutdown();
        }
    }
}

mod test_http_handler {
    use super::*;

    /// upstream: broadcast/http_test.go#TestHttpHandler/Handles_broadcasts
    /// upstream: broadcast/http_test.go#TestHttpHandler
    #[test]
    fn handles_broadcasts() {
        let s = Setup::new();

        let code = s.broadcaster.serve("POST", &[], PAYLOAD);

        assert_eq!(201, code);
        s.assert_expected_calls();
    }

    /// upstream: broadcast/http_test.go#TestHttpHandler/Rejects_too_large_bodies
    /// upstream: broadcast/http_test.go#TestHttpHandler
    #[test]
    fn rejects_too_large_bodies() {
        let s = Setup::new();

        let mut limited_config = new_http_config();
        limited_config.max_body_size = 8;
        let limited = Broadcaster::http(&s.handler, &limited_config);
        limited.prepare().expect("Prepare");

        let code = limited.serve("POST", &[], PAYLOAD);

        assert_eq!(413, code);
        s.assert_expected_calls();
    }

    /// upstream: broadcast/http_test.go#TestHttpHandler/Rejects_non-POST_requests
    /// upstream: broadcast/http_test.go#TestHttpHandler
    #[test]
    fn rejects_non_post_requests() {
        let s = Setup::new();

        let code = s.broadcaster.serve("GET", &[], PAYLOAD);

        assert_eq!(422, code);
        s.assert_expected_calls();
    }

    /// upstream: broadcast/http_test.go#TestHttpHandler/Rejects_when_authorization_header_is_missing
    /// upstream: broadcast/http_test.go#TestHttpHandler
    #[test]
    fn rejects_when_authorization_header_is_missing() {
        let s = Setup::new();

        let code = s.protected_broadcaster.serve("POST", &[], PAYLOAD);

        assert_eq!(401, code);
        s.assert_expected_calls();
    }

    /// upstream: broadcast/http_test.go#TestHttpHandler/Accepts_when_authorization_header_is_valid
    /// upstream: broadcast/http_test.go#TestHttpHandler
    #[test]
    fn accepts_when_authorization_header_is_valid() {
        let s = Setup::new();

        let auth = format!("Bearer {BROADCAST_KEY}");
        let code = s.protected_broadcaster.serve("POST", &[("Authorization", &auth)], PAYLOAD);

        assert_eq!(201, code);
        s.assert_expected_calls();
    }
}

/// upstream: broadcast/http_test.go#TestHTTPConfig__ToToml
#[test]
fn test_http_config_to_toml() {
    let mut conf = new_http_config();
    conf.port = 8080;
    conf.path = "/broadcast".into();
    conf.secret = String::new();
    conf.add_cors_headers = true;

    let toml_str = http_config_to_toml(&conf);

    for needle in ["port = 8080", "path = \"/broadcast\"", "# secret = \"\"", "cors_headers = true"] {
        assert!(toml_str.contains(needle), "expected {needle:?} in:\n{toml_str}");
    }

    // Round-trip test
    let conf2 = http_config_decode_toml(new_http_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}
