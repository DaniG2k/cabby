//! upstream file: api/publish_test.go
//!
//! `TestPublishHandler` itself asserts only that its shared payload marshals;
//! every subtest rebuilds that payload, so each also maps to the parent case.
//! `mocks.NewBroker(t)` / `mocks.NewHandler(t)` assert their expectations on
//! cleanup: that is the `assert_expectations` calls at the end of each test.

use std::sync::Arc;

use cabby_testkit::reference::api::*;

/// `json.Marshal(map[string]string{"stream": "test_stream", "data": "test_data"})`
/// (Go sorts map keys).
fn payload() -> Vec<u8> {
    let payload = serde_json::to_vec(&serde_json::json!({"stream": "test_stream", "data": "test_data"}));
    payload.expect("payload marshals")
}

struct Fixture {
    handler: Arc<MockHandler>,
    brk: Arc<MockBroker>,
}

impl Fixture {
    fn new() -> Self {
        Fixture { handler: MockHandler::new(), brk: MockBroker::new() }
    }

    fn start(&self, config: &mut Config) -> Server {
        let api = new_server(config, &self.brk, &self.handler).expect("new server");
        api.start().expect("start");
        api
    }

    fn finish(self, api: Server) {
        api.shutdown().expect("shutdown");
        self.brk.assert_expectations();
        self.handler.assert_expectations();
    }
}

fn origin(rr: &HttpResponse) -> Option<&str> {
    header(rr, "Access-Control-Allow-Origin")
}

mod test_publish_handler {
    use super::*;

    /// upstream: api/publish_test.go#TestPublishHandler/Handles_broadcasts_without_auth
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn handles_broadcasts_without_auth() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();

        let api = f.start(&mut config);

        f.handler.on_handle_broadcast(&payload, Ok(()));

        let rr = api.serve(&request("POST", "/api/publish", &[], &payload));

        assert_eq!(201, rr.status);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/Rejects_too_large_bodies
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn rejects_too_large_bodies() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();
        config.max_body_size = 8;

        let api = f.start(&mut config);

        let rr = api.serve(&request("POST", "/api/publish", &[], &payload));

        assert_eq!(413, rr.status);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/Rejects_non-POST_requests
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn rejects_non_post_requests() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();

        let api = f.start(&mut config);

        let rr = api.serve(&request("GET", "/api/publish", &[], &payload));

        assert_eq!(422, rr.status);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/Accepts_when_authorization_header_is_valid
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn accepts_when_authorization_header_is_valid() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();
        config.secret = "test-secret".into();

        let api = f.start(&mut config);

        f.handler.on_handle_broadcast(&payload, Ok(()));

        let rr = api.serve(&request("POST", "/api/publish", &[("Authorization", "Bearer test-secret")], &payload));

        assert_eq!(201, rr.status);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/Handles_CORS_preflight_requests
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn handles_cors_preflight_requests() {
        let _payload = payload();
        let f = Fixture::new();
        let mut config = new_config();
        config.add_cors_headers = true;

        let api = f.start(&mut config);

        let rr = api.serve(&request("OPTIONS", "/api/publish", &[], b""));

        assert_eq!(200, rr.status);
        assert!(origin(&rr).is_some_and(|o| !o.is_empty()), "headers: {:?}", rr.headers);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/Includes_CORS_headers_on_POST_response
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn includes_cors_headers_on_post_response() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();
        config.add_cors_headers = true;

        let api = f.start(&mut config);

        f.handler.on_handle_broadcast(&payload, Ok(()));

        let rr = api.serve(&request("POST", "/api/publish", &[], &payload));

        assert_eq!(201, rr.status);
        assert!(origin(&rr).is_some_and(|o| !o.is_empty()), "headers: {:?}", rr.headers);
        f.finish(api);
    }

    /// upstream: api/publish_test.go#TestPublishHandler/CORS_with_specific_hosts
    /// upstream: api/publish_test.go#TestPublishHandler
    #[test]
    fn cors_with_specific_hosts() {
        let payload = payload();
        let f = Fixture::new();
        let mut config = new_config();
        config.add_cors_headers = true;
        config.cors_hosts = "example.com,test.com".into();

        let api = f.start(&mut config);

        f.handler.on_handle_broadcast(&payload, Ok(()));

        let rr = api.serve(&request("POST", "/api/publish", &[("Origin", "http://example.com")], &payload));

        assert_eq!(201, rr.status);
        assert_eq!(Some("http://example.com"), origin(&rr));
        f.finish(api);
    }
}
