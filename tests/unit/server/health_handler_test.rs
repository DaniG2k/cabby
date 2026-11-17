//! upstream file: server/health_handler_test.go
//!
//! See also the external scenario `conformance::health`.

use cabby_server::server::HttpRequest;
use cabby_testkit::reference::server::health_handler;

/// upstream: server/health_handler_test.go#TestHealthHandler
#[test]
fn test_health_handler() {
    // http.NewRequest("GET", "/health", nil): no host or peer address.
    let req = HttpRequest { method: "GET".into(), target: "/health".into(), ..HttpRequest::default() };

    let rr = health_handler(&req);

    assert_eq!(200, rr.status);
}
