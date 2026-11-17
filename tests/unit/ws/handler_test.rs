//! upstream file: ws/handler_test.go
//!
//! Upstream builds an `httptest` request and sets its `Origin` header; here the
//! header value is passed directly (`None` when the request has no header).

use cabby_testkit::reference::ws::check_origin;

/// upstream: ws/handler_test.go#TestCheckOriginWithoutHeader
#[test]
fn test_check_origin_without_header() {
    let req = None;

    let mut allowed_origins = "";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "secure.origin";
    assert!(!check_origin(allowed_origins, req));
}

/// upstream: ws/handler_test.go#TestCheckOrigin
#[test]
fn test_check_origin() {
    let mut req = Some("http://my.localhost:8080");

    let mut allowed_origins = "";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "my.localhost:8080";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "MY.localhost:8080";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "localhost:8080";
    assert!(!check_origin(allowed_origins, req));

    allowed_origins = "*.localhost:8080";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "secure.origin,my.localhost:8080";
    assert!(check_origin(allowed_origins, req));

    allowed_origins = "secure.origin,*.localhost:8080";
    assert!(check_origin(allowed_origins, req));

    req = Some("http://MY.localhost:8080");
    assert!(check_origin(allowed_origins, req));
}

/// upstream: ws/handler_test.go#TestCheckOriginWildcards
#[test]
fn test_check_origin_wildcards() {
    let mut req = Some("https://evilexample.com");
    assert!(!check_origin("*example.com", req));
    assert!(!check_origin("*.example.com", req));
    assert!(check_origin("*", req));

    req = Some("https://example.com");
    assert!(check_origin("*example.com", req));
    assert!(!check_origin("*.example.com", req));

    req = Some("https://app.example.com");
    assert!(check_origin("*example.com", req));
    assert!(check_origin("*.example.com", req));
}

/// upstream: ws/handler_test.go#TestCheckOriginEmptyEntries
#[test]
fn test_check_origin_empty_entries() {
    let req = Some("https://example.com");

    assert!(check_origin("example.com,", req));
    assert!(!check_origin("other.com, ,", req));
}
