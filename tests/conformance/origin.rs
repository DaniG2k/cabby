//! WebSocket handshake origin policy (`--allowed_origins`), observed externally.
//! Allowed handshakes must switch protocols (101); rejected ones get 403, which is
//! what the Go reference answers (gorilla/websocket's origin rejection).

use cabby_testkit::sut::Sut;

fn status(sut: &Sut, origin: Option<&str>) -> u16 {
    let headers: Vec<(&str, &str)> = origin.map(|o| ("Origin", o)).into_iter().collect();
    sut.handshake_status("/cable", &headers)
}

/// external-for: ws/handler_test.go#TestCheckOriginWithoutHeader
#[test]
fn absent_origin_header() {
    let open = Sut::start(&["--noauth"]);
    assert_eq!(101, status(&open, None));

    let restricted = Sut::start(&["--noauth", "--allowed_origins=secure.origin"]);
    assert_eq!(403, status(&restricted, None));
}

/// external-for: ws/handler_test.go#TestCheckOrigin
#[test]
fn case_insensitive_hosts_and_lists() {
    let sut = Sut::start(&["--noauth", "--allowed_origins=secure.origin,*.localhost:8080"]);
    assert_eq!(101, status(&sut, Some("http://my.localhost:8080")));
    assert_eq!(101, status(&sut, Some("http://MY.localhost:8080")));
    assert_eq!(403, status(&sut, Some("http://localhost:8080")));
}

/// external-for: ws/handler_test.go#TestCheckOriginWildcards
#[test]
fn wildcard_domain_boundaries() {
    let sut = Sut::start(&["--noauth", "--allowed_origins=*.example.com"]);
    assert_eq!(403, status(&sut, Some("https://evilexample.com")));
    assert_eq!(403, status(&sut, Some("https://example.com")));
    assert_eq!(101, status(&sut, Some("https://app.example.com")));
}

/// external-for: ws/handler_test.go#TestCheckOriginEmptyEntries
#[test]
fn empty_entries_are_ignored() {
    let sut = Sut::start(&["--noauth", "--allowed_origins=other.com, ,"]);
    assert_eq!(403, status(&sut, Some("https://example.com")));
}
