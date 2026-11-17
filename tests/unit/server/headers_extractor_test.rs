//! upstream file: server/headers_extractor_test.go
//!
//! Upstream builds one request for all subtests and the WebSocket-protocol
//! subtests add a header to it (`Header.Set`); each Rust test builds the request
//! with the headers set so far.

mod test_headers_extractor_from_request {
    use cabby_server::server::{DefaultHeadersExtractor, HttpRequest};
    use cabby_testkit::reference::server::headers_from_request;

    use crate::support::{new_request, set_header};

    fn request() -> HttpRequest {
        let mut req = new_request("GET", "/");
        set_header(&mut req, "Cookie", "yummy_cookie=raisin;tasty_cookie=strawberry");
        set_header(&mut req, "X-Api-Token", "42");
        set_header(&mut req, "Accept-Language", "ru");
        req
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn empty(v: Option<&String>) -> bool {
        v.is_none_or(|s| s.is_empty())
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/Without_specified_headers
    #[test]
    fn without_specified_headers() {
        let extractor = DefaultHeadersExtractor::default();
        let headers = headers_from_request(&extractor, &request());

        assert_eq!(headers.len(), 1);
        assert_eq!("192.0.2.1", headers["REMOTE_ADDR"]);
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/With_specified_headers
    #[test]
    fn with_specified_headers() {
        let extractor =
            DefaultHeadersExtractor { headers: strings(&["cookie", "x-api-token", "x-jid"]), ..Default::default() };
        let headers = headers_from_request(&extractor, &request());

        assert_eq!(headers.len(), 3);

        assert!(empty(headers.get("accept-language")));
        assert_eq!("42", headers["x-api-token"]);
        assert_eq!("yummy_cookie=raisin;tasty_cookie=strawberry", headers["cookie"]);
        assert_eq!("192.0.2.1", headers["REMOTE_ADDR"]);

        assert!(!headers.contains_key("x-jid"));
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/With_specified_headers_and_cookie_filter
    #[test]
    fn with_specified_headers_and_cookie_filter() {
        let extractor = DefaultHeadersExtractor {
            headers: strings(&["cookie"]),
            cookies: strings(&["yummy_cookie"]),
            ..Default::default()
        };
        let headers = headers_from_request(&extractor, &request());

        assert_eq!(headers.len(), 2);

        assert!(empty(headers.get("accept-language")));
        assert_eq!("yummy_cookie=raisin;", headers["cookie"]);
        assert_eq!("192.0.2.1", headers["REMOTE_ADDR"]);
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/With_specified_auth_header
    #[test]
    fn with_specified_auth_header() {
        let extractor = DefaultHeadersExtractor {
            auth_header: "x-api-token".into(),
            headers: strings(&["cookie", "x-jid"]),
            ..Default::default()
        };
        let headers = headers_from_request(&extractor, &request());

        assert_eq!(headers.len(), 3);

        assert!(empty(headers.get("accept-language")));
        assert_eq!("42", headers["x-api-token"]);
        assert_eq!("yummy_cookie=raisin;tasty_cookie=strawberry", headers["cookie"]);
        assert_eq!("192.0.2.1", headers["REMOTE_ADDR"]);

        assert!(!headers.contains_key("x-jid"));
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/With_auth_header_and_WebSocket_protocols
    #[test]
    fn with_auth_header_and_websocket_protocols() {
        let mut req = request();
        set_header(&mut req, "Sec-Websocket-Protocol", "actioncable-v1-json,anycable-token.s3crett0");
        let extractor = DefaultHeadersExtractor {
            auth_header: "x-jid".into(),
            headers: strings(&["cookie"]),
            ..Default::default()
        };
        let headers = headers_from_request(&extractor, &req);

        assert_eq!("s3crett0", headers["x-jid"]);
    }

    /// upstream: server/headers_extractor_test.go#TestHeadersExtractor_FromRequest/With_auth_header_and_WebSocket_protocols_with_spaces
    #[test]
    fn with_auth_header_and_websocket_protocols_with_spaces() {
        let mut req = request();
        set_header(&mut req, "Sec-Websocket-Protocol", "actioncable-v1-json, anycable-token.s3crett0");
        let extractor = DefaultHeadersExtractor {
            auth_header: "x-jid".into(),
            headers: strings(&["cookie"]),
            ..Default::default()
        };
        let headers = headers_from_request(&extractor, &req);

        assert_eq!("s3crett0", headers["x-jid"]);
    }
}
