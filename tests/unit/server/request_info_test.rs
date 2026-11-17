//! upstream file: server/request_info_test.go

use cabby_testkit::reference::server::{RequestInfo, fetch_uid};

use crate::support::{new_request, set_header};

mod test_fetch_uid {
    use super::*;

    /// upstream: server/request_info_test.go#TestFetchUID/Without_request_id
    #[test]
    fn without_request_id() {
        let req = new_request("GET", "/");
        let uid = fetch_uid(&req).unwrap_or_default();

        assert_ne!("", uid);
    }

    /// upstream: server/request_info_test.go#TestFetchUID/With_request_id
    #[test]
    fn with_request_id() {
        let mut req = new_request("GET", "/");
        set_header(&mut req, "x-request-id", "external-request-id");

        let uid = fetch_uid(&req).unwrap_or_default();

        assert_eq!("external-request-id", uid);
    }

    /// upstream: server/request_info_test.go#TestFetchUID/With_invalid_request_id
    #[test]
    fn with_invalid_request_id() {
        for val in ["a".repeat(129), "with space".into(), "new\tline".into(), "юникод".into()] {
            let mut req = new_request("GET", "/");
            set_header(&mut req, "x-request-id", &val);

            let uid = fetch_uid(&req);

            let uid = uid.expect("require.NoError");
            assert_ne!("", uid);
            assert_ne!(val, uid);
        }
    }
}

mod test_request_info {
    use super::*;

    /// upstream: server/request_info_test.go#TestRequestInfo/With_just_path
    #[test]
    fn with_just_path() {
        let req = new_request("GET", "/cable");
        let info = RequestInfo::new(&req).expect("require.NoError");

        assert_eq!("http://example.com/cable", info.url());
    }

    /// upstream: server/request_info_test.go#TestRequestInfo/With_just_path_+_TLS
    #[test]
    fn with_just_path_tls() {
        let mut req = new_request("GET", "/cable");
        req.tls = true;
        let info = RequestInfo::new(&req).expect("require.NoError");

        assert_eq!("https://example.com/cable", info.url());
    }

    /// upstream: server/request_info_test.go#TestRequestInfo/With_fully_qualified_URL
    #[test]
    fn with_fully_qualified_url() {
        let req = new_request("GET", "ws://anycable.io/cable");
        let info = RequestInfo::new(&req).expect("require.NoError");

        assert_eq!("ws://anycable.io/cable", info.url());
    }

    /// upstream: server/request_info_test.go#TestRequestInfo/With_params
    #[test]
    fn with_params() {
        let req = new_request("GET", "ws://anycable.io/cable?pi=3&pp=no&pi=5");
        let info = RequestInfo::new(&req).expect("require.NoError");

        assert_eq!("5", info.param("pi"));
        assert_eq!("no", info.param("pp"));

        let blank_info = RequestInfo::blank();
        assert_eq!("", blank_info.param("pi"));
    }

    /// upstream: server/request_info_test.go#TestRequestInfo/With_AnyCable_Headers
    #[test]
    fn with_anycable_headers() {
        let mut req = new_request("GET", "ws://anycable.io/cable?pi=3&pp=no&pi=5");
        set_header(&mut req, "x-anycable-sid", "432");
        set_header(&mut req, "X-AnyCable-Ping-Interval", "10");
        let info = RequestInfo::new(&req).expect("require.NoError");

        assert_eq!("432", info.anycable_header("X-ANYCABLE-SID"));
        assert_eq!("10", info.anycable_header("x-anycable-ping-interval"));

        let blank_info = RequestInfo::blank();
        assert_eq!("", blank_info.anycable_header("SID"));
    }
}
