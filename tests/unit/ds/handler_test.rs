//! upstream file: ds/handler_test.go
//!
//! Upstream subtests share one node, mock broker and handler; testify answers with
//! the first matching expectation, so each Rust test replays the broker
//! expectations the earlier subtests programmed (`program_get`) before its own.

use cabby_core::common::StreamMessage;
use cabby_server::ds::Config;
use cabby_testkit::reference::ds::{
    Handler, Harness, Recorded, Request, StreamsKind, etag_matches, new_config, read_cache_control,
};

/// `conf := NewConfig(); conf.Path = "/ds"; conf.SkipAuth = true`.
fn conf() -> Config {
    let mut conf = new_config();
    conf.path = "/ds".into();
    conf.skip_auth = true;
    conf
}

fn handler(h: &Harness) -> Handler {
    h.handler(&conf(), StreamsKind::Echo).expect("DSHandler")
}

fn header<'a>(w: &'a Recorded, name: &str) -> &'a str {
    w.header(name).unwrap_or_default()
}

fn assert_no_unexpected_calls(h: &Harness) {
    assert_eq!(h.unexpected_calls(), Vec::<String>::new(), "unexpected mock calls");
}

mod test_dshandler_head {
    use super::*;

    /// upstream: ds/handler_test.go#TestDSHandler_HEAD
    #[test]
    fn setup() {
        let h = Harness::build_node();
        let handler = h.handler(&conf(), StreamsKind::Echo);
        assert!(handler.is_ok(), "{:?}", handler.err());
    }

    /// upstream: ds/handler_test.go#TestDSHandler_HEAD/returns_stream_metadata_for_unknown_stream
    #[test]
    fn returns_stream_metadata_for_unknown_stream() {
        let h = Harness::build_node();
        let handler = handler(&h);

        let w = handler.serve(&Request::new("HEAD", "/ds/test-stream"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "Content-Type"), "application/json");
        assert_eq!(header(&w, "Cache-Control"), "no-store");
        assert_eq!(header(&w, "Stream-Next-Offset"), "0");
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_HEAD/returns_stream_metadata_for_existing_stream
    #[test]
    fn returns_stream_metadata_for_existing_stream() {
        let h = Harness::build_node();
        let handler = handler(&h);
        h.on_peak(
            "test-existing-stream",
            Some(StreamMessage { epoch: "a321".into(), offset: 44, ..Default::default() }),
        );

        let w = handler.serve(&Request::new("HEAD", "/ds/test-existing-stream"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "Content-Type"), "application/json");
        assert_eq!(header(&w, "Cache-Control"), "no-store");
        assert_eq!(header(&w, "Stream-Next-Offset"), "44::a321");
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_HEAD/requires_stream_path
    #[test]
    fn requires_stream_path() {
        let h = Harness::build_node();
        let handler = handler(&h);

        let w = handler.serve(&Request::new("HEAD", "/ds/"));

        assert_eq!(w.status, 400);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_HEAD/stream_authorization
    /// note: the resolver's own assertions (identifier decodes as JSON, `stream_name` is `test-stream`, a present `signed_stream_name` is `s1t2r3e4a5m`) run on every identifier it recorded.
    #[test]
    fn stream_authorization() {
        let h = Harness::build_node();
        let _ = handler(&h);

        let handler = h.handler(&conf(), StreamsKind::RequireSigned).expect("DSHandler");

        let w = handler.serve(&Request::new("HEAD", "/ds/test-stream?signed=s1t2r3e4a5m"));
        assert_eq!(w.status, 200);

        let w = handler.serve(&Request::new("HEAD", "/ds/test-stream").header("X-Signed", "s1t2r3e4a5m"));
        assert_eq!(w.status, 200);

        let w = handler.serve(&Request::new("HEAD", "/ds/test-stream"));
        assert_eq!(w.status, 401);

        for r in handler.resolved() {
            assert_eq!(r.error, None, "identifier {:?} is not JSON", r.identifier);
            let resolved = r.decoded.unwrap_or_default();
            assert_eq!(resolved.get("stream_name").map(String::as_str), Some("test-stream"));
            let signed = resolved.get("signed_stream_name").cloned().unwrap_or_default();
            if !signed.is_empty() {
                assert_eq!(signed, "s1t2r3e4a5m");
            }
        }
        assert_no_unexpected_calls(&h);
    }
}

/// The broker expectations the `TestDSHandler_GET` subtests before `upto`
/// (1-based, inclusive) programmed.
fn program_get(h: &Harness, upto: usize) {
    if upto >= 1 {
        // catch-up mode w/ empty stream
        h.on_history_since("test-stream", 0, Ok(Some(vec![])));
    }
    if upto >= 2 {
        // catch-up with valid offset
        h.on_history_from(
            "test-stream",
            "epoch1",
            10,
            Ok(Some(vec![
                StreamMessage { data: r#"{"id":1}"#.into(), ..Default::default() },
                StreamMessage { data: r#"{"id":2}"#.into(), ..Default::default() },
            ])),
        );
    }
    if upto >= 3 {
        // catch-up sets ETag
        h.on_history_from(
            "test-stream",
            "epoch1",
            20,
            Ok(Some(vec![
                StreamMessage { data: r#"{"id":21}"#.into(), offset: 21, epoch: "epoch1".into(), ..Default::default() },
                StreamMessage { data: r#"{"id":22}"#.into(), offset: 22, epoch: "epoch1".into(), ..Default::default() },
            ])),
        );
    }
    if upto >= 9 {
        // catch-up with stale offset
        h.on_history_from("test-stream", "poch", 11, Err("invalid offset".into()));
    }
}

const PUBLIC_CACHEABLE: &str = "public, max-age=60, stale-while-revalidate=300";

mod test_dshandler_get {
    use super::*;

    /// upstream: ds/handler_test.go#TestDSHandler_GET
    #[test]
    fn setup() {
        let h = Harness::build_node();
        let handler = h.handler(&conf(), StreamsKind::Echo);
        assert!(handler.is_ok(), "{:?}", handler.err());
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_mode_w/_empty_stream
    #[test]
    fn catch_up_mode_w_empty_stream() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 1);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "Content-Type"), "application/json");
        assert!(!header(&w, "Stream-Next-Offset").is_empty());

        assert_eq!(w.text(), "[]");
        assert_no_unexpected_calls(&h);
    }

    /// regression-for: ds/handler_test.go#TestDSHandler_GET/catch-up_mode_w/_empty_stream
    /// note: upstream only asserts a non-empty Stream-Next-Offset; an unknown/empty stream (Peak returns nil) has the start-of-stream offset "0" (EncodeOffset with no epoch), which is pinned here along with Stream-Up-To-Date.
    #[test]
    fn catch_up_mode_w_empty_stream_next_offset_is_zero() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 1);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "Stream-Next-Offset"), "0");
        assert_eq!(header(&w, "Stream-Up-To-Date"), "true");
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_valid_offset
    #[test]
    fn catch_up_with_valid_offset() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 2);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?offset=10::epoch1"));

        assert_eq!(w.status, 200);
        assert_eq!(w.text(), r#"[{"id":1},{"id":2}]"#);
        assert_eq!(header(&w, "Cache-Control"), PUBLIC_CACHEABLE);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_sets_ETag
    #[test]
    fn catch_up_sets_etag() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 3);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?offset=20::epoch1"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "ETag"), r#""dGVzdC1zdHJlYW0=:20::epoch1:22::epoch1""#);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_matching_If-None-Match
    /// note: uses the HistoryFrom(20) expectation of "catch-up sets ETag" (replayed).
    #[test]
    fn catch_up_with_matching_if_none_match() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 4);

        let w = handler.serve(
            &Request::new("GET", "/ds/test-stream?offset=20::epoch1")
                .header("If-None-Match", r#""dGVzdC1zdHJlYW0=:20::epoch1:22::epoch1""#),
        );

        assert_eq!(w.status, 304);
        assert!(w.text().is_empty());
        assert_eq!(header(&w, "ETag"), r#""dGVzdC1zdHJlYW0=:20::epoch1:22::epoch1""#);
        assert_eq!(header(&w, "Stream-Next-Offset"), "22::epoch1");
        assert_eq!(header(&w, "Cache-Control"), PUBLIC_CACHEABLE);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_outdated_If-None-Match
    /// note: uses the HistoryFrom(20) expectation of "catch-up sets ETag" (replayed).
    #[test]
    fn catch_up_with_outdated_if_none_match() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 5);

        let w = handler.serve(
            &Request::new("GET", "/ds/test-stream?offset=20::epoch1")
                .header("If-None-Match", r#""dGVzdC1zdHJlYW0=:20::epoch1:21::epoch1""#),
        );

        assert_eq!(w.status, 200);
        assert_eq!(w.text(), r#"[{"id":21},{"id":22}]"#);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_signed_stream_name_in_header
    /// note: uses the HistoryFrom(10) expectation of "catch-up with valid offset" (replayed).
    #[test]
    fn catch_up_with_signed_stream_name_in_header() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 6);

        let w =
            handler.serve(&Request::new("GET", "/ds/test-stream?offset=10::epoch1").header("x-signed", "s1t2r3e4a5m"));

        assert_eq!(w.status, 200);
        assert_eq!(header(&w, "Cache-Control"), "private, max-age=60, stale-while-revalidate=300");
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_now_offset
    #[test]
    fn catch_up_with_now_offset() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 7);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?offset=now"));

        assert_eq!(w.status, 200);
        assert_eq!(w.text(), "[]");
        assert_eq!(header(&w, "Stream-Up-To-Date"), "true");
        assert_eq!(header(&w, "Cache-Control"), "no-store");
        assert!(header(&w, "ETag").is_empty());
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_now_offset_ignores_If-None-Match
    #[test]
    fn catch_up_with_now_offset_ignores_if_none_match() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 8);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?offset=now").header("If-None-Match", "*"));

        assert_eq!(w.status, 200);
        assert_eq!(w.text(), "[]");
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/catch-up_with_stale_offset
    #[test]
    fn catch_up_with_stale_offset() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 9);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?offset=11::poch"));

        assert_eq!(w.status, 410);
        assert_eq!(header(&w, "Cache-Control"), "no-store");
        assert!(header(&w, "ETag").is_empty());
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/requires_stream_path
    #[test]
    fn requires_stream_path() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 10);

        let w = handler.serve(&Request::new("GET", "/ds/"));

        assert_eq!(w.status, 400);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/validates_live_mode
    #[test]
    fn validates_live_mode() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 11);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?live=invalid"));

        assert_eq!(w.status, 400);
        assert_no_unexpected_calls(&h);
    }

    /// upstream: ds/handler_test.go#TestDSHandler_GET/requires_offset_for_live_mode
    #[test]
    fn requires_offset_for_live_mode() {
        let h = Harness::build_node();
        let handler = handler(&h);
        program_get(&h, 12);

        let w = handler.serve(&Request::new("GET", "/ds/test-stream?live=long-poll"));

        assert_eq!(w.status, 400);
        assert_no_unexpected_calls(&h);
    }
}

mod test_read_cache_control {
    use super::*;

    const CACHEABLE: &str = "max-age=60, stale-while-revalidate=300";

    fn check(skip_auth: bool, url: &str, header: &str, expected: &str) {
        let mut conf = new_config();
        conf.skip_auth = skip_auth;

        let mut req = Request::new("GET", url);
        if !header.is_empty() {
            req = req.header("x-signed", header);
        }

        assert_eq!(read_cache_control(&conf, &req), expected);
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/public_stream_w/o_auth
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read (memory broker, one message in the stream).
    #[test]
    fn public_stream_wo_auth() {
        check(true, "/ds/test?offset=-1", "", &format!("public, {CACHEABLE}"));
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/signed_stream_in_query_w/o_auth
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read (memory broker, one message in the stream).
    #[test]
    fn signed_stream_in_query_wo_auth() {
        check(true, "/ds/test?offset=-1&signed=abc", "", &format!("public, {CACHEABLE}"));
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/signed_stream_in_query_and_header_w/o_auth
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read (memory broker, one message in the stream).
    #[test]
    fn signed_stream_in_query_and_header_wo_auth() {
        check(true, "/ds/test?offset=-1&signed=abc", "abc", &format!("public, {CACHEABLE}"));
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/signed_stream_in_header_w/o_auth
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read (memory broker, one message in the stream).
    #[test]
    fn signed_stream_in_header_wo_auth() {
        check(true, "/ds/test?offset=-1", "abc", &format!("private, {CACHEABLE}"));
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/with_auth
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read (memory broker, one message in the stream, Authenticate answered with success).
    #[test]
    fn with_auth() {
        check(false, "/ds/test?offset=-1&signed=abc", "", &format!("private, {CACHEABLE}"));
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/now_offset
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of a DSHandler catch-up read.
    #[test]
    fn now_offset() {
        check(true, "/ds/test?offset=now", "", "no-store");
    }

    /// upstream: ds/handler_test.go#TestReadCacheControl/now_offset_in_long-poll_mode
    /// note: readCacheControl is unexported; on Go it is observed as the Cache-Control of the long-poll data response to a live message (the PollConnection's CacheControl), not of a 204, which is no-store regardless.
    #[test]
    fn now_offset_in_long_poll_mode() {
        check(true, "/ds/test?offset=now&live=long-poll", "", "no-store");
    }
}

mod test_etag_matches {
    use super::*;

    const ETAG: &str = r#""abc:-1:1::e""#;

    fn check(if_none_match: &str, expected: bool) {
        assert_eq!(etag_matches(if_none_match, ETAG), expected);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/empty
    /// note: etagMatches is unexported; on Go it is observed through DSHandler: a catch-up read of /ds/abc from -1 with next offset 1::e has the ETag "YWJj:-1:1::e" (base64 path), and If-None-Match (this row with abc replaced by YWJj) matches when the response is 304.
    #[test]
    fn empty() {
        check("", false);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/exact
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn exact() {
        check(r#""abc:-1:1::e""#, true);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/weak
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn weak() {
        check(r#"W/"abc:-1:1::e""#, true);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/list
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn list() {
        check(r#""other", "abc:-1:1::e""#, true);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/wildcard
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn wildcard() {
        check("*", true);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/different
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn different() {
        check(r#""abc:-1:2::e""#, false);
    }

    /// upstream: ds/handler_test.go#TestETagMatches/unquoted
    /// note: etagMatches is unexported; on Go it is observed through DSHandler (see the "empty" row).
    #[test]
    fn unquoted() {
        check("abc:-1:1::e", false);
    }
}
