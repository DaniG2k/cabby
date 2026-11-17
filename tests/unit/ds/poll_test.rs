//! upstream file: ds/poll_test.go

use cabby_core::common::{EncodedMessage, PingMessage, Reply};
use cabby_testkit::reference::ds::{PollConnection, poll_encoder_encode, poll_encoder_id};
use serde_json::json;

/// `ws.CloseNormalClosure`.
const CLOSE_NORMAL_CLOSURE: i32 = 1000;

mod test_poll_encoder_encode {
    use super::*;

    /// upstream: ds/poll_test.go#TestPollEncoder_Encode/returns_nil_for_non-reply_messages
    #[test]
    fn returns_nil_for_non_reply_messages() {
        let msg = EncodedMessage::Ping(PingMessage { kind: "ping".into(), message: Some(json!(123)) });
        let frame = poll_encoder_encode(&msg).expect("no error");

        assert_eq!(frame, None);
    }

    /// upstream: ds/poll_test.go#TestPollEncoder_Encode/returns_nil_for_reply_with_type
    #[test]
    fn returns_nil_for_reply_with_type() {
        let reply = EncodedMessage::Reply(Reply {
            kind: "confirm_subscription".into(),
            offset: 1,
            epoch: "epoch1".into(),
            ..Default::default()
        });
        let frame = poll_encoder_encode(&reply).expect("no error");

        assert_eq!(frame, None);
    }

    /// upstream: ds/poll_test.go#TestPollEncoder_Encode/returns_nil_for_reply_without_offset
    #[test]
    fn returns_nil_for_reply_without_offset() {
        let reply = EncodedMessage::Reply(Reply { message: Some(json!("test")), ..Default::default() });
        let frame = poll_encoder_encode(&reply).expect("no error");

        assert_eq!(frame, None);
    }

    /// upstream: ds/poll_test.go#TestPollEncoder_Encode/encodes_reply_with_offset_and_epoch
    #[test]
    fn encodes_reply_with_offset_and_epoch() {
        let reply = EncodedMessage::Reply(Reply {
            message: Some(json!({"data": "test"})),
            offset: 123,
            epoch: "epoch1".into(),
            ..Default::default()
        });
        let frame = poll_encoder_encode(&reply).expect("no error").expect("a frame");

        assert_eq!(String::from_utf8(frame.payload).unwrap(), "123::epoch1\n{\"data\":\"test\"}");
    }
}

/// upstream: ds/poll_test.go#TestPollEncoder_ID
#[test]
fn test_poll_encoder_id() {
    assert_eq!(poll_encoder_id(), "dspoll");
}

mod test_poll_connection_write {
    use super::*;

    /// upstream: ds/poll_test.go#TestPollConnection_Write/parses_message_and_sets_offset_header
    #[test]
    fn parses_message_and_sets_offset_header() {
        let conn = PollConnection::new(&[]);

        conn.write(b"123::epoch1\n{\"data\":\"test\"}").expect("write");

        assert_eq!(conn.header("Stream-Next-Offset"), "123::epoch1");
        assert_eq!(conn.body(), "[{\"data\":\"test\"}]");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_Write/handles_body_with_newlines
    #[test]
    fn handles_body_with_newlines() {
        let conn = PollConnection::new(&[]);

        conn.write(b"42::epoch2\n{\"text\":\"line1\\nline2\"}").expect("write");

        assert_eq!(conn.header("Stream-Next-Offset"), "42::epoch2");
        assert_eq!(conn.body(), "[{\"text\":\"line1\\nline2\"}]");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_Write/sets_cache_control_header
    #[test]
    fn sets_cache_control_header() {
        let conn = PollConnection::new(&[]);
        conn.set_cache_control("public, max-age=60");

        conn.write(b"123::epoch1\n{\"data\":\"test\"}").expect("write");

        assert_eq!(conn.header("Cache-Control"), "public, max-age=60");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_Write/sets_ETag_header
    #[test]
    fn sets_etag_header() {
        let conn = PollConnection::new(&[]);
        conn.set_etag("\"test:", "\"");

        conn.write(b"123::epoch1\n{\"data\":\"test\"}").expect("write");

        assert_eq!(conn.header("ETag"), r#""test:123::epoch1""#);
    }

    /// upstream: ds/poll_test.go#TestPollConnection_Write/returns_error_for_invalid_format
    #[test]
    fn returns_error_for_invalid_format() {
        let conn = PollConnection::new(&[]);

        let err = conn.write(b"no-newline-here").expect_err("an error");
        assert!(err.contains("invalid poll message format"), "{err}");
    }
}

/// upstream: ds/poll_test.go#TestPollConnection_Close
/// note: Go has no accessor for the unexported `conn.done`; on Go it is probed on a replayed copy of the connection (a done connection drops a later Write without an error and leaves the body unchanged). The assertions after it run on Go directly.
#[test]
fn test_poll_connection_close() {
    let conn = PollConnection::new(&[]);

    conn.close(CLOSE_NORMAL_CLOSURE, "test");

    assert!(conn.is_done());

    // Writing after close should not error
    conn.write(b"123::epoch1\ntest").expect("no error after close");

    // But should not write anything
    assert_eq!(conn.body().len(), 0);
}

mod test_poll_connection_close_cache_control {
    use super::*;

    /// upstream: ds/poll_test.go#TestPollConnection_CloseCacheControl/disables_caching_for_no_content
    #[test]
    fn disables_caching_for_no_content() {
        let conn = PollConnection::new(&[]);
        conn.set_cache_control("public, max-age=60");

        conn.close(204, "No content");

        assert_eq!(conn.code(), 204);
        assert_eq!(conn.header("Cache-Control"), "no-store");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_CloseCacheControl/disables_caching_for_errors
    #[test]
    fn disables_caching_for_errors() {
        let conn = PollConnection::new(&[]);

        conn.close(410, "");

        assert_eq!(conn.code(), 410);
        assert_eq!(conn.header("Cache-Control"), "no-store");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_CloseCacheControl/keeps_cache_control_for_not_modified
    #[test]
    fn keeps_cache_control_for_not_modified() {
        let conn = PollConnection::new(&[("Cache-Control", "public, max-age=60")]);

        conn.close(304, "");

        assert_eq!(conn.code(), 304);
        assert_eq!(conn.header("Cache-Control"), "public, max-age=60");
    }

    /// upstream: ds/poll_test.go#TestPollConnection_CloseCacheControl/keeps_cache_control_for_OK
    #[test]
    fn keeps_cache_control_for_ok() {
        let conn = PollConnection::new(&[("Cache-Control", "public, max-age=60")]);

        conn.close(200, "");

        assert_eq!(conn.header("Cache-Control"), "public, max-age=60");
    }
}
