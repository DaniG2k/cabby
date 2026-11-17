//! upstream file: ds/sse_test.go

use cabby_core::common::{EncodedMessage, PingMessage, Reply};
use cabby_server::ds::SseEncoder;
use cabby_testkit::reference::ds::{sse_encoder_encode, sse_encoder_encode_transmission};
use serde_json::json;

mod test_sse_encoder_encode {
    use super::*;

    /// upstream: ds/sse_test.go#TestSSEEncoder_Encode/encodes_data_with_control_event
    #[test]
    fn encodes_data_with_control_event() {
        let encoder = SseEncoder { cursor: "test-cursor".into(), ..Default::default() };

        let msg = EncodedMessage::Reply(Reply {
            message: Some(json!({"id": "1", "text": "hello"})),
            offset: 10,
            epoch: "epoch1".into(),
            stream_id: "test".into(),
            ..Default::default()
        });

        let frame = sse_encoder_encode(&encoder, &msg).expect("no error").expect("a frame");

        let payload = String::from_utf8(frame.payload).unwrap();

        assert!(payload.contains("event: data"), "{payload}");
        assert!(payload.contains(r#""id":"1""#), "{payload}");
        assert!(payload.contains(r#""text":"hello""#), "{payload}");

        assert!(payload.contains("event: control"), "{payload}");
        assert!(payload.contains(r#""streamNextOffset":"10::epoch1""#), "{payload}");
        assert!(payload.contains(r#""streamCursor":"test-cursor""#), "{payload}");
    }

    /// upstream: ds/sse_test.go#TestSSEEncoder_Encode/encodes_ping_as_comment
    #[test]
    fn encodes_ping_as_comment() {
        let encoder = SseEncoder::default();

        let msg = EncodedMessage::Ping(PingMessage { kind: "ping".into(), message: Some(json!(1694041735)) });

        let frame = sse_encoder_encode(&encoder, &msg).expect("no error").expect("a frame");

        assert_eq!(String::from_utf8(frame.payload).unwrap(), ": ping");
    }

    /// upstream: ds/sse_test.go#TestSSEEncoder_Encode/skips_non-data_messages
    #[test]
    fn skips_non_data_messages() {
        let encoder = SseEncoder::default();

        let msg = EncodedMessage::Reply(Reply { kind: "welcome".into(), ..Default::default() });

        let frame = sse_encoder_encode(&encoder, &msg).expect("no error");
        assert_eq!(frame, None);

        let msg = EncodedMessage::Reply(Reply { kind: "disconnect".into(), ..Default::default() });

        let frame = sse_encoder_encode(&encoder, &msg).expect("no error");
        assert_eq!(frame, None);
    }
}

/// upstream: ds/sse_test.go#TestSSEEncoder_EncodeTransmission
#[test]
fn test_sse_encoder_encode_transmission() {
    let encoder = SseEncoder::default();

    let r = sse_encoder_encode_transmission(&encoder, r#"{"message":"test"}"#);
    assert_eq!(r.frame, None);
    assert_eq!(r.error, None);
}
