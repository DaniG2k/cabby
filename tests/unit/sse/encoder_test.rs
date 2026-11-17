//! upstream file: sse/encoder_test.go

use cabby_core::common::{DisconnectMessage, EncodedMessage, PingMessage, Reply, SentFrame};
use cabby_server::sse::{Encoder, RETRY_NO_RECONNECT};
use cabby_testkit::reference::sse::{CachedEncodedMessage, decode, encode, encode_transmission, encoder_id};
use serde_json::json;

/// `string(actual.Payload)` (upstream asserts `NoError` first).
fn payload(res: Result<Option<SentFrame>, String>) -> String {
    let frame = res.expect("no error").expect("a frame");
    String::from_utf8(frame.payload).expect("UTF-8 payload")
}

fn reply(identifier: &str, message: &str) -> Reply {
    Reply { identifier: identifier.into(), message: Some(json!(message)), ..Default::default() }
}

mod test_encoder_encode {
    use super::*;

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/without_type
    #[test]
    fn without_type() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Reply(reply("test_channel", "hello"));
        let expected = r#"data: {"identifier":"test_channel","message":"hello"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/without_type_+_unwrap_data
    #[test]
    fn without_type_unwrap_data() {
        let get_coder = Encoder { unwrap_data: true, ..Default::default() };
        let msg = EncodedMessage::Reply(reply("test_channel", "hello"));
        let expected = "data: hello";

        assert_eq!(expected, payload(encode(&get_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/without_type_+_unwrap_data_+_raw_data
    #[test]
    fn without_type_unwrap_data_raw_data() {
        let get_coder = Encoder { unwrap_data: true, raw_data: true, ..Default::default() };
        let msg = EncodedMessage::Reply(reply("test_channel", "hello"));
        let expected = "data: hello";

        assert_eq!(expected, payload(encode(&get_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/without_type_+_unwrap_data_+_multiline_payload
    #[test]
    fn without_type_unwrap_data_multiline_payload() {
        let get_coder = Encoder { unwrap_data: true, ..Default::default() };
        // Multi-line payloads (e.g. Turbo Stream HTML) must be split across multiple
        // `data:` fields, otherwise EventSource truncates at the first newline. The
        // client rejoins the fields with "\n".
        let msg = EncodedMessage::Reply(reply(
            "test_channel",
            "<turbo-stream>\n<template>\nhi\n</template>\n</turbo-stream>",
        ));
        let expected = "data: <turbo-stream>\n\
                        data: <template>\n\
                        data: hi\n\
                        data: </template>\n\
                        data: </turbo-stream>";

        assert_eq!(expected, payload(encode(&get_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/without_type_+_unwrap_data_+_CRLF_and_CR_terminators
    #[test]
    fn without_type_unwrap_data_crlf_and_cr_terminators() {
        let get_coder = Encoder { unwrap_data: true, ..Default::default() };
        // LF, CR and CRLF are all SSE line terminators and must each start a new
        // data: field.
        let msg = EncodedMessage::Reply(reply("test_channel", "a\r\nb\rc\nd"));
        let expected = "data: a\ndata: b\ndata: c\ndata: d";

        assert_eq!(expected, payload(encode(&get_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/with_type_+_multiline_payload_preserves_event_and_id
    #[test]
    fn with_type_multiline_payload_preserves_event_and_id() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Reply(Reply {
            kind: "test".into(),
            stream_id: "stream-test".into(),
            epoch: "bc320".into(),
            offset: 321,
            ..reply("test_channel", "a\nb")
        });
        let expected = "event: test\n".to_owned()
            + r#"data: {"type":"test","identifier":"test_channel","message":"a\nb","stream_id":"stream-test","epoch":"bc320","offset":321}"#
            + "\n"
            + "id: 321/bc320/stream-test";

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/with_type
    #[test]
    fn with_type() {
        let raw_coder = Encoder { raw_data: true, ..Default::default() };
        let msg = EncodedMessage::Reply(Reply { kind: "test".into(), ..reply("test_channel", "hello") });

        let actual = encode(&raw_coder, &msg);

        assert!(actual.is_ok(), "{actual:?}");
        assert_eq!(None, actual.unwrap());
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/ping
    #[test]
    fn ping() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Ping(PingMessage { kind: "ping".into(), message: Some(json!(1694041735)) });
        let expected = "event: ping\n".to_owned() + r#"data: {"type":"ping","message":1694041735}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/ping_+_raw_data
    #[test]
    fn ping_raw_data() {
        let raw_coder = Encoder { raw_data: true, ..Default::default() };
        let msg = EncodedMessage::Ping(PingMessage { kind: "ping".into(), message: Some(json!(1694041735)) });

        assert_eq!(": ping", payload(encode(&raw_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/ping_+_comment_pings
    #[test]
    fn ping_comment_pings() {
        let comment_coder = Encoder { comment_pings: true, ..Default::default() };
        let msg = EncodedMessage::Ping(PingMessage { kind: "ping".into(), message: Some(json!(1694041735)) });

        assert_eq!(": ping", payload(encode(&comment_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/with_type_+_comment_pings
    #[test]
    fn with_type_comment_pings() {
        let comment_coder = Encoder { comment_pings: true, ..Default::default() };
        let msg = EncodedMessage::Reply(Reply { kind: "test".into(), ..reply("test_channel", "hello") });
        let expected =
            "event: test\n".to_owned() + r#"data: {"type":"test","identifier":"test_channel","message":"hello"}"#;

        assert_eq!(expected, payload(encode(&comment_coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/with_type_+_raw_data
    /// (Upstream uses the default encoder here despite the name.)
    #[test]
    fn with_type_raw_data() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Reply(Reply { kind: "test".into(), ..reply("test_channel", "hello") });
        let expected =
            "event: test\n".to_owned() + r#"data: {"type":"test","identifier":"test_channel","message":"hello"}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/with_offset_and_stream_id
    #[test]
    fn with_offset_and_stream_id() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Reply(Reply {
            kind: "test".into(),
            stream_id: "stream-test".into(),
            epoch: "bc320".into(),
            offset: 321,
            ..reply("test_channel", "hello")
        });
        let expected = "event: test\n".to_owned()
            + r#"data: {"type":"test","identifier":"test_channel","message":"hello","stream_id":"stream-test","epoch":"bc320","offset":321}"#
            + "\n"
            + "id: 321/bc320/stream-test";

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/when_disconnect_with_reconnect=true
    #[test]
    fn when_disconnect_with_reconnect_true() {
        let coder = Encoder::default();
        // A `common.Reply` (not a DisconnectMessage) of type "disconnect".
        let msg = EncodedMessage::Reply(Reply {
            kind: "disconnect".into(),
            reason: "unknown".into(),
            reconnect: true,
            ..Default::default()
        });
        let expected =
            "event: disconnect\n".to_owned() + r#"data: {"type":"disconnect","reason":"unknown","reconnect":true}"#;

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }

    /// upstream: sse/encoder_test.go#TestEncoder_Encode/when_disconnect_with_reconnect=false
    #[test]
    fn when_disconnect_with_reconnect_false() {
        let coder = Encoder::default();
        let msg = EncodedMessage::Disconnect(DisconnectMessage {
            kind: "disconnect".into(),
            reason: "unknown".into(),
            reconnect: false,
        });
        let expected = "event: disconnect\n".to_owned()
            + r#"data: {"type":"disconnect","reason":"unknown","reconnect":false}"#
            + "\n"
            + &format!("retry: {RETRY_NO_RECONNECT}");

        assert_eq!(expected, payload(encode(&coder, &msg)));
    }
}

mod test_encoder_encode_transmission {
    use super::*;

    /// upstream: sse/encoder_test.go#TestEncoder_EncodeTransmission/w/type
    #[test]
    fn w_type() {
        let coder = Encoder::default();
        let msg = "{\"type\":\"test\",\"identifier\":\"test_channel\",\"message\":\"hello\"}";
        let expected = format!("event: test\ndata: {msg}").into_bytes();

        let actual = encode_transmission(&coder, msg).expect("no error").expect("a frame");

        assert_eq!(expected, actual.payload);
    }

    /// upstream: sse/encoder_test.go#TestEncoder_EncodeTransmission/w/o_type
    #[test]
    fn w_o_type() {
        let coder = Encoder::default();
        let msg = "{\"message\":\"hello\"}";
        let expected = format!("data: {msg}").into_bytes();

        let actual = encode_transmission(&coder, msg).expect("no error").expect("a frame");

        assert_eq!(expected, actual.payload);
    }
}

/// upstream: sse/encoder_test.go#TestEncoder_Decode
#[test]
fn test_encoder_decode() {
    let coder = Encoder::default();

    let msg = b"{\"command\":\"test\",\"identifier\":\"test_channel\",\"data\":\"hello\"}";

    let actual = decode(&coder, msg);

    // `assert.Error(err)` and `assert.Nil(actual)`: an Err carries no message.
    assert!(actual.is_err(), "{actual:?}");
}

/// upstream: sse/encoder_test.go#TestEncoder_ID
#[test]
fn test_encoder_id() {
    let e = |unwrap_data, raw_data, comment_pings| Encoder { unwrap_data, raw_data, comment_pings };
    assert_eq!("sse", encoder_id(&e(false, false, false)));
    assert_eq!("sse:u", encoder_id(&e(true, false, false)));
    assert_eq!("sse:r", encoder_id(&e(false, true, false)));
    assert_eq!("sse:u:r", encoder_id(&e(true, true, false)));
    assert_eq!("sse:c", encoder_id(&e(false, false, true)));
    assert_eq!("sse:u:r:c", encoder_id(&e(true, true, true)));
}

/// upstream: sse/encoder_test.go#TestEncoder_CachedEncodedMessage
#[test]
fn test_encoder_cached_encoded_message() {
    let wrapped = Encoder::default();
    let unwrapped = Encoder { unwrap_data: true, ..Default::default() };

    let msg = CachedEncodedMessage::new(EncodedMessage::Reply(reply("test_channel", "hello")));

    let actual = msg.fetch(&wrapped);
    assert_eq!(r#"data: {"identifier":"test_channel","message":"hello"}"#, payload(actual));

    let actual = msg.fetch(&unwrapped);
    assert_eq!("data: hello", payload(actual));
}
