//! upstream file: encoders/json_test.go

mod test_json_encoder {
    use cabby_core::common::Reply;
    use cabby_testkit::reference::encoders::{json_decode, json_encode, json_encode_transmission};
    use serde_json::json;

    /// upstream: encoders/json_test.go#TestJSONEncoder/.Encode
    #[test]
    fn encode() {
        let msg = Reply {
            r#type: "test".into(),
            identifier: "test_channel".into(),
            message: Some(json!("hello")),
            ..Reply::default()
        };

        let expected = b"{\"type\":\"test\",\"identifier\":\"test_channel\",\"message\":\"hello\"}".to_vec();

        let actual = json_encode(&msg);

        assert!(actual.is_ok(), "{actual:?}");
        assert_eq!(expected, actual.unwrap().payload);
    }

    /// upstream: encoders/json_test.go#TestJSONEncoder/.EncodeTransmission
    #[test]
    fn encode_transmission() {
        let msg = "{\"type\":\"test\",\"identifier\":\"test_channel\",\"message\":\"hello\"}";
        let expected = msg.as_bytes().to_vec();

        let actual = json_encode_transmission(msg);

        assert!(actual.is_ok(), "{actual:?}");
        assert_eq!(expected, actual.unwrap().payload);
    }

    /// upstream: encoders/json_test.go#TestJSONEncoder/.Decode
    #[test]
    fn decode() {
        let msg = b"{\"command\":\"test\",\"identifier\":\"test_channel\",\"data\":\"hello\"}";

        let actual = json_decode(msg);

        assert!(actual.is_ok(), "{actual:?}");
        let actual = actual.unwrap();
        assert_eq!(actual.command, "test");
        assert_eq!(actual.identifier, "test_channel");
        assert_eq!(actual.data, Some(json!("hello")));
    }
}
