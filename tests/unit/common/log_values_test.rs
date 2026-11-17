//! upstream file: common/log_values_test.go

use cabby_core::common::{CommandResult, Status};
use cabby_testkit::reference::common::{
    command_result_log_text, confirmation_message, filtered_identifier, filtered_transmissions,
};

const DIGEST: &str = "2fe0d3c5d4ebd07ff8f1bc2ba6d93d1e2b1ad8a0ab2dbd7a5da5ebd4a19a4b91";

mod test_filtered_identifier {
    use super::*;

    fn check(input: &str, expected: &str) {
        assert_eq!(expected, filtered_identifier(input));
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/signed_stream
    #[test]
    fn signed_stream() {
        check(
            &format!(r#"{{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--{DIGEST}"}}"#),
            r#"{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--2f***91"}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/turbo_stream_with_spaces
    #[test]
    fn turbo_stream_with_spaces() {
        check(
            &format!(r#"{{"channel": "Turbo::StreamsChannel", "signed_stream_name": "ImNoYXQ6MjAyMSI=--{DIGEST}"}}"#),
            r#"{"channel": "Turbo::StreamsChannel", "signed_stream_name": "ImNoYXQ6MjAyMSI=--2f***91"}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/cable_ready_stream
    #[test]
    fn cable_ready_stream() {
        check(
            &format!(r#"{{"channel":"CableReady::Stream","identifier":"ImNoYXQ6MjAyMSI=--{DIGEST}"}}"#),
            r#"{"channel":"CableReady::Stream","identifier":"ImNoYXQ6MjAyMSI=--2f***91"}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/public_stream
    #[test]
    fn public_stream() {
        check(
            r#"{"channel":"$pubsub","stream_name":"chat:2021"}"#,
            r#"{"channel":"$pubsub","stream_name":"chat:2021"}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/regular_channel
    #[test]
    fn regular_channel() {
        check(r#"{"channel":"ChatChannel","id":"2021"}"#, r#"{"channel":"ChatChannel","id":"2021"}"#);
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/signed_stream_without_digest
    #[test]
    fn signed_stream_without_digest() {
        check(
            r#"{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI="}"#,
            r#"{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI="}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/confirmation_message
    #[test]
    fn confirmation_message_() {
        check(
            &confirmation_message(&format!(
                r#"{{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--{DIGEST}"}}"#
            )),
            r#"{"type":"confirm_subscription","identifier":"{\"channel\":\"$pubsub\",\"signed_stream_name\":\"ImNoYXQ6MjAyMSI=--2f***91\"}"}"#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/multiple_signed_streams
    #[test]
    fn multiple_signed_streams() {
        check(&format!(r#"{{"a":"x--{DIGEST}","b":"y--{DIGEST}"}}"#), r#"{"a":"x--2f***91","b":"y--2f***91"}"#);
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/invalid_JSON
    #[test]
    fn invalid_json() {
        check(
            &format!(r#"{{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--{DIGEST}","#),
            r#"{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--2f***91","#,
        );
    }

    /// upstream: common/log_values_test.go#TestFilteredIdentifier/not_a_JSON
    #[test]
    fn not_a_json() {
        check("chat_1", "chat_1");
    }
}

/// upstream: common/log_values_test.go#TestCommandResultLogValue_masksSignedStreams
#[test]
fn test_command_result_log_value_masks_signed_streams() {
    let identifier = format!(r#"{{"channel":"$pubsub","signed_stream_name":"ImNoYXQ6MjAyMSI=--{DIGEST}"}}"#);

    let res = CommandResult {
        status: Status::Success,
        transmissions: vec![confirmation_message(&identifier)],
        istate: Some([("user_email".to_owned(), "john@example.com".to_owned())].into()),
        ..CommandResult::default()
    };

    // slog.New(slog.NewTextHandler(&buf, nil)).Info("result", "res", res)
    let output = command_result_log_text("result", "res", &res);

    assert!(!output.contains(&DIGEST[..8]), "{output}");
    assert!(!output.contains("john@example.com"), "{output}");
    assert!(output.contains("res.istate=[user_email]"), "{output}");
}

/// upstream: common/log_values_test.go#TestFilteredTransmissions
#[test]
fn test_filtered_transmissions() {
    let long = format!(r#"{{"signed_stream_name":"eDE=--{DIGEST}","padding":"{}"}}"#, "x".repeat(200));

    let res = filtered_transmissions(&[long]);

    // Masking happens before truncation
    assert!(res[0].starts_with(r#"{"signed_stream_name":"eDE=--2f***91","padding":"xxx"#), "{}", res[0]);
    assert!(res[0].contains("...("), "{}", res[0]);
}
