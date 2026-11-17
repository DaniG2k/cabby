//! upstream file: streams/controller_test.go
//!
//! Upstream passes a nil `*SessionEnv` where noted; that is `None` here.

use cabby_core::common::{SessionEnv, Status, StringMap, WHISPER_STREAM_STATE};
use cabby_core::streams::SubscribeRequest;
use cabby_testkit::reference::streams::*;

const KEY: &str = "s3Krit";
// Turbo.signed_stream_verifier_key = 's3Krit'
// Turbo::StreamsChannel.signed_stream_name([:chat, "2021"])
const STREAM: &str = "ImNoYXQ6MjAyMSI=--f9ee45dbccb1da04d8ceb99cc820207804370ba0d06b46fc3b8b373af1315628";

/// Expected value of `common.RejectionMessage(identifier)`: the JSON encoding of
/// `Reply{Type: "reject_subscription", Identifier: identifier}` (identifier
/// omitted when empty). Checked against Go by these tests.
fn rejection_message(identifier: &str) -> String {
    reply("reject_subscription", identifier)
}

/// Expected value of `common.ConfirmationMessage(identifier)`.
fn confirmation_message(identifier: &str) -> String {
    reply("confirm_subscription", identifier)
}

fn reply(kind: &str, identifier: &str) -> String {
    if identifier.is_empty() {
        format!(r#"{{"type":"{kind}"}}"#)
    } else {
        format!(r#"{{"type":"{kind}","identifier":{}}}"#, serde_json::to_string(identifier).unwrap())
    }
}

/// `common.NewSessionEnv(url, headers)`.
/// Built locally (not via `reference::common::new_session_env`) so these cases
/// depend only on this package's implementation, not on `common`'s.
fn cookie_env() -> SessionEnv {
    SessionEnv {
        url: "ws://demo.anycable.io/cable".into(),
        headers: Some([("cookie".to_owned(), "val=1;".to_owned())].into()),
        connection_state: Some(StringMap::new()),
        channel_states: Some(Default::default()),
        ..Default::default()
    }
}

mod test_new_controller {
    use super::*;

    /// upstream: streams/controller_test.go#TestNewController/No_stream_name
    #[test]
    fn no_stream_name() {
        let subject = new_controller_with_fixed_request(KEY, SubscribeRequest::default());

        let res = subject.subscribe("42", None, "name=jack", "");

        assert!(res.error.is_some());
        let res = res.result.expect("result");

        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![rejection_message("")], res.transmissions);
    }
}

mod test_streams_controller {
    use super::*;

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_public
    #[test]
    fn subscribe_public() {
        let mut conf = new_config();
        conf.public = true;
        let subject = new_streams_controller(&conf);

        let identifier = r#"{"channel":"$pubsub","stream_name":"chat:2024"}"#;
        let res = subject.subscribe("42", None, "name=jack", identifier);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation_message(identifier)], res.transmissions);
        assert_eq!(vec!["chat:2024".to_owned()], res.streams);
        assert_eq!(-1, res.disconnect_interest);
        assert_eq!(
            Some("chat:2024"),
            res.istate.as_ref().and_then(|s| s.get(WHISPER_STREAM_STATE)).map(String::as_str)
        );
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_no_public_allowed
    #[test]
    fn subscribe_no_public_allowed() {
        let conf = new_config();
        let subject = new_streams_controller(&conf);

        let identifier = r#"{"channel":"$pubsub","stream_name":"chat:2024"}"#;
        let res = subject.subscribe("42", None, "name=jack", identifier);

        assert!(res.error.is_some());
        let res = res.result.expect("result");
        assert_eq!(vec![rejection_message(identifier)], res.transmissions);
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_signed
    #[test]
    fn subscribe_signed() {
        let mut conf = new_config();
        conf.secret = KEY.into();
        conf.presence = false;
        let subject = new_streams_controller(&conf);

        let identifier = format!(r#"{{"channel":"$pubsub","signed_stream_name":"{STREAM}"}}"#);

        let res = subject.subscribe("42", None, "name=jack", &identifier);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation_message(&identifier)], res.transmissions);
        assert_eq!(vec!["chat:2021".to_owned()], res.streams);
        assert_eq!(-1, res.disconnect_interest);
        assert_eq!(None, res.istate);
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_signed_-_no_key
    #[test]
    fn subscribe_signed_no_key() {
        let mut conf = new_config();
        conf.secret = String::new();
        conf.presence = false;
        let subject = new_streams_controller(&conf);

        let identifier = format!(r#"{{"channel":"$pubsub","signed_stream_name":"{STREAM}"}}"#);

        let res = subject.subscribe("42", None, "name=jack", &identifier);

        assert!(res.error.is_some());
        let res = res.result.expect("result");
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![rejection_message(&identifier)], res.transmissions);
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_signed_with_empty_key_-_no_key
    #[test]
    fn subscribe_signed_with_empty_key_no_key() {
        let mut conf = new_config();
        conf.secret = String::new();
        let subject = new_streams_controller(&conf);

        let forged = "Im5vdGlmaWNhdGlvbnNfMiI=--e3f666cd48e2a59bb441b94c424ab6a52294db6e4d620c4f8459e659dcab29f8";
        let identifier = format!(r#"{{"channel":"$pubsub","signed_stream_name":"{forged}"}}"#);

        let res = subject.subscribe("42", None, "name=jack", &identifier);

        assert!(res.error.is_some());
        let res = res.result.expect("result");
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![rejection_message(&identifier)], res.transmissions);
        assert!(res.streams.is_empty());
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_public_-_no_key
    #[test]
    fn subscribe_public_no_key() {
        let mut conf = new_config();
        conf.secret = String::new();
        conf.public = true;
        let subject = new_streams_controller(&conf);

        let identifier = r#"{"channel":"$pubsub","stream_name":"chat:2024"}"#;

        let res = subject.subscribe("42", None, "name=jack", identifier);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec!["chat:2024".to_owned()], res.streams);
    }

    /// upstream: streams/controller_test.go#TestStreamsController/Subscribe_-_signed_-_whisper
    #[test]
    fn subscribe_signed_whisper() {
        let mut conf = new_config();
        conf.secret = KEY.into();
        conf.whisper = true;
        conf.presence = false;
        let subject = new_streams_controller(&conf);

        let identifier = format!(r#"{{"channel":"$pubsub","signed_stream_name":"{STREAM}"}}"#);

        let res = subject.subscribe("42", None, "name=jack", &identifier);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![confirmation_message(&identifier)], res.transmissions);
        assert_eq!(vec!["chat:2021".to_owned()], res.streams);
        assert_eq!(-1, res.disconnect_interest);
        assert_eq!(
            Some("chat:2021"),
            res.istate.as_ref().and_then(|s| s.get(WHISPER_STREAM_STATE)).map(String::as_str)
        );
    }
}

/// Shared by the Turbo and CableReady suites, which differ only in the
/// controller and the identifier's JSON shape.
fn assert_subscribe_success(subject: &Subject, channel: &str) {
    let env = cookie_env();
    let res = subject.subscribe("42", Some(&env), "name=jack", channel);

    assert_eq!(None, res.error);
    let res = res.result.expect("result");
    assert_eq!(Status::Success, res.status);
    assert_eq!(vec![confirmation_message(channel)], res.transmissions);
    assert_eq!(vec!["chat:2021".to_owned()], res.streams);
    assert_eq!(-1, res.disconnect_interest);
}

fn assert_subscribe_rejected(subject: &Subject, channel: &str) {
    let env = cookie_env();
    let res = subject.subscribe("42", Some(&env), "name=jack", channel);

    assert!(res.error.is_some());
    let res = res.result.expect("result");
    assert_eq!(Status::Failure, res.status);
    assert_eq!(vec![rejection_message(channel)], res.transmissions);
}

/// Upstream asserts `Transmissions` and `Streams` equal `[]string{}` (empty,
/// non-nil); `CommandResult` uses `Vec`, which has no nil/empty distinction, so
/// the Rust check is emptiness. Against Go the facade also reports nil-ness,
/// and the slices must be non-nil there.
fn assert_unsubscribe(subject: &Subject, channel: &str) {
    let env = cookie_env();
    let (res, go_nil) = subject.unsubscribe_observing_nil("42", Some(&env), "name=jack", channel);

    assert_eq!(None, res.error);
    let res = res.result.expect("result");
    assert_eq!(Status::Success, res.status);
    assert_eq!(Vec::<String>::new(), res.transmissions);
    assert_eq!(Vec::<String>::new(), res.streams);
    if let Some(nil) = go_nil {
        assert!(!nil.transmissions, "Transmissions must be []string{{}}, not nil");
        assert!(!nil.streams, "Streams must be []string{{}}, not nil");
    }
    assert!(res.stop_all_streams);
}

const NOT_A_STRING: &str =
    "WyJjaGF0LzIwMjMiLDE2ODUwMjQwMTdd--5b6661024d4c463c4936cd1542bc9a7672dd8039ac407d0b6c901697190e8aeb";

mod test_turbo_controller {
    use super::*;

    fn channel(signed: &str) -> String {
        format!("{{\"channel\":\"Turbo::StreamsChannel\",\"signed_stream_name\":\"{signed}\"}}")
    }

    /// upstream: streams/controller_test.go#TestTurboController/Subscribe_(success)
    #[test]
    fn subscribe_success() {
        assert_subscribe_success(&new_turbo_controller(KEY), &channel(STREAM));
    }

    /// upstream: streams/controller_test.go#TestTurboController/Subscribe_(failure)
    #[test]
    fn subscribe_failure() {
        assert_subscribe_rejected(&new_turbo_controller(KEY), &channel("fake_id"));
    }

    /// upstream: streams/controller_test.go#TestTurboController/Subscribe_(failure_+_not_a_string)
    #[test]
    fn subscribe_failure_not_a_string() {
        assert_subscribe_rejected(&new_turbo_controller(KEY), &channel(NOT_A_STRING));
    }

    /// upstream: streams/controller_test.go#TestTurboController/Unsubscribe
    #[test]
    fn unsubscribe() {
        assert_unsubscribe(&new_turbo_controller(KEY), &channel(STREAM));
    }
}

mod test_cable_ready_controller {
    use super::*;

    fn channel(signed: &str) -> String {
        format!("{{\"channel\":\"CableReady::Stream\",\"identifier\":\"{signed}\"}}")
    }

    /// upstream: streams/controller_test.go#TestCableReadyController/Subscribe_(success)
    #[test]
    fn subscribe_success() {
        assert_subscribe_success(&new_cable_ready_controller(KEY), &channel(STREAM));
    }

    /// upstream: streams/controller_test.go#TestCableReadyController/Subscribe_(failure)
    #[test]
    fn subscribe_failure() {
        assert_subscribe_rejected(&new_cable_ready_controller(KEY), &channel("fake_id"));
    }

    /// upstream: streams/controller_test.go#TestCableReadyController/Subscribe_(failure_+_not_a_string)
    #[test]
    fn subscribe_failure_not_a_string() {
        assert_subscribe_rejected(&new_cable_ready_controller(KEY), &channel(NOT_A_STRING));
    }

    /// upstream: streams/controller_test.go#TestCableReadyController/Unsubscribe
    #[test]
    fn unsubscribe() {
        assert_unsubscribe(&new_cable_ready_controller(KEY), &channel(STREAM));
    }
}
