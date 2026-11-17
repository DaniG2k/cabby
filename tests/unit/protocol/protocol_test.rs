//! upstream file: protocol/protocol_test.go

use std::collections::HashMap;

use cabby_core::common::{SessionEnv, Status, StringMap};
use cabby_core::protos;
use cabby_testkit::reference::protocol::*;

fn map(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

fn build_session_env() -> SessionEnv {
    SessionEnv { url: "/cable-test".into(), headers: Some(map(&[("cookie", "token=secret;")])), ..Default::default() }
}

/// upstream: protocol/protocol_test.go#TestNewConnectMessage
#[test]
fn test_new_connect_message() {
    let env = build_session_env();

    let msg = new_connect_message(&env);

    let menv = msg.env.as_ref();
    assert!(menv.is_some());
    let menv = menv.unwrap();
    assert_eq!("/cable-test", menv.url);
    assert_eq!(map(&[("cookie", "token=secret;")]), menv.headers);
}

mod test_new_command_message {
    use super::*;

    /// upstream: protocol/protocol_test.go#TestNewCommandMessage/Base
    #[test]
    fn base() {
        let env = build_session_env();

        let msg = new_command_message(&env, "subscribe", "test_channel", "user=john", "");

        assert_eq!("subscribe", msg.command);
        assert_eq!("test_channel", msg.identifier);
        assert_eq!("user=john", msg.connection_identifiers);
        assert!(msg.env.is_some());
        let menv = msg.env.unwrap();

        // Upstream: `assert.NotNil(t, "/cable-test", msg.Env.Url)` checks that the
        // literal is non-nil, which is always true. Kept as written; the intended
        // equality is the linked regression `base_url_equality`.
        let literal: Option<&str> = Some("/cable-test");
        assert!(literal.is_some());
        assert_eq!(map(&[("cookie", "token=secret;")]), menv.headers);
    }

    /// regression-for: protocol/protocol_test.go#TestNewCommandMessage/Base
    /// The upstream URL assertion is vacuous; this asserts the intended equality.
    #[test]
    fn base_url_equality() {
        let env = build_session_env();

        let msg = new_command_message(&env, "subscribe", "test_channel", "user=john", "");

        assert_eq!("/cable-test", msg.env.expect("env").url);
    }

    /// upstream: protocol/protocol_test.go#TestNewCommandMessage/With_connection,_channel_state_and_data
    #[test]
    fn with_connection_channel_state_and_data() {
        let mut env = build_session_env();
        let cstate = map(&[("_s_", "id=42")]);
        env.connection_state = Some(cstate.clone());

        let istate = map(&[("room", "room:1")]);
        let mut channels = HashMap::new();
        channels.insert("test_channel".to_owned(), istate.clone());

        env.channel_states = Some(channels);

        let msg = new_command_message(&env, "subscribe", "test_channel", "user=john", "action");

        assert_eq!("subscribe", msg.command);
        assert_eq!("test_channel", msg.identifier);
        assert_eq!("user=john", msg.connection_identifiers);
        assert_eq!("action", msg.data);
        assert!(msg.env.is_some());
        let menv = msg.env.unwrap();

        assert_eq!(cstate, menv.cstate);
        assert_eq!(istate, menv.istate);
    }
}

/// upstream: protocol/protocol_test.go#TestNewDisconnectRequest
#[test]
fn test_new_disconnect_request() {
    let mut env = build_session_env();

    let cstate = map(&[("_s_", "id=42")]);
    let istate = map(&[("test_channel", "{\"room\":\"room:1\"}")]);

    let mut channels = HashMap::new();
    channels.insert("test_channel".to_owned(), map(&[("room", "room:1")]));

    env.connection_state = Some(cstate.clone());
    env.channel_states = Some(channels);

    let msg = new_disconnect_message(&env, "user=john", &["chat_42".to_owned()]);

    assert_eq!("user=john", msg.identifiers);
    assert_eq!(vec!["chat_42".to_owned()], msg.subscriptions);
    let menv = msg.env.expect("env");
    assert_eq!(cstate, menv.cstate);
    assert_eq!(istate, menv.istate);
}

fn env_response(cstate: &[(&str, &str)], istate: &[(&str, &str)]) -> Option<protos::EnvResponse> {
    Some(protos::EnvResponse { cstate: map(cstate), istate: map(istate) })
}

mod test_parse_connect_response {
    use super::*;

    /// upstream: protocol/protocol_test.go#TestParseConnectResponse/Success
    #[test]
    fn success() {
        let res = protos::ConnectionResponse {
            identifiers: "user=john".into(),
            transmissions: vec!["welcome".into()],
            status: protos::Status::Success as i32,
            env: env_response(&[("_s_", "test-session")], &[]),
            ..Default::default()
        };

        let parsed = parse_connect_response(&res);

        assert!(parsed.error.is_none());
        let result = parsed.result;
        assert_eq!("user=john", result.identifier);
        assert_eq!(vec!["welcome".to_owned()], result.transmissions);
        assert_eq!(Some(map(&[("_s_", "test-session")])), result.cstate);
        assert_eq!(Status::Success, result.status);
    }

    /// upstream: protocol/protocol_test.go#TestParseConnectResponse/Failure
    #[test]
    fn failure() {
        let res = protos::ConnectionResponse {
            transmissions: vec!["unauthorized".into()],
            status: protos::Status::Failure as i32,
            env: env_response(&[("_s_", "test-session")], &[]),
            error_msg: "Authentication failed".into(),
            ..Default::default()
        };

        let parsed = parse_connect_response(&res);

        assert!(parsed.error.is_none());
        assert_eq!(Status::Failure, parsed.result.status);
    }

    /// upstream: protocol/protocol_test.go#TestParseConnectResponse/Error
    #[test]
    fn error() {
        let res = protos::ConnectionResponse {
            transmissions: vec!["unauthorized".into()],
            status: protos::Status::Error as i32,
            error_msg: "Failed".into(),
            ..Default::default()
        };

        let parsed = parse_connect_response(&res);

        assert!(parsed.error.is_some());
        assert_eq!(Status::Error, parsed.result.status);
    }
}

mod test_parse_command_response {
    use super::*;

    /// upstream: protocol/protocol_test.go#TestParseCommandResponse/Success
    #[test]
    fn success() {
        let res = protos::CommandResponse {
            status: protos::Status::Success as i32,
            streams: vec!["chat_42".into()],
            stopped_streams: vec!["chat_41".into()],
            stop_streams: true,
            transmissions: vec!["message_sent".into()],
            ..Default::default()
        };

        let parsed = parse_command_response(&res);

        assert!(parsed.error.is_none());
        let result = parsed.result;
        assert_eq!(vec!["chat_42".to_owned()], result.streams);
        assert_eq!(vec!["message_sent".to_owned()], result.transmissions);
        assert!(result.stop_all_streams);
        assert_eq!(vec!["chat_41".to_owned()], result.stopped_streams);
        assert_eq!(Status::Success, result.status);
    }

    /// upstream: protocol/protocol_test.go#TestParseCommandResponse/Success_with_connection_and_channel_state
    #[test]
    fn success_with_connection_and_channel_state() {
        let res = protos::CommandResponse {
            status: protos::Status::Success as i32,
            streams: vec!["chat_42".into()],
            env: env_response(&[("_s_", "sentCount=1")], &[("count", "1")]),
            transmissions: vec!["message_sent".into()],
            ..Default::default()
        };

        let parsed = parse_command_response(&res);

        assert!(parsed.error.is_none());
        let result = parsed.result;
        assert_eq!(vec!["chat_42".to_owned()], result.streams);
        assert_eq!(vec!["message_sent".to_owned()], result.transmissions);
        assert_eq!(Some(map(&[("_s_", "sentCount=1")])), result.cstate);
        assert_eq!(Some(map(&[("count", "1")])), result.istate);
    }

    /// upstream: protocol/protocol_test.go#TestParseCommandResponse/Failure
    #[test]
    fn failure() {
        let res = protos::CommandResponse {
            status: protos::Status::Failure as i32,
            error_msg: "Unknown command".into(),
            ..Default::default()
        };

        let parsed = parse_command_response(&res);

        assert!(parsed.error.is_none());
        assert_eq!(Status::Failure, parsed.result.status);
    }

    /// upstream: protocol/protocol_test.go#TestParseCommandResponse/Error
    #[test]
    fn error() {
        let res = protos::CommandResponse {
            status: protos::Status::Error as i32,
            error_msg: "Unknown command".into(),
            ..Default::default()
        };

        let parsed = parse_command_response(&res);

        assert!(parsed.error.is_some());
        assert_eq!(Status::Error, parsed.result.status);
    }
}

mod test_parse_disconnect_response {
    use super::*;

    /// upstream: protocol/protocol_test.go#TestParseDisconnectResponse/Success
    #[test]
    fn success() {
        let res = protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() };

        let err = parse_disconnect_response(&res);

        assert!(err.is_ok());
    }

    /// upstream: protocol/protocol_test.go#TestParseDisconnectResponse/Failure
    #[test]
    fn failure() {
        let res = protos::DisconnectResponse { status: protos::Status::Failure as i32, ..Default::default() };

        let err = parse_disconnect_response(&res);

        assert!(err.is_ok());
    }

    /// upstream: protocol/protocol_test.go#TestParseDisconnectResponse/Error
    #[test]
    fn error() {
        let res = protos::DisconnectResponse { status: protos::Status::Error as i32, ..Default::default() };

        let err = parse_disconnect_response(&res);

        assert!(err.is_err());
    }
}
