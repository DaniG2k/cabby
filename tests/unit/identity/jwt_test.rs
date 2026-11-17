//! upstream file: identity/jwt_test.go
//!
//! Tokens are minted with `cabby_testkit::jwt` (upstream: golang-jwt), with the
//! same claims and expirations relative to the wall clock.

use cabby_core::common::{SessionEnv, Status, StringMap};
use cabby_core::identity::JwtConfig;
use cabby_testkit::jwt::{sign_hs256, unix_from_now, unsigned};
use cabby_testkit::reference::identity::*;
use serde_json::json;

const SECRET: &str = "ruby-to-go";
const IDS: &str = "{\"user_id\":\"15\"}";
const UNAUTHORIZED: &str = "{\"type\":\"disconnect\",\"reason\":\"unauthorized\",\"reconnect\":false}";

/// `common.NewSessionEnv(url, headers)`.
fn new_session_env(url: &str, headers: Option<StringMap>) -> SessionEnv {
    SessionEnv {
        url: url.into(),
        headers,
        connection_state: Some(StringMap::new()),
        channel_states: Some(Default::default()),
        ..Default::default()
    }
}

fn header(k: &str, v: &str) -> Option<StringMap> {
    Some([(k.to_owned(), v.to_owned())].into())
}

mod test_jwt_identifier_identify {
    use super::*;

    fn config() -> JwtConfig {
        new_jwt_config(SECRET)
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/with_valid_token_passed_as_query_param
    #[test]
    fn with_valid_token_passed_as_query_param() {
        let token_string = sign_hs256(&json!({"ext": IDS, "exp": unix_from_now(3600)}), SECRET);

        let env = new_session_env(&format!("ws://demo.anycable.io/cable?jid={token_string}"), None);

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(IDS, res.identifier);
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![r#"{"type":"welcome","sid":"12"}"#.to_owned()], res.transmissions);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/with_valid_token_passed_as_a_header
    #[test]
    fn with_valid_token_passed_as_a_header() {
        let token_string = sign_hs256(&json!({"ext": IDS, "exp": unix_from_now(3600)}), SECRET);

        let env = new_session_env("ws://demo.anycable.io/cable", header("x-jid", &token_string));

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!(IDS, res.identifier);
        assert_eq!(Status::Success, res.status);
        assert_eq!(vec![r#"{"type":"welcome","sid":"12"}"#.to_owned()], res.transmissions);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_token
    #[test]
    fn with_invalid_token() {
        let token_string = "secret-token-not-a-jwt-at-all";
        let env = new_session_env(&format!("ws://demo.anycable.io/cable?jid={token_string}"), None);

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![UNAUTHORIZED.to_owned()], res.transmissions);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_algo
    #[test]
    fn with_invalid_algo() {
        let token_string = unsigned(&json!({"ext": IDS, "exp": unix_from_now(3600)}));

        let env = new_session_env(&format!("ws://demo.anycable.io/cable?jid={token_string}"), None);

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![UNAUTHORIZED.to_owned()], res.transmissions);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_secret
    #[test]
    fn with_invalid_secret() {
        let token_string = sign_hs256(&json!({"ext": IDS, "exp": unix_from_now(3600)}), "not-a-valid-secret");

        let env = new_session_env(&format!("ws://demo.anycable.io/cable?jid={token_string}"), None);

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![UNAUTHORIZED.to_owned()], res.transmissions);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/when_token_expired
    #[test]
    fn when_token_expired() {
        let token_string = sign_hs256(&json!({"ext": IDS, "exp": unix_from_now(-3600)}), SECRET);

        let env = new_session_env("ws://demo.anycable.io/cable", header("x-jid", &token_string));

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(
            vec!["{\"type\":\"disconnect\",\"reason\":\"token_expired\",\"reconnect\":false}".to_owned()],
            res.transmissions
        );
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/when_token_is_missing_and_not_required
    #[test]
    fn when_token_is_missing_and_not_required() {
        let env = new_session_env("ws://demo.anycable.io/cable", None);

        let res = jwt_identify(&config(), "12", &env);

        assert_eq!(None, res.error);
        assert_eq!(None, res.result);
    }

    /// upstream: identity/jwt_test.go#TestJWTIdentifierIdentify/when_token_is_missing_and_required
    #[test]
    fn when_token_is_missing_and_required() {
        let mut config = new_jwt_config(SECRET);
        config.force = true;

        let env = new_session_env("ws://demo.anycable.io/cable", None);

        let res = jwt_identify(&config, "12", &env);

        assert_eq!(None, res.error);
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![UNAUTHORIZED.to_owned()], res.transmissions);
    }
}

/// upstream: identity/jwt_test.go#TestConfig__ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_jwt_config("jwt-secret");
    conf.force = false;
    conf.param = "token".into();

    let toml_str = jwt_config_to_toml(&conf);

    assert!(toml_str.contains("param = \"token\""), "{toml_str}");
    assert!(toml_str.contains("secret = \"jwt-secret\""), "{toml_str}");
    assert!(toml_str.contains("# force = true"), "{toml_str}");

    // Round-trip test
    let mut conf2 = new_jwt_config("bla");

    jwt_config_merge_toml(&mut conf2, &toml_str).expect("decode");

    assert_eq!(conf, conf2);
}
