//! upstream file: identity/jwt_test.go
//!
//! Tokens are minted with `cabby_testkit::jwt` (upstream: golang-jwt), with the
//! same claims and expirations relative to the wall clock.

use cabby_core::common::{SessionEnv, Status, StringMap};
use cabby_core::identity::JwtConfig;
use cabby_testkit::jwt::{sign_hs256, sign_raw, unix_from_now, unsigned};
use cabby_testkit::reference::identity::*;
use serde_json::json;

const SECRET: &str = "ruby-to-go";
const IDS: &str = "{\"user_id\":\"15\"}";
const UNAUTHORIZED: &str = "{\"type\":\"disconnect\",\"reason\":\"unauthorized\",\"reconnect\":false}";

/// `common.NewSessionEnv(url, headers)`.
/// Built locally (not via `reference::common::new_session_env`) so these cases
/// depend only on this package's implementation, not on `common`'s.
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

/// Cabby-only checks next to `TestJWTIdentifierIdentify`: upstream covers
/// `alg: none`, a wrong secret and an expired token. A token whose header names
/// a non-HMAC method (signed with the secret as an HMAC key), names no method,
/// is not valid yet or has a non-numeric `exp` is unauthorized too.
mod rejected_tokens {
    use super::*;

    fn assert_unauthorized(token: &str) {
        let env = new_session_env("ws://demo.anycable.io/cable", header("x-jid", token));

        let res = jwt_identify(&new_jwt_config(SECRET), "12", &env);

        assert_eq!(None, res.error, "{token}");
        let res = res.result.expect("result");
        assert_eq!("", res.identifier);
        assert_eq!(Status::Failure, res.status);
        assert_eq!(vec![UNAUTHORIZED.to_owned()], res.transmissions, "{token}");
    }

    fn claims() -> String {
        json!({"ext": IDS, "exp": unix_from_now(3600)}).to_string()
    }

    /// regression-for: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_algo
    #[test]
    fn a_header_naming_another_method_is_unauthorized() {
        for alg in ["RS256", "ES256", "EdDSA", "PS512", "none", "XS256"] {
            assert_unauthorized(&sign_raw(&format!(r#"{{"alg":"{alg}","typ":"JWT"}}"#), &claims(), SECRET));
        }
    }

    /// regression-for: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_algo
    #[test]
    fn a_header_without_a_string_alg_is_unauthorized() {
        for header in [r#"{"typ":"JWT"}"#, r#"{"alg":256}"#, r#"{"alg":null}"#] {
            assert_unauthorized(&sign_raw(header, &claims(), SECRET));
        }
    }

    /// regression-for: identity/jwt_test.go#TestJWTIdentifierIdentify/when_token_expired
    #[test]
    fn a_token_not_valid_yet_is_unauthorized() {
        assert_unauthorized(&sign_hs256(&json!({"ext": IDS, "nbf": unix_from_now(3600)}), SECRET));
    }

    /// regression-for: identity/jwt_test.go#TestJWTIdentifierIdentify/when_token_expired
    #[test]
    fn a_non_numeric_exp_is_unauthorized() {
        let exp = unix_from_now(3600);
        for value in [json!(exp.to_string()), json!(true), json!({"at": exp})] {
            assert_unauthorized(&sign_hs256(&json!({"ext": IDS, "exp": value}), SECRET));
        }
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
