//! Connection identification without RPC (upstream package `identity`).
//!
//! An [`Identifier`] decides a connection from its session env alone (public
//! mode, JWT). [`IdentifiableController`] runs it before delegating to the
//! wrapped controller.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Map, Value};
use sha2::{Sha256, Sha384, Sha512};

use crate::common::{
    CommandResult, ConnectResult, Controller, ControllerError, Outcome, SessionEnv, Status, StringMap,
};
use crate::logger::{self, LogValuer, Logger};
use crate::toml_config::{self, DeValue, DecodeError, Decoder, FromToml};

const DISCONNECT_UNAUTHORIZED: &str = "{\"type\":\"disconnect\",\"reason\":\"unauthorized\",\"reconnect\":false}";
const DISCONNECT_TOKEN_EXPIRED: &str = "{\"type\":\"disconnect\",\"reason\":\"token_expired\",\"reconnect\":false}";

/// `sid` as a JSON string; `<`, `>` and `&` are written as they are, as upstream
/// does (D-076).
fn json_sid(sid: &str) -> String {
    Value::from(sid).to_string()
}

fn welcome_message(sid: &str) -> String {
    format!("{{\"type\":\"welcome\",\"sid\":{}}}", json_sid(sid))
}

/// Identifies a connection (upstream `identity.Identifier`). Returning neither a
/// result nor an error means "not identified here, pass through".
pub trait Identifier: Send + Sync {
    fn identify(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult>;
}

/// Tries identifiers in order; the first result or error wins (upstream
/// `IdentifierPipeline`).
pub struct IdentifierPipeline {
    identifiers: Vec<Arc<dyn Identifier>>,
}

impl IdentifierPipeline {
    pub fn new(identifiers: Vec<Arc<dyn Identifier>>) -> Self {
        Self { identifiers }
    }
}

impl Identifier for IdentifierPipeline {
    fn identify(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
        for identifier in &self.identifiers {
            let outcome = identifier.identify(sid, env);
            if outcome.result.is_some() || outcome.error.is_some() {
                return outcome;
            }
        }
        Outcome { result: None, error: None }
    }
}

/// Identifies every client by its session id (upstream `PublicIdentifier`).
#[derive(Debug, Clone, Default)]
pub struct PublicIdentifier;

impl PublicIdentifier {
    pub fn new() -> Self {
        Self
    }
}

impl Identifier for PublicIdentifier {
    fn identify(&self, sid: &str, _env: &SessionEnv) -> Outcome<ConnectResult> {
        Outcome {
            result: Some(ConnectResult {
                identifier: format!("{{\"sid\":{}}}", json_sid(sid)),
                transmissions: vec![welcome_message(sid)],
                status: Status::Success,
                ..Default::default()
            }),
            error: None,
        }
    }
}

/// Wraps a controller with an identifier (upstream `IdentifiableController`).
pub struct IdentifiableController {
    controller: Arc<dyn Controller>,
    identifier: Arc<dyn Identifier>,
}

impl IdentifiableController {
    pub fn new(controller: Arc<dyn Controller>, identifier: Arc<dyn Identifier>) -> Self {
        Self { controller, identifier }
    }
}

impl Controller for IdentifiableController {
    fn start(&self) -> Result<(), ControllerError> {
        self.controller.start()
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        self.controller.shutdown()
    }

    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        // Upstream hands a nil env to the identifier as is; identifiers here take a
        // value, so a missing env identifies as an empty one.
        let empty;
        let identify_env = match env {
            Some(env) => env,
            None => {
                empty = SessionEnv::default();
                &empty
            }
        };
        let outcome = self.identifier.identify(sid, identify_env);

        if let Some(error) = outcome.error {
            return Outcome { result: None, error: Some(error) };
        }

        match outcome.result {
            // Passthrough
            None => self.controller.authenticate(sid, env),
            Some(mut res) => {
                res.cstate.get_or_insert_with(StringMap::new);
                res.disconnect_interest = -1;
                Outcome { result: Some(res), error: None }
            }
        }
    }

    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.controller.subscribe(sid, env, ids, channel)
    }

    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.controller.unsubscribe(sid, env, ids, channel)
    }

    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        self.controller.perform(sid, env, ids, channel, data)
    }

    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        self.controller.disconnect(sid, env, ids, subscriptions)
    }
}

// ------------------------------------------------------------------ JWT config

/// JWT signing algorithm (upstream `jwt.SigningMethod`; only the default is
/// configurable). The identifier accepts every HMAC algorithm regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JwtAlgo {
    #[default]
    Hs256,
}

/// Upstream `Algo` is an untagged interface field, which BurntSushi/toml refuses
/// to decode into.
impl FromToml for JwtAlgo {
    fn decode_value(&mut self, _value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        Err(d.error("unsupported type jwt.SigningMethod"))
    }
}

/// Invalid TOML or a type mismatch while decoding a config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ConfigError {
    pub message: String,
}

impl From<DecodeError> for ConfigError {
    fn from(e: DecodeError) -> Self {
        Self { message: e.0 }
    }
}

/// JWT identification settings (upstream `identity.JWTConfig`, TOML table `[jwt]`).
/// `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JwtConfig {
    pub secret: String,
    /// URL query parameter carrying the token; the header is `x-<param>`.
    pub param: String,
    pub algo: JwtAlgo,
    /// Reject connections without a token.
    pub force: bool,
}

crate::toml_section!(JwtConfig, "identity.JWTConfig" {
    "secret" => secret,
    "param" => param,
    "Algo" => algo,
    "force" => force,
});

impl JwtConfig {
    /// Upstream `NewJWTConfig(secret)`.
    pub fn new(secret: &str) -> Self {
        Self { secret: secret.to_owned(), param: "jid".to_owned(), algo: JwtAlgo::Hs256, force: false }
    }

    pub fn enabled(&self) -> bool {
        !self.secret.is_empty()
    }

    pub fn header_key(&self) -> String {
        format!("x-{}", self.param).to_lowercase()
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Secret key\n");
        out.push_str(&format!("secret = \"{}\"\n", self.secret));
        out.push_str("# Parameter name (an URL query or a header name carrying a token, e.g., `x-<param>`)\n");
        out.push_str(&format!("param = \"{}\"\n", self.param));
        out.push_str("# Enfore JWT authentication\n");
        out.push_str(if self.force { "force = true\n" } else { "# force = true\n" });
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values, keeping fields the document omits
    /// (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), ConfigError> {
        toml_config::decode(self, toml)?;
        Ok(())
    }
}

// ------------------------------------------------------------------ JWT identifier

/// Identifies connections by a JWT in a header or query parameter (upstream
/// `JWTIdentifier`).
pub struct JwtIdentifier {
    secret: Vec<u8>,
    param_name: String,
    header_name: String,
    required: bool,
    /// Upstream `l.With("context", "jwt")`.
    log: Logger,
}

impl JwtIdentifier {
    pub fn new(config: &JwtConfig) -> Self {
        Self {
            secret: config.secret.as_bytes().to_vec(),
            param_name: config.param.clone(),
            header_name: config.header_key(),
            required: config.force,
            log: Logger::default().with("context", "jwt"),
        }
    }

    /// The token from the `x-<param>` header, else from the `<param>` query
    /// parameter (`""` when absent).
    fn find_token(&self, env: &SessionEnv) -> Result<String, ControllerError> {
        if let Some(token) = env.headers.as_ref().and_then(|h| h.get(&self.header_name))
            && !token.is_empty()
        {
            return Ok(token.clone());
        }

        let raw_query = url_raw_query(&env.url)?;
        let params = parse_query(raw_query).map_err(ControllerError::new)?;
        Ok(params.into_iter().find(|(k, _)| *k == self.param_name).map(|(_, v)| v).unwrap_or_default())
    }
}

impl Identifier for JwtIdentifier {
    fn identify(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
        let raw_token = match self.find_token(env) {
            Ok(token) => token,
            Err(error) => return Outcome { result: None, error: Some(error) },
        };

        if raw_token.is_empty() {
            if logger::enabled(logger::Level::Debug) {
                let headers: Option<std::collections::BTreeMap<String, String>> =
                    env.headers.as_ref().map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                crate::log_debug!(self.log, "no token is found",
                    "url" => logger::filtered_url(&env.url).log_value(),
                    "headers" => logger::filtered_map(headers.as_ref()).log_value());
            }
            if self.required {
                return failure(DISCONNECT_UNAUTHORIZED);
            }
            return Outcome { result: None, error: None };
        }

        let claims = match verify_hmac_jwt(&raw_token, &self.secret, SystemTime::now()) {
            Ok(claims) => claims,
            Err(JwtError::Expired) => {
                crate::log_debug!(self.log, "token has expired");
                return failure(DISCONNECT_TOKEN_EXPIRED);
            }
            Err(JwtError::Invalid(error)) => {
                crate::log_debug!(self.log, "invalid token", "error" => error);
                return failure(DISCONNECT_UNAUTHORIZED);
            }
        };

        match claims.get("ext") {
            Some(Value::String(ids)) => Outcome {
                result: Some(ConnectResult {
                    identifier: ids.clone(),
                    transmissions: vec![welcome_message(sid)],
                    status: Status::Success,
                    ..Default::default()
                }),
                error: None,
            },
            _ => Outcome {
                result: None,
                error: Some(ControllerError::new("JWT token doesn't contain identifiers: ext claim is missing")),
            },
        }
    }
}

fn failure(transmission: &str) -> Outcome<ConnectResult> {
    Outcome {
        result: Some(ConnectResult {
            status: Status::Failure,
            transmissions: vec![transmission.to_owned()],
            ..Default::default()
        }),
        error: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JwtError {
    /// The signature is valid but `exp` has passed (golang-jwt `ErrTokenExpired`).
    Expired,
    /// Any other parse, signature or claims failure, with golang-jwt's error text.
    Invalid(String),
}

/// golang-jwt `newError(message, kind, more...)`.
fn jwt_error(kind: &str, message: &str, more: &[&str]) -> JwtError {
    let mut out = kind.to_owned();
    if !message.is_empty() {
        out.push_str(": ");
        out.push_str(message);
    }
    for m in more {
        out.push_str(": ");
        out.push_str(m);
    }
    JwtError::Invalid(out)
}

const ERR_TOKEN_MALFORMED: &str = "token is malformed";
const ERR_TOKEN_UNVERIFIABLE: &str = "token is unverifiable";
const ERR_TOKEN_SIGNATURE_INVALID: &str = "token signature is invalid";
const ERR_TOKEN_INVALID_CLAIMS: &str = "token has invalid claims";

/// Signing methods golang-jwt registers (`GetSigningMethod`).
const KNOWN_METHODS: &[&str] = &[
    "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512",
    "EdDSA", "none",
];

/// golang-jwt `RawURLEncoding` segment decoding (non-strict: trailing bits are
/// ignored, `\r` and `\n` are skipped, padding is rejected). The error is Go's
/// `base64.CorruptInputError` text.
fn decode_segment(seg: &str) -> Result<Vec<u8>, String> {
    const ENGINE: GeneralPurpose = GeneralPurpose::new(
        &base64::alphabet::URL_SAFE,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireNone),
    );
    let corrupt = |at: usize| format!("illegal base64 data at input byte {at}");
    let mut last = 0;
    let mut count = 0usize;
    for (i, b) in seg.bytes().enumerate() {
        match b {
            b'\r' | b'\n' => {}
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' => {
                last = i;
                count += 1;
            }
            _ => return Err(corrupt(i)),
        }
    }
    if count % 4 == 1 {
        return Err(corrupt(last));
    }
    let decoded = if seg.contains(['\r', '\n']) {
        let cleaned: String = seg.chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
        ENGINE.decode(cleaned)
    } else {
        ENGINE.decode(seg)
    };
    decoded.map_err(|e| e.to_string())
}

/// A JSON object segment (Go `json` decoding into a map: `null` is an empty
/// map); a value of another kind fails with `encoding/json`'s type error.
fn decode_object(bytes: &[u8], go_type: &str) -> Result<Map<String, Value>, String> {
    if let Some(e) = crate::gojson::syntax::syntax_error(bytes) {
        return Err(e);
    }
    let kind = match serde_json::from_slice::<Value>(bytes).map_err(|e| e.to_string())? {
        Value::Object(map) => return Ok(map),
        Value::Null => return Ok(Map::new()),
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
        Value::Array(_) => "array",
    };
    Err(format!("json: cannot unmarshal {kind} into Go value of type {go_type}"))
}

fn verify_mac<M: Mac + KeyInit>(key: &[u8], signing_input: &[u8], signature: &[u8]) -> bool {
    let Ok(mut mac) = <M as KeyInit>::new_from_slice(key) else { return false };
    mac.update(signing_input);
    mac.verify_slice(signature).is_ok()
}

/// Longest token accepted, in bytes (D-079).
const MAX_TOKEN_BYTES: usize = 128 * 1024;

/// Longest header segment accepted, as encoded, in bytes (D-079). It names the
/// algorithm, so it is decoded before the signature can be checked.
const MAX_HEADER_SEGMENT_BYTES: usize = 8 * 1024;

/// Parses and verifies an HMAC-signed JWT as golang-jwt v5 `jwt.Parse` does with a
/// key function accepting only HMAC methods: structure, header, the signing
/// method, the signature segment, the key function, the signature, the claims,
/// then the time claims (`exp` and `nbf`, no leeway). Errors carry golang-jwt's
/// text. The claims are decoded after the signature is checked, and the sizes
/// are bounded (D-079).
fn verify_hmac_jwt(token: &str, secret: &[u8], now: SystemTime) -> Result<Map<String, Value>, JwtError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(jwt_error(ERR_TOKEN_MALFORMED, "token is too long", &[]));
    }
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(jwt_error(ERR_TOKEN_MALFORMED, "token contains an invalid number of segments", &[]));
    };
    if header.len() > MAX_HEADER_SEGMENT_BYTES {
        return Err(jwt_error(ERR_TOKEN_MALFORMED, "header is too long", &[]));
    }

    let header_bytes =
        decode_segment(header).map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not base64 decode header", &[&e]))?;
    let header_map = decode_object(&header_bytes, "map[string]interface {}")
        .map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not JSON decode header", &[&e]))?;
    let claims_bytes =
        decode_segment(payload).map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not base64 decode claim", &[&e]))?;

    let alg = match header_map.get("alg") {
        Some(Value::String(alg)) if KNOWN_METHODS.contains(&alg.as_str()) => alg.as_str(),
        Some(Value::String(_)) => {
            return Err(jwt_error(ERR_TOKEN_UNVERIFIABLE, "signing method (alg) is unavailable", &[]));
        }
        _ => return Err(jwt_error(ERR_TOKEN_UNVERIFIABLE, "signing method (alg) is unspecified", &[])),
    };
    let signature = decode_segment(signature)
        .map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not base64 decode signature", &[&e]))?;

    let signing_input = &token.as_bytes()[..header.len() + 1 + payload.len()];
    let valid = match alg {
        "HS256" => verify_mac::<Hmac<Sha256>>(secret, signing_input, &signature),
        "HS384" => verify_mac::<Hmac<Sha384>>(secret, signing_input, &signature),
        "HS512" => verify_mac::<Hmac<Sha512>>(secret, signing_input, &signature),
        // Other methods are rejected by the key function.
        other => {
            let reason = format!("unexpected signing method: {other}");
            return Err(jwt_error(ERR_TOKEN_UNVERIFIABLE, "error while executing keyfunc", &[&reason]));
        }
    };
    if !valid {
        return Err(jwt_error(ERR_TOKEN_SIGNATURE_INVALID, "", &["signature is invalid"]));
    }

    let claims = decode_object(&claims_bytes, "jwt.MapClaims")
        .map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not JSON decode claim", &[&e]))?;

    let now_nanos = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    };

    // golang-jwt's validator collects every failing claim; an expired token is
    // reported as such whatever else fails.
    let mut errors: Vec<&str> = Vec::new();
    match numeric_date(&claims, "exp") {
        Ok(Some(exp)) if now_nanos >= exp => return Err(JwtError::Expired),
        Ok(_) => {}
        Err(()) => errors.push("invalid type for claim: exp is invalid"),
    }
    match numeric_date(&claims, "nbf") {
        Ok(Some(nbf)) if now_nanos < nbf => errors.push("token is not valid yet"),
        Ok(_) => {}
        Err(()) => errors.push("invalid type for claim: nbf is invalid"),
    }
    if !errors.is_empty() {
        return Err(jwt_error(ERR_TOKEN_INVALID_CLAIMS, "", &[&errors.join(", ")]));
    }

    Ok(claims)
}

/// A NumericDate claim in Unix nanoseconds, truncated to whole seconds
/// (golang-jwt `MapClaims.parseNumericDate`; `0` counts as absent).
fn numeric_date(claims: &Map<String, Value>, key: &str) -> Result<Option<i128>, ()> {
    match claims.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => {
            let f = n.as_f64().ok_or(())?;
            if f == 0.0 {
                return Ok(None);
            }
            Ok(Some(i128::from(f.floor() as i64) * 1_000_000_000))
        }
        Some(_) => Err(()),
    }
}

// ------------------------------------------------------------------ URL query

/// The raw query of a URL (Go `url.Parse(u).RawQuery`).
fn url_raw_query(url: &str) -> Result<&str, ControllerError> {
    let parse_error = |reason: &str| ControllerError::new(format!("parse {}: {reason}", toml_config::go_quote(url)));
    if url.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(parse_error("net/url: invalid control character in URL"));
    }
    let (rest, fragment) = url.split_once('#').unwrap_or((url, ""));
    unescape(fragment, false).map_err(|e| parse_error(&e))?;
    if rest.ends_with('?') && rest.matches('?').count() == 1 {
        return Ok("");
    }
    Ok(rest.split_once('?').map_or("", |(_, q)| q))
}

/// Go `url.ParseQuery`, in document order. Parsing continues past bad pairs and
/// the first error is returned (a semicolon error overrides earlier ones, as
/// upstream).
fn parse_query(query: &str) -> Result<Vec<(String, String)>, String> {
    let mut params = Vec::new();
    let mut err: Option<String> = None;
    for pair in query.split('&') {
        if pair.contains(';') {
            err = Some("invalid semicolon separator in query".to_owned());
            continue;
        }
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match (unescape(key, true), unescape(value, true)) {
            (Ok(k), Ok(v)) => params.push((k, v)),
            (Err(e), _) | (_, Err(e)) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e),
        None => Ok(params),
    }
}

/// Go percent-decoding (`url.QueryUnescape` when `query`: `+` is a space).
fn unescape(s: &str, query: bool) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = |b: u8| (b as char).to_digit(16);
                match (bytes.get(i + 1).copied().and_then(hex), bytes.get(i + 2).copied().and_then(hex)) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                    }
                    _ => {
                        let end = (i + 3).min(bytes.len());
                        let bad = String::from_utf8_lossy(&bytes[i..end]);
                        return Err(format!("invalid URL escape {}", toml_config::go_quote(&bad)));
                    }
                }
            }
            b'+' if query => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_queries_like_go() {
        assert_eq!(
            parse_query("a=1&b=x+y%21&&c"),
            Ok(vec![("a".into(), "1".into()), ("b".into(), "x y!".into()), ("c".into(), String::new())])
        );
        assert_eq!(parse_query("a=%zz&b=1"), Err("invalid URL escape \"%zz\"".into()));
        assert_eq!(parse_query("a=1;b=2"), Err("invalid semicolon separator in query".into()));
        assert_eq!(url_raw_query("ws://x/cable?jid=1#frag").unwrap(), "jid=1");
        assert!(url_raw_query("ws://x/\ncable").is_err());
    }

    /// Deviation D-076: the session id is a JSON string in the welcome message
    /// and the public identifiers.
    #[test]
    fn deviation_d076_session_ids_are_json_strings() {
        let identify = |sid: &str| {
            let result = PublicIdentifier::new().identify(sid, &SessionEnv::default()).result.unwrap();
            (result.identifier, result.transmissions)
        };
        for sid in ["a1", r#"a"b"#, r"a\b"] {
            let (identifier, transmissions) = identify(sid);
            assert_eq!(serde_json::json!({"sid": sid}), serde_json::from_str::<Value>(&identifier).unwrap());
            let welcome: Value = serde_json::from_str(&transmissions[0]).unwrap();
            assert_eq!(serde_json::json!({"type": "welcome", "sid": sid}), welcome);
        }
        // The bytes of any other id are Go's.
        for sid in ["a1", "a<b>&c'/{}~"] {
            let welcome = format!(r#"{{"type":"welcome","sid":"{sid}"}}"#);
            assert_eq!((format!(r#"{{"sid":"{sid}"}}"#), vec![welcome]), identify(sid));
        }
    }

    /// Malformed JWT segments are rejected with golang-jwt's prefix and Go's
    /// `encoding/json` type and syntax error texts.
    #[test]
    fn jwt_json_errors_read_like_go() {
        let now = SystemTime::now();
        let token =
            |header: &[u8]| format!("{}.e30.c2ln", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header));
        assert_eq!(
            verify_hmac_jwt(&token(b"[1]"), b"s", now),
            Err(JwtError::Invalid(
                "token is malformed: could not JSON decode header: json: cannot unmarshal array into Go value of type map[string]interface {}"
                    .into()
            ))
        );
        let Err(JwtError::Invalid(syntax)) = verify_hmac_jwt(&token(b"x"), b"s", now) else {
            panic!("a malformed header is rejected");
        };
        assert_eq!(
            "token is malformed: could not JSON decode header: invalid character 'x' looking for beginning of value",
            syntax
        );
    }

    fn encode(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    const HS256: &[u8] = br#"{"alg":"HS256"}"#;

    /// A token with this header and (encoded) claims, signed with `secret`.
    fn signed(header: &[u8], claims: &str, secret: &[u8]) -> String {
        let input = format!("{}.{claims}", encode(header));
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(input.as_bytes());
        format!("{input}.{}", encode(&mac.finalize().into_bytes()))
    }

    /// Claims (encoded) of about `len` bytes, a little less.
    fn claims_of(len: usize) -> String {
        encode(format!(r#"{{"ext":"{}"}}"#, "x".repeat(len / 4 * 3 - 12)).as_bytes())
    }

    fn error(token: &str) -> String {
        match verify_hmac_jwt(token, b"s", SystemTime::now()) {
            Err(JwtError::Invalid(text)) => text,
            other => panic!("expected an invalid token, got {other:?}"),
        }
    }

    /// Pins D-079: the claims are decoded only when the signature is valid. A
    /// token whose only problem is its claims reads as golang-jwt's.
    #[test]
    fn deviation_d079_the_signature_is_checked_before_the_claims() {
        for claims in ["[1]", "x", "{\"a\":"] {
            let token = signed(HS256, &encode(claims.as_bytes()), b"other");
            assert_eq!("token signature is invalid: signature is invalid", error(&token));
        }
        assert_eq!(
            "token is malformed: could not JSON decode claim: \
             json: cannot unmarshal array into Go value of type jwt.MapClaims",
            error(&signed(HS256, &encode(b"[1]"), b"s"))
        );
        assert_eq!(
            "token is malformed: could not JSON decode claim: invalid character 'x' looking for beginning of value",
            error(&signed(HS256, &encode(b"x"), b"s"))
        );
        // The encoding of the segments is still checked first.
        assert_eq!(
            "token is malformed: could not base64 decode claim: illegal base64 data at input byte 0",
            error(&signed(HS256, "!", b"other"))
        );
    }

    /// Pins D-079: tokens up to 128 KiB are accepted, with a header segment up to 8 KiB.
    #[test]
    fn deviation_d079_token_sizes_are_bounded() {
        let now = SystemTime::now();
        let around = signed(HS256, "", b"s").len();
        let fits = signed(HS256, &claims_of(MAX_TOKEN_BYTES - around), b"s");
        assert!((MAX_TOKEN_BYTES - 16..=MAX_TOKEN_BYTES).contains(&fits.len()), "{}", fits.len());
        assert!(verify_hmac_jwt(&fits, b"s", now).is_ok());
        let long = signed(HS256, &claims_of(MAX_TOKEN_BYTES + 16 - around), b"s");
        assert!(long.len() > MAX_TOKEN_BYTES, "{}", long.len());
        assert_eq!("token is malformed: token is too long", error(&long));

        let with_header = |len: usize| {
            let header = format!(r#"{{"alg":"HS256","kid":"{}"}}"#, "x".repeat(len / 4 * 3 - 24));
            signed(header.as_bytes(), "e30", b"s")
        };
        let segment = |token: &str| token.find('.').unwrap();
        let fits = with_header(MAX_HEADER_SEGMENT_BYTES);
        assert!((MAX_HEADER_SEGMENT_BYTES - 16..=MAX_HEADER_SEGMENT_BYTES).contains(&segment(&fits)));
        assert!(verify_hmac_jwt(&fits, b"s", now).is_ok());
        let long = with_header(MAX_HEADER_SEGMENT_BYTES + 16);
        assert!(segment(&long) > MAX_HEADER_SEGMENT_BYTES);
        assert_eq!("token is malformed: header is too long", error(&long));
    }

    /// Pins D-079: a token over the limit gets the answer of any other invalid one.
    #[test]
    fn deviation_d079_a_long_token_is_unauthorized() {
        let identifier = JwtIdentifier::new(&JwtConfig::new("s"));
        let token = signed(HS256, &claims_of(MAX_TOKEN_BYTES), b"s");
        let headers = std::collections::HashMap::from([("x-jid".to_owned(), token)]);
        let outcome = identifier.identify("1", &SessionEnv { headers: Some(headers), ..Default::default() });
        assert_eq!(None, outcome.error);
        let result = outcome.result.unwrap();
        assert_eq!(Status::Failure, result.status);
        assert_eq!(vec![DISCONNECT_UNAUTHORIZED.to_owned()], result.transmissions);
    }

    /// The fixed clock of the time-claim tests.
    const NOW_SECS: u64 = 1_700_000_000;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(secs)
    }

    /// A token with this header and claims JSON, signed with MAC `M` and "s".
    fn signed_with<M: Mac + KeyInit>(header: &str, claims: &str) -> String {
        let input = format!("{}.{}", encode(header.as_bytes()), encode(claims.as_bytes()));
        let mut mac = <M as KeyInit>::new_from_slice(b"s").unwrap();
        mac.update(input.as_bytes());
        format!("{input}.{}", encode(&mac.finalize().into_bytes()))
    }

    fn hs256(claims: &str) -> String {
        signed_with::<Hmac<Sha256>>(r#"{"alg":"HS256"}"#, claims)
    }

    fn verify_at(token: &str, secs: u64) -> Result<Map<String, Value>, JwtError> {
        verify_hmac_jwt(token, b"s", at(secs))
    }

    fn invalid(text: &str) -> Result<Map<String, Value>, JwtError> {
        Err(JwtError::Invalid(text.to_owned()))
    }

    /// A token naming a method other than HMAC is rejected by the key
    /// function, whatever its signature; an unknown, missing or non-string
    /// `alg` is unverifiable.
    #[test]
    fn only_hmac_signing_methods_are_accepted() {
        let claims = r#"{"ext":"x"}"#;
        for alg in ["RS256", "RS384", "PS256", "ES256", "ES512", "EdDSA", "none"] {
            let header = format!(r#"{{"alg":"{alg}"}}"#);
            let token = signed_with::<Hmac<Sha256>>(&header, claims);
            let expected =
                format!("token is unverifiable: error while executing keyfunc: unexpected signing method: {alg}");
            assert_eq!(invalid(&expected), verify_at(&token, NOW_SECS), "{alg}");
        }
        // `alg: none` with the empty signature golang-jwt produces.
        let unsigned = format!("{}.{}.", encode(br#"{"alg":"none"}"#), encode(claims.as_bytes()));
        assert_eq!(
            invalid("token is unverifiable: error while executing keyfunc: unexpected signing method: none"),
            verify_at(&unsigned, NOW_SECS)
        );
        for header in [r#"{"alg":"XS256"}"#, r#"{"alg":"hs256"}"#] {
            let token = signed_with::<Hmac<Sha256>>(header, claims);
            assert_eq!(
                invalid("token is unverifiable: signing method (alg) is unavailable"),
                verify_at(&token, NOW_SECS)
            );
        }
        for header in [r#"{}"#, r#"{"typ":"JWT"}"#, r#"{"alg":256}"#, r#"{"alg":null}"#, r#"{"alg":["HS256"]}"#] {
            let token = signed_with::<Hmac<Sha256>>(header, claims);
            assert_eq!(
                invalid("token is unverifiable: signing method (alg) is unspecified"),
                verify_at(&token, NOW_SECS),
                "{header}"
            );
        }
    }

    /// HS384 and HS512 tokens are verified with their own MAC: the header's
    /// method must match the one the signature was made with.
    #[test]
    fn hs384_and_hs512_need_their_own_mac() {
        let claims = r#"{"ext":"x"}"#;
        assert!(verify_at(&signed_with::<Hmac<Sha384>>(r#"{"alg":"HS384"}"#, claims), NOW_SECS).is_ok());
        assert!(verify_at(&signed_with::<Hmac<Sha512>>(r#"{"alg":"HS512"}"#, claims), NOW_SECS).is_ok());
        let mismatched = [
            signed_with::<Hmac<Sha256>>(r#"{"alg":"HS384"}"#, claims),
            signed_with::<Hmac<Sha256>>(r#"{"alg":"HS512"}"#, claims),
            signed_with::<Hmac<Sha512>>(r#"{"alg":"HS256"}"#, claims),
            signed_with::<Hmac<Sha384>>(r#"{"alg":"HS512"}"#, claims),
        ];
        for token in &mismatched {
            assert_eq!(invalid("token signature is invalid: signature is invalid"), verify_at(token, NOW_SECS));
        }
    }

    /// `exp` is a NumericDate in whole seconds: a token expires at that second
    /// (no leeway), `0` counts as absent and any other JSON type is invalid.
    #[test]
    fn exp_is_checked_against_the_clock() {
        let exp = |v: &str| hs256(&format!(r#"{{"ext":"x","exp":{v}}}"#));
        let now = NOW_SECS;
        assert_eq!(Err(JwtError::Expired), verify_at(&exp(&now.to_string()), now));
        assert_eq!(Err(JwtError::Expired), verify_at(&exp(&(now - 1).to_string()), now));
        assert_eq!(Err(JwtError::Expired), verify_at(&exp(&format!("{now}.9")), now));
        assert!(verify_at(&exp(&(now + 1).to_string()), now).is_ok());
        assert!(verify_at(&exp("0"), now).is_ok());
        let invalid_exp = invalid("token has invalid claims: invalid type for claim: exp is invalid");
        for v in [format!("\"{}\"", now + 60), "true".into(), "null".into(), "{}".into(), format!("[{}]", now + 60)] {
            assert_eq!(invalid_exp, verify_at(&exp(&v), now), "exp {v}");
        }
    }

    /// `nbf` is a NumericDate in whole seconds: the token is valid from that
    /// second on; any other JSON type is invalid. An expired token is reported
    /// as expired even if `nbf` fails too.
    #[test]
    fn nbf_is_checked_against_the_clock() {
        let nbf = |v: &str| hs256(&format!(r#"{{"ext":"x","nbf":{v}}}"#));
        let now = NOW_SECS;
        assert!(verify_at(&nbf(&now.to_string()), now).is_ok());
        assert!(verify_at(&nbf(&(now - 1).to_string()), now).is_ok());
        assert!(verify_at(&nbf("0"), now).is_ok());
        let not_yet = invalid("token has invalid claims: token is not valid yet");
        assert_eq!(not_yet, verify_at(&nbf(&(now + 1).to_string()), now));
        assert_eq!(not_yet, verify_at(&nbf(&format!("{}.5", now + 1)), now));
        let invalid_nbf = invalid("token has invalid claims: invalid type for claim: nbf is invalid");
        for v in [format!("\"{now}\""), "false".into(), "null".into()] {
            assert_eq!(invalid_nbf, verify_at(&nbf(&v), now), "nbf {v}");
        }
        let both = hs256(&format!(r#"{{"ext":"x","exp":{now},"nbf":{}}}"#, now + 60));
        assert_eq!(Err(JwtError::Expired), verify_at(&both, now));
    }

    /// A valid token without a string `ext` claim is not identified: an error,
    /// no result.
    #[test]
    fn a_token_without_a_string_ext_claim_is_an_error() {
        let identifier = JwtIdentifier::new(&JwtConfig::new("s"));
        for claims in [r#"{}"#, r#"{"ext":null}"#, r#"{"ext":15}"#, r#"{"ext":{"user_id":"15"}}"#, r#"{"ext":["15"]}"#]
        {
            let headers = std::collections::HashMap::from([("x-jid".to_owned(), hs256(claims))]);
            let outcome = identifier.identify("1", &SessionEnv { headers: Some(headers), ..Default::default() });
            assert_eq!(None, outcome.result, "{claims}");
            assert_eq!(
                Some("JWT token doesn't contain identifiers: ext claim is missing".to_owned()),
                outcome.error.map(|e| e.message),
                "{claims}"
            );
        }
        let headers = std::collections::HashMap::from([("x-jid".to_owned(), hs256(r#"{"ext":""}"#))]);
        let outcome = identifier.identify("1", &SessionEnv { headers: Some(headers), ..Default::default() });
        assert_eq!(None, outcome.error);
        assert_eq!(Status::Success, outcome.result.unwrap().status);
    }

    #[test]
    fn decodes_segments_without_padding() {
        assert_eq!(decode_segment("Zm8"), Ok(b"fo".to_vec()));
        assert_eq!(decode_segment("Zm8="), Err("illegal base64 data at input byte 3".to_owned()));
        assert_eq!(decode_segment("-_8"), Ok(vec![0xfb, 0xff]));
    }

    #[test]
    fn rejects_tampered_and_expired_tokens() {
        let now = SystemTime::now();
        // {"alg":"HS256","typ":"JWT"} / {"ext":"x","exp":1} signed with "s".
        let header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        let payload = "eyJleHQiOiJ4IiwiZXhwIjoxfQ";
        let mut mac = Hmac::<Sha256>::new_from_slice(b"s").unwrap();
        mac.update(format!("{header}.{payload}").as_bytes());
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let token = format!("{header}.{payload}.{sig}");
        assert_eq!(verify_hmac_jwt(&token, b"s", now), Err(JwtError::Expired));
        assert_eq!(
            verify_hmac_jwt(&token, b"t", now),
            Err(JwtError::Invalid("token signature is invalid: signature is invalid".into()))
        );
        assert_eq!(
            verify_hmac_jwt(&format!("{token}.x"), b"s", now),
            Err(JwtError::Invalid("token is malformed: token contains an invalid number of segments".into()))
        );
        assert!(verify_hmac_jwt(&token, b"s", UNIX_EPOCH).is_ok());
    }
}
