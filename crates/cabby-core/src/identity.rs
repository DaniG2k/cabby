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

fn welcome_message(sid: &str) -> String {
    format!("{{\"type\":\"welcome\",\"sid\":\"{sid}\"}}")
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
                identifier: format!("{{\"sid\":\"{sid}\"}}"),
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

/// Parses and verifies an HMAC-signed JWT as golang-jwt v5 `jwt.Parse` does with a
/// key function accepting only HMAC methods: structure, header and claims, the
/// signing method, the signature segment, the key function, the signature, then
/// the time claims (`exp` and `nbf`, no leeway). Errors carry golang-jwt's text.
fn verify_hmac_jwt(token: &str, secret: &[u8], now: SystemTime) -> Result<Map<String, Value>, JwtError> {
    let parts: Vec<&str> = token.split('.').collect();
    let [header, payload, signature] = parts[..] else {
        return Err(jwt_error(ERR_TOKEN_MALFORMED, "token contains an invalid number of segments", &[]));
    };

    let header_bytes =
        decode_segment(header).map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not base64 decode header", &[&e]))?;
    let header_map = decode_object(&header_bytes, "map[string]interface {}")
        .map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not JSON decode header", &[&e]))?;
    let claims_bytes =
        decode_segment(payload).map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not base64 decode claim", &[&e]))?;
    let claims = decode_object(&claims_bytes, "jwt.MapClaims")
        .map_err(|e| jwt_error(ERR_TOKEN_MALFORMED, "could not JSON decode claim", &[&e]))?;

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
