//! wsdirector scenario steps (wsdirector-core 1.1.1, MIT, © palkan), as used by
//! the upstream `features/*.testfile` scenarios.
//!
//! * [`Expect`]: a `receive` expectation, `data` (exact) or `data>` (partial),
//!   with wsdirector's matching rules ported literally (see [`matches`] and
//!   [`partially_matches`]).
//! * [`Client`]: one scenario client. `receive` scans the mailbox and new
//!   messages for the first match, keeping non-matching messages for later
//!   steps (`Client#each_message` / `#consumed`), and fails after the step
//!   timeout (5 s by default).
//! * Action Cable protocol helpers (`protocol: "action_cable"`): the client
//!   ignores pings, waits for `welcome` on connect, `subscribe` waits for the
//!   confirmation, `perform` sends `{"command":"message",...}` and a `receive`
//!   with a channel wraps the expectation as `{"identifier":…,"message":…}`.

use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use crate::cable_client::{CableClient, ClientError, Options};

/// Default `receive` step timeout (`step.fetch("timeout", 5)`).
pub const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// `receive` expectation.
#[derive(Debug, Clone)]
pub enum Expect {
    /// `data:` (`Hash#matches?`).
    Exact(Value),
    /// `data>:` (`PartialMatcher`).
    Partial(Value),
}

impl Expect {
    pub fn matches(&self, actual: &Value) -> bool {
        match self {
            Expect::Exact(e) => matches(e, actual),
            Expect::Partial(e) => partially_matches(e, actual),
        }
    }
}

/// Coerces `other` the way the refinements do for a Hash/Array receiver: a
/// String is parsed as JSON; a non-collection is not a match.
fn coerce(other: &Value, want_object: bool) -> Option<Value> {
    match other {
        Value::Object(_) if want_object => Some(other.clone()),
        Value::Array(_) if !want_object => Some(other.clone()),
        Value::String(s) => serde_json::from_str(s).ok(),
        // `JSON.parse(non-string)` raises; the refinement rescues it as nil.
        _ => None,
    }
}

fn index(v: &Value, key: &str) -> Value {
    v.as_object().and_then(|m| m.get(key)).cloned().unwrap_or(Value::Null)
}

fn index_at(v: &Value, i: usize) -> Value {
    v.as_array().and_then(|a| a.get(i)).cloned().unwrap_or(Value::Null)
}

fn scalar_eq(a: &Value, b: &Value) -> bool {
    crate::anyt_app::ruby_eq(a, b)
}

/// `receiver.matches?(other)` from wsdirector's protocol refinements.
///
/// For a Hash receiver it iterates the keys of `other` and calls
/// `other[k].matches?(self[k])` (roles swap at each level); for an Array it
/// iterates its own elements; anything else is `==`.
pub fn matches(receiver: &Value, other: &Value) -> bool {
    match receiver {
        Value::Object(r) => {
            let Some(actual) = coerce(other, true) else { return false };
            let Some(actual_map) = actual.as_object() else {
                // Parsed into a non-Hash: `actual.each_key` would raise NoMethodError.
                return false;
            };
            actual_map.iter().all(|(k, v)| matches(v, &r.get(k).cloned().unwrap_or(Value::Null)))
        }
        Value::Array(r) => {
            let Some(actual) = coerce(other, false) else { return false };
            r.iter().enumerate().all(|(i, e)| matches(e, &index_at(&actual, i)))
        }
        _ => scalar_eq(receiver, other),
    }
}

/// `receiver.partially_matches?(other)`: every key of a Hash receiver (every
/// element of an Array receiver) partially matches; anything else is `==`.
pub fn partially_matches(receiver: &Value, other: &Value) -> bool {
    match receiver {
        Value::Object(r) => {
            let Some(actual) = coerce(other, true) else { return false };
            r.iter().all(|(k, v)| partially_matches(v, &index(&actual, k)))
        }
        Value::Array(r) => {
            let Some(actual) = coerce(other, false) else { return false };
            r.iter().enumerate().all(|(i, e)| partially_matches(e, &index_at(&actual, i)))
        }
        _ => scalar_eq(receiver, other),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StepError {
    /// `NoMessageError`.
    NoMessage(String),
    /// `UnmatchedExpectationError`.
    Unmatched { expected: String, got: Value },
    /// Connection or protocol failure.
    Client(String),
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepError::NoMessage(e) => write!(f, "Expected to receive {e} but nothing has been received"),
            StepError::Unmatched { expected, got } => {
                write!(f, "Action failed: #receive\n   -- expected: {expected}\n   ++ got: {got}")
            }
            StepError::Client(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StepError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// wsdirector's base protocol: no subprotocol, nothing ignored.
    Base,
    /// `protocol: "action_cable"`.
    ActionCable,
}

/// Connection options (`connection_options`).
#[derive(Debug, Clone, Default)]
pub struct Connection {
    /// `url:` (otherwise the scenario's URL).
    pub url: Option<String>,
    /// `query:` (already URL-encoded `k=v` pairs joined by `&`).
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    /// `subprotocol:`; Action Cable defaults to `actioncable-v1-json`.
    pub subprotocol: Option<String>,
}

pub struct Client {
    inner: CableClient,
    mailbox: Vec<Value>,
    protocol: Protocol,
}

fn escape(s: &str) -> String {
    // CGI.escape
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => (b as char).to_string(),
            b' ' => "+".to_owned(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl Client {
    /// Connects (`init_client`); the Action Cable protocol then waits for
    /// `welcome` like wsdirector does.
    pub fn connect(url: &str, protocol: Protocol, conn: &Connection) -> Result<Self, StepError> {
        let url = conn.url.clone().unwrap_or_else(|| url.to_owned()).replacen("http://", "ws://", 1);
        let qs = conn.query.iter().map(|(k, v)| format!("{k}={}", escape(v))).collect::<Vec<_>>().join("&");
        let subprotocol = match (protocol, &conn.subprotocol) {
            (_, Some(p)) => p.clone(),
            (Protocol::ActionCable, None) => "actioncable-v1-json".into(),
            (Protocol::Base, None) => String::new(),
        };
        let ignore = if protocol == Protocol::ActionCable { vec!["ping".to_owned()] } else { Vec::new() };
        let opts = Options { ignore, qs, cookies: String::new(), headers: conn.headers.clone(), protocol: subprotocol };
        let inner = CableClient::connect(&url, opts)
            .map_err(|e| StepError::Client(format!("Failed to connect to {url}: {e}")))?;
        let mut client = Self { inner, mailbox: Vec::new(), protocol };
        if protocol == Protocol::ActionCable {
            client.receive(&Expect::Partial(json!({"type": "welcome"})))?;
        }
        Ok(client)
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// `receive` step with the default timeout.
    pub fn receive(&mut self, expected: &Expect) -> Result<Value, StepError> {
        self.receive_within(expected, STEP_TIMEOUT)
    }

    pub fn receive_within(&mut self, expected: &Expect, timeout: Duration) -> Result<Value, StepError> {
        if let Some(i) = self.mailbox.iter().position(|m| expected.matches(m)) {
            return Ok(self.mailbox.remove(i));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.inner.receive_within(left) {
                Ok(msg) => {
                    if expected.matches(&msg) {
                        return Ok(msg);
                    }
                    self.mailbox.push(msg);
                }
                Err(ClientError::Timeout) => break,
                Err(e) => {
                    return match self.mailbox.last() {
                        Some(got) => Err(StepError::Unmatched { expected: format!("{expected:?}"), got: got.clone() }),
                        None => Err(StepError::Client(e.to_string())),
                    };
                }
            }
        }
        match self.mailbox.last() {
            Some(got) => Err(StepError::Unmatched { expected: format!("{expected:?}"), got: got.clone() }),
            None => Err(StepError::NoMessage(format!("{expected:?}"))),
        }
    }

    /// `send` step (`data` serialized as JSON).
    pub fn send(&mut self, data: &Value) {
        self.inner.send(data);
    }

    /// `subscribe` step: waits for the exact confirmation.
    pub fn subscribe(&mut self, channel: &str, params: &[(&str, Value)]) -> Result<String, StepError> {
        let identifier = identifier(channel, params);
        self.inner.send(&json!({"command": "subscribe", "identifier": identifier}));
        let confirm = Expect::Exact(json!({"type": "confirm_subscription", "identifier": identifier}));
        match self.receive(&confirm) {
            Ok(_) => Ok(identifier),
            Err(StepError::Unmatched { got, .. }) if got.to_string().contains("reject_subscription") => {
                Err(StepError::Client(format!("Subscription rejected to {identifier}")))
            }
            Err(e) => Err(e),
        }
    }

    /// `perform` step: `data.merge(action:)` sent as a `message` command.
    pub fn perform(&mut self, channel: &str, params: &[(&str, Value)], action: &str, data: &Value) {
        let identifier = identifier(channel, params);
        let mut data = data.as_object().cloned().unwrap_or_default();
        data.insert("action".into(), json!(action));
        let data = Value::Object(data).to_string();
        self.inner.send(&json!({"command": "message", "data": data, "identifier": identifier}));
    }

    /// A Action Cable `receive` step with `channel:`: the expectation becomes
    /// `{"identifier":…,"message":<data>}` plus any of `offset`/`stream_id`/`epoch`.
    pub fn receive_channel(
        &mut self,
        channel: &str,
        params: &[(&str, Value)],
        partial: bool,
        message: Value,
        extra: &[(&str, Value)],
    ) -> Result<Value, StepError> {
        let mut m = Map::new();
        m.insert("identifier".into(), json!(identifier(channel, params)));
        m.insert("message".into(), message);
        for (k, v) in extra {
            m.insert((*k).into(), v.clone());
        }
        let expected = if partial { Expect::Partial(Value::Object(m)) } else { Expect::Exact(Value::Object(m)) };
        self.receive(&expected)
    }

    pub fn into_inner(self) -> CableClient {
        self.inner
    }
}

/// `(params || {}).merge(channel: channel).to_json`: parameters first, then
/// `channel`, in Ruby hash order.
pub fn identifier(channel: &str, params: &[(&str, Value)]) -> String {
    let mut parts: Vec<String> = params.iter().map(|(k, v)| format!("{}:{v}", Value::String((*k).into()))).collect();
    parts.push(format!("\"channel\":{}", Value::String(channel.into())));
    format!("{{{}}}", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_matching_follows_the_refinements() {
        // Every key of the actual message must be expected with an equal value.
        assert!(matches(&json!({"type": "welcome", "sid": "x"}), &json!({"type": "welcome"})));
        assert!(!matches(&json!({"type": "welcome"}), &json!({"type": "welcome", "sid": "x"})));
        // Raw strings are parsed.
        assert!(matches(&json!({"a": 1}), &json!("{\"a\":1}")));
        assert!(!matches(&json!({"a": 1}), &json!("not json")));
        // Nested hashes swap roles: here the nested keys of the expectation are checked.
        assert!(!matches(&json!({"m": {"a": 1, "b": 2}}), &json!({"m": {"a": 1}})));
        assert!(matches(&json!({"m": {"a": 1}}), &json!({"m": {"a": 1, "b": 2}})));
        assert!(matches(&json!({"n": 1}), &json!({"n": 1.0})));
    }

    #[test]
    fn partial_matching_checks_expected_keys() {
        assert!(partially_matches(&json!({"type": "welcome"}), &json!({"type": "welcome", "sid": "x"})));
        assert!(partially_matches(&json!({"m": {"a": 1}}), &json!({"m": {"a": 1, "b": 2}})));
        assert!(!partially_matches(&json!({"m": {"a": 1}}), &json!({"m": {"a": 2}})));
        // A JSON string value is parsed when a Hash is expected.
        assert!(partially_matches(&json!({"data": {"a": 1}}), &json!({"data": "{\"a\":1,\"b\":2}"})));
        // A String expectation compares the raw string.
        assert!(!partially_matches(&json!({"data": "{\"a\": 1}"}), &json!({"data": "{\"a\":1}"})));
    }

    #[test]
    fn identifiers_put_params_before_the_channel() {
        assert_eq!(r#"{"channel":"BenchmarkChannel"}"#, identifier("BenchmarkChannel", &[]));
        assert_eq!(
            r#"{"signed_stream_name":"x--y","channel":"$pubsub"}"#,
            identifier("$pubsub", &[("signed_stream_name", json!("x--y"))])
        );
    }

    #[test]
    fn query_values_are_cgi_escaped() {
        assert_eq!("a+b%2Fc%3D", escape("a b/c="));
    }
}
