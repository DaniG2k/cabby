//! The AnyT RPC application (`anyt --only-rpc`), reimplemented as test
//! infrastructure on top of [`FakeRpc`].
//!
//! Upstream runs a Ruby process: anyt-core 1.4.0 (MIT, © palkan) loads its test
//! files, whose `channel do … end` blocks define `Anyt::TestChannels::*` and
//! whose `connect_handler` blocks define authentication rules, on top of
//! `anyt/dummy/application.rb` (`ApplicationCable::Connection identified_by
//! :uid`, `BenchmarkChannel`), served by anycable 1.6.4 / anycable-rails 1.6.2
//! (Action Cable 7.0). This module answers `Connect`, `Command` and `Disconnect`
//! the way that stack does:
//!
//! * **Connect** (`AnyCable::Rails::Connection#handle_open`): runs every connect
//!   handler whose tag equals the `test` query parameter; any falsy handler
//!   rejects with `FAILURE` and the transmission
//!   `{"type":"disconnect","reason":"unauthorized","reconnect":false}`. Success
//!   answers `{"type":"welcome","sid":<metadata sid>}` and identifiers
//!   `{"uid":...}` (or `{}`).
//! * **Command**: `subscribe` runs `#subscribed` and transmits
//!   `confirm_subscription`, or `reject_subscription` with `stop_streams` and
//!   `FAILURE` when the channel rejects; `unsubscribe` runs `#unsubscribed` and
//!   stops all streams; `message` dispatches the `action` of the decoded `data`.
//!   Unknown channels and commands answer `ERROR` (anycable's exceptions
//!   middleware). Streams, stopped streams, transmissions, channel state
//!   (`state_attr_accessor`, `$w` whisper and `$p` presence keys in istate) and
//!   presence join/leave map onto `CommandResponse` as in anycable-core's
//!   `Socket` and `RPC::Handlers::Command`.
//! * **Disconnect** restores every listed subscription with its istate and runs
//!   its `#unsubscribed` callback.
//!
//! Broadcasts made by channels (`ActionCable.server.broadcast`) and by tests go
//! through the configured broadcast adapter ([`publish::Target`]), encoded as the
//! `anycable` gem does (`{"stream":…,"data":<ActiveSupport JSON>,"meta":…}`).
//! They are sent synchronously before the RPC reply (the Ruby HTTP adapter uses a
//! background queue; ordering per process is the same and synchronous sending
//! only removes a delay).
//!
//! One addition that is not part of AnyT: [`BARRIER_CHANNEL`], an empty channel
//! scenarios subscribe to as an in-session ordering barrier (the server handles a
//! session's commands in order, so its confirmation proves that earlier commands,
//! such as an unsubscribe, have been applied). It replaces AnyT's `sleep 1`.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use cabby_core::protos;
use serde_json::{Map, Value, json};

use crate::fake_rpc::{Call, FakeRpc, Method, Reply, Transport};
use crate::publish::Target;
use crate::sync::Watched;

/// `Anyt::TestChannels` namespace of channels defined by `channel do … end`.
pub const NAMESPACE: &str = "Anyt::TestChannels::";
/// Test-infrastructure barrier channel (see the module docs). Not part of AnyT.
pub const BARRIER_CHANNEL: &str = "Cabby::BarrierChannel";
/// `BenchmarkChannel` from `anyt/dummy/application.rb`.
pub const BENCHMARK_CHANNEL: &str = "BenchmarkChannel";

/// Fully qualified name of the AnyT channel class generated for `class_name`
/// (`channel` in feature "Single stream" is `SingleStreamChannel`).
pub fn channel(class_name: &str) -> String {
    format!("{NAMESPACE}{class_name}")
}

#[derive(Debug, Clone, Default)]
pub struct AppOptions {
    /// HTTP RPC `Authorization: Bearer` secret (`ANYCABLE_HTTP_RPC_SECRET`);
    /// requests without it get 401 like `AnyCable::HTTRPC::Server`.
    pub http_rpc_secret: Option<String>,
    /// Host of AnyT's `--target-url`, used by the `request_url` connect handler.
    pub target_host: String,
    /// Prepended to every stream name the channels and broadcasts use (empty:
    /// AnyT's names). Stream names are opaque to clients; a per-test prefix
    /// keeps servers sharing one Redis or NATS from seeing each other's
    /// streams (the Redis pub/sub subscribes to one channel per stream).
    pub stream_prefix: String,
}

struct Shared {
    options: AppOptions,
    broadcaster: Mutex<Option<Target>>,
    errors: Watched<Vec<String>>,
}

/// The running application. Dropping it stops the RPC servers.
pub struct AnytApp {
    rpc: FakeRpc,
    shared: Arc<Shared>,
}

impl AnytApp {
    pub fn start(options: AppOptions) -> Self {
        let rpc = FakeRpc::start();
        let shared = Arc::new(Shared { options, broadcaster: Mutex::new(None), errors: Watched::new(Vec::new()) });
        for method in [Method::Connect, Method::Command, Method::Disconnect] {
            let s = Arc::clone(&shared);
            rpc.on(method).reply(Reply::with(move |call| s.answer(call)));
        }
        Self { rpc, shared }
    }

    /// The fake RPC server (call log, connection counts).
    pub fn rpc(&self) -> &FakeRpc {
        &self.rpc
    }

    /// gRPC `rpc_host` (`127.0.0.1:<port>`).
    pub fn grpc_target(&self) -> String {
        self.rpc.grpc_target()
    }

    /// HTTP RPC base URL; any mount path may be appended (`/_anycable`).
    pub fn http_url(&self) -> String {
        self.rpc.http_url()
    }

    /// Sets the broadcast adapter (`ANYCABLE_BROADCAST_ADAPTER` and its URL).
    pub fn set_broadcaster(&self, target: Target) {
        *self.shared.broadcaster.lock().unwrap_or_else(|e| e.into_inner()) = Some(target);
    }

    /// Errors raised while answering calls or broadcasting from callbacks.
    pub fn errors(&self) -> Vec<String> {
        self.shared.errors.read(Clone::clone)
    }

    /// `ActionCable.server.broadcast(stream, payload)`.
    pub fn broadcast(&self, stream: &str, payload: &Value) {
        self.shared.broadcast(stream, payload, None).unwrap_or_else(|e| panic!("{e}"));
    }

    /// `AnyCable.broadcast_adapter.broadcast_command(command, **payload)`; the
    /// payload is the already ordered JSON object text.
    pub fn broadcast_command(&self, command: &str, payload_json: &str) {
        let raw = format!("{{\"command\":{},\"payload\":{payload_json}}}", json_str(command));
        self.shared.publish(&raw).unwrap_or_else(|e| panic!("{e}"));
    }

    /// `ActionCable.server.remote_connections.where(uid: uid).disconnect`
    /// (anycable-rails: a `disconnect` command with `reconnect: true`).
    pub fn remote_disconnect_uid(&self, uid: &str) {
        let identifiers = format!("{{\"uid\":{}}}", json_str(uid));
        self.broadcast_command(
            "disconnect",
            &format!("{{\"identifier\":{},\"reconnect\":true}}", json_str(&identifiers)),
        );
    }
}

// ------------------------------------------------------------------ encoding

/// `ActiveSupport::JSON.encode`: JSON with `<`, `>`, `&`, U+2028 and U+2029
/// escaped (`escape_html_entities_in_json`, on in Rails apps).
pub fn active_support_json(value: &Value) -> String {
    escape_html(&value.to_string())
}

fn escape_html(json: &str) -> String {
    // These characters only occur inside JSON strings, so replacing them keeps
    // the document equivalent.
    let mut out = String::with_capacity(json.len());
    for c in json.chars() {
        match c {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c => out.push(c),
        }
    }
    out
}

fn json_str(s: &str) -> String {
    escape_html(&Value::String(s.to_owned()).to_string())
}

/// A JSON object with keys in the given order (Ruby hashes keep insertion order;
/// `serde_json::Map` here sorts keys). Values are JSON texts.
fn ordered(pairs: &[(&str, String)]) -> String {
    let body: Vec<String> = pairs.iter().map(|(k, v)| format!("{}:{v}", json_str(k))).collect();
    format!("{{{}}}", body.join(","))
}

// ------------------------------------------------------------------ assertions

/// Ruby `==` on parsed JSON: like `Value` equality, but `1 == 1.0`.
pub fn ruby_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y || x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(a, b)| ruby_eq(a, b)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| ruby_eq(v, w)))
        }
        _ => a == b,
    }
}

/// AnyT `assert_message(expected, actual)`: `actual.slice(*expected.keys) == expected`.
pub fn message_matches(expected: &Value, actual: &Value) -> bool {
    let (Some(e), Some(a)) = (expected.as_object(), actual.as_object()) else { return false };
    e.iter().all(|(k, v)| a.get(k).is_some_and(|w| ruby_eq(v, w)))
}

#[track_caller]
pub fn assert_message(expected: &Value, actual: &Value) {
    assert!(message_matches(expected, actual), "expected a message including {expected}, got {actual}");
}

/// AnyT `assert_includes_message(collection, expected)`.
#[track_caller]
pub fn assert_includes_message(collection: &[Value], expected: &Value) {
    assert!(
        collection.iter().any(|m| message_matches(expected, m)),
        "Expecte {collection:?} to include a message matching {expected}"
    );
}

// ------------------------------------------------------------------ request (Rack env)

fn pct_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("zz"), 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The parts of `ActionDispatch::Request` the AnyT application reads, built
/// from the RPC `Env` like `AnyCable::Socket#build_rack_env`.
#[derive(Debug, Clone, Default)]
struct Request {
    url: String,
    host: String,
    /// Query parameters (Rack `parse_nested_query` for flat keys; last wins).
    params: HashMap<String, String>,
    /// Rack env header keys: `HTTP_X_API_TOKEN`.
    headers: HashMap<String, String>,
}

impl Request {
    fn from_env(env: Option<&protos::Env>) -> Self {
        let Some(env) = env else { return Self::default() };
        let url = env.url.clone();
        let rest = url.split_once("://").map_or(url.as_str(), |(_, r)| r);
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let host = match authority.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h.to_owned(),
            _ => authority.to_owned(),
        };
        let query = url.split_once('?').map(|(_, q)| q.split('#').next().unwrap_or("")).unwrap_or("");
        let mut params = HashMap::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(pct_decode(k), pct_decode(v));
        }
        let headers = env
            .headers
            .iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case("REMOTE_ADDR"))
            .map(|(k, v)| (format!("HTTP_{}", k.to_ascii_uppercase().replace('-', "_")), v.clone()))
            .collect();
        Self { url, host, params, headers }
    }

    /// `cookies[name]` (Rack `parse_cookies_header`: first occurrence wins).
    fn cookie(&self, name: &str) -> Option<String> {
        let header = self.headers.get("HTTP_COOKIE")?;
        header.split(';').map(str::trim_start).filter(|c| !c.is_empty()).find_map(|c| {
            let (k, v) = c.split_once('=').unwrap_or((c, ""));
            (k == name).then(|| pct_decode(v))
        })
    }
}

/// Ruby `present?` for an optional string.
fn present(s: Option<&str>) -> bool {
    s.is_some_and(|s| !s.trim().is_empty())
}

// ------------------------------------------------------------------ connection

/// `ApplicationCable::Connection` (`identified_by :uid`).
#[derive(Debug, Clone, Default)]
struct Connection {
    request: Request,
    uid: Option<String>,
}

impl Connection {
    /// `identifiers_json` (anycable-rails `SerializableIdentification`).
    fn identifiers_json(&self) -> String {
        match &self.uid {
            Some(uid) => ordered(&[("uid", json_str(uid))]),
            None => "{}".into(),
        }
    }

    fn from_identifiers(request: Request, identifiers: &str) -> Self {
        let ids: Map<String, Value> = serde_json::from_str(identifiers).unwrap_or_default();
        let uid = ids.get("uid").and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        });
        Self { request, uid }
    }

    /// Action Cable `connection_identifier` (identifier values joined by `:`).
    fn connection_identifier(&self) -> String {
        self.uid.clone().unwrap_or_default()
    }

    /// `Anyt::ConnectHandlers.call`: every handler tagged with `params["test"]`
    /// must be truthy.
    fn run_connect_handlers(&mut self, target_host: &str) -> bool {
        let tag = self.request.params.get("test").cloned().unwrap_or_default();
        let p = |k: &str| self.request.params.get(k).cloned();
        match tag.as_str() {
            // features/remote_disconnect_test.rb
            "uid" => {
                self.uid = p("uid");
                present(self.uid.as_deref())
            }
            // features/server_restart_test.rb and request/disconnect_reasons_test.rb
            // register the same handler twice.
            "reasons" => p("reason").as_deref() != Some("unauthorized"),
            // request/connection_test.rb
            "request_url" => self.request.url.contains("test=request_url") && self.request.host == target_host,
            "cookies" => self.request.cookie("username").as_deref() == Some("john green"),
            "headers" => self.request.headers.get("HTTP_X_API_TOKEN").map(String::as_str) == Some("abc"),
            // etc/anyt/presence_tests/{perform,subscribe}_test.rb
            "presence_perform" | "presence_subscribe" => {
                self.uid = p("user_id");
                true
            }
            _ => true,
        }
    }
}

// ------------------------------------------------------------------ socket

/// `AnyCable::Socket::State`: a string map with dirty tracking.
#[derive(Debug, Default)]
struct State {
    source: Option<HashMap<String, String>>,
    dirty: Vec<String>,
}

impl State {
    fn new(from: &HashMap<String, String>) -> Self {
        Self { source: (!from.is_empty()).then(|| from.clone()), dirty: Vec::new() }
    }

    fn read(&self, key: &str) -> Option<&str> {
        self.source.as_ref()?.get(key).map(String::as_str)
    }

    fn write(&mut self, key: &str, val: String) {
        if self.read(key) == Some(val.as_str()) {
            return;
        }
        self.dirty.push(key.to_owned());
        self.source.get_or_insert_with(HashMap::new).insert(key.to_owned(), val);
    }

    fn changed_fields(&self) -> HashMap<String, String> {
        let Some(source) = &self.source else { return HashMap::new() };
        self.dirty.iter().filter_map(|k| source.get(k).map(|v| (k.clone(), v.clone()))).collect()
    }
}

const WHISPER_KEY: &str = "$w";
const PRESENCE_KEY: &str = "$p";

#[derive(Debug, Default)]
struct Socket {
    transmissions: Vec<String>,
    start: Vec<String>,
    stop: Vec<String>,
    stop_all: bool,
    istate: State,
    presence: Option<protos::PresenceResponse>,
}

impl Socket {
    fn subscribe(&mut self, stream: &str) {
        if !self.start.iter().any(|s| s == stream) {
            self.start.push(stream.to_owned());
        }
    }

    fn unsubscribe(&mut self, stream: &str) {
        if !self.stop.iter().any(|s| s == stream) {
            self.stop.push(stream.to_owned());
        }
        if self.istate.read(WHISPER_KEY) == Some(stream) {
            self.istate.write(WHISPER_KEY, String::new());
        }
    }

    fn unsubscribe_from_all(&mut self) {
        self.stop_all = true;
        if self.istate.read(WHISPER_KEY).is_some() {
            self.istate.write(WHISPER_KEY, String::new());
        }
    }

    fn presence_join(&mut self, stream: &str, id: String, info: Option<&Value>) {
        self.subscribe(stream);
        self.istate.write(PRESENCE_KEY, stream.to_owned());
        let info = info.map(|i| match i {
            Value::String(s) => s.clone(),
            other => active_support_json(other),
        });
        self.presence =
            Some(protos::PresenceResponse { r#type: "join".into(), id, info: info.unwrap_or_default(), ttl: 0 });
    }

    fn presence_leave(&mut self, id: String) {
        self.presence = Some(protos::PresenceResponse { r#type: "leave".into(), id, ..Default::default() });
    }
}

// ------------------------------------------------------------------ channels

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ch {
    // anyt-core lib/anyt/tests/**
    ChannelState,
    Request,
    RequestA,
    RequestB,
    RequestC,
    BroadcastDataToStream,
    StreamsWithManyClients,
    MultipleStreams,
    SingleStream,
    StopStreams,
    SubscriptionAck,
    SubscriptionAckRejector,
    SubscriptionWithParams,
    SubscriptionPerform,
    SubscriptionTransmissions,
    // etc/anyt/**
    BroadcastWithOptions,
    Whisper,
    History,
    SessionCache,
    Presence,
    PresenceImplicit,
    PresenceExplicit,
    // anyt/dummy/application.rb
    Benchmark,
    // test infrastructure
    Barrier,
}

fn lookup(name: &str) -> Option<Ch> {
    if name == BENCHMARK_CHANNEL {
        return Some(Ch::Benchmark);
    }
    if name == BARRIER_CHANNEL {
        return Some(Ch::Barrier);
    }
    Some(match name.strip_prefix(NAMESPACE)? {
        "ChannelStateChannel" => Ch::ChannelState,
        "RequestChannel" => Ch::Request,
        "RequestAChannel" => Ch::RequestA,
        "RequestBChannel" => Ch::RequestB,
        "RequestCChannel" => Ch::RequestC,
        "BroadcastDataToStreamChannel" => Ch::BroadcastDataToStream,
        "StreamsWithManyClientsChannel" => Ch::StreamsWithManyClients,
        "MultipleStreamsChannel" => Ch::MultipleStreams,
        "SingleStreamChannel" => Ch::SingleStream,
        "StopStreamsChannel" => Ch::StopStreams,
        "SubscriptionAknowledgementChannel" => Ch::SubscriptionAck,
        "SubscriptionAknowledgementRejectorChannel" => Ch::SubscriptionAckRejector,
        "SubscriptionWithParamsChannel" => Ch::SubscriptionWithParams,
        "SubscriptionPerformMethodsChannel" => Ch::SubscriptionPerform,
        "SubscriptionTransmissionsChannel" => Ch::SubscriptionTransmissions,
        "BroadcastWithOptionsChannel" => Ch::BroadcastWithOptions,
        "WhisperChannel" => Ch::Whisper,
        "HistoryChannel" => Ch::History,
        "SessionCacheChannel" => Ch::SessionCache,
        "PresenceChannel" => Ch::Presence,
        "PresenceImplicitChannel" => Ch::PresenceImplicit,
        "PresenceExplicitChannel" => Ch::PresenceExplicit,
        _ => return None,
    })
}

/// One channel instance handling one RPC call (Action Cable channel + its
/// connection and AnyCable socket).
struct Chan<'a> {
    shared: &'a Shared,
    ch: Ch,
    identifier: &'a str,
    params: Map<String, Value>,
    conn: &'a Connection,
    sid: Option<&'a str>,
    socket: &'a mut Socket,
    rejected: bool,
}

impl Chan<'_> {
    fn param(&self, key: &str) -> Option<&Value> {
        self.params.get(key).filter(|v| !v.is_null())
    }

    fn stream_from(&mut self, stream: &str) {
        let stream = self.shared.stream(stream);
        self.socket.subscribe(&stream);
    }

    fn stream_from_whisper(&mut self, stream: &str) {
        let stream = self.shared.stream(stream);
        self.socket.subscribe(&stream);
        self.socket.istate.write(WHISPER_KEY, stream);
    }

    fn transmit(&mut self, message: &Value) {
        let msg = ordered(&[("identifier", json_str(self.identifier)), ("message", active_support_json(message))]);
        self.socket.transmissions.push(msg);
    }

    fn broadcast(&self, stream: &str, payload: &Value, to_others: bool) -> Result<(), String> {
        let meta = to_others.then(|| self.sid.map(|sid| ordered(&[("exclude_socket", json_str(sid))]))).flatten();
        self.shared.broadcast(stream, payload, meta)
    }

    /// `state_attr_accessor` setter: `istate[name] = serialize(val).to_json`.
    fn set_state(&mut self, name: &str, val: &Value) {
        self.socket.istate.write(name, active_support_json(val));
    }

    /// `state_attr_accessor` getter: `JSON.parse(istate[name])` when present.
    fn state(&self, name: &str) -> Value {
        self.socket
            .istate
            .read(name)
            .filter(|raw| !raw.trim().is_empty())
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or(Value::Null)
    }

    fn join_presence(&mut self, stream: Option<&str>, id: Option<String>, info: Option<Value>) -> Result<(), String> {
        let stream = match stream {
            Some(s) => self.shared.stream(s),
            None => self
                .socket
                .start
                .first()
                .cloned()
                .ok_or_else(|| "Provide a stream name for presence updates".to_owned())?,
        };
        let id = id.unwrap_or_else(|| self.conn.connection_identifier());
        self.socket.presence_join(&stream, id, info.as_ref());
        Ok(())
    }

    fn subscribed(&mut self) -> Result<(), String> {
        match self.ch {
            Ch::ChannelState => {
                let user = json!({"name": self.param("name").cloned().unwrap_or(Value::Null)});
                self.set_state("user", &user);
                self.set_state("count", &json!(1));
                self.stream_from("state_counts");
            }
            Ch::Request => {
                // `delegate :params, to: :connection`
                if self.conn.request.params.get("token").map(String::as_str) != Some("secret") {
                    self.rejected = true;
                }
            }
            Ch::RequestA => self.stream_from("request_a"),
            Ch::RequestB => self.stream_from("request_b"),
            Ch::RequestC => self.stream_from("request_c"),
            Ch::BroadcastDataToStream | Ch::StreamsWithManyClients | Ch::SingleStream => self.stream_from("a"),
            Ch::MultipleStreams | Ch::StopStreams => {
                self.stream_from("a");
                self.stream_from("b");
            }
            Ch::SubscriptionAck | Ch::SubscriptionWithParams | Ch::SubscriptionPerform | Ch::Barrier => {}
            Ch::SubscriptionAckRejector => self.rejected = true,
            Ch::SubscriptionTransmissions => {
                self.transmit(&json!("hello"));
                self.transmit(&json!("world"));
            }
            Ch::BroadcastWithOptions | Ch::Presence => self.stream_from("a"),
            Ch::Whisper => self.stream_from_whisper("a"),
            Ch::History => self.stream_from("history_a"),
            Ch::SessionCache => self.stream_from("cache_a"),
            Ch::PresenceImplicit => {
                self.stream_from("a");
                if present(self.conn.uid.as_deref()) {
                    // user_presence_info: {name: connection.params["name"]}
                    let name = self.conn.request.params.get("name").cloned();
                    self.join_presence(None, None, Some(json!({ "name": name })))?;
                }
            }
            Ch::PresenceExplicit => {
                self.stream_from("b");
                if present(self.conn.uid.as_deref()) {
                    let p = &self.conn.request.params;
                    let id = p.get("user_id").map(|u| u.chars().rev().collect::<String>()).unwrap_or_default();
                    let info = json!({"whois": p.get("name")});
                    self.join_presence(Some("b_p"), Some(id), Some(info))?;
                }
            }
            Ch::Benchmark => {
                let id = match self.param("id") {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                self.stream_from(&format!("all{id}"));
            }
        }
        Ok(())
    }

    fn unsubscribed(&mut self) -> Result<(), String> {
        match self.ch {
            Ch::ChannelState => {
                if self.param("notify_disconnect").is_some_and(|v| v != &Value::Bool(false)) {
                    let user = self.state("user");
                    let name = match user.get("name") {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Null) | None => String::new(),
                        Some(other) => other.to_string(),
                    };
                    self.broadcast("state_counts", &json!({"data": format!("user left: {name}")}), false)?;
                }
            }
            Ch::RequestA => self.broadcast("request_a", &json!({"data": "user left"}), false)?,
            Ch::RequestB => self.broadcast("request_b", &json!({"data": "user left"}), false)?,
            Ch::RequestC => {
                // "user left#{params[:id].presence}"
                let id = match self.param("id") {
                    Some(Value::String(s)) => s.trim().to_owned(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                self.broadcast("request_c", &json!({"data": format!("user left{id}")}), false)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// `perform_action`: returns whether the action exists (`processable_action?`).
    fn perform(&mut self, action: &str, mut data: Map<String, Value>) -> Result<bool, String> {
        let s = |v: Option<&Value>| match v {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        match (self.ch, action) {
            (Ch::ChannelState, "tick") => {
                let count = self.state("count").as_i64().unwrap_or(0) + 2;
                self.set_state("count", &json!(count));
                let name = self.state("user").get("name").cloned().unwrap_or(Value::Null);
                self.transmit(&json!({"count": count, "name": name}));
            }
            (Ch::StopStreams, "ping") => {
                self.broadcast(&s(data.get("name")), &json!({"reply": "pong"}), false)?;
            }
            (Ch::StopStreams, "unfollow") => {
                let name = s(data.get("name"));
                let stream = self.shared.stream(&name);
                self.socket.unsubscribe(&stream);
            }
            (Ch::SubscriptionPerform, "tick") => self.transmit(&json!("tock")),
            (Ch::SubscriptionPerform, "echo") => {
                let text = data.get("text").cloned().unwrap_or(Value::Null);
                self.transmit(&json!({ "response": text }));
            }
            (Ch::BroadcastWithOptions, "speak") => {
                data.remove("action");
                self.broadcast("a", &Value::Object(data), true)?;
            }
            (Ch::Presence, "follow") => {
                let name = data.get("name").cloned().unwrap_or(Value::Null);
                self.join_presence(Some("a"), None, Some(json!({ "name": name })))?;
            }
            (Ch::Presence, "unfollow") => {
                let id = self.conn.connection_identifier();
                self.socket.presence_leave(id);
            }
            (Ch::Benchmark, "echo") => {
                // ECHO_DELAY (ANYCABLE_BENCHMARK_ECHO_DELAY) is unset: no default delay.
                let delay = data.get("delay").and_then(Value::as_f64).unwrap_or(0.0);
                if delay > 0.0 {
                    std::thread::sleep(std::time::Duration::from_secs_f64(delay.min(10.0)));
                }
                self.transmit(&Value::Object(data));
            }
            (Ch::Benchmark, "broadcast") => {
                let id = match self.param("id") {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                self.broadcast(&format!("all{id}"), &Value::Object(data.clone()), false)?;
                data.insert("action".into(), json!("broadcastResult"));
                self.transmit(&Value::Object(data));
            }
            (Ch::Benchmark, "counter") => {
                let num = data.get("num").and_then(Value::as_i64).unwrap_or(100);
                for i in 0..num {
                    self.broadcast("all", &json!({"text": format!("Count: {i}")}), false)?;
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

// ------------------------------------------------------------------ RPC handlers

fn success() -> i32 {
    protos::Status::Success as i32
}
fn failure() -> i32 {
    protos::Status::Failure as i32
}
fn error() -> i32 {
    protos::Status::Error as i32
}

impl Shared {
    fn stream(&self, name: &str) -> String {
        format!("{}{name}", self.options.stream_prefix)
    }

    fn publish(&self, raw: &str) -> Result<(), String> {
        let target = self.broadcaster.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let result = match target {
            Some(t) => t.publish(raw).map_err(|e| format!("broadcast {raw} via {t:?} failed: {e:#}")),
            None => Err(format!("broadcast {raw} without a configured broadcast adapter")),
        };
        if let Err(e) = &result {
            self.errors.update(|v| v.push(e.clone()));
        }
        result
    }

    /// `AnyCable.broadcast(stream, ActiveSupport::JSON.encode(payload), meta)`.
    fn broadcast(&self, stream: &str, payload: &Value, meta: Option<String>) -> Result<(), String> {
        let mut pairs =
            vec![("stream", json_str(&self.stream(stream))), ("data", json_str(&active_support_json(payload)))];
        if let Some(m) = meta {
            pairs.push(("meta", m));
        }
        self.publish(&ordered(&pairs))
    }

    fn answer(&self, call: &Call) -> Reply {
        if call.transport == Transport::Http
            && let Some(secret) = &self.options.http_rpc_secret
            && call.meta("authorization") != Some(format!("Bearer {secret}").as_str())
        {
            self.errors.update(|v| v.push(format!("unauthorized HTTP RPC call: {:?}", call.request)));
            return Reply::http(401, "Unauthorized");
        }
        let sid = call.meta("sid").or_else(|| call.meta("x-anycable-meta-sid")).map(str::to_owned);
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            // Channels may broadcast (blocking I/O) while answering.
            tokio::task::block_in_place(|| match &call.request {
                crate::fake_rpc::Request::Connect(r) => Reply::from(self.connect(r, sid.as_deref())),
                crate::fake_rpc::Request::Command(r) => Reply::from(self.command(r, sid.as_deref())),
                crate::fake_rpc::Request::Disconnect(r) => Reply::from(self.disconnect(r)),
            })
        }));
        outcome.unwrap_or_else(|p| {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_default();
            self.errors.update(|v| v.push(format!("panic answering {:?}: {msg}", call.request)));
            Reply::status(tonic::Code::Internal, msg)
        })
    }

    fn connect(&self, req: &protos::ConnectionRequest, sid: Option<&str>) -> protos::ConnectionResponse {
        let mut conn = Connection { request: Request::from_env(req.env.as_ref()), uid: None };
        if !conn.run_connect_handlers(&self.options.target_host) {
            // reject_unauthorized_connection -> close(reason: "unauthorized", reconnect: false)
            let msg = ordered(&[
                ("type", json_str("disconnect")),
                ("reason", json_str("unauthorized")),
                ("reconnect", "false".into()),
            ]);
            return protos::ConnectionResponse { status: failure(), transmissions: vec![msg], ..Default::default() };
        }
        let mut welcome = vec![("type", json_str("welcome"))];
        if let Some(sid) = sid {
            welcome.push(("sid", json_str(sid)));
        }
        protos::ConnectionResponse {
            status: success(),
            identifiers: conn.identifiers_json(),
            transmissions: vec![ordered(&welcome)],
            env: Some(protos::EnvResponse::default()),
            ..Default::default()
        }
    }

    fn command(&self, msg: &protos::CommandMessage, sid: Option<&str>) -> protos::CommandResponse {
        let err = |m: String| protos::CommandResponse { status: error(), error_msg: m, ..Default::default() };
        let params: Map<String, Value> = match serde_json::from_str(&msg.identifier) {
            Ok(Value::Object(m)) => m,
            _ => return err(format!("invalid identifier: {}", msg.identifier)),
        };
        let name = params.get("channel").and_then(Value::as_str).unwrap_or_default().to_owned();
        let Some(ch) = lookup(&name) else { return err(format!("Channel not found: {name}")) };
        let env = msg.env.as_ref();
        let conn = Connection::from_identifiers(Request::from_env(env), &msg.connection_identifiers);
        let mut socket =
            Socket { istate: State::new(&env.map(|e| e.istate.clone()).unwrap_or_default()), ..Default::default() };
        let mut chan = Chan {
            shared: self,
            ch,
            identifier: &msg.identifier,
            params,
            conn: &conn,
            sid,
            socket: &mut socket,
            rejected: false,
        };
        let outcome: Result<bool, String> = match msg.command.as_str() {
            "subscribe" => chan.subscribed().and_then(|()| {
                if chan.rejected {
                    // reject_subscription: remove_subscription (unsubscribe callbacks,
                    // stop_all_streams), then transmit the rejection.
                    chan.unsubscribed()?;
                    chan.socket.unsubscribe_from_all();
                    let m = ordered(&[
                        ("identifier", json_str(chan.identifier)),
                        ("type", json_str("reject_subscription")),
                    ]);
                    chan.socket.transmissions.push(m);
                    Ok(false)
                } else {
                    let m = ordered(&[
                        ("identifier", json_str(chan.identifier)),
                        ("type", json_str("confirm_subscription")),
                    ]);
                    chan.socket.transmissions.push(m);
                    Ok(true)
                }
            }),
            "unsubscribe" => chan.unsubscribed().map(|()| {
                chan.socket.unsubscribe_from_all();
                true
            }),
            "message" => {
                let data: Map<String, Value> = serde_json::from_str(&msg.data).unwrap_or_default();
                let action = match data.get("action") {
                    Some(Value::String(a)) if !a.trim().is_empty() => a.clone(),
                    _ => "receive".to_owned(),
                };
                // An unknown action is logged by Action Cable; the command still succeeds.
                chan.perform(&action, data).map(|_| true)
            }
            // anycable-rails `Subscriptions#whisper`: a fresh channel instance has no
            // whisper stream (the server handles whispers through istate `$w`).
            "whisper" => Err("Whispering stream is not set".to_owned()),
            other => Err(format!("Unknown command: {other}")),
        };
        match outcome {
            Ok(ok) => protos::CommandResponse {
                status: if ok { success() } else { failure() },
                disconnect: false,
                stop_streams: socket.stop_all,
                streams: socket.start.clone(),
                stopped_streams: socket.stop.clone(),
                transmissions: socket.transmissions.clone(),
                env: Some(protos::EnvResponse { cstate: HashMap::new(), istate: socket.istate.changed_fields() }),
                presence: socket.presence.clone(),
                ..Default::default()
            },
            Err(e) => err(e),
        }
    }

    fn disconnect(&self, req: &protos::DisconnectRequest) -> protos::DisconnectResponse {
        let env = req.env.as_ref();
        let conn = Connection::from_identifiers(Request::from_env(env), &req.identifiers);
        let istates = env.map(|e| e.istate.clone()).unwrap_or_default();
        for identifier in &req.subscriptions {
            let params: Map<String, Value> = serde_json::from_str(identifier).unwrap_or_default();
            let name = params.get("channel").and_then(Value::as_str).unwrap_or_default().to_owned();
            let Some(ch) = lookup(&name) else {
                return protos::DisconnectResponse { status: error(), error_msg: format!("Channel not found: {name}") };
            };
            let state: HashMap<String, String> =
                istates.get(identifier).and_then(|raw| serde_json::from_str(raw).ok()).unwrap_or_default();
            let mut socket = Socket { istate: State::new(&state), ..Default::default() };
            let mut chan = Chan {
                shared: self,
                ch,
                identifier,
                params,
                conn: &conn,
                sid: None,
                socket: &mut socket,
                rejected: false,
            };
            if let Err(e) = chan.unsubscribed() {
                return protos::DisconnectResponse { status: error(), error_msg: e };
            }
        }
        protos::DisconnectResponse { status: success(), ..Default::default() }
    }
}
