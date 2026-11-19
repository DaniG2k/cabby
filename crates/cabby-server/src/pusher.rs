//! Pusher protocol compatibility (upstream package `pusher`): the WebSocket
//! protocol encoder, the `$pusher` channel controller, the executor verifying
//! private/presence channel signatures, and the Pusher REST API.
//!
//! # Mounting (upstream `cli.Runner`)
//!
//! When [`Config::enabled`]:
//! * the WebSocket endpoint at `/app/<app_key>` is [`websocket_handler`];
//! * `$pusher` is routed to [`Controller`] (`router.route(CHANNEL_NAME, ...)`);
//! * the REST API ([`RestApi`]) is mounted at `/apps/<app_id>/*` on the main
//!   server ([`RestApi::into_handler`]) or, with `api_port`, served by its own
//!   [`Server`].

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use cabby_core::broadcast::{BroadcastError, Handler as BroadcastHandler};
use cabby_core::broker::Broker;
use cabby_core::common::{
    self, CommandResult, ConnectResult, Controller as NodeController, ControllerError, Message, MessageData, Outcome,
    PresenceEvent, Reply, SessionEnv, Status, StreamMessage, StreamMessageMetadata, StringMap,
};
use cabby_core::encoders::{EncodeError, EncodedMessageJson, Encoder as NodeEncoder, SentFrame};
use cabby_core::gojson;
use cabby_core::logger::{Attr, Level, LogValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::node::{Executor as NodeExecutor, Node, NodeError, Session, SessionOption};
use crate::server::{self, HttpServer, ServerError};
use crate::ws::{self, SharedHeadersExtractor, WebsocketHandler};

/// Default REST API body limit (upstream `defaultMaxBodySize`, 1 MB).
pub const DEFAULT_MAX_BODY_SIZE: i64 = 1_048_576;

fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("context", LogValue::String("pusher".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

/// Pusher settings (upstream `pusher.Config`). TOML keys follow the upstream tags.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub app_id: String,
    pub app_key: String,
    pub secret: String,
    /// Port for the Pusher HTTP API (0: the main server port).
    pub api_port: i64,
    /// Upstream `AddCORSHeaders` (no TOML tag upstream: matched by field name).
    #[serde(rename = "AddCORSHeaders")]
    pub add_cors_headers: bool,
    /// Upstream `CORSHosts` (comma-separated).
    #[serde(rename = "CORSHosts")]
    pub cors_hosts: String,
    pub max_body_size: i64,
}

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self { max_body_size: DEFAULT_MAX_BODY_SIZE, ..Default::default() }
    }

    pub fn enabled(&self) -> bool {
        !self.app_id.is_empty() && !self.app_key.is_empty()
    }

    /// The WebSocket path (upstream `fmt.Sprintf("/app/%s", AppKey)`).
    pub fn websocket_path(&self) -> String {
        format!("/app/{}", self.app_key)
    }

    /// The REST API mount pattern (upstream `fmt.Sprintf("/apps/%s/*", AppID)`).
    pub fn api_path(&self) -> String {
        format!("/apps/{}/*", self.app_id)
    }

    /// Renders the settings as an annotated TOML section body (upstream `ToToml`).
    pub fn to_toml(&self) -> String {
        fn optional(out: &mut String, key: &str, value: &str) {
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }

        let mut out = String::new();
        out.push_str("# Pusher application ID\n");
        optional(&mut out, "app_id", &self.app_id);
        out.push_str("# Pusher application authentication key\n");
        optional(&mut out, "app_key", &self.app_key);
        out.push_str("# The secret key for Pusher clients\n");
        optional(&mut out, "secret", &self.secret);
        out.push_str("# Port for Pusher HTTP API (0 = use the main server port)\n");
        if self.api_port == 0 {
            out.push_str("# api_port = 0\n");
        } else {
            out.push_str(&format!("api_port = {}\n", self.api_port));
        }
        out.push_str("# Maximum allowed size of the HTTP API request body (in bytes)\n");
        out.push_str(&format!("max_body_size = {}\n", self.max_body_size));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), String> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| e.0)?;
        Ok(())
    }
}

// Untagged upstream fields decode by their Go names (case-insensitively).
cabby_core::toml_section!(Config, "pusher.Config" {
    "app_id" => app_id,
    "app_key" => app_key,
    "secret" => secret,
    "api_port" => api_port,
    "AddCORSHeaders" => add_cors_headers,
    "CORSHosts" => cors_hosts,
    "max_body_size" => max_body_size,
});

// --- Socket IDs (pusher.go) ---

/// Whether `uid` matches `\A\d+\.\d+\z` (Go `\d` is ASCII).
fn valid_socket_id(uid: &str) -> bool {
    match uid.split_once('.') {
        Some((a, b)) => {
            let digits = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
            digits(a) && digits(b)
        }
        None => false,
    }
}

/// Returns `uid` if it is a valid Pusher socket ID (`<digits>.<digits>`), or a newly
/// generated one: `<unix seconds>.<random fraction of 10^6>` (upstream `GenerateSocketID`).
pub fn generate_socket_id(uid: &str) -> String {
    if valid_socket_id(uid) {
        return uid.to_owned();
    }
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let mut bytes = [0u8; 4];
    // Upstream ignores a failure of the random source too (the bytes stay zero).
    let _ = getrandom::fill(&mut bytes);
    let random = f64::from(u32::from_be_bytes(bytes)) / 4_294_967_296.0 * 1_000_000.0;
    format!("{now}.{random:.0}")
}

// --- Verifier (verifier.go) ---

/// Constant-time comparison (Go `hmac.Equal`).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 15)] as char);
    }
    out
}

/// HMAC-SHA256 signatures for channel auth and the REST API (upstream `Verifier`).
/// `app_id` is the prefix of channel auth strings (the application key, as the
/// server constructs it); `key` is the HMAC secret.
#[derive(Debug, Clone)]
pub struct Verifier {
    app_id: String,
    key: String,
}

impl Verifier {
    pub fn new(app_id: &str, key: &str) -> Self {
        Self { app_id: app_id.to_owned(), key: key.to_owned() }
    }

    /// `<socket>:<channel>` signed as `<app_id>:<hex hmac>`.
    pub fn verify_channel(&self, socket: &str, channel: &str, signature: &str) -> bool {
        self.verify_string(&format!("{socket}:{channel}"), signature)
    }

    /// `<socket>:<channel>:<channel_data>` signed as `<app_id>:<hex hmac>`.
    pub fn verify_presence_channel(&self, socket: &str, channel: &str, info: &str, signature: &str) -> bool {
        self.verify_string(&format!("{socket}:{channel}:{info}"), signature)
    }

    /// `<socket>::user::<user_info>` signed as `<app_id>:<hex hmac>`.
    pub fn verify_user(&self, socket: &str, user_info: &str, signature: &str) -> bool {
        self.verify_string(&format!("{socket}::user::{user_info}"), signature)
    }

    fn verify_string(&self, input: &str, signature: &str) -> bool {
        let calculated = format!("{}:{}", self.app_id, self.sign(input));
        constant_time_eq(calculated.as_bytes(), signature.as_bytes())
    }

    /// Whether `signature` is the hex HMAC of `input` (REST API requests).
    pub fn verify(&self, input: &str, signature: &str) -> bool {
        constant_time_eq(self.sign(input).as_bytes(), signature.as_bytes())
    }

    /// The hex HMAC-SHA256 of `input`.
    pub fn sign(&self, input: &str) -> String {
        use hmac::{Hmac, KeyInit, Mac};

        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(self.key.as_bytes()).expect("HMAC accepts any key length");
        mac.update(input.as_bytes());
        hex(&mac.finalize().into_bytes())
    }
}

// --- Protocol (encoder.go) ---

/// Encoder identifier (upstream `encoderID`).
pub const ENCODER_ID: &str = "pusher7";

/// Identifier channel for Pusher subscriptions (upstream `ChannelName`).
pub const CHANNEL_NAME: &str = "$pusher";

pub const CONNECTION_ESTABLISHED_TYPE: &str = "pusher:connection_established";
pub const SUBSCRIBE_TYPE: &str = "pusher:subscribe";
pub const SUBSCRIPTION_SUCCEEDED_TYPE: &str = "pusher_internal:subscription_succeeded";
pub const SUBSCRIPTION_ERROR_TYPE: &str = "pusher_internal:subscription_error";
pub const UNSUBSCRIBE_TYPE: &str = "pusher:unsubscribe";
pub const EVENT_TYPE: &str = "pusher:event";
pub const CLIENT_TYPE_PREFIX: &str = "client-";
pub const PING_TYPE: &str = "pusher:ping";
pub const PONG_TYPE: &str = "pusher:pong";
pub const MEMBER_ADDED_TYPE: &str = "pusher_internal:member_added";
pub const MEMBER_REMOVED_TYPE: &str = "pusher_internal:member_removed";
pub const ERROR_TYPE: &str = "pusher:error";

pub const PING_PAYLOAD: &str = "{\"event\":\"pusher:ping\",\"data\":{}}";
pub const PONG_PAYLOAD: &str = "{\"event\":\"pusher:pong\"}";

/// Error codes: 4009 (no reconnect), 4200 (reconnect).
pub const ERROR_UNAUTHORIZED: i64 = 4009;
pub const ERROR_GENERIC: i64 = 4200;

/// Payload of `pusher:subscribe` / `pusher:unsubscribe` (upstream `PusherSubscriptionData`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PusherSubscriptionData {
    pub channel: String,
    pub auth: String,
    pub channel_data: String,
}

/// A `client-*` event (whisper) as decoded (upstream `PusherClientEvent`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PusherClientEvent {
    pub event: String,
    pub channel: String,
    /// Go `interface{}`.
    pub data: Option<serde_json::Value>,
}

impl PusherClientEvent {
    /// `utils.ToJSON(event)` as a JSON value (what the node whispers).
    fn to_json_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("event".into(), Value::String(self.event.clone()));
        m.insert("channel".into(), Value::String(self.channel.clone()));
        m.insert("data".into(), self.data.clone().unwrap_or(Value::Null));
        Value::Object(m)
    }
}

/// Upstream `PusherMessage`: `data` and `channel` are `omitempty`.
#[derive(Serialize)]
struct PusherMessage<'a, D: Serialize> {
    event: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<D>,
    #[serde(skip_serializing_if = "str::is_empty")]
    channel: &'a str,
}

/// Upstream `PusherErrorData`.
#[derive(Serialize)]
struct PusherErrorData<'a> {
    message: &'a str,
    code: i64,
}

/// Upstream `PusherConnectionData`.
#[derive(Serialize)]
struct PusherConnectionData<'a> {
    socket_id: &'a str,
    activity_timeout: i64,
}

/// Upstream `PusherPresenceData` (`user_info` is `omitempty`).
#[derive(Serialize)]
struct PusherPresenceData<'a> {
    user_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_info: Option<&'a Value>,
}

fn frame<D: Serialize>(event: &str, data: Option<D>, channel: &str) -> SentFrame {
    SentFrame::text(gojson::to_vec(&PusherMessage { event, data, channel }))
}

fn error_frame(message: &str, code: i64) -> SentFrame {
    frame(ERROR_TYPE, Some(PusherErrorData { message, code }), "")
}

fn encode_error(msg: impl Into<String>) -> EncodeError {
    EncodeError(msg.into())
}

/// The JSON type name Go's decoder reports for a value.
fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// A struct field of `obj` matched as Go's decoder does (exactly, else
/// case-insensitively; the last matching key wins).
fn go_field<'a>(obj: &'a Map<String, Value>, name: &'static str) -> Option<&'a Value> {
    let names = [name];
    let mut found = None;
    for (k, v) in obj {
        if gojson::field_index(k, &names).is_some() {
            found = Some(v);
        }
    }
    found
}

/// A `string` struct field: absent or `null` is `""`, another type an error.
fn go_string_field(obj: &Map<String, Value>, name: &'static str, ty: &str) -> Result<String, EncodeError> {
    match go_field(obj, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(v) => Err(encode_error(format!(
            "json: cannot unmarshal {} into Go struct field {ty}.{name} of type string",
            json_kind(v)
        ))),
    }
}

/// An `interface{}` struct field (numbers as Go `float64`); `null` is `None`.
fn go_interface_field(obj: &Map<String, Value>, name: &'static str) -> Result<Option<Value>, EncodeError> {
    match go_field(obj, name) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let mut v = v.clone();
            gojson::to_go_interface(&mut v).map_err(encode_error)?;
            Ok(Some(v))
        }
    }
}

/// Decodes a struct: `Ok(None)` for JSON `null`, an error for non-objects.
fn go_struct<'a>(v: &'a Value, ty: &str) -> Result<Option<&'a Map<String, Value>>, EncodeError> {
    match v {
        Value::Null => Ok(None),
        Value::Object(m) => Ok(Some(m)),
        other => Err(encode_error(format!("json: cannot unmarshal {} into Go value of type {ty}", json_kind(other)))),
    }
}

fn parse_json(raw: &[u8]) -> Result<Value, EncodeError> {
    serde_json::from_slice(raw).map_err(|e| encode_error(e.to_string()))
}

/// The fields of a `common.Reply` the encoder reads.
#[derive(Debug, Default)]
struct ReplyFields {
    kind: String,
    identifier: String,
    message: Option<Value>,
    reason: String,
    reconnect: bool,
    sid: String,
}

impl ReplyFields {
    /// From a reply's JSON (its upstream tags).
    fn from_object(obj: &Map<String, Value>, go_numbers: bool) -> Result<Self, EncodeError> {
        const TY: &str = "common.Reply";
        let message = if go_numbers {
            go_interface_field(obj, "message")?
        } else {
            go_field(obj, "message").filter(|v| !v.is_null()).cloned()
        };
        let reconnect = match go_field(obj, "reconnect") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(v) => {
                return Err(encode_error(format!(
                    "json: cannot unmarshal {} into Go struct field {TY}.reconnect of type bool",
                    json_kind(v)
                )));
            }
        };
        Ok(ReplyFields {
            kind: go_string_field(obj, "type", TY)?,
            identifier: go_string_field(obj, "identifier", TY)?,
            message,
            reason: go_string_field(obj, "reason", TY)?,
            reconnect,
            sid: go_string_field(obj, "sid", TY)?,
        })
    }
}

/// The stream of a `$pusher` identifier (upstream `identifierToChannel`).
fn identifier_to_channel(id: &str) -> Result<String, EncodeError> {
    let [channel, stream] =
        gojson::decode_string_fields(id, "struct { Channel string; Stream string }", &["channel", "stream"])
            .map_err(encode_error)?;
    if channel != CHANNEL_NAME {
        return Err(encode_error(format!("invalid channel name: {channel}")));
    }
    Ok(stream)
}

/// The `$pusher` identifier of a Pusher channel (upstream `channelToIdentifier`).
pub fn channel_to_identifier(channel: &str) -> String {
    #[derive(Serialize)]
    struct Identifier<'a> {
        channel: &'a str,
        stream: &'a str,
    }
    gojson::to_string(&Identifier { channel: CHANNEL_NAME, stream: channel })
}

/// Pusher protocol encoder (upstream `pusher.Encoder`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Encoder {
    /// Seconds advertised in `pusher:connection_established`.
    pub activity_timeout: i64,
}

impl Encoder {
    /// Upstream `NewEncoder()` (activity timeout 30).
    pub fn new() -> Self {
        Self { activity_timeout: 30 }
    }

    /// Upstream `Encode`: `Ok(None)` is upstream's nil frame (a broadcast without
    /// an event name, which Pusher clients cannot receive).
    pub fn encode_message(&self, msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        let mtype = msg.get_type();

        if mtype == common::PING_TYPE {
            return Ok(Some(SentFrame::text(PING_PAYLOAD.as_bytes().to_vec())));
        }
        if mtype == common::PONG_TYPE {
            return Ok(Some(SentFrame::text(PONG_PAYLOAD.as_bytes().to_vec())));
        }

        let value = msg.to_json_value();
        let obj = value.as_object().ok_or_else(|| encode_error(format!("unknown message type: {value}")))?;
        let reply = ReplyFields::from_object(obj, false)?;
        self.encode_reply(&mtype, &reply)
    }

    /// Upstream `EncodeTransmission`: the raw reply decoded as `common.Reply`.
    pub fn encode_raw_transmission(&self, raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        let value = parse_json(raw.as_bytes())?;
        let reply = match go_struct(&value, "common.Reply")? {
            Some(obj) => ReplyFields::from_object(obj, true)?,
            None => ReplyFields::default(),
        };
        let kind = reply.kind.clone();
        self.encode_reply(&kind, &reply)
    }

    fn encode_reply(&self, mtype: &str, r: &ReplyFields) -> Result<Option<SentFrame>, EncodeError> {
        // Transmissions reach this through their `type`, as upstream's `Encode(&msg)`.
        if mtype == common::PING_TYPE {
            return Ok(Some(SentFrame::text(PING_PAYLOAD.as_bytes().to_vec())));
        }
        if mtype == common::PONG_TYPE {
            return Ok(Some(SentFrame::text(PONG_PAYLOAD.as_bytes().to_vec())));
        }

        if mtype == common::DISCONNECT_TYPE {
            let code = if r.reconnect { ERROR_GENERIC } else { ERROR_UNAUTHORIZED };
            return Ok(Some(error_frame(&r.reason, code)));
        }

        if r.kind == common::CONFIRMED_TYPE {
            let channel = identifier_to_channel(&r.identifier)?;
            let empty = Value::String("{}".into());
            let data = r.message.as_ref().unwrap_or(&empty);
            return Ok(Some(frame(SUBSCRIPTION_SUCCEEDED_TYPE, Some(data), &channel)));
        }

        if r.kind == common::REJECTED_TYPE {
            return Ok(Some(error_frame("rejected", ERROR_UNAUTHORIZED)));
        }

        if r.kind == common::WELCOME_TYPE {
            let data =
                gojson::to_string(&PusherConnectionData { socket_id: &r.sid, activity_timeout: self.activity_timeout });
            return Ok(Some(frame(CONNECTION_ESTABLISHED_TYPE, Some(data), "")));
        }

        if r.kind == common::PRESENCE_TYPE {
            let invalid = || encode_error(format!("failed to encode presence event: {:?}", r.message));
            let event = r.message.as_ref().and_then(Value::as_object).ok_or_else(invalid)?;
            // Both fields must be strings.
            let user_id = event.get("id").and_then(Value::as_str).ok_or_else(invalid)?;
            let presence_type = event.get("type").and_then(Value::as_str).ok_or_else(invalid)?;

            let mut data = PusherPresenceData { user_id, user_info: None };
            let pusher_event = if presence_type == common::PRESENCE_JOIN_TYPE {
                data.user_info = event.get("info").filter(|v| !v.is_null());
                MEMBER_ADDED_TYPE
            } else if presence_type == common::PRESENCE_LEAVE_TYPE {
                MEMBER_REMOVED_TYPE
            } else {
                ""
            };

            let channel = identifier_to_channel(&r.identifier)?;
            return Ok(Some(frame(pusher_event, Some(gojson::to_string(&data)), &channel)));
        }

        let channel = identifier_to_channel(&r.identifier)?;

        let mut event_name = "";
        let mut event_data = r.message.as_ref();

        if let Some(Value::Object(m)) = &r.message {
            if let Some(Value::String(event)) = m.get("event") {
                event_name = event;
            }
            if let Some(data) = m.get("data") {
                event_data = Some(data);
            }
        }

        if event_name.is_empty() {
            return Ok(None);
        }

        Ok(Some(frame(event_name, event_data.filter(|v| !v.is_null()), &channel)))
    }

    /// Upstream `Decode`. `Message::data` is a typed `PusherSubscriptionData`
    /// (subscribe and unsubscribe) or `PusherClientEvent` (client events).
    pub fn decode_message(&self, raw: &[u8]) -> Result<Message, EncodeError> {
        const TY: &str = "pusher.PusherMessage";

        let value = parse_json(raw)?;
        let empty = Map::new();
        let obj = go_struct(&value, TY)?.unwrap_or(&empty);
        let event = go_string_field(obj, "event", TY)?;
        // Upstream decodes these too (their type errors fail the message).
        go_string_field(obj, "channel", TY)?;
        go_interface_field(obj, "data")?;

        let mut msg = Message::default();

        if event == SUBSCRIBE_TYPE || event == UNSUBSCRIBE_TYPE {
            const SUB: &str = "pusher.PusherSubscriptionData";
            // A message without data is a decode error (D-029).
            let data = go_field(obj, "data").unwrap_or(&Value::Null);
            let data = go_struct(data, SUB)?.ok_or_else(|| encode_error("missing subscription data"))?;
            let sub = PusherSubscriptionData {
                channel: go_string_field(data, "channel", SUB)?,
                auth: go_string_field(data, "auth", SUB)?,
                channel_data: go_string_field(data, "channel_data", SUB)?,
            };
            msg.identifier = channel_to_identifier(&sub.channel);
            msg.command = if event == SUBSCRIBE_TYPE { "subscribe" } else { "unsubscribe" }.to_owned();
            msg.data = Some(MessageData::Typed(Arc::new(sub)));
        }

        if event.starts_with(CLIENT_TYPE_PREFIX) {
            const CLIENT: &str = "pusher.PusherClientEvent";
            let client = PusherClientEvent {
                event: event.clone(),
                channel: go_string_field(obj, "channel", CLIENT)?,
                data: go_interface_field(obj, "data")?,
            };
            msg.identifier = channel_to_identifier(&client.channel);
            msg.command = "whisper".to_owned();
            msg.data = Some(MessageData::Typed(Arc::new(client)));
        }

        if event == PING_TYPE {
            msg.command = "ping".to_owned();
        }

        if event == PONG_TYPE {
            msg.command = "pong".to_owned();
        }

        if msg.command.is_empty() {
            return Err(encode_error(format!("unsupported event: {event}")));
        }

        Ok(msg)
    }
}

/// Upstream's nil frame (no error) has no equivalent in the canonical encoder
/// interface: it becomes an error, which sessions skip silently, as they skip
/// nil frames upstream.
fn no_frame() -> EncodeError {
    encode_error("encoding failed")
}

impl NodeEncoder for Encoder {
    fn id(&self) -> String {
        ENCODER_ID.to_owned()
    }

    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        self.encode_message(msg)?.ok_or_else(no_frame)
    }

    fn encode_transmission(&self, msg: &str) -> Result<SentFrame, EncodeError> {
        self.encode_raw_transmission(msg)?.ok_or_else(no_frame)
    }

    fn decode(&self, payload: &[u8]) -> Result<Message, EncodeError> {
        self.decode_message(payload)
    }
}

// --- Controller (controller.go) ---

/// Upstream `PresenceInfo` (the Pusher presence payload, struct field order).
#[derive(Serialize)]
struct PusherPresenceInfo {
    count: usize,
    ids: Vec<String>,
    hash: std::collections::BTreeMap<String, Option<Value>>,
}

#[derive(Serialize)]
struct PresenceConfirmation {
    presence: PusherPresenceInfo,
}

/// Upstream `UserPresence`, decoded from the subscription's `channel_data`.
fn decode_user_presence(raw: &str) -> Result<PresenceEvent, ControllerError> {
    const TY: &str = "pusher.UserPresence";
    let decorate = |e: EncodeError| ControllerError::new(format!("failed to unmarshal presence data, cause: {}", e.0));

    let value = parse_json(raw.as_bytes()).map_err(decorate)?;
    // `null` is a decode error (D-030).
    let obj =
        go_struct(&value, TY).map_err(decorate)?.ok_or_else(|| decorate(encode_error("presence data is null")))?;
    Ok(PresenceEvent {
        kind: common::PRESENCE_JOIN_TYPE.to_owned(),
        id: go_string_field(obj, "user_id", TY).map_err(decorate)?,
        info: go_interface_field(obj, "user_info").map_err(decorate)?,
    })
}

fn error_outcome(message: String) -> Outcome<CommandResult> {
    Outcome {
        result: Some(CommandResult { status: Status::Error, ..Default::default() }),
        error: Some(ControllerError::new(message)),
    }
}

/// Terminates `$pusher` subscriptions: public, `private-` and `presence-` channels
/// (upstream `pusher.Controller`). Authorization happens in [`Executor`].
pub struct Controller {
    broker: Option<Arc<dyn Broker>>,
    _conf: Config,
}

impl Controller {
    pub fn new(broker: Option<Arc<dyn Broker>>, conf: Config) -> Self {
        Self { broker, _conf: conf }
    }
}

impl NodeController for Controller {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        Outcome { result: None, error: None }
    }

    fn subscribe(&self, _sid: &str, env: Option<&SessionEnv>, _ids: &str, identifier: &str) -> Outcome<CommandResult> {
        let rejection = || CommandResult {
            status: Status::Failure,
            transmissions: vec![common::rejection_message(identifier)],
            ..Default::default()
        };

        let stream = match gojson::decode_string_fields(identifier, "pusher.SubscribeRequest", &["channel", "stream"]) {
            Ok([_channel, stream]) => stream,
            Err(e) => {
                return Outcome {
                    result: Some(rejection()),
                    error: Some(ControllerError::new(format!("invalid identifier, cause: {e}"))),
                };
            }
        };

        if stream.is_empty() {
            return Outcome { result: Some(rejection()), error: None };
        }

        let mut state: Option<StringMap> = None;
        let mut presence_event = None;
        let mut confirmation =
            Reply { kind: common::CONFIRMED_TYPE.to_owned(), identifier: identifier.to_owned(), ..Default::default() };

        if stream.starts_with("private-") || stream.starts_with("presence-") {
            state.get_or_insert_with(StringMap::new).insert(common::WHISPER_STREAM_STATE.to_owned(), stream.clone());
        }

        if stream.starts_with("presence-") {
            state.get_or_insert_with(StringMap::new).insert(common::PRESENCE_STREAM_STATE.to_owned(), stream.clone());

            // The join event comes from the channel state set by the executor.
            let presence_data = env.map(|e| e.get_channel_state_field(identifier, "channel_data")).unwrap_or_default();
            if !presence_data.is_empty() {
                match decode_user_presence(&presence_data) {
                    Ok(event) => presence_event = Some(event),
                    Err(e) => return error_outcome(e.message),
                }
            }

            if let Some(broker) = &self.broker {
                let info = match broker.presence_info(&stream, None) {
                    Ok(info) => info,
                    Err(e) => return error_outcome(format!("failed to load presence info, cause: {}", e.message)),
                };

                let mut pusher_info = PusherPresenceInfo {
                    count: info.total,
                    ids: vec![String::new(); info.total],
                    hash: Default::default(),
                };
                for (i, r) in info.records.iter().enumerate() {
                    // Records beyond the total are appended.
                    match pusher_info.ids.get_mut(i) {
                        Some(slot) => slot.clone_from(&r.id),
                        None => pusher_info.ids.push(r.id.clone()),
                    }
                    pusher_info.hash.insert(r.id.clone(), r.info.clone());
                }

                // Add the current presence if missing
                if let Some(event) = &presence_event
                    && !pusher_info.hash.contains_key(&event.id)
                {
                    pusher_info.ids.push(event.id.clone());
                    pusher_info.hash.insert(event.id.clone(), event.info.clone());
                    pusher_info.count += 1;
                }

                confirmation.message =
                    Some(Value::String(gojson::to_string(&PresenceConfirmation { presence: pusher_info })));
            }
        }

        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                transmissions: vec![gojson::to_string(&confirmation)],
                streams: vec![stream],
                disconnect_interest: -1,
                istate: state,
                presence: presence_event,
                ..Default::default()
            }),
            error: None,
        }
    }

    fn unsubscribe(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str) -> Outcome<CommandResult> {
        Outcome {
            result: Some(CommandResult { status: Status::Success, stop_all_streams: true, ..Default::default() }),
            error: None,
        }
    }

    fn perform(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str, _: &str) -> Outcome<CommandResult> {
        Outcome { result: None, error: None }
    }

    fn disconnect(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &[String]) -> Result<(), ControllerError> {
        Ok(())
    }
}

// --- Executor (executor.go) ---

/// Verifies `private-`/`presence-` subscription signatures before passing commands to
/// the application executor (upstream `pusher.Executor`).
pub struct Executor {
    app: Arc<dyn NodeExecutor>,
    verifier: Verifier,
}

impl Executor {
    pub fn new(app: Arc<dyn NodeExecutor>, verifier: Verifier) -> Self {
        Self { app, verifier }
    }

    fn verify_private_channel(&self, s: &Arc<Session>, msg: &Message) -> Result<(), String> {
        let channel = identifier_to_channel(&msg.identifier).map_err(|e| e.0)?;

        if channel.starts_with("private-") || channel.starts_with("presence-") {
            let data = msg
                .data
                .as_ref()
                .and_then(|d| d.downcast_ref::<PusherSubscriptionData>())
                .ok_or_else(|| "missing auth data".to_owned())?;

            let verified = if channel.starts_with("presence-") {
                let verified =
                    self.verifier.verify_presence_channel(&s.get_id(), &channel, &data.channel_data, &data.auth);
                if verified {
                    // The controller triggers the presence join event from this state
                    // (there is no other way to pass the original Pusher payload through).
                    let state = StringMap::from([("channel_data".to_owned(), data.channel_data.clone())]);
                    s.update_env(&mut |env| env.merge_channel_state(&msg.identifier, &state));
                }
                verified
            } else {
                self.verifier.verify_channel(&s.get_id(), &channel, &data.auth)
            };

            if !verified {
                return Err("invalid signature".to_owned());
            }
        }

        Ok(())
    }
}

impl NodeExecutor for Executor {
    fn handle_command(&self, session: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        if msg.command == "subscribe" {
            let mut subscribe =
                Message { command: msg.command.clone(), identifier: msg.identifier.clone(), ..Default::default() };

            if let Err(err) = self.verify_private_channel(session, msg) {
                log(Level::Debug, "pusher authorization failed", &[Attr::new("err", LogValue::String(err))]);
                subscribe.identifier = channel_to_identifier("");
            }

            return self.app.handle_command(session, &subscribe);
        }

        // The node whispers the message data as JSON: pass the client event as its
        // JSON value (upstream marshals the `*PusherClientEvent` there).
        if let Some(event) = msg.data.as_ref().and_then(|d| d.downcast_ref::<PusherClientEvent>()) {
            let whisper = Message { data: Some(MessageData::Json(event.to_json_value())), ..msg.clone() };
            return self.app.handle_command(session, &whisper);
        }

        self.app.handle_command(session, msg)
    }

    fn disconnect(&self, session: &Arc<Session>) -> Result<(), NodeError> {
        self.app.disconnect(session)
    }

    fn clock(&self) -> Option<Arc<dyn crate::node::Clock>> {
        self.app.clock()
    }
}

// --- WebSocket endpoint (cli.Runner.pusherWebsocketHandler) ---

/// Sets up a Pusher session on an upgraded connection (upstream
/// `pusherWebsocketHandler`'s session callback): the Pusher encoder and executor,
/// no server pings (Pusher clients ping), presence keepalives every half
/// `presence_ttl` seconds, the socket ID from `X-Request-ID` when valid, an
/// immediate `Authenticated` with `{"sid":"<socket id>"}` and the
/// `pusher:connection_established` welcome.
pub fn session_handler(node: Arc<Node>, conf: &Config, presence_ttl: i64) -> ws::SessionHandler {
    let executor: Arc<dyn NodeExecutor> =
        Arc::new(Executor::new(node.clone(), Verifier::new(&conf.app_key, &conf.secret)));
    let presence_interval = Duration::from_millis(u64::try_from(presence_ttl.saturating_mul(500)).unwrap_or(0));

    Arc::new(move |conn: ws::WsConnection, info: server::RequestInfo, callback: ws::SessionCallback| {
        if !node.admit_new_client() {
            crate::node::Connection::close(&conn, ws::CLOSE_TRY_AGAIN_LATER, "Try again later");
            return Ok(());
        }

        let opts = vec![
            SessionOption::Encoder(Arc::new(Encoder::new())),
            SessionOption::Executor(executor.clone()),
            // Pusher uses client-to-server pings
            SessionOption::PingInterval(Duration::ZERO),
            // Enable presence timers
            SessionOption::KeepaliveIntervals { sessions_ttl: Duration::ZERO, presence_ttl: presence_interval },
        ];

        let socket_id = generate_socket_id(&info.uid);
        let session = Session::new(&node, Arc::new(conn), &info.url, info.headers, &socket_id, opts);

        let sid = session.get_id();
        node.authenticated(&session, &format!(r#"{{"sid":"{sid}"}}"#));

        session.mark_disconnectable(false);

        session.send(&Reply { kind: common::WELCOME_TYPE.to_owned(), sid, ..Default::default() });

        session.serve(callback).map_err(|e| e.0)
    })
}

/// The Pusher WebSocket endpoint: mount it at [`Config::websocket_path`]
/// (upstream `ws.WebsocketHandler(nil, extractor, &c.WS, ...)` with
/// [`session_handler`]).
pub fn websocket_handler(
    node: Arc<Node>,
    conf: &Config,
    ws_config: &ws::Config,
    extractor: Option<SharedHeadersExtractor>,
    presence_ttl: i64,
) -> WebsocketHandler {
    ws::websocket_handler(Vec::new(), extractor, ws_config, session_handler(node, conf, presence_ttl))
}

// --- REST API (http.go) ---

/// The maximum allowed difference between `auth_timestamp` and the server time,
/// in seconds (upstream `authTimestampGracePeriod`).
const AUTH_TIMESTAMP_GRACE_PERIOD: i64 = 600;

/// A REST API request; the body is streamed so oversized or unauthenticated bodies
/// need not be read.
pub type ApiRequest = http::Request<Box<dyn Read + Send>>;

/// Upstream `PusherEvent` (the `POST .../events` body).
#[derive(Debug, Default)]
struct PusherEvent {
    name: String,
    data: String,
    channels: Vec<String>,
    channel: String,
    socket_id: String,
    info: String,
}

impl PusherEvent {
    fn decode(body: &[u8]) -> Result<Self, EncodeError> {
        const TY: &str = "pusher.PusherEvent";
        let value = parse_json(body)?;
        // `null` is a decode error (D-030).
        let obj = go_struct(&value, TY)?.ok_or_else(|| encode_error("event is null"))?;
        let channels = match go_field(obj, "channels") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    Value::Null => Ok(String::new()),
                    other => Err(encode_error(format!(
                        "json: cannot unmarshal {} into Go struct field {TY}.channels of type string",
                        json_kind(other)
                    ))),
                })
                .collect::<Result<_, _>>()?,
            Some(other) => {
                return Err(encode_error(format!(
                    "json: cannot unmarshal {} into Go struct field {TY}.channels of type []string",
                    json_kind(other)
                )));
            }
        };
        Ok(PusherEvent {
            name: go_string_field(obj, "name", TY)?,
            data: go_string_field(obj, "data", TY)?,
            channels,
            channel: go_string_field(obj, "channel", TY)?,
            socket_id: go_string_field(obj, "socket_id", TY)?,
            info: go_string_field(obj, "info", TY)?,
        })
    }
}

/// Upstream `PusherBroadcast`.
#[derive(Serialize)]
struct PusherBroadcast<'a> {
    event: &'a str,
    data: &'a str,
}

/// Go's `unhex`.
fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Percent-decoding (Go `url.QueryUnescape` with `plus`, `url.PathUnescape`-like
/// otherwise). `None` for a malformed escape.
fn unescape(s: &str, plus: bool) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hi = unhex(*bytes.get(i + 1)?)?;
                let lo = unhex(*bytes.get(i + 2)?)?;
                out.push(hi << 4 | lo);
                i += 3;
            }
            b'+' if plus => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// The first value of a query parameter (Go `r.URL.Query().Get(key)`): pairs with
/// a `;` or a malformed escape are skipped, as Go's `ParseQuery` does.
fn query_param(query: &str, key: &str) -> String {
    for pair in query.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let (Some(k), Some(v)) = (unescape(k, true), unescape(v, true)) else {
            continue;
        };
        if k == key {
            return v;
        }
    }
    String::new()
}

/// Go's `r.URL.Path` (percent-decoded).
fn request_path(uri: &http::Uri) -> String {
    let raw = uri.path();
    unescape(raw, false).unwrap_or_else(|| raw.to_owned())
}

/// The channel of `.../channels/<channel>/users` (upstream `channelUsersPathPattern`).
fn channel_users_path(path: &str) -> Option<&str> {
    let rest = path.strip_suffix("/users")?;
    let (prefix, channel) = rest.rsplit_once('/')?;
    (!channel.is_empty() && prefix.ends_with("/channels")).then_some(channel)
}

/// An authorized request, before the body is read.
enum Authorized {
    Get,
    Post { body_md5: String },
}

/// A request body as read: the bytes, or the status a failed read answers.
type BodyResult = Result<Vec<u8>, u16>;

/// A REST API response.
struct ApiResponse {
    status: u16,
    content_type: Option<&'static str>,
    body: Vec<u8>,
}

impl ApiResponse {
    fn status(status: u16) -> Self {
        Self { status, content_type: None, body: Vec::new() }
    }
}

/// Pusher HTTP API: `POST .../events` broadcasts, `GET .../channels/<presence>/users`
/// (upstream `pusher.RestAPI`).
pub struct RestApi {
    handler: Arc<dyn BroadcastHandler>,
    broker: Arc<dyn Broker>,
    conf: Config,
    verifier: Verifier,
    /// `Some(hosts)` when CORS headers are enabled; no hosts allow any origin.
    cors_hosts: Option<Vec<String>>,
}

impl RestApi {
    /// Upstream `NewRestAPI(h, b, c, l)`.
    pub fn new(handler: Arc<dyn BroadcastHandler>, broker: Arc<dyn Broker>, conf: Config) -> Self {
        let verifier = Verifier::new(&conf.app_key, &conf.secret);
        let cors_hosts = conf.add_cors_headers.then(|| {
            if conf.cors_hosts.is_empty() {
                Vec::new()
            } else {
                conf.cors_hosts.split(',').map(str::to_owned).collect()
            }
        });
        Self { handler, broker, conf, verifier, cors_hosts }
    }

    /// The request body limit (Go `http.MaxBytesReader` with `max_body_size`).
    fn body_limit(&self) -> u64 {
        u64::try_from(self.conf.max_body_size).unwrap_or(0)
    }

    /// CORS headers, method and signature checks; `Err(status)` answers right away.
    fn authorize(
        &self,
        method: &http::Method,
        uri: &http::Uri,
        req_headers: &http::HeaderMap,
        out: &mut http::HeaderMap,
    ) -> Result<Authorized, u16> {
        if let Some(hosts) = &self.cors_hosts {
            let origin = req_headers.get(http::header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or_default();
            server::write_cors_headers(out, origin, hosts);

            if method == http::Method::OPTIONS {
                return Err(200);
            }
        }

        if method != http::Method::POST && method != http::Method::GET {
            log(Level::Debug, "invalid request method", &[Attr::new("method", LogValue::String(method.to_string()))]);
            return Err(422);
        }

        // See https://pusher.com/docs/channels/library_auth_reference/rest-api/#worked-authentication-example
        let path = request_path(uri);
        let query = uri.query().unwrap_or_default();
        let key = query_param(query, "auth_key");
        let auth_timestamp = query_param(query, "auth_timestamp");
        let auth_version = query_param(query, "auth_version");
        let signature = query_param(query, "auth_signature");

        let Ok(ts) = auth_timestamp.parse::<i64>() else {
            log(Level::Debug, "invalid auth_timestamp", &[Attr::new("timestamp", LogValue::String(auth_timestamp))]);
            return Err(401);
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        // Go's int64 arithmetic wraps.
        let delta = now.wrapping_sub(ts);
        if !(-AUTH_TIMESTAMP_GRACE_PERIOD..=AUTH_TIMESTAMP_GRACE_PERIOD).contains(&delta) {
            log(
                Level::Debug,
                "auth_timestamp out of range",
                &[Attr::new("timestamp", LogValue::String(auth_timestamp)), Attr::new("delta", LogValue::Int(delta))],
            );
            return Err(401);
        }

        let body_md5 = query_param(query, "body_md5");

        let string_to_sign = if method == http::Method::POST {
            format!(
                "POST\n{path}\nauth_key={key}&auth_timestamp={auth_timestamp}&auth_version={auth_version}&body_md5={body_md5}"
            )
        } else {
            format!("GET\n{path}\nauth_key={key}&auth_timestamp={auth_timestamp}&auth_version={auth_version}")
        };

        // Verify the signature before reading the body: body_md5 is a part of the signed
        // string, so the request is authenticated without materializing the body.
        if !self.verifier.verify(&string_to_sign, &signature) {
            return Err(401);
        }

        Ok(if method == http::Method::POST { Authorized::Post { body_md5 } } else { Authorized::Get })
    }

    /// Body checks and the endpoint itself.
    fn complete(&self, auth: Authorized, uri: &http::Uri, body: impl FnOnce(u64) -> BodyResult) -> ApiResponse {
        match auth {
            Authorized::Post { body_md5 } => {
                let body = match body(self.body_limit()) {
                    Ok(body) => body,
                    Err(status) => {
                        if status == 413 {
                            log(
                                Level::Debug,
                                "request body is too large",
                                &[Attr::new("limit", LogValue::Int(self.conf.max_body_size))],
                            );
                        } else {
                            log(Level::Error, "failed to read request body", &[]);
                        }
                        return ApiResponse::status(status);
                    }
                };

                use md5::Digest as _;
                let actual_md5 = hex(&md5::Md5::digest(&body));
                if actual_md5 != body_md5 {
                    log(
                        Level::Debug,
                        "body_md5 mismatch",
                        &[
                            Attr::new("expected", LogValue::String(body_md5)),
                            Attr::new("actual", LogValue::String(actual_md5)),
                        ],
                    );
                    return ApiResponse::status(401);
                }

                self.handle_events(&body)
            }
            Authorized::Get => self.handle_get_users(&request_path(uri)),
        }
    }

    fn handle_events(&self, body: &[u8]) -> ApiResponse {
        let event = match PusherEvent::decode(body) {
            Ok(event) => event,
            Err(_) => {
                log(Level::Error, "failed to parse Pusher event", &[]);
                return ApiResponse::status(422);
            }
        };

        let channels = if event.channel.is_empty() { event.channels.clone() } else { vec![event.channel.clone()] };

        log(
            Level::Debug,
            "event received",
            &[
                Attr::new("event", LogValue::String(event.name.clone())),
                Attr::new("channels", LogValue::List(channels.iter().cloned().map(LogValue::String).collect())),
                Attr::new("info", LogValue::String(event.info.clone())),
            ],
        );

        let data = gojson::to_string(&PusherBroadcast { event: &event.name, data: &event.data });

        for channel in channels {
            let msg = StreamMessage {
                stream: channel,
                data: data.clone(),
                meta: (!event.socket_id.is_empty())
                    .then(|| StreamMessageMetadata { exclude_socket: event.socket_id.clone(), ..Default::default() }),
                ..Default::default()
            };

            // Upstream ignores the result.
            let _ = self.handler.handle_broadcast(&gojson::to_vec(&msg));
        }

        // Go's server sniffs the content type of the `{}` body.
        ApiResponse { status: 200, content_type: Some("text/plain; charset=utf-8"), body: b"{}".to_vec() }
    }

    fn handle_get_users(&self, path: &str) -> ApiResponse {
        let Some(channel) = channel_users_path(path) else {
            log(Level::Debug, "invalid path for get users", &[Attr::new("path", LogValue::String(path.to_owned()))]);
            return ApiResponse::status(404);
        };

        if !channel.starts_with("presence-") {
            log(
                Level::Debug,
                "get users is only supported for presence channels",
                &[Attr::new("channel", LogValue::String(channel.to_owned()))],
            );
            return ApiResponse::status(400);
        }

        if !self.broker.supports_presence() {
            return ApiResponse::status(501);
        }

        let info = match self.broker.presence_info(channel, None) {
            Ok(info) => info,
            Err(e) => {
                log(
                    Level::Debug,
                    "failed to get presence info",
                    &[
                        Attr::new("channel", LogValue::String(channel.to_owned())),
                        Attr::new("error", LogValue::String(e.message)),
                    ],
                );
                return ApiResponse::status(400);
            }
        };

        #[derive(Serialize)]
        struct User<'a> {
            id: &'a str,
        }
        #[derive(Serialize)]
        struct Users<'a> {
            users: Vec<User<'a>>,
        }

        let users = Users { users: info.records.iter().map(|r| User { id: &r.id }).collect() };
        ApiResponse { status: 200, content_type: Some("application/json"), body: gojson::to_vec(&users) }
    }

    /// Serves one request synchronously (upstream `RestAPI.Handler`); the body is
    /// read only after the signature checks, and at most `max_body_size + 1` bytes.
    pub fn handle(&self, req: ApiRequest) -> http::Response<Vec<u8>> {
        let (parts, mut body) = req.into_parts();
        let mut headers = http::HeaderMap::new();
        let res = match self.authorize(&parts.method, &parts.uri, &parts.headers, &mut headers) {
            Err(status) => ApiResponse::status(status),
            Ok(auth) => self.complete(auth, &parts.uri, |limit| {
                let mut buf = Vec::new();
                match (&mut body).take(limit.saturating_add(1)).read_to_end(&mut buf) {
                    Ok(_) if buf.len() as u64 > limit => Err(413),
                    Ok(_) => Ok(buf),
                    Err(_) => Err(422),
                }
            }),
        };
        build_response(res, headers, |b| b)
    }

    /// Serves one request read from the HTTP server: the body is read (with the
    /// limit) only after the signature checks.
    pub async fn serve(&self, req: server::Request) -> server::Response {
        let (parts, body) = req.into_parts();
        let mut headers = http::HeaderMap::new();
        let res = match self.authorize(&parts.method, &parts.uri, &parts.headers, &mut headers) {
            Err(status) => ApiResponse::status(status),
            Ok(auth) => {
                let read = match auth {
                    Authorized::Post { .. } => Some(match server::read_body(body, Some(self.body_limit())).await {
                        Ok(bytes) => Ok(bytes.to_vec()),
                        Err(server::BodyError::TooLarge(_)) => Err(413),
                        Err(server::BodyError::Read(_)) => Err(422),
                    }),
                    Authorized::Get => None,
                };
                self.complete(auth, &parts.uri, move |_| read.unwrap_or_else(|| Ok(Vec::new())))
            }
        };
        build_response(res, headers, server::full)
    }

    /// The handler to mount at [`Config::api_path`] (upstream
    /// `http.HandlerFunc(api.Handler)`).
    pub fn into_handler(self: Arc<Self>) -> impl server::Handler {
        move |req: server::Request| {
            let api = Arc::clone(&self);
            async move { api.serve(req).await }
        }
    }
}

fn build_response<B>(res: ApiResponse, headers: http::HeaderMap, body: impl FnOnce(Vec<u8>) -> B) -> http::Response<B> {
    let mut out = http::Response::new(body(res.body));
    *out.status_mut() = http::StatusCode::from_u16(res.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    *out.headers_mut() = headers;
    if let Some(ct) = res.content_type {
        out.headers_mut().insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static(ct));
    }
    out
}

/// The node as the REST API's broadcast handler (upstream passes the node, a
/// `broadcast.Handler`).
pub struct NodeBroadcastHandler(pub Arc<Node>);

impl BroadcastHandler for NodeBroadcastHandler {
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), BroadcastError> {
        self.0.handle_broadcast(json).map_err(|e| BroadcastError(e.0))
    }

    fn handle_pubsub(&self, json: &[u8]) {
        self.0.handle_pub_sub(json);
    }
}

/// The Pusher REST API on its own port (`api_port`; upstream `pusher.Server`).
pub struct Server {
    api: Arc<RestApi>,
    conf: Config,
    server: std::sync::Mutex<Option<Arc<HttpServer>>>,
    closed: std::sync::atomic::AtomicBool,
}

impl Server {
    /// Upstream `NewServer(h, b, c, l)`.
    pub fn new(handler: Arc<dyn BroadcastHandler>, broker: Arc<dyn Broker>, conf: Config) -> Self {
        Self {
            api: Arc::new(RestApi::new(handler, broker, conf.clone())),
            conf,
            server: std::sync::Mutex::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn http_server(&self) -> Option<Arc<HttpServer>> {
        self.server.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Creates the HTTP server for `api_port` and mounts the API (upstream `Start`).
    pub fn start(&self) -> Result<(), ServerError> {
        let srv = server::for_port(&self.conf.api_port.to_string())
            .map_err(|e| ServerError(format!("failed to initialize Pusher API server: {e}")))?;
        srv.setup_handler(&self.conf.api_path(), Arc::clone(&self.api).into_handler());
        *self.server.lock().unwrap_or_else(|e| e.into_inner()) = Some(srv);
        Ok(())
    }

    /// Starts listening (upstream `Run`; returns once listening).
    pub fn run(&self) -> Result<(), ServerError> {
        let Some(srv) = self.http_server() else {
            return Err(ServerError("server not initialized, call Start() first".to_owned()));
        };
        log(Level::Info, &format!("Handle Pusher API requests at {}{}", srv.address(), self.conf.api_path()), &[]);
        match srv.start_and_announce("Pusher API server") {
            Err(e) if !srv.stopped() => {
                Err(ServerError(format!("Pusher API server at {} stopped: {e}", srv.address())))
            }
            _ => Ok(()),
        }
    }

    /// Stops the HTTP server, waiting up to `timeout` for open requests (upstream
    /// `Shutdown(ctx)`). Later calls are no-ops.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), ServerError> {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        match self.http_server() {
            Some(srv) => srv.shutdown_graceful(timeout).await,
            None => Ok(()),
        }
    }

    /// The server address (upstream `Address`; empty before `start`).
    pub fn address(&self) -> String {
        self.http_server().map(|s| s.address()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
