//! Session and RPC result types (upstream package `common`).
//!
//! Go pointer fields that distinguish *absent* from *empty* (`*map[string]string`,
//! nil maps in results) are `Option`s here, so translated tests can observe the
//! difference.

use std::collections::{BTreeMap, HashMap};

use crate::logger::{self, Attr, CompactValue, LogValue, LogValuer};

mod json;

pub type StringMap = HashMap<String, String>;

/// Outcome of an RPC call as seen by the server (upstream `common.SUCCESS` etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Success,
    Failure,
    Error,
}

impl Status {
    /// Upstream `StatusName`.
    pub fn name(self) -> &'static str {
        match self {
            Status::Success => "success",
            Status::Failure => "failure",
            Status::Error => "error",
        }
    }
}

/// Underlying HTTP connection data plus connection/channel state for RPC calls.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionEnv {
    pub url: String,
    pub headers: Option<StringMap>,
    pub identifiers: String,
    pub connection_state: Option<StringMap>,
    /// Per-channel state keyed by channel identifier.
    pub channel_states: Option<HashMap<String, StringMap>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamMessageMetadata {
    pub exclude_socket: String,
    pub broadcast_type: String,
    pub transient: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamMessage {
    pub stream: String,
    pub data: String,
    pub meta: Option<StreamMessageMetadata>,
    pub offset: u64,
    pub epoch: String,
}

/// Result of a connect RPC.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectResult {
    pub identifier: String,
    pub transmissions: Vec<String>,
    pub broadcasts: Vec<StreamMessage>,
    pub cstate: Option<StringMap>,
    pub istate: Option<StringMap>,
    pub disconnect_interest: i32,
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PresenceEvent {
    pub kind: String,
    /// Upstream decodes `info` as JSON and falls back to the raw text as a string
    /// (Go `interface{}`), which this models exactly: the fallback is `Value::String`.
    pub info: Option<serde_json::Value>,
    pub id: String,
}

/// Result of a command (subscribe/unsubscribe/message) RPC.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CommandResult {
    pub stop_all_streams: bool,
    pub disconnect: bool,
    pub streams: Vec<String>,
    pub stopped_streams: Vec<String>,
    pub transmissions: Vec<String>,
    pub broadcasts: Vec<StreamMessage>,
    pub cstate: Option<StringMap>,
    pub istate: Option<StringMap>,
    pub presence: Option<PresenceEvent>,
    pub disconnect_interest: i32,
    pub status: Status,
}

// --- added for identity, streams, broker, api ---
//
// Mirrors of upstream `common` definitions these packages use, plus the
// `node.Controller` interface (placed here because identity and streams, which
// live in `cabby-core`, implement or wrap it; `cabby_server::node::Controller`
// re-exports it as the canonical name).

/// Channel state key carrying the whisper stream (upstream `WHISPER_STREAM_STATE`).
pub const WHISPER_STREAM_STATE: &str = "$w";
/// Channel state key carrying the presence stream (upstream `PRESENCE_STREAM_STATE`).
pub const PRESENCE_STREAM_STATE: &str = "$p";

/// Presence event types (upstream `PresenceJoinType` etc.).
pub const PRESENCE_JOIN_TYPE: &str = "join";
pub const PRESENCE_LEAVE_TYPE: &str = "leave";
pub const PRESENCE_INFO_TYPE: &str = "info";

/// Presence information for a stream (upstream `common.PresenceInfo`).
/// `records` is omitted from JSON when empty (Go `omitempty`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PresenceInfo {
    /// `"info"` when built by upstream `NewPresenceInfo`; omitted from JSON when empty.
    pub kind: String,
    pub total: usize,
    pub records: Vec<PresenceEvent>,
}

/// A command for other nodes (upstream `common.RemoteCommandMessage`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteCommandMessage {
    pub command: String,
    /// Raw JSON payload (Go `json.RawMessage`), kept as written.
    pub payload: String,
}

/// An error returned by a controller or identifier (Go `error`), compared by message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ControllerError {
    pub message: String,
}

impl ControllerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

/// A Go `(*T, error)` pair: upstream controllers may return a result *and* an
/// error (e.g. a rejection transmission alongside the failure), or neither.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome<T> {
    pub result: Option<T>,
    pub error: Option<ControllerError>,
}

/// Executes connection and channel commands (upstream `node.Controller`).
///
/// Upstream methods also take a `context.Context`; cancellation is not part of
/// these signatures yet. `env` is a Go pointer that callers may pass as nil.
pub trait Controller: Send + Sync {
    fn start(&self) -> Result<(), ControllerError>;
    fn shutdown(&self) -> Result<(), ControllerError>;
    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult>;
    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult>;
    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult>;
    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult>;
    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError>;
}

// --- Session env operations -------------------------------------------------

impl SessionEnv {
    /// Upstream `NewSessionEnv`: empty connection and channel state maps.
    pub fn new(url: &str, headers: Option<StringMap>) -> Self {
        SessionEnv {
            url: url.to_owned(),
            headers,
            identifiers: String::new(),
            connection_state: Some(StringMap::new()),
            channel_states: Some(HashMap::new()),
        }
    }

    /// Adds or rewrites keys; an empty value removes the key.
    pub fn merge_connection_state(&mut self, other: &StringMap) {
        merge_state(self.connection_state.get_or_insert_with(StringMap::new), other);
    }

    /// Adds or rewrites keys of one channel's state; an empty value removes the key.
    pub fn merge_channel_state(&mut self, id: &str, other: &StringMap) {
        let states = self.channel_states.get_or_insert_with(HashMap::new);
        if !states.contains_key(id) {
            states.insert(id.to_owned(), StringMap::new());
        }
        if let Some(state) = states.get_mut(id) {
            merge_state(state, other);
        }
    }

    /// The field of a channel's state (`""` when missing).
    pub fn get_channel_state_field(&self, id: &str, field: &str) -> String {
        self.channel_states
            .as_ref()
            .and_then(|states| states.get(id))
            .and_then(|state| state.get(field))
            .cloned()
            .unwrap_or_default()
    }

    /// The field of the connection state (`""` when missing).
    pub fn get_connection_state_field(&self, field: &str) -> String {
        self.connection_state.as_ref().and_then(|state| state.get(field)).cloned().unwrap_or_default()
    }

    /// Drops one channel's state (upstream `RemoveChannelState`).
    pub fn remove_channel_state(&mut self, id: &str) {
        if let Some(states) = self.channel_states.as_mut() {
            states.remove(id);
        }
    }

    /// Adds a header, creating the headers map if needed (upstream `SetHeader`).
    pub fn set_header(&mut self, key: &str, val: &str) {
        self.headers.get_or_insert_with(StringMap::new).insert(key.to_owned(), val.to_owned());
    }
}

/// Go `MergeConnectionState` / `MergeChannelState`: an empty value removes the key.
fn merge_state(state: &mut StringMap, other: &StringMap) {
    for (k, v) in other {
        if v.is_empty() {
            state.remove(k);
        } else if let Some(cur) = state.get_mut(k) {
            cur.clone_from(v);
        } else {
            state.insert(k.clone(), v.clone());
        }
    }
}

// --- Client messages --------------------------------------------------------

/// A stream position known to the client.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryPosition {
    pub epoch: String,
    pub offset: u64,
}

/// History request of a subscribe command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryRequest {
    /// UTC timestamp in milliseconds.
    pub since: i64,
    pub streams: HashMap<String, HistoryPosition>,
}

/// A JSON decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

impl Message {
    /// Decodes a client message from its JSON form (upstream `json.Unmarshal`).
    ///
    /// As in Go, field names match case-insensitively, unknown fields are ignored,
    /// `null` leaves a field unset, and numbers inside `data` and `presence.info`
    /// (Go `interface{}`) are decoded as `float64`.
    pub fn from_json(raw: &[u8]) -> Result<Message, DecodeError> {
        json::decode_message(raw)
    }
}

/// An outgoing client message (upstream `common.Reply`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reply {
    pub kind: String,
    pub identifier: String,
    /// Arbitrary JSON (`None`: absent). Upstream decodes stream data with
    /// `UseNumber` and re-encodes it: keys come out sorted (as with serde_json's
    /// default map) and number text is preserved exactly. The implementation must
    /// enable serde_json's `arbitrary_precision` to keep the latter.
    pub message: Option<serde_json::Value>,
    pub presence: Option<PresenceEvent>,
    pub reason: String,
    pub reconnect: bool,
    pub stream_id: String,
    pub epoch: String,
    pub offset: u64,
    pub sid: String,
    pub restored: bool,
    pub restored_ids: Vec<String>,
}

impl StreamMessage {
    /// Builds the client reply for a subscription identifier. The data is decoded as
    /// JSON when valid (keeping integer precision), otherwise sent as a string.
    pub fn to_reply_for(&self, identifier: &str) -> Reply {
        // Go: `json.Valid`, then a `UseNumber` decode. JSON `null` decodes to nil, which
        // falls back to the raw string too. `arbitrary_precision` keeps number text.
        let message = match serde_json::from_str::<serde_json::Value>(&self.data) {
            Ok(serde_json::Value::Null) | Err(_) => serde_json::Value::String(self.data.clone()),
            Ok(value) => value,
        };

        // Only include the stream if offset/epoch is present.
        let stream_id = if self.epoch.is_empty() { String::new() } else { self.stream.clone() };

        Reply {
            identifier: identifier.to_owned(),
            message: Some(message),
            stream_id,
            offset: self.offset,
            epoch: self.epoch.clone(),
            ..Reply::default()
        }
    }
}

/// Upstream `ConfirmationMessage`.
pub fn confirmation_message(identifier: &str) -> String {
    json::typed_identifier_message(CONFIRMED_TYPE, identifier)
}

/// Upstream `RejectionMessage`.
pub fn rejection_message(identifier: &str) -> String {
    json::typed_identifier_message(REJECTED_TYPE, identifier)
}

// --- Pub/sub messages -------------------------------------------------------

// `RemoteCommandMessage` is defined above (shared with identity/streams/broker).

/// Payload of a remote disconnect command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteDisconnectMessage {
    pub identifier: String,
    pub reconnect: bool,
}

impl RemoteCommandMessage {
    pub fn to_remote_disconnect_message(&self) -> Result<RemoteDisconnectMessage, DecodeError> {
        json::decode_remote_disconnect(self.payload.as_bytes())
    }
}

/// A decoded pub/sub publication (upstream returns one of these as `interface{}`).
#[derive(Debug, Clone, PartialEq)]
pub enum PubSubMessage {
    Stream(StreamMessage),
    Batch(Vec<StreamMessage>),
    RemoteCommand(RemoteCommandMessage),
}

/// Upstream `PubSubMessageFromJSON`.
///
/// Tries, in order: a stream message with a stream, a batch whose first message has
/// a stream, and a remote command with a command.
pub fn pub_sub_message_from_json(raw: &[u8]) -> Result<PubSubMessage, DecodeError> {
    json::decode_pub_sub_message(raw)
}

// --- Log values -------------------------------------------------------------

/// A channel identifier (or a string containing one) shown with signed stream
/// digests masked (upstream `filteredIdentifier`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredIdentifier {
    val: String,
}

impl FilteredIdentifier {
    /// The identifier with signed stream digests masked (upstream `String()`).
    pub fn filtered(&self) -> String {
        if logger::LogFilter::global().enabled { mask_signed_stream_digests(&self.val) } else { self.val.clone() }
    }
}

impl LogValuer for FilteredIdentifier {
    fn log_value(&self) -> LogValue {
        LogValue::String(self.filtered())
    }
}

/// Upstream `FilteredIdentifier`.
pub fn filtered_identifier(identifier: &str) -> FilteredIdentifier {
    FilteredIdentifier { val: identifier.to_owned() }
}

/// Signed streams (Turbo Streams, CableReady, AnyCable signed streams) have the form
/// `<data>--<digest>` with a hex HMAC digest. Upstream masks every leftmost,
/// non-overlapping match of `--([0-9a-fA-F]{32,})`, which also covers escaped
/// identifiers (e.g. inside confirmation messages). A hand-written scan does the same
/// without a regex engine.
fn mask_signed_stream_digests(val: &str) -> String {
    const MIN_DIGEST: usize = 32;
    let bytes = val.as_bytes();
    let mut out: Option<String> = None;
    let mut last = 0;
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'-' && bytes[i + 1] == b'-' {
            let start = i + 2;
            let end = start + bytes[start..].iter().take_while(|b| b.is_ascii_hexdigit()).count();
            if end - start >= MIN_DIGEST {
                let buf = out.get_or_insert_with(|| String::with_capacity(val.len()));
                buf.push_str(&val[last..start]);
                buf.push_str(&logger::mask_value(&val[start..end]));
                last = end;
                i = end;
                continue;
            }
        }
        i += 1;
    }
    match out {
        Some(mut buf) => {
            buf.push_str(&val[last..]);
            buf
        }
        None => val.to_owned(),
    }
}

/// Transmissions masked and truncated for logs (upstream `filteredTransmissions`).
pub fn filtered_transmissions(transmissions: &[String]) -> Vec<CompactValue> {
    transmissions.iter().map(|msg| logger::compact_value(&filtered_identifier(msg).filtered())).collect()
}

fn log_list<T: LogValuer>(values: &[T]) -> LogValue {
    LogValue::List(values.iter().map(LogValuer::log_value).collect())
}

fn compact_list(values: &[String]) -> LogValue {
    LogValue::List(values.iter().map(|v| logger::compact_value(v).log_value()).collect())
}

/// Upstream `logger.FilteredMap(state)`: only the (sorted) keys are shown.
fn filtered_state(state: Option<&StringMap>) -> LogValue {
    let sorted: Option<BTreeMap<String, String>> =
        state.map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    logger::filtered_map(sorted.as_ref()).log_value()
}

impl LogValuer for CommandResult {
    fn log_value(&self) -> LogValue {
        LogValue::Group(vec![
            Attr::new("status", LogValue::String(self.status.name().to_owned())),
            Attr::new("streams", compact_list(&self.streams)),
            Attr::new("transmissions", log_list(&filtered_transmissions(&self.transmissions))),
            Attr::new("stopped_streams", compact_list(&self.stopped_streams)),
            Attr::new("stop_all_streams", LogValue::Bool(self.stop_all_streams)),
            Attr::new("broadcasts", log_list(&self.broadcasts)),
            Attr::new("disconnect", LogValue::Bool(self.disconnect)),
            Attr::new("disconnect_interest", LogValue::Int(self.disconnect_interest.into())),
            Attr::new("cstate", filtered_state(self.cstate.as_ref())),
            Attr::new("istate", filtered_state(self.istate.as_ref())),
        ])
    }
}

impl LogValuer for ConnectResult {
    fn log_value(&self) -> LogValue {
        LogValue::Group(vec![
            Attr::new("status", LogValue::String(self.status.name().to_owned())),
            Attr::new("transmissions", log_list(&filtered_transmissions(&self.transmissions))),
            Attr::new("broadcasts", log_list(&self.broadcasts)),
            Attr::new("identifier", LogValue::String(self.identifier.clone())),
            Attr::new("disconnect_interest", LogValue::Int(self.disconnect_interest.into())),
            Attr::new("cstate", filtered_state(self.cstate.as_ref())),
            Attr::new("istate", filtered_state(self.istate.as_ref())),
        ])
    }
}

impl LogValuer for StreamMessage {
    fn log_value(&self) -> LogValue {
        let mut attrs = vec![
            Attr::new("stream", LogValue::String(self.stream.clone())),
            Attr::new("data", logger::compact_value(&self.data).log_value()),
        ];
        if !self.epoch.is_empty() {
            attrs.push(Attr::new("offset", LogValue::Uint(self.offset)));
            attrs.push(Attr::new("epoch", LogValue::String(self.epoch.clone())));
        }
        if let Some(meta) = &self.meta {
            let meta = vec![Attr::new("exclude_socket", LogValue::String(meta.exclude_socket.clone()))];
            attrs.push(Attr::new("meta", LogValue::Group(meta)));
        }
        LogValue::Group(attrs)
    }
}

// --- added for pusher/sse ---
// `EncodedMessage` below is the data enum of the upstream implementors; it
// implements the `encoders::EncodedMessage` / `EncodedMessageJson` traits. The hub
// wraps it in `encoders::CachedEncodedMessage` (re-exported as `hub::EncodedMessage`).
//
// Mirrors of upstream `common` (and the `encoders.EncodedMessage` / `ws.SentFrame`
// data shapes) needed by the pusher and sse translations. Go `interface{}` payloads
// are `serde_json::Value`; nil is `None`.

/// Reply message types (upstream `common.WelcomeType` etc.).
pub const WELCOME_TYPE: &str = "welcome";
pub const PING_TYPE: &str = "ping";
pub const PONG_TYPE: &str = "pong";
pub const DISCONNECT_TYPE: &str = "disconnect";
pub const CONFIRMED_TYPE: &str = "confirm_subscription";
pub const REJECTED_TYPE: &str = "reject_subscription";
pub const PRESENCE_TYPE: &str = "presence";

/// Disconnect reason sent on shutdown (upstream `common.SERVER_RESTART_REASON`).
pub const SERVER_RESTART_REASON: &str = "server_restart";

/// Server ping (upstream `common.PingMessage`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PingMessage {
    /// Go `Type`.
    pub kind: String,
    pub message: Option<serde_json::Value>,
}

/// Server disconnect notice (upstream `common.DisconnectMessage`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisconnectMessage {
    /// Go `Type`.
    pub kind: String,
    pub reason: String,
    pub reconnect: bool,
}

/// A message an encoder can serialize (upstream `encoders.EncodedMessage`
/// implementors: `*common.Reply`, `*common.PingMessage`, `*common.DisconnectMessage`).
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // a data mirror; boxing would only add noise at call sites
pub enum EncodedMessage {
    Reply(Reply),
    Ping(PingMessage),
    Disconnect(DisconnectMessage),
}

pub use crate::encoders::{FrameType, SentFrame};

/// Go `Message.Data interface{}`: decoded JSON, or a typed payload attached by a
/// protocol decoder (Go stores a struct pointer, e.g. `*pusher.PusherSubscriptionData`).
#[derive(Clone)]
pub enum MessageData {
    Json(serde_json::Value),
    Typed(std::sync::Arc<dyn std::any::Any + Send + Sync>),
}

impl MessageData {
    /// The typed payload if it is a `T` (Go type assertion `msg.Data.(*T)`).
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        match self {
            MessageData::Typed(v) => v.downcast_ref::<T>(),
            MessageData::Json(_) => None,
        }
    }
}

impl PartialEq for MessageData {
    /// JSON payloads compare by value; typed payloads only when they are the same
    /// allocation (Go compares interface pointers the same way).
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MessageData::Json(a), MessageData::Json(b)) => a == b,
            (MessageData::Typed(a), MessageData::Typed(b)) => std::sync::Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl std::fmt::Debug for MessageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageData::Json(v) => f.debug_tuple("Json").field(v).finish(),
            MessageData::Typed(_) => f.write_str("Typed(..)"),
        }
    }
}

/// Client command (upstream `common.Message`). `data` mirrors Go's `interface{}`:
/// decoded JSON, or a typed payload attached by a protocol decoder.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Message {
    pub command: String,
    pub identifier: String,
    pub data: Option<MessageData>,
    pub history: HistoryRequest,
    pub presence: Option<PresenceEvent>,
}
// --- added for hub/pubsub/broadcast ---
// (RemoteCommandMessage, RemoteDisconnectMessage, DisconnectMessage and Reply are
// defined above.)

/// Disconnect reason used by remote (broadcast) disconnects (upstream `common.REMOTE_DISCONNECT_REASON`).
pub const REMOTE_DISCONNECT_REASON: &str = "remote";
