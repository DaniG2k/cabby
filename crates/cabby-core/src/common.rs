//! Session and RPC result types (upstream package `common`).
//!
//! Go pointer fields that distinguish *absent* from *empty* (`*map[string]string`,
//! nil maps in results) are `Option`s here, so translated tests can observe the
//! difference.

use std::collections::HashMap;

pub type StringMap = HashMap<String, String>;

/// Outcome of an RPC call as seen by the server (upstream `common.SUCCESS` etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Success,
    Failure,
    Error,
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

// --- added for pusher/sse ---
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

/// Presence event types (upstream `common.PresenceInfoType` etc.).
pub const PRESENCE_INFO_TYPE: &str = "info";
pub const PRESENCE_JOIN_TYPE: &str = "join";
pub const PRESENCE_LEAVE_TYPE: &str = "leave";

/// Disconnect reason sent on shutdown (upstream `common.SERVER_RESTART_REASON`).
pub const SERVER_RESTART_REASON: &str = "server_restart";

/// Channel state keys for whisper/presence streams (upstream `common.WHISPER_STREAM_STATE`).
pub const WHISPER_STREAM_STATE: &str = "$w";
pub const PRESENCE_STREAM_STATE: &str = "$p";

/// Server-to-client message (upstream `common.Reply`; JSON fields are `omitempty`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reply {
    /// Go `Type`.
    pub kind: String,
    pub identifier: String,
    /// Go `Message interface{}`: `None` is nil (omitted from JSON).
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

/// Frame kinds (upstream `ws.FrameType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrameType {
    #[default]
    Text,
    Close,
    Binary,
}

/// An encoded frame (upstream `ws.SentFrame`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SentFrame {
    pub frame_type: FrameType,
    pub payload: Vec<u8>,
    pub close_code: i32,
    pub close_reason: String,
}

/// Stream position (upstream `common.HistoryPosition`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryPosition {
    pub epoch: String,
    pub offset: u64,
}

/// Client history request (upstream `common.HistoryRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryRequest {
    /// UTC timestamp in ms (0: unset).
    pub since: i64,
    pub streams: HashMap<String, HistoryPosition>,
}

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

impl std::fmt::Debug for MessageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageData::Json(v) => f.debug_tuple("Json").field(v).finish(),
            MessageData::Typed(_) => f.write_str("Typed(..)"),
        }
    }
}

/// Client command (upstream `common.Message`).
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub command: String,
    pub identifier: String,
    pub data: Option<MessageData>,
    pub history: HistoryRequest,
    pub presence: Option<PresenceEvent>,
}

/// Presence snapshot of a stream (upstream `common.PresenceInfo`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PresenceInfo {
    /// Go `Type`: always `"info"` when set.
    pub kind: String,
    pub total: i64,
    pub records: Vec<PresenceEvent>,
}
