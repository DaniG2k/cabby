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

// --- added for identity, streams, broker, api ---
//
// Mirrors of upstream `common` definitions these packages use, plus the
// `node.Controller` interface (placed here because identity and streams both
// implement or wrap it; move it to the `node` module when that lands).

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
