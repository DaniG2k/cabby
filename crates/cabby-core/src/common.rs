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

// --- added for hub/pubsub/broadcast ---

/// Disconnect reason used by remote (broadcast) disconnects (upstream `common.REMOTE_DISCONNECT_REASON`).
pub const REMOTE_DISCONNECT_REASON: &str = "remote";

/// Command distributed to every node through pub/sub (upstream `common.RemoteCommandMessage`).
/// `payload` keeps the raw JSON bytes (`json.RawMessage`); empty means absent.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteCommandMessage {
    pub command: String,
    pub payload: Vec<u8>,
}

/// Remote disconnect command (upstream `common.RemoteDisconnectMessage`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteDisconnectMessage {
    pub identifier: String,
    pub reconnect: bool,
}

/// Server-initiated disconnect notification (upstream `common.DisconnectMessage`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisconnectMessage {
    pub kind: String,
    pub reason: String,
    pub reconnect: bool,
}

/// Outgoing client message (upstream `common.Reply`, fields used by hub routing).
/// `message` holds the raw JSON text of the payload (upstream embeds stream data
/// verbatim); `None` is omitted.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reply {
    pub kind: String,
    pub identifier: String,
    pub message: Option<String>,
    pub reason: String,
    pub reconnect: bool,
    pub stream_id: String,
    pub epoch: String,
    pub offset: u64,
}
