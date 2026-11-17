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

// --- Session env operations -------------------------------------------------

impl SessionEnv {
    /// Upstream `NewSessionEnv`: empty connection and channel state maps.
    pub fn new(_url: &str, _headers: Option<StringMap>) -> Self {
        crate::pending!("common::new_session_env")
    }

    /// Adds or rewrites keys; an empty value removes the key.
    pub fn merge_connection_state(&mut self, _other: &StringMap) {
        crate::pending!("common::merge_connection_state")
    }

    /// Adds or rewrites keys of one channel's state; an empty value removes the key.
    pub fn merge_channel_state(&mut self, _id: &str, _other: &StringMap) {
        crate::pending!("common::merge_channel_state")
    }

    /// The field of a channel's state (`""` when missing).
    pub fn get_channel_state_field(&self, _id: &str, _field: &str) -> String {
        crate::pending!("common::get_channel_state_field")
    }

    /// The field of the connection state (`""` when missing).
    pub fn get_connection_state_field(&self, _field: &str) -> String {
        crate::pending!("common::get_connection_state_field")
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

/// An incoming client message (upstream `common.Message`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Message {
    pub command: String,
    pub identifier: String,
    /// Arbitrary JSON (`None`: absent).
    pub data: Option<serde_json::Value>,
    pub history: HistoryRequest,
    pub presence: Option<PresenceEvent>,
}

/// A JSON decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

impl Message {
    /// Decodes a client message from its JSON form (upstream `json.Unmarshal`).
    pub fn from_json(_raw: &[u8]) -> Result<Message, DecodeError> {
        crate::pending!("common::message_from_json")
    }
}

/// An outgoing client message (upstream `common.Reply`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reply {
    pub r#type: String,
    pub identifier: String,
    /// Arbitrary JSON (`None`: absent).
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
    pub fn to_reply_for(&self, _identifier: &str) -> Reply {
        crate::pending!("common::stream_message_to_reply_for")
    }
}

/// Upstream `ConfirmationMessage`.
pub fn confirmation_message(_identifier: &str) -> String {
    crate::pending!("common::confirmation_message")
}

/// Upstream `RejectionMessage`.
pub fn rejection_message(_identifier: &str) -> String {
    crate::pending!("common::rejection_message")
}

// --- Pub/sub messages -------------------------------------------------------

/// A pub/sub message carrying a remote command (e.g. disconnect).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteCommandMessage {
    pub command: String,
    /// Raw JSON payload, kept verbatim.
    pub payload: String,
}

/// Payload of a remote disconnect command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteDisconnectMessage {
    pub identifier: String,
    pub reconnect: bool,
}

impl RemoteCommandMessage {
    pub fn to_remote_disconnect_message(&self) -> Result<RemoteDisconnectMessage, DecodeError> {
        crate::pending!("common::to_remote_disconnect_message")
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
pub fn pub_sub_message_from_json(_raw: &[u8]) -> Result<PubSubMessage, DecodeError> {
    crate::pending!("common::pub_sub_message_from_json")
}

// --- Log values -------------------------------------------------------------

/// A channel identifier (or a string containing one) shown with signed stream
/// digests masked (upstream `filteredIdentifier`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredIdentifier {
    _val: String,
}

impl crate::logger::LogValuer for FilteredIdentifier {
    fn log_value(&self) -> crate::logger::LogValue {
        crate::pending!("common::filtered_identifier")
    }
}

/// Upstream `FilteredIdentifier`.
pub fn filtered_identifier(identifier: &str) -> FilteredIdentifier {
    FilteredIdentifier { _val: identifier.to_owned() }
}

/// Transmissions masked and truncated for logs (upstream `filteredTransmissions`).
pub fn filtered_transmissions(_transmissions: &[String]) -> Vec<crate::logger::CompactValue> {
    crate::pending!("common::filtered_transmissions")
}

impl crate::logger::LogValuer for CommandResult {
    fn log_value(&self) -> crate::logger::LogValue {
        crate::pending!("common::command_result_log_value")
    }
}
