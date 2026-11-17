//! Facade for upstream package `common`.
//!
//! Also holds the Go JSON shapes of `common` types (`pub(crate)`) reused by other
//! facades.

use std::collections::HashMap;

use cabby_core::common::{
    self as rs, CommandResult, ConnectResult, DecodeError, HistoryPosition, HistoryRequest, Message, PresenceEvent,
    PubSubMessage, RemoteCommandMessage, RemoteDisconnectMessage, Reply, SessionEnv, Status, StreamMessage,
    StreamMessageMetadata, StringMap,
};
use cabby_core::logger::{Attr, Level, LogValuer, format_text};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Backend, backend, bridge};

// --- Go shapes ------------------------------------------------------------------

/// The bridge's `sessionEnv` (pointer fields keep nil distinct from empty).
#[derive(Serialize, Deserialize)]
pub(crate) struct GoSessionEnv {
    url: String,
    headers: Option<StringMap>,
    identifiers: String,
    connection_state: Option<StringMap>,
    channel_states: Option<HashMap<String, StringMap>>,
}

impl From<&SessionEnv> for GoSessionEnv {
    fn from(e: &SessionEnv) -> Self {
        GoSessionEnv {
            url: e.url.clone(),
            headers: e.headers.clone(),
            identifiers: e.identifiers.clone(),
            connection_state: e.connection_state.clone(),
            channel_states: e.channel_states.clone(),
        }
    }
}

impl From<GoSessionEnv> for SessionEnv {
    fn from(g: GoSessionEnv) -> Self {
        SessionEnv {
            url: g.url,
            headers: g.headers,
            identifiers: g.identifiers,
            connection_state: g.connection_state,
            channel_states: g.channel_states,
        }
    }
}

/// `common.StreamMessage` (json tags).
#[derive(Serialize, Deserialize)]
pub(crate) struct GoStreamMessage {
    stream: String,
    data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    meta: Option<GoStreamMeta>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    epoch: String,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct GoStreamMeta {
    exclude_socket: String,
    broadcast_type: String,
    transient: bool,
}

impl From<&StreamMessage> for GoStreamMessage {
    fn from(m: &StreamMessage) -> Self {
        GoStreamMessage {
            stream: m.stream.clone(),
            data: m.data.clone(),
            meta: m.meta.as_ref().map(|m| GoStreamMeta {
                exclude_socket: m.exclude_socket.clone(),
                broadcast_type: m.broadcast_type.clone(),
                transient: m.transient,
            }),
            offset: m.offset,
            epoch: m.epoch.clone(),
        }
    }
}

impl From<GoStreamMessage> for StreamMessage {
    fn from(g: GoStreamMessage) -> Self {
        StreamMessage {
            stream: g.stream,
            data: g.data,
            meta: g.meta.map(|m| StreamMessageMetadata {
                exclude_socket: m.exclude_socket,
                broadcast_type: m.broadcast_type,
                transient: m.transient,
            }),
            offset: g.offset,
            epoch: g.epoch,
        }
    }
}

/// `common.PresenceEvent` (json tags).
#[derive(Serialize, Deserialize)]
pub(crate) struct GoPresenceEvent {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    info: Option<Value>,
    #[serde(default)]
    id: String,
}

impl From<&PresenceEvent> for GoPresenceEvent {
    fn from(p: &PresenceEvent) -> Self {
        GoPresenceEvent { kind: p.kind.clone(), info: p.info.clone(), id: p.id.clone() }
    }
}

impl From<GoPresenceEvent> for PresenceEvent {
    fn from(g: GoPresenceEvent) -> Self {
        PresenceEvent { kind: g.kind, info: g.info, id: g.id }
    }
}

pub(crate) fn go_status(code: i64) -> Status {
    match code {
        0 => Status::Success,
        1 => Status::Failure,
        2 => Status::Error,
        other => panic!("unknown Go common status {other}"),
    }
}

pub(crate) fn status_code(s: Status) -> i64 {
    match s {
        Status::Success => 0,
        Status::Failure => 1,
        Status::Error => 2,
    }
}

/// `common.ConnectResult` (no json tags: exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoConnectResult {
    identifier: String,
    transmissions: Option<Vec<String>>,
    broadcasts: Option<Vec<GoStreamMessage>>,
    #[serde(rename = "CState")]
    cstate: Option<StringMap>,
    #[serde(rename = "IState")]
    istate: Option<StringMap>,
    disconnect_interest: i32,
    status: i64,
}

impl From<&ConnectResult> for GoConnectResult {
    fn from(r: &ConnectResult) -> Self {
        GoConnectResult {
            identifier: r.identifier.clone(),
            transmissions: Some(r.transmissions.clone()),
            broadcasts: Some(r.broadcasts.iter().map(Into::into).collect()),
            cstate: r.cstate.clone(),
            istate: r.istate.clone(),
            disconnect_interest: r.disconnect_interest,
            status: status_code(r.status),
        }
    }
}

impl From<GoConnectResult> for ConnectResult {
    fn from(g: GoConnectResult) -> Self {
        ConnectResult {
            identifier: g.identifier,
            transmissions: g.transmissions.unwrap_or_default(),
            broadcasts: g.broadcasts.unwrap_or_default().into_iter().map(Into::into).collect(),
            cstate: g.cstate,
            istate: g.istate,
            disconnect_interest: g.disconnect_interest,
            status: go_status(g.status),
        }
    }
}

/// `common.CommandResult` (no json tags: exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoCommandResult {
    stop_all_streams: bool,
    disconnect: bool,
    streams: Option<Vec<String>>,
    stopped_streams: Option<Vec<String>>,
    transmissions: Option<Vec<String>>,
    broadcasts: Option<Vec<GoStreamMessage>>,
    #[serde(rename = "CState")]
    cstate: Option<StringMap>,
    #[serde(rename = "IState")]
    istate: Option<StringMap>,
    presence: Option<GoPresenceEvent>,
    disconnect_interest: i32,
    status: i64,
}

impl From<&CommandResult> for GoCommandResult {
    fn from(r: &CommandResult) -> Self {
        GoCommandResult {
            stop_all_streams: r.stop_all_streams,
            disconnect: r.disconnect,
            streams: Some(r.streams.clone()),
            stopped_streams: Some(r.stopped_streams.clone()),
            transmissions: Some(r.transmissions.clone()),
            broadcasts: Some(r.broadcasts.iter().map(Into::into).collect()),
            cstate: r.cstate.clone(),
            istate: r.istate.clone(),
            presence: r.presence.as_ref().map(Into::into),
            disconnect_interest: r.disconnect_interest,
            status: status_code(r.status),
        }
    }
}

impl From<GoCommandResult> for CommandResult {
    fn from(g: GoCommandResult) -> Self {
        CommandResult {
            stop_all_streams: g.stop_all_streams,
            disconnect: g.disconnect,
            streams: g.streams.unwrap_or_default(),
            stopped_streams: g.stopped_streams.unwrap_or_default(),
            transmissions: g.transmissions.unwrap_or_default(),
            broadcasts: g.broadcasts.unwrap_or_default().into_iter().map(Into::into).collect(),
            cstate: g.cstate,
            istate: g.istate,
            presence: g.presence.map(Into::into),
            disconnect_interest: g.disconnect_interest,
            status: go_status(g.status),
        }
    }
}

/// `common.Reply` (json tags, omitempty).
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct GoReply {
    #[serde(rename = "type", skip_serializing_if = "String::is_empty")]
    kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    identifier: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<GoPresenceEvent>,
    #[serde(skip_serializing_if = "String::is_empty")]
    reason: String,
    reconnect: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    stream_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    epoch: String,
    offset: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    sid: String,
    restored: bool,
    restored_ids: Option<Vec<String>>,
}

impl From<&Reply> for GoReply {
    fn from(r: &Reply) -> Self {
        GoReply {
            kind: r.r#type.clone(),
            identifier: r.identifier.clone(),
            message: r.message.clone(),
            presence: r.presence.as_ref().map(Into::into),
            reason: r.reason.clone(),
            reconnect: r.reconnect,
            stream_id: r.stream_id.clone(),
            epoch: r.epoch.clone(),
            offset: r.offset,
            sid: r.sid.clone(),
            restored: r.restored,
            restored_ids: (!r.restored_ids.is_empty()).then(|| r.restored_ids.clone()),
        }
    }
}

impl From<GoReply> for Reply {
    fn from(g: GoReply) -> Self {
        Reply {
            r#type: g.kind,
            identifier: g.identifier,
            message: g.message,
            presence: g.presence.map(Into::into),
            reason: g.reason,
            reconnect: g.reconnect,
            stream_id: g.stream_id,
            epoch: g.epoch,
            offset: g.offset,
            sid: g.sid,
            restored: g.restored,
            restored_ids: g.restored_ids.unwrap_or_default(),
        }
    }
}

/// `common.Message` (json tags).
#[derive(Deserialize)]
pub(crate) struct GoMessage {
    command: String,
    identifier: String,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    history: GoHistoryRequest,
    #[serde(default)]
    presence: Option<GoPresenceEvent>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GoHistoryRequest {
    since: i64,
    streams: Option<HashMap<String, GoHistoryPosition>>,
}

#[derive(Deserialize)]
struct GoHistoryPosition {
    epoch: String,
    offset: u64,
}

impl From<GoMessage> for Message {
    fn from(g: GoMessage) -> Self {
        Message {
            command: g.command,
            identifier: g.identifier,
            data: g.data,
            history: HistoryRequest {
                since: g.history.since,
                streams: g
                    .history
                    .streams
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, p)| (k, HistoryPosition { epoch: p.epoch, offset: p.offset }))
                    .collect(),
            },
            presence: g.presence.map(Into::into),
        }
    }
}

/// A Go `(result, error)` pair from the bridge.
#[derive(Deserialize)]
pub(crate) struct GoParsed<T> {
    pub result: Option<T>,
    pub error: Option<String>,
}

impl<T> GoParsed<T> {
    /// `Ok(result)` when Go returned no error.
    pub fn into_result<E>(self, err: impl FnOnce(String) -> E) -> Result<Option<T>, E> {
        match self.error {
            Some(e) => Err(err(e)),
            None => Ok(self.result),
        }
    }
}

// --- Facade -----------------------------------------------------------------------

pub fn new_session_env(url: &str, headers: Option<StringMap>) -> SessionEnv {
    match backend() {
        Backend::Rust => SessionEnv::new(url, headers),
        Backend::Go => {
            let g: GoSessionEnv = bridge::call("common.NewSessionEnv", json!({"URL": url, "Headers": headers}));
            g.into()
        }
    }
}

pub fn merge_connection_state(env: &mut SessionEnv, other: &StringMap) {
    match backend() {
        Backend::Rust => env.merge_connection_state(other),
        Backend::Go => {
            let g: GoSessionEnv = bridge::call(
                "common.SessionEnv.MergeConnectionState",
                json!({"Env": GoSessionEnv::from(&*env), "Other": other}),
            );
            *env = g.into();
        }
    }
}

pub fn merge_channel_state(env: &mut SessionEnv, id: &str, other: &StringMap) {
    match backend() {
        Backend::Rust => env.merge_channel_state(id, other),
        Backend::Go => {
            let g: GoSessionEnv = bridge::call(
                "common.SessionEnv.MergeChannelState",
                json!({"Env": GoSessionEnv::from(&*env), "ID": id, "Other": other}),
            );
            *env = g.into();
        }
    }
}

pub fn get_channel_state_field(env: &SessionEnv, id: &str, field: &str) -> String {
    match backend() {
        Backend::Rust => env.get_channel_state_field(id, field),
        Backend::Go => bridge::call(
            "common.SessionEnv.GetChannelStateField",
            json!({"Env": GoSessionEnv::from(env), "ID": id, "Field": field}),
        ),
    }
}

pub fn get_connection_state_field(env: &SessionEnv, field: &str) -> String {
    match backend() {
        Backend::Rust => env.get_connection_state_field(field),
        Backend::Go => bridge::call(
            "common.SessionEnv.GetConnectionStateField",
            json!({"Env": GoSessionEnv::from(env), "Field": field}),
        ),
    }
}

pub fn pub_sub_message_from_json(raw: &[u8]) -> Result<PubSubMessage, DecodeError> {
    match backend() {
        Backend::Rust => rs::pub_sub_message_from_json(raw),
        Backend::Go => {
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            struct Out {
                kind: Option<String>,
                value: Option<Value>,
                error: Option<String>,
            }
            let raw = std::str::from_utf8(raw).expect("UTF-8 publication");
            let out: Out = bridge::call("common.PubSubMessageFromJSON", json!({"Raw": raw}));
            if let Some(e) = out.error {
                return Err(DecodeError(e));
            }
            let value = out.value.unwrap_or(Value::Null);
            let decode = |v: Value| -> GoStreamMessage { serde_json::from_value(v).expect("Go StreamMessage") };
            Ok(match out.kind.as_deref() {
                Some("stream") => PubSubMessage::Stream(decode(value).into()),
                Some("batch") => PubSubMessage::Batch(
                    serde_json::from_value::<Vec<Value>>(value)
                        .expect("Go batch")
                        .into_iter()
                        .map(|v| decode(v).into())
                        .collect(),
                ),
                Some("remote_command") => PubSubMessage::RemoteCommand(RemoteCommandMessage {
                    command: value["Command"].as_str().unwrap_or_default().to_owned(),
                    payload: value["Payload"].as_str().unwrap_or_default().to_owned(),
                }),
                other => panic!("unexpected PubSubMessageFromJSON kind {other:?}"),
            })
        }
    }
}

pub fn to_remote_disconnect_message(msg: &RemoteCommandMessage) -> Result<RemoteDisconnectMessage, DecodeError> {
    match backend() {
        Backend::Rust => msg.to_remote_disconnect_message(),
        Backend::Go => {
            #[derive(Deserialize)]
            struct GoRemoteDisconnect {
                identifier: String,
                reconnect: bool,
            }
            let p: GoParsed<GoRemoteDisconnect> = bridge::call(
                "common.RemoteCommandMessage.ToRemoteDisconnectMessage",
                json!({"Command": msg.command, "Payload": msg.payload}),
            );
            p.into_result(DecodeError).map(|r| {
                let r = r.expect("result");
                RemoteDisconnectMessage { identifier: r.identifier, reconnect: r.reconnect }
            })
        }
    }
}

pub fn confirmation_message(identifier: &str) -> String {
    match backend() {
        Backend::Rust => rs::confirmation_message(identifier),
        Backend::Go => bridge::call("common.ConfirmationMessage", json!({"Identifier": identifier})),
    }
}

pub fn rejection_message(identifier: &str) -> String {
    match backend() {
        Backend::Rust => rs::rejection_message(identifier),
        Backend::Go => bridge::call("common.RejectionMessage", json!({"Identifier": identifier})),
    }
}

pub fn to_reply_for(msg: &StreamMessage, identifier: &str) -> Reply {
    match backend() {
        Backend::Rust => msg.to_reply_for(identifier),
        Backend::Go => {
            let g: GoReply = bridge::call(
                "common.StreamMessage.ToReplyFor",
                json!({"Message": GoStreamMessage::from(msg), "Identifier": identifier}),
            );
            g.into()
        }
    }
}

/// Decodes a client message (upstream `json.Unmarshal(raw, &msg)`).
pub fn message_from_json(raw: &str) -> Result<Message, DecodeError> {
    match backend() {
        Backend::Rust => Message::from_json(raw.as_bytes()),
        Backend::Go => {
            let p: GoParsed<GoMessage> = bridge::call("common.Message.Unmarshal", json!({"Raw": raw}));
            p.into_result(DecodeError).map(|m| m.expect("result").into())
        }
    }
}

/// `FilteredIdentifier(input).LogValue().String()`.
pub fn filtered_identifier(input: &str) -> String {
    match backend() {
        Backend::Rust => rs::filtered_identifier(input).log_value().to_string(),
        Backend::Go => bridge::call("common.FilteredIdentifier", json!({"Input": input})),
    }
}

/// The output of `slog.New(slog.NewTextHandler(&buf, nil)).Info(msg, key, res)`.
pub fn command_result_log_text(msg: &str, key: &str, res: &CommandResult) -> String {
    match backend() {
        Backend::Rust => format_text(Level::Info, msg, &[Attr::new(key, res.log_value())]),
        Backend::Go => bridge::call(
            "common.CommandResult.LogText",
            json!({"Msg": msg, "Key": key, "Result": GoCommandResult::from(res)}),
        ),
    }
}

/// `filteredTransmissions(ts)[i].LogValue().String()` for every element.
pub fn filtered_transmissions(transmissions: &[String]) -> Vec<String> {
    match backend() {
        Backend::Rust => rs::filtered_transmissions(transmissions).iter().map(|v| v.log_value().to_string()).collect(),
        Backend::Go => {
            // Observed through the exported CommandResult.LogValue(), whose
            // "transmissions" attribute is the filteredTransmissions output.
            let out: Option<Vec<String>> =
                bridge::call("common.CommandResult.LogValue.Transmissions", json!({"Transmissions": transmissions}));
            out.expect("transmissions attribute in CommandResult.LogValue()")
        }
    }
}
