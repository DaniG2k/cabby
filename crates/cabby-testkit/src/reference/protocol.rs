//! Facade for upstream package `protocol`.

use cabby_core::common::{
    CommandResult, ConnectResult, PresenceEvent, SessionEnv, Status, StreamMessage, StreamMessageMetadata, StringMap,
};
use cabby_core::protocol::{self as rs, ApplicationError, Parsed};
use cabby_core::protos;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Backend, backend, bridge};

fn env_json(env: &SessionEnv) -> Value {
    json!({
        "url": env.url,
        "headers": env.headers,
        "identifiers": env.identifiers,
        "connection_state": env.connection_state,
        "channel_states": env.channel_states,
    })
}

pub fn new_connect_message(env: &SessionEnv) -> protos::ConnectionRequest {
    match backend() {
        Backend::Rust => rs::new_connect_message(env),
        Backend::Go => bridge::call("protocol.NewConnectMessage", json!({"Env": env_json(env)})),
    }
}

pub fn new_command_message(
    env: &SessionEnv,
    command: &str,
    channel: &str,
    identifiers: &str,
    data: &str,
) -> protos::CommandMessage {
    match backend() {
        Backend::Rust => rs::new_command_message(env, command, channel, identifiers, data),
        Backend::Go => bridge::call(
            "protocol.NewCommandMessage",
            json!({"Env": env_json(env), "Command": command, "Channel": channel,
                   "Identifiers": identifiers, "Data": data}),
        ),
    }
}

pub fn new_disconnect_message(
    env: &SessionEnv,
    identifiers: &str,
    subscriptions: &[String],
) -> protos::DisconnectRequest {
    match backend() {
        Backend::Rust => rs::new_disconnect_message(env, identifiers, subscriptions),
        Backend::Go => bridge::call(
            "protocol.NewDisconnectMessage",
            json!({"Env": env_json(env), "Identifiers": identifiers, "Subscriptions": subscriptions}),
        ),
    }
}

pub fn parse_connect_response(response: &protos::ConnectionResponse) -> Parsed<ConnectResult> {
    match backend() {
        Backend::Rust => rs::parse_connect_response(response),
        Backend::Go => {
            let p: GoParsed<GoConnectResult> = bridge::call("protocol.ParseConnectResponse", response);
            Parsed { result: p.result.expect("result").into(), error: p.error.map(go_error) }
        }
    }
}

pub fn parse_command_response(response: &protos::CommandResponse) -> Parsed<CommandResult> {
    match backend() {
        Backend::Rust => rs::parse_command_response(response),
        Backend::Go => {
            let p: GoParsed<GoCommandResult> = bridge::call("protocol.ParseCommandResponse", response);
            Parsed { result: p.result.expect("result").into(), error: p.error.map(go_error) }
        }
    }
}

pub fn parse_disconnect_response(response: &protos::DisconnectResponse) -> Result<(), ApplicationError> {
    match backend() {
        Backend::Rust => rs::parse_disconnect_response(response),
        Backend::Go => {
            let p: GoParsed<Value> = bridge::call("protocol.ParseDisconnectResponse", response);
            p.error.map_or(Ok(()), |e| Err(go_error(e)))
        }
    }
}

// --- Go JSON shapes (encoding/json of upstream structs, exported field names) ---

#[derive(Deserialize)]
struct GoParsed<T> {
    result: Option<T>,
    error: Option<String>,
}

/// Go formats the error as `Application error: <msg>`; recover the message so the
/// Rust error type carries the same information.
fn go_error(text: String) -> ApplicationError {
    let message = text.strip_prefix("Application error: ").map(str::to_owned).unwrap_or(text);
    ApplicationError { message }
}

fn go_status(code: i64) -> Status {
    match code {
        0 => Status::Success,
        1 => Status::Failure,
        2 => Status::Error,
        other => panic!("unknown Go common status {other}"),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConnectResult {
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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoCommandResult {
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
            presence: g.presence.map(|p| PresenceEvent { kind: p.kind, info: p.info, id: p.id }),
            disconnect_interest: g.disconnect_interest,
            status: go_status(g.status),
        }
    }
}

#[derive(Deserialize)]
struct GoPresenceEvent {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    info: Option<Value>,
    id: String,
}

#[derive(Deserialize)]
struct GoStreamMessage {
    stream: String,
    data: String,
    meta: Option<GoStreamMeta>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    epoch: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GoStreamMeta {
    exclude_socket: String,
    broadcast_type: String,
    transient: bool,
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
