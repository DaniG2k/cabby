//! Conversion between session data and RPC messages (upstream package `protocol`).

use std::collections::{BTreeMap, HashMap};

use crate::common::{CommandResult, ConnectResult, PresenceEvent, SessionEnv, Status, StringMap};
use crate::gojson;
use crate::protos;

/// The application answered with status ERROR (upstream: `Application error: <msg>`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Application error: {message}")]
pub struct ApplicationError {
    pub message: String,
}

/// A parsed RPC response. Upstream returns the result *and* an error for ERROR
/// responses, so both are kept.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed<T> {
    pub result: T,
    pub error: Option<ApplicationError>,
}

pub fn new_connect_message(env: &SessionEnv) -> protos::ConnectionRequest {
    protos::ConnectionRequest { env: Some(build_env(env)) }
}

pub fn new_command_message(
    env: &SessionEnv,
    command: &str,
    channel: &str,
    identifiers: &str,
    data: &str,
) -> protos::CommandMessage {
    let mut proto_env = build_env(env);
    if let Some(state) = env.channel_states.as_ref().and_then(|states| states.get(channel)) {
        proto_env.istate = state.clone();
    }

    protos::CommandMessage {
        command: command.to_owned(),
        identifier: channel.to_owned(),
        connection_identifiers: identifiers.to_owned(),
        data: data.to_owned(),
        env: Some(proto_env),
    }
}

pub fn new_disconnect_message(
    env: &SessionEnv,
    identifiers: &str,
    subscriptions: &[String],
) -> protos::DisconnectRequest {
    let mut proto_env = build_env(env);
    if let Some(states) = &env.channel_states {
        // Each channel state is sent as its Go JSON encoding (`json.Marshal` of a map:
        // keys sorted).
        proto_env.istate = states
            .iter()
            .map(|(id, state)| {
                let sorted: BTreeMap<&str, &str> = state.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                (id.clone(), gojson::to_string(&sorted))
            })
            .collect();
    }

    protos::DisconnectRequest {
        identifiers: identifiers.to_owned(),
        subscriptions: subscriptions.to_vec(),
        env: Some(proto_env),
    }
}

pub fn parse_connect_response(response: &protos::ConnectionResponse) -> Parsed<ConnectResult> {
    let mut result = ConnectResult {
        transmissions: response.transmissions.clone(),
        cstate: response.env.as_ref().and_then(|env| state(&env.cstate)),
        ..ConnectResult::default()
    };

    let error = match status(response.status) {
        Status::Success => {
            result.identifier.clone_from(&response.identifiers);
            None
        }
        Status::Failure => None,
        Status::Error => Some(ApplicationError { message: response.error_msg.clone() }),
    };
    result.status = status(response.status);

    Parsed { result, error }
}

pub fn parse_command_response(response: &protos::CommandResponse) -> Parsed<CommandResult> {
    let mut result = CommandResult {
        disconnect: response.disconnect,
        stop_all_streams: response.stop_streams,
        streams: response.streams.clone(),
        stopped_streams: response.stopped_streams.clone(),
        transmissions: response.transmissions.clone(),
        status: status(response.status),
        ..CommandResult::default()
    };

    if let Some(env) = &response.env {
        result.cstate = state(&env.cstate);
        result.istate = state(&env.istate);
    }

    if let Some(presence) = &response.presence {
        result.presence = Some(PresenceEvent {
            kind: presence.r#type.clone(),
            id: presence.id.clone(),
            info: presence_info(&presence.info),
        });
    }

    let error = (result.status == Status::Error).then(|| ApplicationError { message: response.error_msg.clone() });
    Parsed { result, error }
}

pub fn parse_disconnect_response(response: &protos::DisconnectResponse) -> Result<(), ApplicationError> {
    match status(response.status) {
        Status::Error => Err(ApplicationError { message: response.error_msg.clone() }),
        _ => Ok(()),
    }
}

/// Upstream compares `Status.String()` with "SUCCESS" / "FAILURE"; anything else
/// (including unknown enum values) is an error.
fn status(code: i32) -> Status {
    match protos::Status::try_from(code) {
        Ok(protos::Status::Success) => Status::Success,
        Ok(protos::Status::Failure) => Status::Failure,
        _ => Status::Error,
    }
}

/// A protobuf map field as Go sees it after decoding: an absent (empty) map is nil.
fn state(map: &HashMap<String, String>) -> Option<StringMap> {
    (!map.is_empty()).then(|| map.clone())
}

/// Upstream decodes presence info as JSON (`interface{}`, numbers as `float64`) and
/// falls back to the raw text when it is not valid JSON.
fn presence_info(info: &str) -> Option<serde_json::Value> {
    if info.is_empty() {
        return None;
    }
    match serde_json::from_str::<serde_json::Value>(info) {
        Ok(mut value) => match gojson::to_go_interface(&mut value) {
            Ok(()) if value.is_null() => None,
            Ok(()) => Some(value),
            Err(_) => Some(serde_json::Value::String(info.to_owned())),
        },
        Err(_) => Some(serde_json::Value::String(info.to_owned())),
    }
}

/// Upstream `buildEnv`: URL, headers and connection state.
fn build_env(env: &SessionEnv) -> protos::Env {
    protos::Env {
        url: env.url.clone(),
        headers: env.headers.clone().unwrap_or_default(),
        cstate: env.connection_state.clone().unwrap_or_default(),
        istate: HashMap::new(),
    }
}
