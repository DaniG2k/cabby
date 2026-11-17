//! Conversions between Cabby `common` types and the JSON the bridge exchanges for
//! upstream `common` structs (encoding/json: exported field names, or the
//! struct's json tags). Shape conversion only.

use std::collections::HashMap;

use cabby_core::common::{
    CommandResult, ConnectResult, ControllerError, Outcome, PresenceEvent, PresenceInfo, SessionEnv, Status,
    StreamMessage, StreamMessageMetadata, StringMap,
};
use serde::Deserialize;
use serde_json::{Value, json};

/// Mirrors the bridge's `sessionEnv` (pointer fields: `null` is nil).
pub(crate) fn env_to_go(env: &SessionEnv) -> Value {
    json!({
        "url": env.url,
        "headers": env.headers,
        "identifiers": env.identifiers,
        "connection_state": env.connection_state,
        "channel_states": env.channel_states,
    })
}

pub(crate) fn opt_env_to_go(env: Option<&SessionEnv>) -> Value {
    env.map_or(Value::Null, env_to_go)
}

#[derive(Deserialize)]
struct GoEnv {
    url: String,
    headers: Option<StringMap>,
    identifiers: String,
    connection_state: Option<StringMap>,
    channel_states: Option<HashMap<String, StringMap>>,
}

pub(crate) fn opt_env_from_go(v: &Value) -> Option<SessionEnv> {
    if v.is_null() {
        return None;
    }
    let g: GoEnv = serde_json::from_value(v.clone()).expect("session env shape");
    Some(SessionEnv {
        url: g.url,
        headers: g.headers,
        identifiers: g.identifiers,
        connection_state: g.connection_state,
        channel_states: g.channel_states,
    })
}

pub(crate) fn status_to_go(s: Status) -> i64 {
    match s {
        Status::Success => 0,
        Status::Failure => 1,
        Status::Error => 2,
    }
}

pub(crate) fn status_from_go(v: &Value) -> Status {
    match v.as_i64() {
        Some(0) | None => Status::Success,
        Some(1) => Status::Failure,
        Some(2) => Status::Error,
        Some(other) => panic!("unknown Go common status {other}"),
    }
}

fn strings(v: &Value) -> Vec<String> {
    match v {
        Value::Null => Vec::new(),
        v => serde_json::from_value(v.clone()).expect("string list"),
    }
}

fn string_map(v: &Value) -> Option<StringMap> {
    match v {
        Value::Null => None,
        v => Some(serde_json::from_value(v.clone()).expect("string map")),
    }
}

fn str_field(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or_default().to_owned()
}

pub(crate) fn stream_message_to_go(m: &StreamMessage) -> Value {
    json!({
        "stream": m.stream,
        "data": m.data,
        "meta": m.meta.as_ref().map(|x| json!({
            "exclude_socket": x.exclude_socket,
            "broadcast_type": x.broadcast_type,
            "transient": x.transient,
        })),
        "offset": m.offset,
        "epoch": m.epoch,
    })
}

pub(crate) fn stream_message_from_go(v: &Value) -> StreamMessage {
    let meta = v.get("meta").filter(|m| !m.is_null()).map(|m| StreamMessageMetadata {
        exclude_socket: str_field(m, "exclude_socket"),
        broadcast_type: str_field(m, "broadcast_type"),
        transient: m.get("transient").and_then(Value::as_bool).unwrap_or(false),
    });
    StreamMessage {
        stream: str_field(v, "stream"),
        data: str_field(v, "data"),
        meta,
        offset: v.get("offset").and_then(Value::as_u64).unwrap_or(0),
        epoch: str_field(v, "epoch"),
    }
}

fn messages_from_go(v: &Value) -> Vec<StreamMessage> {
    v.as_array().map(|a| a.iter().map(stream_message_from_go).collect()).unwrap_or_default()
}

pub(crate) fn presence_event_to_go(e: &PresenceEvent) -> Value {
    json!({"type": e.kind, "info": e.info, "id": e.id})
}

pub(crate) fn presence_event_from_go(v: &Value) -> PresenceEvent {
    PresenceEvent {
        kind: str_field(v, "type"),
        info: v.get("info").filter(|i| !i.is_null()).cloned(),
        id: str_field(v, "id"),
    }
}

pub(crate) fn presence_info_to_go(i: &PresenceInfo) -> Value {
    json!({
        "type": i.kind,
        "total": i.total,
        "records": i.records.iter().map(presence_event_to_go).collect::<Vec<_>>(),
    })
}

pub(crate) fn presence_info_from_go(v: &Value) -> PresenceInfo {
    PresenceInfo {
        kind: str_field(v, "type"),
        total: v.get("total").and_then(Value::as_u64).unwrap_or(0) as usize,
        records: v
            .get("records")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(presence_event_from_go).collect())
            .unwrap_or_default(),
    }
}

pub(crate) fn connect_result_to_go(r: &ConnectResult) -> Value {
    json!({
        "Identifier": r.identifier,
        "Transmissions": r.transmissions,
        "Broadcasts": r.broadcasts.iter().map(stream_message_to_go).collect::<Vec<_>>(),
        "CState": r.cstate,
        "IState": r.istate,
        "DisconnectInterest": r.disconnect_interest,
        "Status": status_to_go(r.status),
    })
}

pub(crate) fn connect_result_from_go(v: &Value) -> ConnectResult {
    ConnectResult {
        identifier: str_field(v, "Identifier"),
        transmissions: strings(&v["Transmissions"]),
        broadcasts: messages_from_go(&v["Broadcasts"]),
        cstate: string_map(&v["CState"]),
        istate: string_map(&v["IState"]),
        disconnect_interest: v["DisconnectInterest"].as_i64().unwrap_or(0) as i32,
        status: status_from_go(&v["Status"]),
    }
}

/// An empty list as Go's nil slice: upstream fixtures leave unset result lists
/// nil, and the node tests `!= nil` on them (e.g. to mark state dirty).
fn nil_if_empty<T: serde::Serialize>(v: &[T]) -> Value {
    if v.is_empty() { Value::Null } else { json!(v) }
}

pub(crate) fn command_result_to_go(r: &CommandResult) -> Value {
    json!({
        "StopAllStreams": r.stop_all_streams,
        "Disconnect": r.disconnect,
        "Streams": nil_if_empty(&r.streams),
        "StoppedStreams": nil_if_empty(&r.stopped_streams),
        "Transmissions": nil_if_empty(&r.transmissions),
        "Broadcasts": nil_if_empty(&r.broadcasts.iter().map(stream_message_to_go).collect::<Vec<_>>()),
        "CState": r.cstate,
        "IState": r.istate,
        "Presence": r.presence.as_ref().map(presence_event_to_go),
        "DisconnectInterest": r.disconnect_interest,
        "Status": status_to_go(r.status),
    })
}

pub(crate) fn command_result_from_go(v: &Value) -> CommandResult {
    CommandResult {
        stop_all_streams: v["StopAllStreams"].as_bool().unwrap_or(false),
        disconnect: v["Disconnect"].as_bool().unwrap_or(false),
        streams: strings(&v["Streams"]),
        stopped_streams: strings(&v["StoppedStreams"]),
        transmissions: strings(&v["Transmissions"]),
        broadcasts: messages_from_go(&v["Broadcasts"]),
        cstate: string_map(&v["CState"]),
        istate: string_map(&v["IState"]),
        presence: v.get("Presence").filter(|p| !p.is_null()).map(presence_event_from_go),
        disconnect_interest: v["DisconnectInterest"].as_i64().unwrap_or(0) as i32,
        status: status_from_go(&v["Status"]),
    }
}

/// A Go `(*T, error)` pair as the bridge returns it.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct GoOutcome {
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<String>,
}

impl GoOutcome {
    pub(crate) fn map<T>(self, f: impl Fn(&Value) -> T) -> Outcome<T> {
        Outcome {
            result: (!self.result.is_null()).then(|| f(&self.result)),
            error: self.error.map(ControllerError::new),
        }
    }
}

pub(crate) fn outcome_to_go<T>(o: &Outcome<T>, f: impl Fn(&T) -> Value) -> Value {
    json!({
        "result": o.result.as_ref().map(f),
        "error": o.error.as_ref().map(|e| e.message.clone()),
    })
}

pub(crate) fn error_to_go(e: &Result<(), ControllerError>) -> Value {
    match e {
        Ok(()) => Value::Null,
        Err(e) => Value::String(e.message.clone()),
    }
}
