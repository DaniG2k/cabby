//! Facade for upstream package `pusher`.
//!
//! Go shapes: `pusher.Config` and `common.CommandResult` have no JSON tags (Go field
//! names); `common.Reply`, `common.Message` and friends use their upstream tags.

use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cabby_core::common::{
    CommandResult, EncodedMessage, HistoryPosition, HistoryRequest, Message, MessageData, PresenceEvent, PresenceInfo,
    SentFrame, SessionEnv, Status, StreamMessage, StreamMessageMetadata, StringMap,
};
use cabby_server::pusher::{self as rs, PusherClientEvent, PusherSubscriptionData};
use cabby_server::pusher_sse_deps::{
    self as deps, BroadcastHandler, Broker, CallResult, Controller as _, Encoder as _, Error, Executor as _,
    ExecutorSession,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Backend, backend, bridge};

// ------------------------------------------------------------------ config

pub(crate) fn go_config(c: &rs::Config) -> Value {
    json!({
        "AppID": c.app_id, "AppKey": c.app_key, "Secret": c.secret, "APIPort": c.api_port,
        "AddCORSHeaders": c.add_cors_headers, "CORSHosts": c.cors_hosts, "MaxBodySize": c.max_body_size,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    #[serde(rename = "AppID")]
    app_id: String,
    app_key: String,
    secret: String,
    #[serde(rename = "APIPort")]
    api_port: i64,
    #[serde(rename = "AddCORSHeaders")]
    add_cors_headers: bool,
    #[serde(rename = "CORSHosts")]
    cors_hosts: String,
    max_body_size: i64,
}

/// `conf.ToToml()`.
pub fn config_to_toml(conf: &rs::Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("pusher.Config.ToToml", go_config(conf)),
    }
}

/// `toml.Decode(s, &pusher.Config{})` (BurntSushi/toml in Go, serde + `toml` in Rust).
pub fn decode_config_toml(s: &str) -> Result<rs::Config, String> {
    match backend() {
        Backend::Rust => toml::from_str(s).map_err(|e| e.to_string()),
        Backend::Go => {
            let p: GoParsed<GoConfig> = bridge::call("pusher.DecodeConfigToml", json!({"Toml": s}));
            if let Some(e) = p.error {
                return Err(e);
            }
            let g = p.result.expect("decoded config");
            Ok(rs::Config {
                app_id: g.app_id,
                app_key: g.app_key,
                secret: g.secret,
                api_port: g.api_port,
                add_cors_headers: g.add_cors_headers,
                cors_hosts: g.cors_hosts,
                max_body_size: g.max_body_size,
            })
        }
    }
}

// ------------------------------------------------------------------ verifier

/// `pusher.NewVerifier(app_id, key)`.
pub struct Verifier {
    app_id: String,
    key: String,
}

impl Verifier {
    pub fn new(app_id: &str, key: &str) -> Self {
        Self { app_id: app_id.to_owned(), key: key.to_owned() }
    }

    fn go<R: serde::de::DeserializeOwned>(&self, method: &str, args: Value) -> R {
        let mut a = args;
        a["AppID"] = json!(self.app_id);
        a["Key"] = json!(self.key);
        a["Method"] = json!(method);
        bridge::call("pusher.Verifier", a)
    }

    fn rust(&self) -> rs::Verifier {
        rs::Verifier::new(&self.app_id, &self.key)
    }

    pub fn verify_channel(&self, socket: &str, channel: &str, signature: &str) -> bool {
        match backend() {
            Backend::Rust => self.rust().verify_channel(socket, channel, signature),
            Backend::Go => {
                self.go("VerifyChannel", json!({"Socket": socket, "Channel": channel, "Signature": signature}))
            }
        }
    }

    pub fn verify_presence_channel(&self, socket: &str, channel: &str, info: &str, signature: &str) -> bool {
        match backend() {
            Backend::Rust => self.rust().verify_presence_channel(socket, channel, info, signature),
            Backend::Go => self.go(
                "VerifyPresenceChannel",
                json!({"Socket": socket, "Channel": channel, "Info": info, "Signature": signature}),
            ),
        }
    }

    pub fn verify_user(&self, socket: &str, user_info: &str, signature: &str) -> bool {
        match backend() {
            Backend::Rust => self.rust().verify_user(socket, user_info, signature),
            Backend::Go => self.go("VerifyUser", json!({"Socket": socket, "Info": user_info, "Signature": signature})),
        }
    }

    pub fn verify(&self, input: &str, signature: &str) -> bool {
        match backend() {
            Backend::Rust => self.rust().verify(input, signature),
            Backend::Go => self.go("Verify", json!({"Input": input, "Signature": signature})),
        }
    }

    pub fn sign(&self, input: &str) -> String {
        match backend() {
            Backend::Rust => self.rust().sign(input),
            Backend::Go => self.go("Sign", json!({"Input": input})),
        }
    }
}

// ------------------------------------------------------------------ messages (shared with sse)

fn presence_event_json(p: &PresenceEvent) -> Value {
    json!({"type": p.kind, "info": p.info, "id": p.id})
}

/// An `encoders.EncodedMessage` as `{"kind", "msg"}` with the upstream JSON tags.
pub(crate) fn encoded_message_json(m: &EncodedMessage) -> Value {
    match m {
        EncodedMessage::Reply(r) => json!({"kind": "reply", "msg": {
            "type": r.kind, "identifier": r.identifier, "message": r.message,
            "presence": r.presence.as_ref().map(presence_event_json),
            "reason": r.reason, "reconnect": r.reconnect, "stream_id": r.stream_id, "epoch": r.epoch,
            "offset": r.offset, "sid": r.sid, "restored": r.restored, "restored_ids": r.restored_ids,
        }}),
        EncodedMessage::Ping(p) => json!({"kind": "ping", "msg": {"type": p.kind, "message": p.message}}),
        EncodedMessage::Disconnect(d) => {
            json!({"kind": "disconnect", "msg": {"type": d.kind, "reason": d.reason, "reconnect": d.reconnect}})
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct GoFrameResult {
    frame: Option<GoFrame>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct GoFrame {
    frame_type: i64,
    payload: String,
}

impl GoFrameResult {
    pub(crate) fn into_result(self) -> Result<Option<SentFrame>, String> {
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok(self.frame.map(|f| SentFrame {
            frame_type: match f.frame_type {
                0 => cabby_core::common::FrameType::Text,
                1 => cabby_core::common::FrameType::Close,
                2 => cabby_core::common::FrameType::Binary,
                other => panic!("unknown Go frame type {other}"),
            },
            payload: f.payload.into_bytes(),
            ..Default::default()
        }))
    }
}

pub(crate) fn rust_frame(r: Result<Option<SentFrame>, Error>) -> Result<Option<SentFrame>, String> {
    r.map_err(|e| e.0)
}

#[derive(Deserialize)]
pub(crate) struct GoDecoded {
    message: Option<GoMessage>,
    data_type: String,
    error: Option<String>,
}

#[derive(Deserialize)]
struct GoMessage {
    command: String,
    identifier: String,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    history: GoHistory,
    #[serde(default)]
    presence: Option<GoPresenceEvent>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GoHistory {
    since: i64,
    streams: Option<std::collections::HashMap<String, GoHistoryPosition>>,
}

#[derive(Deserialize)]
struct GoHistoryPosition {
    epoch: String,
    offset: u64,
}

fn str_field(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or_default().to_owned()
}

impl GoDecoded {
    pub(crate) fn into_result(self) -> Result<Message, String> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let m = self.message.expect("decoded message");
        let data = m.data.map(|d| match self.data_type.as_str() {
            "*pusher.PusherSubscriptionData" => MessageData::Typed(Arc::new(PusherSubscriptionData {
                channel: str_field(&d, "channel"),
                auth: str_field(&d, "auth"),
                channel_data: str_field(&d, "channel_data"),
            })),
            "*pusher.PusherClientEvent" => MessageData::Typed(Arc::new(PusherClientEvent {
                event: str_field(&d, "event"),
                channel: str_field(&d, "channel"),
                data: d.get("data").cloned().filter(|v| !v.is_null()),
            })),
            _ => MessageData::Json(d),
        });
        Ok(Message {
            command: m.command,
            identifier: m.identifier,
            data,
            history: HistoryRequest {
                since: m.history.since,
                streams: m
                    .history
                    .streams
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, p)| (k, HistoryPosition { epoch: p.epoch, offset: p.offset }))
                    .collect(),
            },
            presence: m.presence.map(Into::into),
        })
    }
}

// ------------------------------------------------------------------ encoder

fn encoder_args(enc: &rs::Encoder) -> Value {
    json!({"ActivityTimeout": enc.activity_timeout})
}

/// `coder.Encode(msg)`.
pub fn encode(enc: &rs::Encoder, msg: &EncodedMessage) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(enc.encode(msg)),
        Backend::Go => {
            let mut a = encoder_args(enc);
            a["Msg"] = encoded_message_json(msg);
            bridge::call::<_, GoFrameResult>("pusher.Encoder.Encode", a).into_result()
        }
    }
}

/// `coder.EncodeTransmission(raw)`.
pub fn encode_transmission(enc: &rs::Encoder, raw: &str) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(enc.encode_transmission(raw)),
        Backend::Go => {
            let mut a = encoder_args(enc);
            a["Raw"] = json!(raw);
            bridge::call::<_, GoFrameResult>("pusher.Encoder.EncodeTransmission", a).into_result()
        }
    }
}

/// `coder.Decode(raw)`.
pub fn decode(enc: &rs::Encoder, raw: &[u8]) -> Result<Message, String> {
    match backend() {
        Backend::Rust => enc.decode(raw).map_err(|e| e.0),
        Backend::Go => {
            let mut a = encoder_args(enc);
            a["Raw"] = json!(String::from_utf8(raw.to_vec()).expect("UTF-8 payload"));
            bridge::call::<_, GoDecoded>("pusher.Encoder.Decode", a).into_result()
        }
    }
}

// ------------------------------------------------------------------ controller

/// A `mocks.Broker` whose only expectation is `PresenceInfo(mock.Anything)` returning
/// `presence_info` (`None`: no expectations). Unexpected calls fail, as with testify.
#[derive(Debug, Clone, Default)]
pub struct StubBroker {
    pub presence_info: Option<PresenceInfo>,
}

impl Broker for StubBroker {
    fn supports_presence(&self) -> bool {
        panic!("mocks.Broker: unexpected call SupportsPresence()")
    }

    fn presence_add(&self, stream: &str, _: &str, _: &str, _: Value) -> Result<Option<PresenceEvent>, Error> {
        panic!("mocks.Broker: unexpected call PresenceAdd({stream:?}, ...)")
    }

    fn presence_info(&self, stream: &str) -> Result<PresenceInfo, Error> {
        match &self.presence_info {
            Some(p) => Ok(p.clone()),
            None => panic!("mocks.Broker: unexpected call PresenceInfo({stream:?})"),
        }
    }
}

fn go_presence_info(p: &PresenceInfo) -> Value {
    json!({"type": p.kind, "total": p.total, "records": p.records.iter().map(presence_event_json).collect::<Vec<_>>()})
}

pub(crate) fn env_json(env: &SessionEnv) -> Value {
    json!({
        "url": env.url, "headers": env.headers, "identifiers": env.identifiers,
        "connection_state": env.connection_state, "channel_states": env.channel_states,
    })
}

/// `pusher.NewController(&broker, &conf, logger)`.
pub struct Controller {
    broker: StubBroker,
    conf: rs::Config,
}

impl Controller {
    pub fn new(broker: StubBroker, conf: &rs::Config) -> Self {
        Self { broker, conf: conf.clone() }
    }

    fn rust(&self) -> rs::Controller {
        rs::Controller::new(Some(Arc::new(self.broker.clone())), self.conf.clone())
    }

    fn go(&self, op: &str, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> CallResult<CommandResult> {
        let p: GoParsed<GoCommandResult> = bridge::call(
            op,
            json!({"Config": go_config(&self.conf), "Presence": self.broker.presence_info.as_ref().map(go_presence_info),
                   "Env": env_json(env), "Sid": sid, "Ids": ids, "Identifier": identifier}),
        );
        CallResult { result: p.result.map(Into::into), error: p.error.map(Error) }
    }

    pub fn subscribe(&self, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> CallResult<CommandResult> {
        match backend() {
            Backend::Rust => self.rust().subscribe(sid, env, ids, identifier),
            Backend::Go => self.go("pusher.Controller.Subscribe", sid, env, ids, identifier),
        }
    }

    pub fn unsubscribe(&self, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> CallResult<CommandResult> {
        match backend() {
            Backend::Rust => self.rust().unsubscribe(sid, env, ids, identifier),
            Backend::Go => self.go("pusher.Controller.Unsubscribe", sid, env, ids, identifier),
        }
    }
}

// ------------------------------------------------------------------ executor

/// `pusher.NewExecutor(app, pusher.NewVerifier(app_id, key))`.
pub struct Executor {
    inner: rs::Executor,
}

impl Executor {
    pub fn new(app: Arc<dyn deps::Executor>, app_id: &str, key: &str) -> Self {
        Self { inner: rs::Executor::new(app, rs::Verifier::new(app_id, key)) }
    }

    pub fn handle_command(&self, session: &mut dyn ExecutorSession, msg: &Message) -> Result<(), Error> {
        match backend() {
            Backend::Rust => self.inner.handle_command(session, msg),
            Backend::Go => unimplemented!(
                "not_observable: the upstream case drives pusher.Executor through a real node.Session \
                 (mocks.Connection) and a test-defined MockExecutor; covered externally by \
                 tests/conformance/pusher.rs"
            ),
        }
    }
}

// ------------------------------------------------------------------ REST API

/// Upstream `broker.NewMemoryBroker(nil, nil, &broker.NewConfig())` for the REST API.
#[derive(Clone)]
pub struct MemoryBroker {
    rust: Option<Arc<deps::MemoryBroker>>,
    /// Go: presence records replayed into a fresh memory broker on every request.
    adds: Arc<Mutex<Vec<Value>>>,
}

impl MemoryBroker {
    pub fn new() -> Self {
        let rust = match backend() {
            Backend::Rust => Some(Arc::new(deps::MemoryBroker::new())),
            Backend::Go => None,
        };
        Self { rust, adds: Default::default() }
    }

    pub fn start(&self) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.start().map_err(|e| e.0),
            None => Ok(()), // the bridge starts its broker per request
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.shutdown().map_err(|e| e.0),
            None => Ok(()),
        }
    }

    /// `PresenceAdd(stream, sid, pid, info)`. Against Go the call is replayed in the
    /// bridge, which reports a failure as a harness error.
    pub fn presence_add(&self, stream: &str, sid: &str, pid: &str, info: Value) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.presence_add(stream, sid, pid, info).map(|_| ()).map_err(|e| e.0),
            None => {
                self.adds.lock().unwrap().push(json!({"Stream": stream, "Sid": sid, "Pid": pid, "Info": info}));
                Ok(())
            }
        }
    }
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ApiBroker {
    /// `&mocks.Broker{}` without expectations.
    Mock,
    Memory(MemoryBroker),
}

/// `mocks.Handler` with `On("HandleBroadcast", ...).Return(nil)`, recording payloads.
#[derive(Default)]
struct BroadcastRecorder(Mutex<Vec<Vec<u8>>>);

impl BroadcastHandler for BroadcastRecorder {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), Error> {
        self.0.lock().unwrap().push(payload.to_vec());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    /// Lower-cased names.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Request body bytes the handler consumed.
    pub body_read: usize,
}

struct CountingReader {
    inner: Cursor<Vec<u8>>,
    n: Arc<AtomicUsize>,
}

impl Read for CountingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.n.fetch_add(n, Ordering::SeqCst);
        Ok(n)
    }
}

/// `pusher.NewRestAPI(handler, broker, &conf, logger)`, served through
/// `http.HandlerFunc(api.Handler).ServeHTTP` with an `httptest` recorder.
pub struct RestApi {
    conf: rs::Config,
    broker: ApiBroker,
    handler: Arc<BroadcastRecorder>,
    rust: Option<rs::RestApi>,
}

impl RestApi {
    pub fn new(broker: ApiBroker, conf: &rs::Config) -> Self {
        let handler = Arc::new(BroadcastRecorder::default());
        let rust = match backend() {
            Backend::Rust => {
                let b: Arc<dyn Broker> = match &broker {
                    ApiBroker::Mock => Arc::new(StubBroker::default()),
                    ApiBroker::Memory(m) => m.rust.clone().expect("Rust memory broker"),
                };
                Some(rs::RestApi::new(handler.clone(), b, conf.clone()))
            }
            Backend::Go => None,
        };
        Self { conf: conf.clone(), broker, handler, rust }
    }

    /// Serves `method target` (`target` is `path?query`) with `body`.
    pub fn serve(&self, method: &str, target: &str, body: &[u8]) -> ApiResponse {
        match &self.rust {
            Some(api) => {
                let n = Arc::new(AtomicUsize::new(0));
                let reader: Box<dyn Read + Send> =
                    Box::new(CountingReader { inner: Cursor::new(body.to_vec()), n: n.clone() });
                let req = http::Request::builder().method(method).uri(target).body(reader).expect("valid request");
                let res = api.handle(req);
                ApiResponse {
                    status: res.status().as_u16(),
                    headers: res
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned()))
                        .collect(),
                    body: res.into_body(),
                    body_read: n.load(Ordering::SeqCst),
                }
            }
            None => {
                let (kind, presence) = match &self.broker {
                    ApiBroker::Mock => ("mock", vec![]),
                    ApiBroker::Memory(m) => ("memory", m.adds.lock().unwrap().clone()),
                };
                let r: GoApiResponse = bridge::call(
                    "pusher.RestAPI.Handler",
                    json!({"Config": go_config(&self.conf), "Broker": kind, "Presence": presence,
                           "Method": method, "Target": target,
                           "Body": String::from_utf8(body.to_vec()).expect("UTF-8 body")}),
                );
                let mut calls = self.handler.0.lock().unwrap();
                calls.extend(r.broadcasts.iter().map(|b| b.clone().into_bytes()));
                r.into_response()
            }
        }
    }

    /// Payloads passed to `HandleBroadcast`, in order.
    pub fn broadcasts(&self) -> Vec<Vec<u8>> {
        self.handler.0.lock().unwrap().clone()
    }
}

#[derive(Deserialize)]
pub(crate) struct GoApiResponse {
    status: u16,
    headers: std::collections::HashMap<String, Vec<String>>,
    body: String,
    #[serde(default)]
    body_read: usize,
    #[serde(default)]
    broadcasts: Vec<String>,
}

impl GoApiResponse {
    pub(crate) fn into_response(self) -> ApiResponse {
        let mut headers: Vec<(String, String)> = self
            .headers
            .into_iter()
            .flat_map(|(k, vs)| vs.into_iter().map(move |v| (k.to_ascii_lowercase(), v)))
            .collect();
        headers.sort();
        ApiResponse { status: self.status, headers, body: self.body.into_bytes(), body_read: self.body_read }
    }
}

// ------------------------------------------------------------------ Go result shapes

#[derive(Deserialize)]
pub(crate) struct GoParsed<T> {
    pub(crate) result: Option<T>,
    pub(crate) error: Option<String>,
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
            presence: g.presence.map(Into::into),
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

impl From<GoPresenceEvent> for PresenceEvent {
    fn from(p: GoPresenceEvent) -> Self {
        PresenceEvent { kind: p.kind, info: p.info, id: p.id }
    }
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
