//! Facade for upstream package `pusher`.
//!
//! Go shapes: `pusher.Config` and `common.CommandResult` have no JSON tags (Go field
//! names); `common.Reply`, `common.Message` and friends use their upstream tags.

use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cabby_core::broadcast::{BroadcastError, Handler as BroadcastHandler};
use cabby_core::broker::{self as core_broker, Broker, BrokerError, Cacheable, PresenceInfoOptions};
use cabby_core::common::{
    CommandResult, Controller as _, ControllerError, EncodedMessage, HistoryPosition, HistoryRequest, Message,
    MessageData, Outcome, PresenceEvent, PresenceInfo, RemoteCommandMessage, SentFrame, SessionEnv, Status,
    StreamMessage, StreamMessageMetadata, StringMap,
};
use cabby_server::node::{Executor as NodeExecutor, NodeError, Session, SessionOption};
use cabby_server::pusher::{self as rs, PusherClientEvent, PusherSubscriptionData};
// Shared with the sse/ds facades (`rust_frame`).
use serde::Deserialize;
use serde_json::{Value, json};

use super::node_doubles::MockConnection;
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

/// `pusher.NewConfig()`: Go's defaults against Go, the Rust constants against Rust.
pub fn new_config() -> rs::Config {
    match backend() {
        Backend::Rust => rs::Config::new(),
        Backend::Go => {
            let g: GoConfig = bridge::call("pusher.NewConfig", json!({}));
            rs::Config {
                app_id: g.app_id,
                app_key: g.app_key,
                secret: g.secret,
                api_port: g.api_port,
                add_cors_headers: g.add_cors_headers,
                cors_hosts: g.cors_hosts,
                max_body_size: g.max_body_size,
            }
        }
    }
}

/// `pusher.NewEncoder()`: Go's default activity timeout against Go.
pub fn new_encoder() -> rs::Encoder {
    match backend() {
        Backend::Rust => rs::Encoder::new(),
        Backend::Go => {
            let v: Value = bridge::call("pusher.NewEncoder", json!({}));
            rs::Encoder { activity_timeout: v["ActivityTimeout"].as_i64().expect("ActivityTimeout") }
        }
    }
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
            // Upstream asserts `Error(err)` *and* `Nil(actual)`: a message returned
            // alongside the error must not be hidden by the `Err`.
            assert!(self.message.is_none(), "Go returned a non-nil message together with error {e:?}");
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
        Backend::Rust => enc.encode_message(msg).map_err(|e| e.0),
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
        Backend::Rust => enc.encode_raw_transmission(raw).map_err(|e| e.0),
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
        Backend::Rust => enc.decode_message(raw).map_err(|e| e.0),
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

fn unexpected(call: &str) -> ! {
    panic!("mocks.Broker: unexpected call {call}")
}

impl Broker for StubBroker {
    fn start(&self) -> Result<(), BrokerError> {
        unexpected("Start()")
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        unexpected("Shutdown()")
    }

    fn announce(&self) -> String {
        unexpected("Announce()")
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        unexpected(&format!("HandleBroadcast({:?})", msg.stream))
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        unexpected(&format!("HandleCommand({:?})", msg.command))
    }

    fn subscribe(&self, stream: &str) -> String {
        unexpected(&format!("Subscribe({stream:?})"))
    }

    fn unsubscribe(&self, stream: &str) -> String {
        unexpected(&format!("Unsubscribe({stream:?})"))
    }

    fn history_from(&self, stream: &str, _: &str, _: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        unexpected(&format!("HistoryFrom({stream:?}, ...)"))
    }

    fn history_since(&self, stream: &str, _: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        unexpected(&format!("HistorySince({stream:?}, ...)"))
    }

    fn commit_session(&self, sid: &str, _: &dyn Cacheable) -> Result<(), BrokerError> {
        unexpected(&format!("CommitSession({sid:?}, ...)"))
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        unexpected(&format!("RestoreSession({sid:?})"))
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        unexpected(&format!("TouchSession({sid:?})"))
    }

    fn supports_presence(&self) -> bool {
        unexpected("SupportsPresence()")
    }

    fn presence_add(&self, stream: &str, _: &str, _: &str, _: Value) -> Result<Option<PresenceEvent>, BrokerError> {
        unexpected(&format!("PresenceAdd({stream:?}, ...)"))
    }

    fn presence_remove(&self, stream: &str, _: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        unexpected(&format!("PresenceRemove({stream:?}, ...)"))
    }

    fn presence_info(&self, stream: &str, _: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        match &self.presence_info {
            Some(p) => Ok(p.clone()),
            None => unexpected(&format!("PresenceInfo({stream:?})")),
        }
    }

    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        unexpected(&format!("TouchPresence({sid:?})"))
    }

    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        unexpected(&format!("Peak({stream:?})"))
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

    fn go(&self, op: &str, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> ControllerCall {
        let p: GoParsed<Value> = bridge::call(
            op,
            json!({"Config": go_config(&self.conf), "Presence": self.broker.presence_info.as_ref().map(go_presence_info),
                   "Env": env_json(env), "Sid": sid, "Ids": ids, "Identifier": identifier}),
        );
        let raw = p.result.unwrap_or(Value::Null);
        let nil = |k: &str| raw.get(k).is_none_or(Value::is_null);
        let (nil_transmissions, nil_streams) = (nil("Transmissions"), nil("Streams"));
        let result =
            (!raw.is_null()).then(|| serde_json::from_value::<GoCommandResult>(raw).expect("CommandResult").into());
        ControllerCall { result, error: p.error.map(ControllerError::new), nil_transmissions, nil_streams }
    }

    fn rust_call(out: Outcome<CommandResult>) -> ControllerCall {
        // Rust `CommandResult` slices are never nil.
        ControllerCall { result: out.result, error: out.error, nil_transmissions: false, nil_streams: false }
    }

    pub fn subscribe(&self, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> ControllerCall {
        match backend() {
            Backend::Rust => Self::rust_call(self.rust().subscribe(sid, Some(env), ids, identifier)),
            Backend::Go => self.go("pusher.Controller.Subscribe", sid, env, ids, identifier),
        }
    }

    pub fn unsubscribe(&self, sid: &str, env: &SessionEnv, ids: &str, identifier: &str) -> ControllerCall {
        match backend() {
            Backend::Rust => Self::rust_call(self.rust().unsubscribe(sid, Some(env), ids, identifier)),
            Backend::Go => self.go("pusher.Controller.Unsubscribe", sid, env, ids, identifier),
        }
    }
}

/// A controller `(*CommandResult, error)` plus the Go nil-slice distinction that
/// `CommandResult` cannot carry (`[]string{}` vs nil; always non-nil in Rust).
#[derive(Debug, Clone)]
pub struct ControllerCall {
    pub result: Option<CommandResult>,
    pub error: Option<ControllerError>,
    pub nil_transmissions: bool,
    pub nil_streams: bool,
}

// ------------------------------------------------------------------ executor

/// The upstream test's `MockExecutor`: runs `Subscribe` on the pusher controller and
/// sends the transmissions to the session; anything else is "not implemented".
struct MockExecutor {
    ctrl: Controller,
}

impl NodeExecutor for MockExecutor {
    fn handle_command(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        let mut res = None;
        let mut err = None;

        if msg.command == "subscribe" {
            let out = self.ctrl.subscribe(&s.get_id(), &s.get_env(), &s.get_identifiers(), &msg.identifier);
            res = out.result;
            err = out.error;
        }

        if let Some(r) = &res {
            for t in &r.transmissions {
                s.send_json_transmission(t);
            }
        }

        if res.is_none() {
            return Err(NodeError("not implemeented".into()));
        }

        err.map_or(Ok(()), |e| Err(NodeError(e.message)))
    }

    fn disconnect(&self, _s: &Arc<Session>) -> Result<(), NodeError> {
        Ok(())
    }
}

/// The outcome of one `executor.HandleCommand` and the following `conn.Read()`s.
#[derive(Debug, Clone)]
pub struct ExecutorRun {
    pub error: Option<String>,
    pub reads: Vec<Result<Vec<u8>, String>>,
}

/// `TestHandleCommand` setup: config with `AppKey = app_id`, `Secret = key`; a
/// `mocks.Broker` whose `PresenceInfo` returns `presence`; `MockExecutor` over the
/// pusher controller; `pusher.NewExecutor(app, pusher.NewVerifier(app_id, key))`.
pub struct Executor {
    app_id: String,
    key: String,
    presence: PresenceInfo,
}

impl Executor {
    pub fn new(app_id: &str, key: &str, presence: PresenceInfo) -> Self {
        Self { app_id: app_id.into(), key: key.into(), presence }
    }

    /// `executor.HandleCommand(session, &msg)` on a fresh `buildSession`, then
    /// `conn.Read()` `reads` times.
    pub fn handle_command(&self, msg: &Message, reads: usize) -> ExecutorRun {
        match backend() {
            Backend::Rust => {
                let mut conf = rs::Config::new();
                conf.app_key = self.app_id.clone();
                conf.secret = self.key.clone();
                let bro = StubBroker { presence_info: Some(self.presence.clone()) };
                let app = Arc::new(MockExecutor { ctrl: Controller::new(bro, &conf) });
                let executor = rs::Executor::new(app, rs::Verifier::new(&self.app_id, &self.key));
                // `buildSession`: a session over a mock connection with sid "1234.1234",
                // the test URL, no headers and the zero-value encoder `&Encoder{}`.
                let conn = MockConnection::new();
                let session = Session::build(
                    conn.clone(),
                    SessionEnv { url: "ws://anycable.io/pusher/app-id".into(), ..Default::default() },
                );
                session.apply(vec![SessionOption::Encoder(Arc::new(rs::Encoder::default()))]);
                session.set_id("1234.1234");
                session.send_messages();
                let error = NodeExecutor::handle_command(&executor, &session, msg).err().map(|e| e.0);
                let reads = (0..reads).map(|_| cabby_server::node::Connection::read(&*conn).map_err(|e| e.0)).collect();
                ExecutorRun { error, reads }
            }
            Backend::Go => {
                let data = msg.data.as_ref().map(|d| {
                    let sub = d.downcast_ref::<PusherSubscriptionData>().expect("subscription data");
                    json!({"channel": sub.channel, "auth": sub.auth, "channel_data": sub.channel_data})
                });
                let r: Value = bridge::call(
                    "pusher.Executor.HandleCommand",
                    json!({"AppID": self.app_id, "Key": self.key, "Presence": go_presence_info(&self.presence),
                           "Command": msg.command, "Identifier": msg.identifier, "SubscriptionData": data,
                           "Reads": reads}),
                );
                let reads = r["reads"]
                    .as_array()
                    .expect("reads")
                    .iter()
                    .map(|x| match x["error"].as_str() {
                        Some(e) => Err(e.to_owned()),
                        None => Ok(x["msg"].as_str().unwrap_or_default().as_bytes().to_vec()),
                    })
                    .collect();
                ExecutorRun { error: r["error"].as_str().map(str::to_owned), reads }
            }
        }
    }
}

// ------------------------------------------------------------------ REST API

/// Upstream `broker.NewMemoryBroker(nil, nil, &broker.NewConfig())` for the REST API.
#[derive(Clone)]
pub struct MemoryBroker {
    rust: Option<Arc<core_broker::Memory>>,
    /// Go: presence records replayed into a fresh memory broker on every request.
    adds: Arc<Mutex<Vec<Value>>>,
}

impl MemoryBroker {
    pub fn new() -> Self {
        let rust = match backend() {
            Backend::Rust => Some(Arc::new(core_broker::Memory::new(
                None,
                None,
                &core_broker::Config::new(),
                Arc::new(core_broker::SystemClock),
            ))),
            Backend::Go => None,
        };
        Self { rust, adds: Default::default() }
    }

    pub fn start(&self) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.start().map_err(|e| e.message),
            None => Ok(()), // the bridge starts its broker per request
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.shutdown().map_err(|e| e.message),
            None => Ok(()),
        }
    }

    /// `PresenceAdd(stream, sid, pid, info)`. Against Go the call is replayed in the
    /// bridge, which reports a failure as a harness error.
    pub fn presence_add(&self, stream: &str, sid: &str, pid: &str, info: Value) -> Result<(), String> {
        match &self.rust {
            Some(b) => b.presence_add(stream, sid, pid, info).map(|_| ()).map_err(|e| e.message),
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

/// How the REST API's `mocks.Handler` is programmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerMock {
    /// `On("HandleBroadcast", ...).Return(nil)`: calls are accepted and recorded.
    AcceptBroadcasts,
    /// `&mocks.Handler{}`: every call is forbidden.
    NoExpectations,
}

/// `mocks.Handler`: accepted payloads in order, plus forbidden calls.
struct BroadcastRecorder {
    mode: HandlerMock,
    calls: Mutex<Vec<Vec<u8>>>,
    unexpected: Mutex<Vec<String>>,
}

impl BroadcastHandler for BroadcastRecorder {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), BroadcastError> {
        match self.mode {
            HandlerMock::AcceptBroadcasts => {
                self.calls.lock().unwrap().push(payload.to_vec());
                Ok(())
            }
            HandlerMock::NoExpectations => {
                let call = format!("HandleBroadcast({:?})", String::from_utf8_lossy(payload));
                self.unexpected.lock().unwrap().push(call.clone());
                Err(BroadcastError(format!("mocks.Handler: unexpected call {call}")))
            }
        }
    }

    fn handle_pubsub(&self, payload: &[u8]) {
        let call = format!("HandlePubSub({:?})", String::from_utf8_lossy(payload));
        self.unexpected.lock().unwrap().push(call);
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
    pub fn new(broker: ApiBroker, handler: HandlerMock, conf: &rs::Config) -> Self {
        let handler =
            Arc::new(BroadcastRecorder { mode: handler, calls: Default::default(), unexpected: Default::default() });
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
                           "Body": String::from_utf8(body.to_vec()).expect("UTF-8 body"),
                           "AcceptBroadcasts": self.handler.mode == HandlerMock::AcceptBroadcasts}),
                );
                self.handler.calls.lock().unwrap().extend(r.broadcasts.iter().map(|b| b.clone().into_bytes()));
                self.handler.unexpected.lock().unwrap().extend(r.unexpected.iter().cloned());
                r.into_response()
            }
        }
    }

    /// Payloads passed to `HandleBroadcast` and accepted, in order.
    pub fn broadcasts(&self) -> Vec<Vec<u8>> {
        self.handler.calls.lock().unwrap().clone()
    }

    /// `HandleBroadcast` calls the mock had no expectation for.
    pub fn unexpected_calls(&self) -> Vec<String> {
        self.handler.unexpected.lock().unwrap().clone()
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
    #[serde(default)]
    unexpected: Vec<String>,
}

impl GoApiResponse {
    pub(crate) fn from_parts(
        status: u16,
        headers: std::collections::HashMap<String, Vec<String>>,
        body: String,
    ) -> ApiResponse {
        GoApiResponse { status, headers, body, body_read: 0, broadcasts: vec![], unexpected: vec![] }.into_response()
    }

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
