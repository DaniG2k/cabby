//! Facade for upstream package `sse`.
//!
//! Also provides the Rust analogues of the upstream test's `httptest.ResponseRecorder`
//! and `streamingWriter` (test doubles for `http.ResponseWriter`).

use std::cell::RefCell;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, EncodedMessage, Message, Outcome, SentFrame, SessionEnv,
    StreamMessage,
};
use cabby_core::encoders::{CachedEncodedMessage as RsCached, EncodeError, Encoder as _};
use cabby_server::metrics::{Instrumenter, Metrics};
use cabby_server::node::{self as node_rs, Node as RsNode, NodeOption};
use cabby_server::server::{DefaultHeadersExtractor, ResponseWriter};
use cabby_server::sse as rs;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::go_types::{command_result_to_go, connect_result_to_go};
use super::pusher::{ApiResponse, GoApiResponse, GoDecoded, GoFrameResult, GoParsed, encoded_message_json};
use crate::{Backend, backend, bridge};

// ------------------------------------------------------------------ config

fn go_config(c: &rs::Config) -> Value {
    json!({"Enabled": c.enabled, "Path": c.path, "CommentPings": c.comment_pings,
           "PingInterval": c.ping_interval, "AllowedOrigins": c.allowed_origins})
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    enabled: bool,
    path: String,
    comment_pings: bool,
    ping_interval: i64,
    allowed_origins: String,
}

/// `sse.NewConfig()`: Go's defaults against Go, the Rust constants against Rust.
pub fn new_config() -> rs::Config {
    match backend() {
        Backend::Rust => rs::Config::new(),
        Backend::Go => {
            let g: GoConfig = bridge::call("sse.NewConfig", json!({}));
            rs::Config {
                enabled: g.enabled,
                path: g.path,
                comment_pings: g.comment_pings,
                ping_interval: g.ping_interval,
                allowed_origins: g.allowed_origins,
            }
        }
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(conf: &rs::Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("sse.Config.ToToml", go_config(conf)),
    }
}

/// `toml.Decode(s, &sse.Config{})` (BurntSushi/toml in Go, serde + `toml` in Rust).
pub fn decode_config_toml(s: &str) -> Result<rs::Config, String> {
    match backend() {
        Backend::Rust => toml::from_str(s).map_err(|e| e.to_string()),
        Backend::Go => {
            let p: GoParsed<GoConfig> = bridge::call("sse.DecodeConfigToml", json!({"Toml": s}));
            if let Some(e) = p.error {
                return Err(e);
            }
            let g = p.result.expect("decoded config");
            Ok(rs::Config {
                enabled: g.enabled,
                path: g.path,
                comment_pings: g.comment_pings,
                ping_interval: g.ping_interval,
                allowed_origins: g.allowed_origins,
            })
        }
    }
}

// ------------------------------------------------------------------ encoder

/// A Rust encoder result as the Go `(frame, err)` pair (`None`: a nil frame).
pub(crate) fn rust_frame(r: Result<Option<SentFrame>, EncodeError>) -> Result<Option<SentFrame>, String> {
    r.map_err(|e| e.0)
}

fn go_encoder(e: &rs::Encoder) -> Value {
    json!({"RawData": e.raw_data, "UnwrapData": e.unwrap_data, "CommentPings": e.comment_pings})
}

/// `coder.ID()`.
pub fn encoder_id(enc: &rs::Encoder) -> String {
    match backend() {
        Backend::Rust => enc.id(),
        Backend::Go => bridge::call("sse.Encoder.ID", json!({"Encoder": go_encoder(enc)})),
    }
}

/// `coder.Encode(msg)`.
pub fn encode(enc: &rs::Encoder, msg: &EncodedMessage) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(enc.encode_optional(msg)),
        Backend::Go => bridge::call::<_, GoFrameResult>(
            "sse.Encoder.Encode",
            json!({"Encoder": go_encoder(enc), "Msg": encoded_message_json(msg)}),
        )
        .into_result(),
    }
}

/// `coder.EncodeTransmission(raw)`.
pub fn encode_transmission(enc: &rs::Encoder, raw: &str) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(enc.encode_transmission_optional(raw)),
        Backend::Go => bridge::call::<_, GoFrameResult>(
            "sse.Encoder.EncodeTransmission",
            json!({"Encoder": go_encoder(enc), "Raw": raw}),
        )
        .into_result(),
    }
}

/// `coder.Decode(raw)`.
pub fn decode(enc: &rs::Encoder, raw: &[u8]) -> Result<Message, String> {
    match backend() {
        Backend::Rust => enc.decode(raw).map_err(|e| e.0),
        Backend::Go => bridge::call::<_, GoDecoded>(
            "sse.Encoder.Decode",
            json!({"Encoder": go_encoder(enc), "Raw": String::from_utf8(raw.to_vec()).expect("UTF-8")}),
        )
        .into_result(),
    }
}

/// `encoders.NewCachedEncodedMessage(msg)`; [`CachedEncodedMessage::fetch`] is
/// `msg.Fetch(enc.ID(), enc.Encode)`.
pub struct CachedEncodedMessage {
    msg: EncodedMessage,
    rust: Option<RsCached>,
    /// Go: fetches so far, replayed in order on a fresh cached message.
    fetched: RefCell<Vec<rs::Encoder>>,
}

impl CachedEncodedMessage {
    pub fn new(msg: EncodedMessage) -> Self {
        let rust = (backend() == Backend::Rust).then(|| RsCached::new(msg.clone()));
        Self { msg, rust, fetched: RefCell::new(Vec::new()) }
    }

    pub fn fetch(&self, enc: &rs::Encoder) -> Result<Option<SentFrame>, String> {
        match &self.rust {
            Some(c) => c.fetch(&enc.id(), |m| enc.encode_optional(m)).map(|f| Some((*f).clone())).map_err(|e| e.0),
            None => {
                self.fetched.borrow_mut().push(*enc);
                let encoders: Vec<Value> = self.fetched.borrow().iter().map(go_encoder).collect();
                let mut results: Vec<GoFrameResult> = bridge::call(
                    "sse.CachedEncodedMessage.Fetch",
                    json!({"Msg": encoded_message_json(&self.msg), "Encoders": encoders}),
                );
                results.pop().expect("one result per fetch").into_result()
            }
        }
    }
}

// ------------------------------------------------------------------ response writers

#[derive(Debug, Default)]
struct RecorderState {
    /// Go `httptest.ResponseRecorder.Code`: 200 unless `WriteHeader` set another code.
    code: Option<u16>,
    wrote_header: bool,
    headers: http::HeaderMap,
    /// `None` until something is written (Go `Body.Bytes()` is nil then).
    body: Option<Vec<u8>>,
    flushed: bool,
}

/// `httptest.NewRecorder()`: a shareable in-memory `http.ResponseWriter`.
#[derive(Clone, Default)]
pub struct Recorder(Arc<Mutex<RecorderState>>);

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn code(&self) -> u16 {
        self.0.lock().unwrap().code.unwrap_or(200)
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().headers.get(name).map(|v| v.to_str().unwrap_or_default().to_owned())
    }

    pub fn body_bytes(&self) -> Option<Vec<u8>> {
        self.0.lock().unwrap().body.clone()
    }

    /// Every published header (lower-cased names, sorted).
    pub fn headers(&self) -> Vec<(String, String)> {
        let s = self.0.lock().unwrap();
        let mut h: Vec<(String, String)> =
            s.headers.iter().map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned())).collect();
        h.sort();
        h
    }

    pub fn body_string(&self) -> String {
        String::from_utf8_lossy(&self.body_bytes().unwrap_or_default()).into_owned()
    }

    fn wrote_header(&self) -> bool {
        self.0.lock().unwrap().wrote_header
    }
}

/// The writer half of a [`Recorder`]: buffers header edits locally and publishes
/// every change to the shared state, so tests can observe it while the handler runs.
pub struct OwnedRecorder {
    shared: Recorder,
    headers: http::HeaderMap,
    /// Streaming: every write split into events at blank lines (`streamingWriter`).
    events: Option<Sender<Vec<u8>>>,
    status_seen: Option<Sender<u16>>,
}

impl OwnedRecorder {
    pub fn new(shared: &Recorder) -> Self {
        Self { shared: shared.clone(), headers: http::HeaderMap::new(), events: None, status_seen: None }
    }

    /// Go's recorder exposes its live header map; publish the local copy.
    fn publish_headers(&self) {
        self.shared.0.lock().unwrap().headers = self.headers.clone();
    }
}

impl ResponseWriter for OwnedRecorder {
    fn headers_mut(&mut self) -> &mut http::HeaderMap {
        // Edits become visible to the test on the next status line or when the
        // handler drops the writer.
        &mut self.headers
    }

    fn write_header(&mut self, status: u16) {
        self.publish_headers();
        let mut s = self.shared.0.lock().unwrap();
        if s.wrote_header {
            return;
        }
        s.wrote_header = true;
        s.code = Some(status);
        drop(s);
        if let Some(tx) = &self.status_seen {
            let _ = tx.send(status);
        }
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if !self.shared.wrote_header() {
            self.write_header(200);
        }
        {
            let mut s = self.shared.0.lock().unwrap();
            s.body.get_or_insert_with(Vec::new).extend_from_slice(data);
        }
        if let Some(tx) = &self.events {
            for event in data.split_inner(b"\n\n") {
                if !event.is_empty() {
                    let _ = tx.send(event.to_vec());
                }
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) {
        if !self.shared.wrote_header() {
            self.write_header(200);
        }
        self.shared.0.lock().unwrap().flushed = true;
    }

    fn finish(&mut self) {
        // The handler returned: Go's recorder shows its final header map.
        self.publish_headers();
    }
}

impl Drop for OwnedRecorder {
    fn drop(&mut self) {
        self.publish_headers();
    }
}

trait SplitInner {
    fn split_inner<'a>(&'a self, sep: &'a [u8]) -> Vec<&'a [u8]>;
}

impl SplitInner for [u8] {
    /// Go `bytes.Split(data, sep)`.
    fn split_inner<'a>(&'a self, sep: &'a [u8]) -> Vec<&'a [u8]> {
        let mut parts = Vec::new();
        let mut rest = self;
        while let Some(pos) = rest.windows(sep.len()).position(|w| w == sep) {
            parts.push(&rest[..pos]);
            rest = &rest[pos + sep.len()..];
        }
        parts.push(rest);
        parts
    }
}

// ------------------------------------------------------------------ connection

/// `sse.NewConnection(httptest.NewRecorder())`. Against Go every call replays the
/// whole call sequence on a fresh connection in the bridge and returns the result
/// of the last call (the sequence is deterministic).
pub struct Connection {
    rust: Option<(rs::Connection, Recorder)>,
    steps: RefCell<Vec<Value>>,
}

#[derive(Deserialize, Default)]
struct GoStep {
    error: Option<String>,
    bytes: Option<String>,
    #[serde(rename = "bool")]
    flag: bool,
    int: i64,
}

impl Connection {
    pub fn new() -> Self {
        let rust = (backend() == Backend::Rust).then(|| {
            let rec = Recorder::new();
            (rs::Connection::new(Box::new(OwnedRecorder::new(&rec))), rec)
        });
        Self { rust, steps: RefCell::new(Vec::new()) }
    }

    fn go(&self, step: Value) -> GoStep {
        self.steps.borrow_mut().push(step);
        let mut results: Vec<GoStep> = bridge::call("sse.Connection", json!({"Steps": self.steps.borrow().clone()}));
        results.pop().expect("one result per step")
    }

    pub fn write(&self, msg: &[u8], deadline: Instant) -> Result<(), String> {
        match &self.rust {
            Some((c, _)) => c.write(msg, deadline).map_err(|e| e.0),
            None => self.go(json!({"Op": "write", "Msg": msg})).error.map_or(Ok(()), Err),
        }
    }

    pub fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), String> {
        match &self.rust {
            Some((c, _)) => c.write_binary(msg, deadline).map_err(|e| e.0),
            None => self.go(json!({"Op": "write_binary", "Msg": msg})).error.map_or(Ok(()), Err),
        }
    }

    /// Go `(msg, err)`.
    pub fn read(&self) -> (Option<Vec<u8>>, Option<String>) {
        match &self.rust {
            Some((c, _)) => match c.read() {
                Ok(m) => (Some(m), None),
                Err(e) => (None, Some(e.0)),
            },
            None => {
                let r = self.go(json!({"Op": "read"}));
                (r.bytes.map(String::into_bytes), r.error)
            }
        }
    }

    /// `c.Descriptor() == nil`.
    pub fn descriptor_is_none(&self) -> bool {
        match &self.rust {
            Some((c, _)) => c.descriptor().is_none(),
            None => self.go(json!({"Op": "descriptor"})).flag,
        }
    }

    pub fn established(&self) {
        match &self.rust {
            Some((c, _)) => c.established(),
            None => {
                self.go(json!({"Op": "established"}));
            }
        }
    }

    pub fn close(&self, code: i32, reason: &str) {
        match &self.rust {
            Some((c, _)) => c.close(code, reason),
            None => {
                self.go(json!({"Op": "close", "Code": code, "Reason": reason}));
            }
        }
    }

    /// `ctx := c.Context()`.
    pub fn context(&self) -> ConnectionContext<'_> {
        ConnectionContext { conn: self, rust: self.rust.as_ref().map(|(c, _)| c.context()) }
    }

    /// The unexported `c.done` flag. Go has no accessor; it is probed on a replayed
    /// copy of the connection (the probe is not added to this connection's calls):
    /// a closed connection drops writes instead of buffering them (connection.go
    /// `if c.done { return nil }`), so `Write` then `Established` leaves the
    /// recorder body nil exactly when `done` is set. Valid while nothing was buffered
    /// before the close.
    pub fn is_done(&self) -> bool {
        match &self.rust {
            Some((c, _)) => c.is_done(),
            None => {
                let mut steps = self.steps.borrow().clone();
                steps.push(json!({"Op": "write", "Msg": b"probe".to_vec()}));
                steps.push(json!({"Op": "established"}));
                steps.push(json!({"Op": "body"}));
                let mut results: Vec<GoStep> = bridge::call("sse.Connection", json!({"Steps": steps}));
                results.pop().expect("body result").bytes.is_none()
            }
        }
    }

    /// The recorder's body as a string (upstream test helper `testResponse`).
    pub fn test_response(&self) -> String {
        match &self.rust {
            Some((_, rec)) => rec.body_string(),
            None => self.go(json!({"Op": "body"})).bytes.unwrap_or_default(),
        }
    }

    /// The recorder's body bytes; `None` is Go's nil slice.
    pub fn body_bytes(&self) -> Option<Vec<u8>> {
        match &self.rust {
            Some((_, rec)) => rec.body_bytes(),
            None => self.go(json!({"Op": "body"})).bytes.map(String::into_bytes),
        }
    }

    /// The recorder's status code (upstream test helper `testStatus`).
    pub fn test_status(&self) -> u16 {
        match &self.rust {
            Some((_, rec)) => rec.code(),
            None => u16::try_from(self.go(json!({"Op": "status"})).int).expect("HTTP status"),
        }
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

/// `c.Context()`.
pub struct ConnectionContext<'a> {
    conn: &'a Connection,
    rust: Option<CancellationToken>,
}

impl ConnectionContext<'_> {
    /// `<-ctx.Done()` bounded by `timeout`: whether the context was cancelled in time.
    pub fn wait_done(&self, timeout: Duration) -> bool {
        match &self.rust {
            Some(t) => tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("runtime")
                .block_on(async { tokio::time::timeout(timeout, t.cancelled()).await.is_ok() }),
            None => {
                let ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
                self.conn.go(json!({"Op": "context_done", "TimeoutMs": ms})).flag
            }
        }
    }
}

// ------------------------------------------------------------------ handler

/// `Subscribe` expectation key: (sid, ids, identifier).
type SubscribeKey = (String, String, String);

/// Upstream `mocks.Controller` as `TestSSEHandler` programs it: `Shutdown` and
/// `Disconnect(mock.Anything...)` return nil; `Authenticate`/`Subscribe` answer per
/// expectation (`mock.Anything` for the context and env). Against Go the
/// expectations go to a real `mocks.Controller` in the bridge.
///
/// An unexpected call is forbidden: the Rust double records it and returns an
/// error (a panic could be swallowed by the handler's thread); Go's testify panic
/// is recovered and recorded by the bridge. Tests assert the list stays empty.
pub struct MockController {
    go: Option<i64>,
    authenticate: Mutex<Vec<(String, ConnectResult)>>,
    subscribe: Mutex<Vec<(SubscribeKey, CommandResult)>>,
    calls: Mutex<Vec<Vec<String>>>,
    unexpected: Mutex<Vec<String>>,
}

impl MockController {
    fn new(go: Option<i64>) -> Self {
        Self {
            go,
            authenticate: Default::default(),
            subscribe: Default::default(),
            calls: Default::default(),
            unexpected: Default::default(),
        }
    }

    /// `On("Authenticate", mock.Anything, sid, mock.Anything).Return(res, nil)`.
    pub fn on_authenticate(&self, sid: &str, res: ConnectResult) {
        if let Some(h) = self.go {
            let e = json!({"Method": "Authenticate", "Sid": sid, "ConnectResult": connect_result_to_go(&res)});
            bridge::call::<_, Value>("sse.Harness.Expect", json!({"Harness": h, "Expectation": e}));
        }
        self.authenticate.lock().unwrap().push((sid.to_owned(), res));
    }

    /// `On("Subscribe", mock.Anything, sid, mock.Anything, ids, identifier).Return(res, nil)`.
    pub fn on_subscribe(&self, sid: &str, ids: &str, identifier: &str, res: CommandResult) {
        if let Some(h) = self.go {
            let e = json!({"Method": "Subscribe", "Sid": sid, "Ids": ids, "Identifier": identifier,
                           "CommandResult": command_result_to_go(&res)});
            bridge::call::<_, Value>("sse.Harness.Expect", json!({"Harness": h, "Expectation": e}));
        }
        self.subscribe.lock().unwrap().push(((sid.to_owned(), ids.to_owned(), identifier.to_owned()), res));
    }

    fn go_calls(&self, h: i64) -> GoCalls {
        bridge::call("sse.Harness.Calls", json!({"Harness": h}))
    }

    /// `AssertCalled(t, "Subscribe", mock.Anything, sid, mock.Anything, ids, identifier)`.
    pub fn assert_called_subscribe(&self, sid: &str, ids: &str, identifier: &str) {
        let calls = match self.go {
            Some(h) => self.go_calls(h).calls,
            None => self.calls.lock().unwrap().clone(),
        };
        let want = vec!["Subscribe".to_owned(), sid.to_owned(), ids.to_owned(), identifier.to_owned()];
        assert!(calls.contains(&want), "{want:?} was not called; calls: {calls:?}");
    }

    /// Calls that matched no expectation (testify would fail the test).
    pub fn unexpected_calls(&self) -> Vec<String> {
        match self.go {
            Some(h) => self.go_calls(h).unexpected,
            None => self.unexpected.lock().unwrap().clone(),
        }
    }

    fn forbidden<T>(&self, call: String) -> Outcome<T> {
        self.unexpected.lock().unwrap().push(call.clone());
        Outcome { result: None, error: Some(ControllerError::new(format!("mocks.Controller: unexpected call {call}"))) }
    }
}

#[derive(Deserialize)]
struct GoCalls {
    calls: Vec<Vec<String>>,
    unexpected: Vec<String>,
}

impl Controller for MockController {
    fn start(&self) -> Result<(), ControllerError> {
        self.unexpected.lock().unwrap().push("Start()".into());
        Err(ControllerError::new("mocks.Controller: unexpected call Start()"))
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn authenticate(&self, sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        self.calls.lock().unwrap().push(vec!["Authenticate".into(), sid.into()]);
        let found = self.authenticate.lock().unwrap().iter().find(|(s, _)| s == sid).map(|(_, r)| r.clone());
        match found {
            Some(res) => Outcome { result: Some(res), error: None },
            None => self.forbidden(format!("Authenticate(_, {sid:?}, _)")),
        }
    }

    fn subscribe(&self, sid: &str, _env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.calls.lock().unwrap().push(vec!["Subscribe".into(), sid.into(), ids.into(), channel.into()]);
        let key = (sid.to_owned(), ids.to_owned(), channel.to_owned());
        let found = self.subscribe.lock().unwrap().iter().find(|(k, _)| *k == key).map(|(_, r)| r.clone());
        match found {
            Some(res) => Outcome { result: Some(res), error: None },
            None => self.forbidden(format!("Subscribe(_, {sid:?}, _, {ids:?}, {channel:?})")),
        }
    }

    fn unsubscribe(&self, sid: &str, _: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.forbidden(format!("Unsubscribe(_, {sid:?}, _, {ids:?}, {channel:?})"))
    }

    fn perform(&self, sid: &str, _: Option<&SessionEnv>, _: &str, channel: &str, _: &str) -> Outcome<CommandResult> {
        self.forbidden(format!("Perform(_, {sid:?}, _, _, {channel:?}, _)"))
    }

    fn disconnect(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &[String]) -> Result<(), ControllerError> {
        Ok(())
    }
}

/// The upstream test's `buildNode()` (node with HubGopoolSize 2, a
/// `mocks.Controller`, legacy broker) with `TestSSEHandler`'s disconnect queue
/// (rate 1); against Go the node is started right away (`go appNode.Start()`).
pub struct Node {
    rust: Option<Arc<RsNode>>,
    go: Option<i64>,
}

pub fn build_node() -> (Node, Arc<MockController>) {
    match backend() {
        Backend::Rust => {
            let controller = Arc::new(MockController::new(None));
            let mut config = node_rs::Config::new();
            config.hub_gopool_size = 2;
            let node = RsNode::new(
                config,
                vec![
                    NodeOption::Controller(controller.clone()),
                    NodeOption::Instrumenter(Arc::new(Metrics::new(Vec::new(), 10)) as Arc<dyn Instrumenter>),
                ],
            );
            let subscriber = Arc::new(node_rs::LegacySubscriber::new(node.clone()));
            node.set_broker(Arc::new(cabby_core::broker::Legacy::new(subscriber)));
            // TestSSEHandler: a disconnect queue with rate 1, never run.
            let mut dconfig = node_rs::DisconnectQueueConfig::new();
            dconfig.rate = 1;
            node.set_disconnector(Arc::new(node_rs::DisconnectQueue::new(node.clone(), &dconfig)));
            (Node { rust: Some(node), go: None }, controller)
        }
        Backend::Go => {
            let h: i64 = bridge::call("sse.Harness.New", json!({}));
            (Node { rust: None, go: Some(h) }, Arc::new(MockController::new(Some(h))))
        }
    }
}

impl Node {
    fn go(&self) -> i64 {
        self.go.expect("Go harness")
    }

    /// `go appNode.Start()` (the Go harness starts its node when built).
    pub fn start(&self) {
        if let Some(n) = &self.rust {
            n.start().expect("node start");
        }
    }

    /// `appNode.Shutdown(context.Background())`.
    pub fn shutdown(&self) {
        match &self.rust {
            Some(n) => n.shutdown(&cabby_server::rpc::Context::default()).expect("node shutdown"),
            None => {
                bridge::call::<_, Value>("sse.Harness.Shutdown", json!({"Harness": self.go()}));
            }
        }
    }

    /// `appNode.Broadcast(msg)`.
    pub fn broadcast(&self, msg: &StreamMessage) {
        match &self.rust {
            Some(n) => n.broadcast(msg),
            None => {
                bridge::call::<_, Value>(
                    "sse.Harness.Broadcast",
                    json!({"Harness": self.go(), "Stream": msg.stream, "Data": msg.data}),
                );
            }
        }
    }

    /// `n.Size()`.
    pub fn size(&self) -> usize {
        match &self.rust {
            Some(n) => n.size(),
            None => bridge::call("sse.Harness.Size", json!({"Harness": self.go()})),
        }
    }

    /// The upstream `assertNoSessions` helper: sessions may be removed
    /// asynchronously, so poll (every 100ms, as upstream) until the node has none,
    /// failing after `timeout`.
    pub fn assert_no_sessions(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while self.size() != 0 {
            assert!(Instant::now() < deadline, "Timeout waiting for sessions to be removed");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A test request (`http.NewRequest(method, target, body)` plus headers).
#[derive(Debug, Clone, Default)]
pub struct TestRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl TestRequest {
    pub fn new(method: &str, target: &str) -> Self {
        Self { method: method.to_owned(), target: target.to_owned(), ..Default::default() }
    }

    /// `req.Header.Set(name, value)`.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn body(mut self, body: &str) -> Self {
        self.body = Some(body.as_bytes().to_vec());
        self
    }

    fn to_http(&self) -> rs::Request {
        let mut b = http::Request::builder().method(self.method.as_str()).uri(self.target.as_str());
        for (k, v) in &self.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        b.version(http::Version::HTTP_11).body(self.body.clone().unwrap_or_default()).expect("valid request")
    }
}

/// `SSEHandler(appNode, nil, shutdownCtx, &server.DefaultHeadersExtractor{}, &conf, logger)`
/// with its own shutdown context, cancelled by [`Handler::shutdown`] (never, for
/// upstream's `context.Background()`).
pub struct Handler {
    rust: Option<Arc<rs::Handler>>,
    shutdown: CancellationToken,
    go: Option<i64>,
}

pub fn sse_handler(node: &Node, conf: &rs::Config) -> Handler {
    let shutdown = CancellationToken::new();
    match &node.rust {
        Some(n) => {
            let extractor: rs::SharedHeadersExtractor = Arc::new(DefaultHeadersExtractor::default());
            let h = rs::sse_handler(n.clone(), None, shutdown.clone(), Some(extractor), conf);
            Handler { rust: Some(Arc::new(h)), shutdown, go: None }
        }
        None => {
            let h: i64 = bridge::call("sse.Harness.Handler", json!({"Harness": node.go(), "Config": go_config(conf)}));
            Handler { rust: None, shutdown, go: Some(h) }
        }
    }
}

/// A completed response (`httptest.ResponseRecorder` after `ServeHTTP` returned).
pub type Recorded = ApiResponse;

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == name).map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Deadline for the handler to return once the request context is done.
const RETURN_DEADLINE: Duration = Duration::from_secs(5);

impl Handler {
    /// Cancels the handler's shutdown context (`shutdownFn()`).
    pub fn shutdown(&self) {
        match self.go {
            Some(h) => {
                bridge::call::<_, Value>("sse.Handler.Shutdown", json!({"Handler": h}));
            }
            None => self.shutdown.cancel(),
        }
    }

    /// `handler.ServeHTTP(w, req)` with a request context that times out after
    /// `timeout`; returns the recorded response once the handler returned.
    pub fn serve(&self, req: &TestRequest, timeout: Duration) -> Recorded {
        let s = self.serve_streaming(req, timeout);
        s.wait(timeout + RETURN_DEADLINE).unwrap_or_else(|| panic!("handler did not return within {timeout:?}"))
    }

    /// `go handler.ServeHTTP(sw, req)` with a `streamingWriter`; the request
    /// context times out after `timeout` (upstream `context.WithTimeout`).
    pub fn serve_streaming(&self, req: &TestRequest, timeout: Duration) -> Streaming {
        if let Some(h) = self.go {
            let headers: std::collections::HashMap<&str, &str> =
                req.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let id: i64 = bridge::call(
                "sse.Handler.Serve",
                json!({"Handler": h, "Method": req.method, "Target": req.target, "Headers": headers,
                       "Body": req.body.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()),
                       "TimeoutMs": u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)}),
            );
            return Streaming { inner: StreamingInner::Go(id) };
        }
        let h = self.rust.clone().expect("Rust handler");
        let rec = Recorder::new();
        let (etx, erx) = channel();
        let (stx, srx) = channel();
        let (dtx, drx) = channel();
        let (stop_tx, stop_rx) = channel::<()>();
        let mut w = OwnedRecorder::new(&rec);
        w.events = Some(etx);
        w.status_seen = Some(stx);
        let request_done = CancellationToken::new();
        // The request context's timeout: cancel unless the stream was finished first.
        let timer_token = request_done.clone();
        std::thread::spawn(move || {
            if let Err(RecvTimeoutError::Timeout) = stop_rx.recv_timeout(timeout) {
                timer_token.cancel();
            }
        });
        let req = req.clone();
        let done = request_done.clone();
        std::thread::spawn(move || {
            // The handler finishes the writer before it returns. A panic is reported
            // to the test (`wait`) instead of looking like a hung handler.
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                h.serve(req.to_http(), Box::new(w), &done);
            }));
            let _ = dtx.send(res.err().map(|p| panic_message(p.as_ref())));
        });
        Streaming {
            inner: StreamingInner::Rust {
                recorder: rec,
                original: Recorder::new(),
                events: erx,
                status: srx,
                finished: Mutex::new(Some(drx)),
                request_done,
                _stop_timer: stop_tx,
            },
        }
    }
}

/// The text of a panic payload.
fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

enum StreamingInner {
    Rust {
        recorder: Recorder,
        /// Upstream's `w`, which the streaming writer copied and nothing writes to.
        original: Recorder,
        events: Receiver<Vec<u8>>,
        status: Receiver<u16>,
        /// The handler returned: `Some(message)` if it panicked.
        finished: Mutex<Option<Receiver<Option<String>>>>,
        request_done: CancellationToken,
        _stop_timer: Sender<()>,
    },
    Go(i64),
}

/// A response being streamed by a handler running on its own thread/goroutine.
pub struct Streaming {
    inner: StreamingInner,
}

#[derive(Deserialize)]
struct GoServeResult {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct GoServeStatus {
    status: Option<u16>,
}

#[derive(Deserialize)]
struct GoServeWait {
    done: bool,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    headers: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    panic: Option<String>,
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The bridge serves one call at a time for all tests of a binary, so a Go-side
/// wait is capped at this slice and repeated until the caller's deadline.
const BRIDGE_SLICE: Duration = Duration::from_millis(20);

impl Streaming {
    fn go(id: i64, op: &str, timeout: Duration) -> Value {
        bridge::call("sse.Serve", json!({"Serve": id, "Op": op, "TimeoutMs": ms(timeout)}))
    }

    /// Repeats `op` in short slices while `pending(result)` and time is left.
    fn go_poll(id: i64, op: &str, timeout: Duration, pending: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let v = Self::go(id, op, left.min(BRIDGE_SLICE));
            if !pending(&v) || left <= BRIDGE_SLICE {
                return v;
            }
        }
    }

    /// `sw.ReadEvent(ctx)`, waiting at most `timeout`.
    pub fn read_event(&self, timeout: Duration) -> Result<String, String> {
        match &self.inner {
            StreamingInner::Go(id) => {
                let v = Self::go_poll(*id, "read_event", timeout, |v| v["timeout"].as_bool() == Some(true));
                let r: GoServeResult = serde_json::from_value(v).expect("serve result");
                r.event.ok_or_else(|| r.error.unwrap_or_default())
            }
            StreamingInner::Rust { events, .. } => match events.recv_timeout(timeout) {
                Ok(e) => Ok(String::from_utf8_lossy(&e).into_owned()),
                Err(RecvTimeoutError::Timeout) => Err("context deadline exceeded".into()),
                Err(RecvTimeoutError::Disconnected) => Err("handler returned".into()),
            },
        }
    }

    /// Waits (bounded) until the handler sent the status line: the barrier replacing
    /// the upstream `time.Sleep` before broadcasting (the handler subscribes before
    /// writing the status).
    pub fn wait_status(&self, timeout: Duration) -> Option<u16> {
        match &self.inner {
            StreamingInner::Go(id) => {
                let v = Self::go_poll(*id, "wait_status", timeout, |v| v["status"].is_null());
                let r: GoServeStatus = serde_json::from_value(v).expect("status");
                r.status
            }
            StreamingInner::Rust { status, .. } => status.recv_timeout(timeout).ok(),
        }
    }

    /// Upstream's `w.Code`: the original recorder, which the streaming writer copied.
    pub fn original_code(&self) -> u16 {
        match &self.inner {
            StreamingInner::Go(id) => serde_json::from_value(Self::go(*id, "original_code", Duration::ZERO)).unwrap(),
            StreamingInner::Rust { original, .. } => original.code(),
        }
    }

    /// Waits up to `timeout` for the handler to return; the recorded response, or
    /// `None` if it is still running. A panic in the handler fails the test.
    pub fn wait(&self, timeout: Duration) -> Option<Recorded> {
        match &self.inner {
            StreamingInner::Go(id) => {
                let v = Self::go_poll(*id, "wait", timeout, |v| v["done"].as_bool() == Some(false));
                let r: GoServeWait = serde_json::from_value(v).expect("wait result");
                if !r.done {
                    return None;
                }
                if let Some(p) = r.panic {
                    panic!("handler panicked in Go: {p}");
                }
                let resp = GoApiResponse::from_parts(r.status, r.headers, r.body);
                Some(resp)
            }
            StreamingInner::Rust { recorder, finished, .. } => {
                let mut f = finished.lock().unwrap();
                if let Some(rx) = f.as_ref() {
                    match rx.recv_timeout(timeout) {
                        Ok(None) => *f = None,
                        Ok(Some(p)) => panic!("handler panicked: {p}"),
                        Err(RecvTimeoutError::Disconnected) => panic!("handler thread vanished"),
                        Err(RecvTimeoutError::Timeout) => return None,
                    }
                }
                let s = recorder.0.lock().unwrap();
                let headers = s
                    .headers
                    .iter()
                    .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned()))
                    .collect();
                Some(ApiResponse {
                    status: s.code.unwrap_or(200),
                    headers,
                    body: s.body.clone().unwrap_or_default(),
                    body_read: 0,
                })
            }
        }
    }

    /// The streaming writer's own status code (after the handler returned).
    pub fn code(&self) -> u16 {
        self.wait(RETURN_DEADLINE).expect("handler returned").status
    }

    /// `cancel()` the request context and wait (bounded) for the handler to return.
    pub fn finish(&self, timeout: Duration) -> bool {
        match &self.inner {
            StreamingInner::Go(id) => {
                Self::go(*id, "cancel", Duration::ZERO);
            }
            StreamingInner::Rust { request_done, .. } => request_done.cancel(),
        }
        self.wait(timeout).is_some()
    }
}

impl Drop for Streaming {
    fn drop(&mut self) {
        // `defer cancel()`: release the handler even when an assertion failed.
        match &self.inner {
            StreamingInner::Go(id) => {
                if !std::thread::panicking() {
                    Self::go(*id, "cancel", Duration::ZERO);
                }
            }
            StreamingInner::Rust { request_done, .. } => request_done.cancel(),
        }
    }
}
