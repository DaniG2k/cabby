//! Facade for upstream package `sse`.
//!
//! Also provides the Rust analogues of the upstream test's `httptest.ResponseRecorder`
//! and `streamingWriter` (test doubles for `http.ResponseWriter`).

use std::cell::RefCell;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::common::{EncodedMessage, Message, SentFrame, StreamMessage};
use cabby_server::pusher_sse_deps::{
    self as deps, CachedEncodedMessage as RsCached, Controller, Encoder as _, HeadersExtractor, NodeOptions,
    ResponseWriter,
};
use cabby_server::sse as rs;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::pusher::{ApiResponse, GoApiResponse, GoDecoded, GoFrameResult, GoParsed, encoded_message_json, rust_frame};
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
        Backend::Rust => rust_frame(enc.encode(msg)),
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
        Backend::Rust => rust_frame(enc.encode_transmission(raw)),
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
            Some(c) => rust_frame(c.fetch(&enc.id(), &|m| enc.encode(m))),
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

    /// The unexported `c.done` flag.
    pub fn is_done(&self) -> bool {
        match &self.rust {
            Some((c, _)) => c.is_done(),
            None => unimplemented!("not_observable: sse.Connection.done is unexported state"),
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

/// Probe interval while waiting on a cancellation token (bounded by a deadline).
const PROBE: Duration = Duration::from_millis(5);

impl ConnectionContext<'_> {
    /// `<-ctx.Done()` bounded by `timeout`: whether the context was cancelled in time.
    pub fn wait_done(&self, timeout: Duration) -> bool {
        match &self.rust {
            Some(t) => {
                let deadline = Instant::now() + timeout;
                while !t.is_cancelled() {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    std::thread::sleep(PROBE);
                }
                true
            }
            None => {
                let ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
                self.conn.go(json!({"Op": "context_done", "TimeoutMs": ms})).flag
            }
        }
    }
}

// ------------------------------------------------------------------ handler

/// The upstream test's `buildNode()` (node with HubGopoolSize 2, the given
/// controller, legacy broker) with `TestSSEHandler`'s disconnect queue (rate 1).
pub struct Node {
    rust: Option<Arc<deps::Node>>,
}

pub fn build_node(controller: Arc<dyn Controller>) -> Node {
    let rust = (backend() == Backend::Rust).then(|| {
        Arc::new(deps::Node::new(
            NodeOptions { hub_pool_size: 2, ping_interval: Duration::from_secs(3), disconnect_rate: Some(1) },
            controller,
        ))
    });
    Node { rust }
}

const NODE_NOT_OBSERVABLE: &str = "not_observable: the upstream case drives sse.SSEHandler through an in-process \
     node.Node with a mocks.Controller RPC stub and node broadcasts; covered externally by tests/conformance/sse.rs";

impl Node {
    /// `go appNode.Start()`. Against Go the bridge builds its own node per request.
    pub fn start(&self) {
        if let Some(n) = &self.rust {
            n.start().expect("node start");
        }
    }

    pub fn shutdown(&self) {
        if let Some(n) = &self.rust {
            n.shutdown().expect("node shutdown");
        }
    }

    /// `appNode.Broadcast(msg)`.
    pub fn broadcast(&self, msg: &StreamMessage) {
        match &self.rust {
            Some(n) => n.broadcast(msg),
            None => unimplemented!("{NODE_NOT_OBSERVABLE}"),
        }
    }

    /// `n.Size()`.
    pub fn size(&self) -> usize {
        match &self.rust {
            Some(n) => n.size(),
            None => unimplemented!("{NODE_NOT_OBSERVABLE}"),
        }
    }

    /// The upstream `assertNoSessions` helper: sessions may be removed
    /// asynchronously, so wait (bounded) until the node has none.
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

/// `SSEHandler(appNode, nil, shutdownCtx, &server.DefaultHeadersExtractor{}, &conf, logger)`.
pub struct Handler {
    conf: rs::Config,
    rust: Option<Arc<rs::Handler>>,
}

pub fn sse_handler(node: &Node, shutdown: &CancellationToken, conf: &rs::Config) -> Handler {
    let rust = node
        .rust
        .as_ref()
        .map(|n| Arc::new(rs::sse_handler(n.clone(), shutdown.clone(), HeadersExtractor::default(), conf)));
    Handler { conf: conf.clone(), rust }
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

impl Handler {
    fn serve_rust(h: &rs::Handler, req: &TestRequest, mut w: OwnedRecorder, done: &CancellationToken) {
        h.serve(req.to_http(), &mut w, done);
    }

    fn recorded(rec: &Recorder) -> Recorded {
        let s = rec.0.lock().unwrap();
        let headers =
            s.headers.iter().map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned())).collect();
        ApiResponse { status: s.code.unwrap_or(200), headers, body: s.body.clone().unwrap_or_default(), body_read: 0 }
    }

    /// `handler.ServeHTTP(w, req)` for a request that never reaches the application
    /// (rejected before authentication).
    pub fn serve(&self, req: &TestRequest) -> Recorded {
        match &self.rust {
            Some(h) => {
                let rec = Recorder::new();
                Self::serve_rust(h, req, OwnedRecorder::new(&rec), &CancellationToken::new());
                Self::recorded(&rec)
            }
            None => {
                let headers: std::collections::HashMap<&str, &str> =
                    req.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let r: GoApiResponse = bridge::call(
                    "sse.SSEHandler.ServeHTTP",
                    json!({"Config": go_config(&self.conf), "Method": req.method, "Target": req.target,
                           "Headers": headers,
                           "Body": req.body.as_ref().map(|b| String::from_utf8_lossy(b).into_owned())}),
                );
                r.into_response()
            }
        }
    }

    /// `handler.ServeHTTP(w, req)` for a request handled by the application (the
    /// node's controller), returning once the handler does. `request_done` is the
    /// request context.
    pub fn serve_app(&self, req: &TestRequest, request_done: &CancellationToken) -> Recorded {
        match &self.rust {
            Some(h) => {
                let rec = Recorder::new();
                Self::serve_rust(h, req, OwnedRecorder::new(&rec), request_done);
                Self::recorded(&rec)
            }
            None => unimplemented!("{NODE_NOT_OBSERVABLE}"),
        }
    }

    /// `go handler.ServeHTTP(sw, req)` with a `streamingWriter`; the request context
    /// is `request_done`.
    pub fn serve_streaming(&self, req: &TestRequest, request_done: &CancellationToken) -> Streaming {
        let Some(h) = &self.rust else { unimplemented!("{NODE_NOT_OBSERVABLE}") };
        let rec = Recorder::new();
        let (etx, erx) = channel();
        let (stx, srx) = channel();
        let (dtx, drx) = channel();
        let mut w = OwnedRecorder::new(&rec);
        w.events = Some(etx);
        w.status_seen = Some(stx);
        let h = h.clone();
        let req = req.clone();
        let done = request_done.clone();
        std::thread::spawn(move || {
            Self::serve_rust(&h, &req, w, &done);
            let _ = dtx.send(());
        });
        Streaming { recorder: rec, events: erx, status: srx, finished: drx, request_done: request_done.clone() }
    }
}

/// A response being streamed by a handler running on its own thread.
pub struct Streaming {
    recorder: Recorder,
    events: Receiver<Vec<u8>>,
    status: Receiver<u16>,
    finished: Receiver<()>,
    request_done: CancellationToken,
}

impl Streaming {
    /// `sw.ReadEvent(ctx)` where the context expires after `timeout`.
    pub fn read_event(&self, timeout: Duration) -> Result<String, String> {
        match self.events.recv_timeout(timeout) {
            Ok(e) => Ok(String::from_utf8_lossy(&e).into_owned()),
            Err(RecvTimeoutError::Timeout) => Err("context deadline exceeded".into()),
            Err(RecvTimeoutError::Disconnected) => Err("handler returned".into()),
        }
    }

    /// Waits (bounded) until the handler sent the status line: the barrier replacing
    /// the upstream `time.Sleep` before broadcasting (the handler subscribes before
    /// writing the status).
    pub fn wait_status(&self, timeout: Duration) -> Option<u16> {
        self.status.recv_timeout(timeout).ok()
    }

    /// The streaming writer's status code.
    pub fn code(&self) -> u16 {
        self.recorder.code()
    }

    /// `cancel()` the request context and wait (bounded) for the handler to return.
    pub fn finish(&self, timeout: Duration) -> bool {
        self.request_done.cancel();
        self.finished.recv_timeout(timeout).is_ok()
    }
}

impl Drop for Streaming {
    fn drop(&mut self) {
        // `defer cancel()`: release the handler even when an assertion failed.
        self.request_done.cancel();
        let _ = self.finished.recv_timeout(Duration::from_secs(2));
    }
}
