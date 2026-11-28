//! Facade for upstream package `ds` (durable streams).
//!
//! Pure functions dispatch to `cabby_server::ds` or the bridge. The handler tests
//! run on a [`Harness`] (node + `mocks.Controller` + either the handler test's
//! `mocks.Broker` or a memory broker) with [`Handler`]s served through a recorder
//! (`httptest.NewRecorder`) or on a loopback HTTP server (`httptest.NewServer`;
//! for Rust, Cabby's own HTTP server with the handler mounted as in production).
//! Against Go the harness and the handlers live in the bridge (`bridge/ds.go`);
//! against Rust the doubles below stand in for the testify mocks.
//!
//! Forbidden (unexpected) mock calls are recorded on both backends and asserted
//! through [`Harness::unexpected_calls`], never enforced by a panic alone.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::broker::{Broker, BrokerError, Cacheable, PresenceInfoOptions};
use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, EncodedMessage, Outcome, PresenceEvent, PresenceInfo,
    RemoteCommandMessage, SentFrame, SessionEnv, StreamMessage,
};
use cabby_core::encoders::Encoder as _;
use cabby_core::streams::{StreamsController, SubscribeRequest};
use cabby_server::ds as rs;
use cabby_server::metrics::{Instrumenter, Metrics};
use cabby_server::node::{self as node_rs, Connection as _, Node, NodeOption};
use cabby_server::server::{DefaultHeadersExtractor, HttpRequest, HttpServer, ResponseWriter};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::go_types::{command_result_to_go, connect_result_to_go, stream_message_to_go};
use super::pusher::{ApiResponse, GoApiResponse, GoFrameResult, GoParsed, encoded_message_json};
use super::sse::{OwnedRecorder, Recorder, rust_frame};
use crate::{Backend, backend, bridge};

/// The bridge serves one call at a time for all tests of a binary: Go-side waits
/// are capped at this slice and repeated up to the caller's deadline.
const BRIDGE_SLICE: Duration = Duration::from_millis(20);

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

// ------------------------------------------------------------------ config

fn go_config(c: &rs::Config) -> Value {
    json!({"Enabled": c.enabled, "Path": c.path, "SkipAuth": c.skip_auth, "PollInterval": c.poll_interval,
           "SSETTL": c.sse_ttl, "SSEPingInterval": c.sse_ping_interval, "AllowedOrigins": c.allowed_origins})
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    enabled: bool,
    path: String,
    skip_auth: bool,
    poll_interval: i64,
    #[serde(rename = "SSETTL")]
    sse_ttl: i64,
    #[serde(rename = "SSEPingInterval")]
    sse_ping_interval: i64,
    allowed_origins: String,
}

impl From<GoConfig> for rs::Config {
    fn from(g: GoConfig) -> Self {
        rs::Config {
            enabled: g.enabled,
            path: g.path,
            skip_auth: g.skip_auth,
            poll_interval: g.poll_interval,
            sse_ttl: g.sse_ttl,
            sse_ping_interval: g.sse_ping_interval,
            allowed_origins: g.allowed_origins,
        }
    }
}

/// `ds.NewConfig()`.
pub fn new_config() -> rs::Config {
    match backend() {
        Backend::Rust => rs::Config::new(),
        Backend::Go => bridge::call::<_, GoConfig>("ds.Config.New", json!({})).into(),
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(conf: &rs::Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("ds.Config.ToToml", json!({"Config": go_config(conf)})),
    }
}

/// `toml.Decode(s, &ds.Config{})` (BurntSushi/toml in Go, serde + `toml` in Rust).
pub fn decode_config_toml(s: &str) -> Result<rs::Config, String> {
    match backend() {
        Backend::Rust => toml::from_str(s).map_err(|e| e.to_string()),
        Backend::Go => {
            let empty = rs::Config::default();
            let p: GoParsed<GoConfig> =
                bridge::call("ds.Config.DecodeToml", json!({"Config": go_config(&empty), "Toml": s}));
            if let Some(e) = p.error {
                return Err(e);
            }
            Ok(p.result.expect("decoded config").into())
        }
    }
}

// ------------------------------------------------------------------ offsets and cursors

/// `EncodeOffset(offset, epoch)`.
pub fn encode_offset(offset: u64, epoch: &str) -> String {
    match backend() {
        Backend::Rust => rs::encode_offset(offset, epoch),
        Backend::Go => bridge::call("ds.EncodeOffset", json!({"Offset": offset, "Epoch": epoch})),
    }
}

/// A Go `(offset, epoch, err)` triple: the values are kept alongside an error.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Decoded {
    pub offset: u64,
    pub epoch: String,
    pub error: Option<String>,
}

/// `DecodeOffset(s)`.
pub fn decode_offset(s: &str) -> Decoded {
    match backend() {
        Backend::Rust => match rs::decode_offset(s) {
            Ok((offset, epoch)) => Decoded { offset, epoch, error: None },
            Err(e) => Decoded { offset: 0, epoch: String::new(), error: Some(e.0) },
        },
        Backend::Go => bridge::call("ds.DecodeOffset", json!({"Offset": s})),
    }
}

/// `(&Stream{Params: &StreamParams{Cursor: cursor}}).NextCursor()`.
pub fn next_cursor(cursor: &str) -> String {
    match backend() {
        Backend::Rust => {
            rs::Stream::new(rs::StreamParams { cursor: cursor.to_owned(), ..Default::default() }).next_cursor()
        }
        Backend::Go => {
            let mut v: Vec<String> = bridge::call("ds.Stream.NextCursor", json!({"Cursor": cursor, "Calls": 1}));
            v.pop().expect("one cursor")
        }
    }
}

// ------------------------------------------------------------------ encoders

/// `PollEncoder{}.ID()`.
pub fn poll_encoder_id() -> String {
    match backend() {
        Backend::Rust => rs::PollEncoder.id(),
        Backend::Go => bridge::call("ds.PollEncoder.ID", json!({})),
    }
}

/// `PollEncoder{}.Encode(msg)`.
pub fn poll_encoder_encode(msg: &EncodedMessage) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(rs::PollEncoder.encode_optional(msg)),
        Backend::Go => {
            bridge::call::<_, GoFrameResult>("ds.PollEncoder.Encode", json!({"Msg": encoded_message_json(msg)}))
                .into_result()
        }
    }
}

fn go_sse_encoder(e: &rs::SseEncoder) -> Value {
    json!({"Cursor": e.cursor, "UpToDate": e.up_to_date})
}

/// `(&SSEEncoder{...}).Encode(msg)`.
pub fn sse_encoder_encode(enc: &rs::SseEncoder, msg: &EncodedMessage) -> Result<Option<SentFrame>, String> {
    match backend() {
        Backend::Rust => rust_frame(enc.encode_optional(msg)),
        Backend::Go => bridge::call::<_, GoFrameResult>(
            "ds.SSEEncoder.Encode",
            json!({"Encoder": go_sse_encoder(enc), "Msg": encoded_message_json(msg)}),
        )
        .into_result(),
    }
}

/// A Go `(frame, err)` pair with both values kept (upstream asserts both nil).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameAndError {
    pub frame: Option<SentFrame>,
    pub error: Option<String>,
}

#[derive(Deserialize)]
struct GoRawFrame {
    frame: Option<Value>,
    error: Option<String>,
}

/// `(&SSEEncoder{...}).EncodeTransmission(raw)`.
pub fn sse_encoder_encode_transmission(enc: &rs::SseEncoder, raw: &str) -> FrameAndError {
    match backend() {
        Backend::Rust => match enc.encode_transmission_optional(raw) {
            Ok(frame) => FrameAndError { frame, error: None },
            Err(e) => FrameAndError { frame: None, error: Some(e.0) },
        },
        Backend::Go => {
            let r: GoRawFrame =
                bridge::call("ds.SSEEncoder.EncodeTransmission", json!({"Encoder": go_sse_encoder(enc), "Raw": raw}));
            let frame = r.frame.map(|f| {
                let ok: GoFrameResult = serde_json::from_value(json!({"frame": f, "error": null})).expect("frame");
                ok.into_result().expect("no error").expect("frame")
            });
            FrameAndError { frame, error: r.error }
        }
    }
}

// ------------------------------------------------------------------ poll connection

/// `NewPollConnection(httptest.NewRecorder())`. Against Go every call replays the
/// whole call sequence on a fresh recorder and connection in the bridge (the
/// sequence is deterministic) and returns the result of the last call.
/// The Rust connection, its recorder and the recorded `ETag` function calls.
type RustPollConnection = (rs::PollConnection, Recorder, Arc<Mutex<Vec<String>>>);

pub struct PollConnection {
    rust: Option<RustPollConnection>,
    steps: RefCell<Vec<Value>>,
}

#[derive(Deserialize, Default)]
struct GoPollStep {
    error: Option<String>,
    #[serde(default)]
    code: u16,
    #[serde(default)]
    headers: Option<HashMap<String, Vec<String>>>,
    #[serde(default)]
    body: String,
    #[serde(rename = "bool", default)]
    flag: bool,
    #[serde(default)]
    etag_calls: Option<Vec<String>>,
}

impl PollConnection {
    /// `w := httptest.NewRecorder(); w.Header().Set(k, v)...; NewPollConnection(w)`.
    pub fn new(preset_headers: &[(&str, &str)]) -> Self {
        let rust = (backend() == Backend::Rust).then(|| {
            let rec = Recorder::new();
            let mut w = OwnedRecorder::new(&rec);
            for (k, v) in preset_headers {
                w.headers_mut().insert(
                    http::HeaderName::from_bytes(k.as_bytes()).expect("header name"),
                    http::HeaderValue::from_str(v).expect("header value"),
                );
            }
            (rs::PollConnection::new(Box::new(w)), rec, Arc::new(Mutex::new(Vec::new())))
        });
        let steps = preset_headers.iter().map(|(k, v)| json!({"Op": "preset_header", "Name": k, "Value": v})).collect();
        Self { rust, steps: RefCell::new(steps) }
    }

    fn go(&self, step: Value) -> GoPollStep {
        self.steps.borrow_mut().push(step);
        self.go_replay(&self.steps.borrow())
    }

    fn go_replay(&self, steps: &[Value]) -> GoPollStep {
        let mut results: Vec<GoPollStep> = bridge::call("ds.PollConnection", json!({"Steps": steps}));
        results.pop().expect("one result per step")
    }

    /// `conn.CacheControl = value`.
    pub fn set_cache_control(&self, value: &str) {
        match &self.rust {
            Some((c, ..)) => *c.cache_control.lock().unwrap() = value.to_owned(),
            None => {
                self.go(json!({"Op": "cache_control", "Value": value}));
            }
        }
    }

    /// `conn.ETag = func(nextOffset string) string { return prefix + nextOffset + suffix }`;
    /// the function's arguments are recorded ([`PollConnection::etag_calls`]).
    pub fn set_etag(&self, prefix: &str, suffix: &str) {
        match &self.rust {
            Some((c, _, calls)) => {
                let (prefix, suffix, calls) = (prefix.to_owned(), suffix.to_owned(), calls.clone());
                *c.etag.lock().unwrap() = Some(Arc::new(move |next: &str| {
                    calls.lock().unwrap().push(next.to_owned());
                    format!("{prefix}{next}{suffix}")
                }));
            }
            None => {
                self.go(json!({"Op": "etag", "Prefix": prefix, "Suffix": suffix}));
            }
        }
    }

    /// `conn.Write(msg, time.Time{})`.
    pub fn write(&self, msg: &[u8]) -> Result<(), String> {
        match &self.rust {
            Some((c, ..)) => c.write(msg, Instant::now()).map_err(|e| e.0),
            None => self.go(json!({"Op": "write", "Msg": msg})).error.map_or(Ok(()), Err),
        }
    }

    /// `conn.Close(code, reason)`.
    pub fn close(&self, code: i32, reason: &str) {
        match &self.rust {
            Some((c, ..)) => c.close(code, reason),
            None => {
                self.go(json!({"Op": "close", "Code": code, "Reason": reason}));
            }
        }
    }

    /// The unexported `conn.done` flag. Go has no accessor; it is probed on a
    /// replayed copy (the probe is not added to this connection's calls): a done
    /// connection drops a later `Write` without an error (poll.go `if c.done
    /// { return nil }`), so the recorder body stays as it was exactly when `done`
    /// is set. Valid while nothing failed to write before.
    pub fn is_done(&self) -> bool {
        match &self.rust {
            Some((c, ..)) => c.is_done(),
            None => {
                let mut steps = self.steps.borrow().clone();
                steps.push(json!({"Op": "state"}));
                let before = self.go_replay(&steps).body;
                steps.push(json!({"Op": "write", "Msg": b"1::probe\n{}".to_vec()}));
                let write = self.go_replay(&steps);
                steps.push(json!({"Op": "state"}));
                let after = self.go_replay(&steps).body;
                write.error.is_none() && before == after
            }
        }
    }

    fn state(&self) -> GoPollStep {
        self.go(json!({"Op": "state"}))
    }

    /// `w.Code`.
    pub fn code(&self) -> u16 {
        match &self.rust {
            Some((_, rec, _)) => rec.code(),
            None => self.state().code,
        }
    }

    /// `w.Header().Get(name)` (`""` when absent).
    pub fn header(&self, name: &str) -> String {
        match &self.rust {
            Some((_, rec, _)) => rec.header(name).unwrap_or_default(),
            None => {
                let headers = self.state().headers.unwrap_or_default();
                headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .and_then(|(_, v)| v.first().cloned())
                    .unwrap_or_default()
            }
        }
    }

    /// `w.Body.String()`.
    pub fn body(&self) -> String {
        match &self.rust {
            Some((_, rec, _)) => rec.body_string(),
            None => self.state().body,
        }
    }

    /// The arguments the `ETag` function was called with.
    pub fn etag_calls(&self) -> Vec<String> {
        match &self.rust {
            Some((_, _, calls)) => calls.lock().unwrap().clone(),
            None => self.state().etag_calls.unwrap_or_default(),
        }
    }

    /// `<-conn.Context().Done()` has fired (non-blocking check).
    pub fn context_done(&self) -> bool {
        match &self.rust {
            Some((c, ..)) => c.context().is_cancelled(),
            None => self.go(json!({"Op": "context_done"})).flag,
        }
    }
}

// ------------------------------------------------------------------ requests

/// A test request (`http.NewRequest(method, target, nil)` plus headers).
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
}

impl Request {
    pub fn new(method: &str, target: &str) -> Self {
        Self { method: method.to_owned(), target: target.to_owned(), headers: Vec::new() }
    }

    /// `req.Header.Set(name, value)`.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn to_rust(&self) -> HttpRequest {
        HttpRequest {
            method: self.method.clone(),
            target: self.target.clone(),
            host: "example.com".into(),
            headers: self.headers.clone(),
            remote_addr: "192.0.2.1:1234".into(),
            tls: false,
        }
    }
}

// ------------------------------------------------------------------ readCacheControl / etagMatches

fn go_harness_for_probe(conf: &rs::Config, epoch: &str) -> (Harness, Handler) {
    let h = Harness::memory(epoch, 0, 0);
    h.on_authenticate(None, ConnectResult { identifier: "ds2026".into(), ..Default::default() });
    let handler = h.handler(conf, StreamsKind::Echo).expect("DSHandler");
    (h, handler)
}

/// `readCacheControl(&conf, req, StreamParamsFromReq(req, &conf))` (unexported).
///
/// Against Go it is observed through `DSHandler`, which sets the header to this
/// value: a catch-up read (one message broadcast first, so the long-poll mode
/// returns data from history too, unless the offset is `now`); for `now` in
/// long-poll mode the data response of a live message (the `PollConnection`'s
/// `CacheControl`). Requests carry `offset`, `live` and signed names as given.
pub fn read_cache_control(conf: &rs::Config, req: &Request) -> String {
    if backend() == Backend::Rust {
        let r = req.to_rust();
        let sp = rs::stream_params_from_req(&r, conf).expect("StreamParamsFromReq");
        return rs::read_cache_control(conf, &r, &sp);
    }
    let mut conf = conf.clone();
    conf.path = "/ds".into();
    conf.poll_interval = 5;
    let (h, handler) = go_harness_for_probe(&conf, "e");
    let stream = req.target.trim_start_matches("/ds/").split('?').next().unwrap_or_default().to_owned();
    h.broadcast(&stream, r#"{"probe":1}"#).expect("broadcast");
    let live_now = req.target.contains("offset=now") && req.target.contains("live=long-poll");
    let server = handler.listen();
    if !live_now {
        let resp = server.request(req);
        assert_eq!(resp.status, 200, "probe read failed: {resp:?}");
        return resp.header("cache-control").unwrap_or_default().to_owned();
    }
    h.on_subscribe(None, CommandResult { streams: vec![stream.clone()], ..Default::default() }, 0, false);
    let pending = server.request_async(req);
    assert!(h.wait_subscribed(&stream, Duration::from_secs(5)), "long-poll session did not subscribe");
    h.broadcast(&stream, r#"{"probe":2}"#).expect("broadcast");
    let resp = pending.wait(Duration::from_secs(10));
    assert_eq!(resp.status, 200, "probe long-poll failed: {resp:?}");
    resp.header("cache-control").unwrap_or_default().to_owned()
}

/// `etagMatches(ifNoneMatch, etag)` (unexported).
///
/// Against Go it is observed through `DSHandler`: a catch-up read of stream `abc`
/// from offset `-1` whose next offset is `1::e` carries the ETag
/// `"YWJj:-1:1::e"` (base64 of the path); the request's `If-None-Match` is
/// `if_none_match` with `etag`'s path component `abc` replaced by `YWJj`, and a
/// match is a `304 Not Modified`. `etag` must be `"abc:-1:1::e"`.
pub fn etag_matches(if_none_match: &str, etag: &str) -> bool {
    if backend() == Backend::Rust {
        return rs::etag_matches(if_none_match, etag);
    }
    assert_eq!(etag, r#""abc:-1:1::e""#, "the Go probe reproduces only this ETag");
    let mut conf = rs::Config { path: "/ds".into(), skip_auth: true, poll_interval: 5, ..Default::default() };
    conf.sse_ttl = 60;
    let (h, handler) = go_harness_for_probe(&conf, "e");
    h.broadcast("abc", r#"{"probe":1}"#).expect("broadcast");
    let server = handler.listen();
    let mut req = Request::new("GET", "/ds/abc?offset=-1");
    if !if_none_match.is_empty() {
        req = req.header("If-None-Match", &if_none_match.replace("abc:", "YWJj:"));
    }
    let resp = server.request(&req);
    assert_eq!(resp.header("etag"), Some(r#""YWJj:-1:1::e""#), "probe ETag: {resp:?}");
    match resp.status {
        304 => true,
        200 => false,
        other => panic!("unexpected probe status {other}"),
    }
}

// ------------------------------------------------------------------ mock controller

/// How often an expectation may match (testify `Once`/`Twice`/`Times`; 0 is
/// unlimited).
#[derive(Debug, Clone)]
struct ControllerExpectation {
    method: &'static str,
    sid: Option<String>,
    identifier_contains: Option<String>,
    /// `None`: unlimited; `Some(n)`: matches left.
    left: Option<usize>,
    connect: Option<ConnectResult>,
    command: Option<CommandResult>,
}

/// Rust stand-in for the tests' `mocks.Controller`: answers with the first
/// matching expectation that has calls left (testify order and repeatability);
/// every call is recorded, unmatched ones are forbidden (recorded and answered
/// with an error).
#[derive(Default)]
pub struct MockController {
    expectations: Mutex<Vec<ControllerExpectation>>,
    calls: Mutex<Vec<Vec<String>>>,
    unexpected: Mutex<Vec<String>>,
}

impl MockController {
    fn find(&self, method: &str, sid: &str, identifier: Option<&str>) -> Option<ControllerExpectation> {
        let mut exps = self.expectations.lock().unwrap();
        let e = exps.iter_mut().find(|e| {
            e.method == method
                && e.sid.as_deref().is_none_or(|s| s == sid)
                && e.identifier_contains.as_deref().is_none_or(|part| identifier.is_some_and(|i| i.contains(part)))
                && e.left != Some(0)
        })?;
        if let Some(n) = &mut e.left {
            *n -= 1;
        }
        Some(e.clone())
    }

    fn forbidden<T>(&self, call: String) -> Outcome<T> {
        self.unexpected.lock().unwrap().push(call.clone());
        Outcome { result: None, error: Some(ControllerError::new(format!("mocks.Controller: unexpected call {call}"))) }
    }
}

impl Controller for MockController {
    fn start(&self) -> Result<(), ControllerError> {
        self.unexpected.lock().unwrap().push("Start()".into());
        Err(ControllerError::new("mocks.Controller: unexpected call Start()"))
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        self.calls.lock().unwrap().push(vec!["Shutdown".into()]);
        match self.find("Shutdown", "", None) {
            Some(_) => Ok(()),
            None => {
                self.unexpected.lock().unwrap().push("Shutdown()".into());
                Err(ControllerError::new("mocks.Controller: unexpected call Shutdown()"))
            }
        }
    }

    fn authenticate(&self, sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        self.calls.lock().unwrap().push(vec!["Authenticate".into(), sid.into()]);
        match self.find("Authenticate", sid, None) {
            Some(e) => Outcome { result: e.connect, error: None },
            None => self.forbidden(format!("Authenticate(_, {sid:?}, _)")),
        }
    }

    fn subscribe(&self, sid: &str, _env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.calls.lock().unwrap().push(vec!["Subscribe".into(), sid.into(), ids.into(), channel.into()]);
        match self.find("Subscribe", sid, Some(channel)) {
            Some(e) => Outcome { result: e.command, error: None },
            None => self.forbidden(format!("Subscribe(_, {sid:?}, _, {ids:?}, {channel:?})")),
        }
    }

    fn unsubscribe(&self, sid: &str, _: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.forbidden(format!("Unsubscribe(_, {sid:?}, _, {ids:?}, {channel:?})"))
    }

    fn perform(&self, sid: &str, _: Option<&SessionEnv>, _: &str, channel: &str, _: &str) -> Outcome<CommandResult> {
        self.forbidden(format!("Perform(_, {sid:?}, _, _, {channel:?}, _)"))
    }

    fn disconnect(&self, sid: &str, _: Option<&SessionEnv>, _: &str, _: &[String]) -> Result<(), ControllerError> {
        self.unexpected.lock().unwrap().push(format!("Disconnect(_, {sid:?}, ...)"));
        Err(ControllerError::new("mocks.Controller: unexpected call Disconnect"))
    }
}

// ------------------------------------------------------------------ mock broker

#[derive(Debug, Clone)]
enum BrokerExpectation {
    Peak(String, Result<Option<StreamMessage>, String>),
    HistorySince(String, i64, Result<Option<Vec<StreamMessage>>, String>),
    HistoryFrom(String, String, u64, Result<Option<Vec<StreamMessage>>, String>),
}

/// Rust stand-in for the handler test's `mocks.Broker`: `Peak`, `HistorySince`
/// and `HistoryFrom` answer per expectation (exact arguments, first match);
/// everything else is forbidden (recorded).
#[derive(Default)]
pub struct MockBroker {
    expectations: Mutex<Vec<BrokerExpectation>>,
    calls: Mutex<Vec<Vec<String>>>,
    unexpected: Mutex<Vec<String>>,
}

impl MockBroker {
    fn forbid(&self, call: String) -> BrokerError {
        self.unexpected.lock().unwrap().push(call.clone());
        BrokerError { message: format!("mocks.Broker: unexpected call {call}") }
    }

    fn history(r: &Result<Option<Vec<StreamMessage>>, String>) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        r.clone().map_err(|message| BrokerError { message })
    }
}

impl Broker for MockBroker {
    fn start(&self) -> Result<(), BrokerError> {
        Err(self.forbid("Start()".into()))
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        Err(self.forbid("Shutdown()".into()))
    }
    fn announce(&self) -> String {
        self.forbid("Announce()".into());
        String::new()
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        Err(self.forbid(format!("HandleBroadcast({:?})", msg.stream)))
    }
    fn handle_command(&self, _: &RemoteCommandMessage) -> Result<(), BrokerError> {
        Err(self.forbid("HandleCommand(...)".into()))
    }
    fn subscribe(&self, stream: &str) -> String {
        self.forbid(format!("Subscribe({stream:?})"));
        String::new()
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.forbid(format!("Unsubscribe({stream:?})"));
        String::new()
    }
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.calls.lock().unwrap().push(vec!["HistoryFrom".into(), stream.into(), epoch.into(), offset.to_string()]);
        let found = self.expectations.lock().unwrap().iter().find_map(|e| match e {
            BrokerExpectation::HistoryFrom(s, ep, o, r) if s == stream && ep == epoch && *o == offset => {
                Some(r.clone())
            }
            _ => None,
        });
        match found {
            Some(r) => Self::history(&r).map(Option::unwrap_or_default),
            None => Err(self.forbid(format!("HistoryFrom({stream:?}, {epoch:?}, {offset})"))),
        }
    }
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.calls.lock().unwrap().push(vec!["HistorySince".into(), stream.into(), ts.to_string()]);
        let found = self.expectations.lock().unwrap().iter().find_map(|e| match e {
            BrokerExpectation::HistorySince(s, t, r) if s == stream && *t == ts => Some(r.clone()),
            _ => None,
        });
        match found {
            Some(r) => Self::history(&r),
            None => Err(self.forbid(format!("HistorySince({stream:?}, {ts})"))),
        }
    }
    fn commit_session(&self, sid: &str, _: &dyn Cacheable) -> Result<(), BrokerError> {
        Err(self.forbid(format!("CommitSession({sid:?}, _)")))
    }
    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        Err(self.forbid(format!("RestoreSession({sid:?})")))
    }
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        Err(self.forbid(format!("TouchSession({sid:?})")))
    }
    fn delete_session(&self, sid: &str) -> Result<(), BrokerError> {
        Err(self.forbid(format!("DeleteSession({sid:?})")))
    }
    fn supports_presence(&self) -> bool {
        self.forbid("SupportsPresence()".into());
        false
    }
    fn presence_add(&self, stream: &str, _: &str, _: &str, _: Value) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(self.forbid(format!("PresenceAdd({stream:?}, ...)")))
    }
    fn presence_remove(&self, stream: &str, _: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(self.forbid(format!("PresenceRemove({stream:?}, ...)")))
    }
    fn presence_info(&self, stream: &str, _: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        Err(self.forbid(format!("PresenceInfo({stream:?}, ...)")))
    }
    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        Err(self.forbid(format!("TouchPresence({sid:?})")))
    }
    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.calls.lock().unwrap().push(vec!["Peak".into(), stream.into()]);
        let found = self.expectations.lock().unwrap().iter().find_map(|e| match e {
            BrokerExpectation::Peak(s, r) if s == stream => Some(r.clone()),
            _ => None,
        });
        match found {
            Some(r) => r.map_err(|message| BrokerError { message }),
            None => Err(self.forbid(format!("Peak({stream:?})"))),
        }
    }
}

// ------------------------------------------------------------------ harness

struct RustHarness {
    node: Arc<Node>,
    controller: Arc<MockController>,
    broker: Arc<dyn Broker>,
    mock_broker: Option<Arc<MockBroker>>,
    memory: Option<Arc<cabby_core::broker::Memory>>,
}

/// A node with a `mocks.Controller` (`Shutdown` expected) and a broker. The node
/// is shut down by [`Harness::shutdown`], or on drop if it was not (upstream's
/// `defer n.Shutdown(ctx)`), on both backends.
pub struct Harness {
    go: Option<i64>,
    rust: Option<RustHarness>,
    shut_down: AtomicBool,
}

#[derive(Deserialize)]
struct GoCalls {
    controller: Vec<Vec<String>>,
    broker: Vec<Vec<String>>,
    unexpected: Vec<String>,
}

/// The stream resolver of a test (`streams.NewController("", resolver)` or
/// `streams.NewStreamsController(&conf)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsKind {
    /// No controller (`nil`).
    None,
    /// `buildNode()`: the identifier's `stream_name`.
    Echo,
    /// `TestDSHandler_HEAD` "stream authorization": rejects identifiers without a
    /// signed name, records what it received ([`Handler::resolved`]).
    RequireSigned,
    /// `setupIntegrationServer()`: rejects identifiers containing `noauth`.
    RejectNoauth,
    /// `streams.NewStreamsController` with this secret and public flag.
    Signed { secret: String, public: bool },
}

fn go_streams(k: &StreamsKind) -> Value {
    match k {
        StreamsKind::None => Value::Null,
        StreamsKind::Echo => json!({"Kind": "echo"}),
        StreamsKind::RequireSigned => json!({"Kind": "require_signed"}),
        StreamsKind::RejectNoauth => json!({"Kind": "reject_noauth"}),
        StreamsKind::Signed { secret, public } => json!({"Kind": "signed", "Secret": secret, "Public": public}),
    }
}

/// One identifier the recording resolver received: the raw identifier, its JSON
/// decoding and the decoding error (upstream `require.NoError` inside the resolver).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Resolved {
    pub identifier: String,
    pub decoded: Option<HashMap<String, String>>,
    pub error: Option<String>,
}

fn identifier_stream_name(identifier: &str) -> Result<String, ControllerError> {
    let v: Value = serde_json::from_str(identifier).map_err(|e| ControllerError::new(e.to_string()))?;
    Ok(v.get("stream_name").and_then(Value::as_str).unwrap_or_default().to_owned())
}

impl Harness {
    /// The handler test's `buildNode()`: `HubGopoolSize = 2`, a started node, a
    /// `mocks.Broker` with `Peak("test-stream") -> (nil, nil)`.
    pub fn build_node() -> Self {
        match backend() {
            Backend::Go => Self {
                go: Some(bridge::call("ds.Harness.New", json!({"Broker": "mock"}))),
                rust: None,
                shut_down: AtomicBool::new(false),
            },
            Backend::Rust => {
                let controller = Arc::new(MockController::default());
                let h = Self::rust_node(controller, false);
                let mock = Arc::new(MockBroker::default());
                let node = h.node.clone();
                node.start().expect("node start");
                node.set_broker(mock.clone());
                let harness = Self {
                    go: None,
                    rust: Some(RustHarness { broker: mock.clone(), mock_broker: Some(mock), memory: None, ..h }),
                    shut_down: AtomicBool::new(false),
                };
                harness.on_peak("test-stream", None);
                harness
            }
        }
    }

    /// `setupIntegrationServer()` / the auth test's setup: `DisconnectMode =
    /// never`, a no-op disconnector, `broker.NewMemoryBroker(pubsub.NewLegacySubscriber(n), n, &bconf)`
    /// with `epoch` (and `history_ttl`/`history_limit` when non-zero), started,
    /// then the node started.
    pub fn memory(epoch: &str, history_ttl: i64, history_limit: usize) -> Self {
        match backend() {
            Backend::Go => Self {
                go: Some(bridge::call(
                    "ds.Harness.New",
                    json!({"Broker": "memory", "Epoch": epoch, "HistoryTTL": history_ttl, "HistoryLimit": history_limit}),
                )),
                rust: None,
                shut_down: AtomicBool::new(false),
            },
            Backend::Rust => {
                let controller = Arc::new(MockController::default());
                let h = Self::rust_node(controller, true);
                let node = h.node.clone();
                node.set_disconnector(Arc::new(node_rs::NoopDisconnectQueue));
                let mut bconf = cabby_core::broker::Config::new();
                if history_ttl > 0 {
                    bconf.history_ttl = history_ttl;
                }
                if history_limit > 0 {
                    bconf.history_limit = history_limit;
                }
                let subscriber = Arc::new(node_rs::LegacySubscriber::new(node.clone()));
                let memory = Arc::new(cabby_core::broker::Memory::new(
                    Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>),
                    Some(node.clone() as Arc<dyn cabby_core::broker::Presenter>),
                    &bconf,
                    Arc::new(cabby_core::broker::SystemClock),
                ));
                memory.set_epoch(epoch);
                node.set_broker(memory.clone());
                memory.start().expect("broker start");
                node.start().expect("node start");
                Self {
                    go: None,
                    rust: Some(RustHarness { broker: memory.clone(), mock_broker: None, memory: Some(memory), ..h }),
                    shut_down: AtomicBool::new(false),
                }
            }
        }
    }

    fn rust_node(controller: Arc<MockController>, disconnect_never: bool) -> RustHarness {
        let mut config = node_rs::Config::new();
        config.hub_gopool_size = 2;
        if disconnect_never {
            config.disconnect_mode = node_rs::DISCONNECT_MODE_NEVER.into();
        }
        controller.expectations.lock().unwrap().push(ControllerExpectation {
            method: "Shutdown",
            sid: None,
            identifier_contains: None,
            left: None,
            connect: None,
            command: None,
        });
        let node = Node::new(
            config,
            vec![
                NodeOption::Controller(controller.clone()),
                NodeOption::Instrumenter(Arc::new(Metrics::new(Vec::new(), 10)) as Arc<dyn Instrumenter>),
            ],
        );
        let broker: Arc<dyn Broker> = Arc::new(MockBroker::default());
        RustHarness { node, controller, broker, mock_broker: None, memory: None }
    }

    fn go_call<T: serde::de::DeserializeOwned>(&self, op: &str, mut args: Value) -> T {
        args["Harness"] = json!(self.go.expect("Go harness"));
        bridge::call(op, args)
    }

    fn rust(&self) -> &RustHarness {
        self.rust.as_ref().expect("Rust harness")
    }

    fn expect_controller(&self, e: ControllerExpectation, maybe: bool) {
        if self.go.is_some() {
            let times = e.left.unwrap_or(0);
            let exp = json!({"Method": e.method, "Sid": e.sid, "IdentifierContains": e.identifier_contains,
                             "Times": times, "Maybe": maybe,
                             "ConnectResult": e.connect.as_ref().map(connect_result_to_go),
                             "CommandResult": e.command.as_ref().map(command_result_to_go)});
            self.go_call::<Value>("ds.Harness.ExpectController", json!({"Expectation": exp}));
        } else {
            self.rust().controller.expectations.lock().unwrap().push(e);
        }
    }

    /// `controller.On("Shutdown").Return(nil)`.
    pub fn on_shutdown(&self) {
        let e = ControllerExpectation {
            method: "Shutdown",
            sid: None,
            identifier_contains: None,
            left: None,
            connect: None,
            command: None,
        };
        self.expect_controller(e, false);
    }

    /// `controller.On("Authenticate", mock.Anything, sid|mock.Anything, mock.Anything).Return(res, nil)`.
    pub fn on_authenticate(&self, sid: Option<&str>, res: ConnectResult) {
        let e = ControllerExpectation {
            method: "Authenticate",
            sid: sid.map(str::to_owned),
            identifier_contains: None,
            left: None,
            connect: Some(res),
            command: None,
        };
        self.expect_controller(e, false);
    }

    /// `controller.On("Subscribe", mock.Anything x4, identifier).Return(res, nil)`
    /// with `identifier` `mock.Anything` or `mock.MatchedBy(strings.Contains(_, part))`;
    /// `times`: 0 unlimited, 1 `.Once()`, 2 `.Twice()`; `maybe`: `.Maybe()`.
    pub fn on_subscribe(&self, identifier_contains: Option<&str>, res: CommandResult, times: usize, maybe: bool) {
        let e = ControllerExpectation {
            method: "Subscribe",
            sid: None,
            identifier_contains: identifier_contains.map(str::to_owned),
            left: (times > 0).then_some(times),
            connect: None,
            command: Some(res),
        };
        self.expect_controller(e, maybe);
    }

    /// `controller.ExpectedCalls = []*mock.Call{}`.
    pub fn reset_controller(&self) {
        match &self.rust {
            Some(r) => r.controller.expectations.lock().unwrap().clear(),
            None => {
                self.go_call::<Value>("ds.Harness.ResetController", json!({}));
            }
        }
    }

    fn expect_broker(&self, go: Value, rust: BrokerExpectation) {
        match &self.rust {
            Some(r) => r.mock_broker.as_ref().expect("mock broker").expectations.lock().unwrap().push(rust),
            None => {
                self.go_call::<Value>("ds.Harness.ExpectBroker", json!({"Expectation": go}));
            }
        }
    }

    /// `brk.On("Peak", stream).Return(msg, nil)` (`None`: nil).
    pub fn on_peak(&self, stream: &str, msg: Option<StreamMessage>) {
        let go = json!({"Method": "Peak", "Stream": stream, "Message": msg.as_ref().map(stream_message_to_go)});
        self.expect_broker(go, BrokerExpectation::Peak(stream.into(), Ok(msg)));
    }

    fn go_history(r: &Result<Option<Vec<StreamMessage>>, String>) -> (Value, Value) {
        match r {
            Ok(None) => (Value::Null, Value::Null),
            Ok(Some(v)) => (Value::Array(v.iter().map(stream_message_to_go).collect()), Value::Null),
            Err(e) => (Value::Null, json!(e)),
        }
    }

    /// `brk.On("HistorySince", stream, ts).Return(history, err)` (`Ok(None)`: a nil slice).
    pub fn on_history_since(&self, stream: &str, ts: i64, res: Result<Option<Vec<StreamMessage>>, String>) {
        let (history, error) = Self::go_history(&res);
        let go = json!({"Method": "HistorySince", "Stream": stream, "Ts": ts, "History": history, "Error": error});
        self.expect_broker(go, BrokerExpectation::HistorySince(stream.into(), ts, res));
    }

    /// `brk.On("HistoryFrom", stream, epoch, offset).Return(history, err)`.
    pub fn on_history_from(
        &self,
        stream: &str,
        epoch: &str,
        offset: u64,
        res: Result<Option<Vec<StreamMessage>>, String>,
    ) {
        let (history, error) = Self::go_history(&res);
        let go = json!({"Method": "HistoryFrom", "Stream": stream, "Epoch": epoch, "Offset": offset,
                        "History": history, "Error": error});
        self.expect_broker(go, BrokerExpectation::HistoryFrom(stream.into(), epoch.into(), offset, res));
    }

    /// `brk.HandleBroadcast(&common.StreamMessage{Stream: stream, Data: data})` on the memory broker.
    pub fn broadcast(&self, stream: &str, data: &str) -> Result<(), String> {
        match &self.rust {
            Some(r) => r
                .memory
                .as_ref()
                .expect("memory broker")
                .handle_broadcast(&StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() })
                .map_err(|e| e.message),
            None => {
                let e: Option<String> = self.go_call("ds.Harness.Broadcast", json!({"Stream": stream, "Data": data}));
                e.map_or(Ok(()), Err)
            }
        }
    }

    /// `brk.Subscribe(stream)`.
    pub fn broker_subscribe(&self, stream: &str) -> String {
        match &self.rust {
            Some(r) => r.broker.subscribe(stream),
            None => self.go_call("ds.Harness.BrokerSubscribe", json!({"Stream": stream})),
        }
    }

    /// `n.Size()`.
    pub fn size(&self) -> usize {
        match &self.rust {
            Some(r) => r.node.size(),
            None => self.go_call("ds.Harness.Size", json!({})),
        }
    }

    fn subscribed_once(&self, stream: &str) -> bool {
        match &self.rust {
            Some(r) => {
                let calls = r.controller.calls.lock().unwrap().clone();
                calls.iter().filter(|c| c[0] == "Subscribe").any(|c| {
                    r.node
                        .lookup_session(&c[2])
                        .is_some_and(|s| s.subscriptions().to_map().values().any(|v| v.iter().any(|x| x == stream)))
                })
            }
            None => self.go_call("ds.Harness.Subscribed", json!({"Stream": stream})),
        }
    }

    /// Waits (bounded) until a session the controller was asked to subscribe is
    /// subscribed to `stream` (its subscription state lists the stream, which the
    /// node records after subscribing it in the hub). Replaces upstream's sleeps
    /// before broadcasting to a live session.
    pub fn wait_subscribed(&self, stream: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.subscribed_once(stream) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Waits (bounded) until `n.Size() == size`; sessions are removed
    /// asynchronously once a request ends.
    pub fn wait_size(&self, size: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.size() == size {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn go_calls(&self) -> GoCalls {
        self.go_call("ds.Harness.Calls", json!({}))
    }

    /// Controller calls as `[method, sid, ids, identifier]` (strings only).
    pub fn controller_calls(&self) -> Vec<Vec<String>> {
        match &self.rust {
            Some(r) => r.controller.calls.lock().unwrap().clone(),
            None => self.go_calls().controller,
        }
    }

    /// Mock broker calls as `[method, args...]`.
    pub fn broker_calls(&self) -> Vec<Vec<String>> {
        match &self.rust {
            Some(r) => r.mock_broker.as_ref().map(|b| b.calls.lock().unwrap().clone()).unwrap_or_default(),
            None => self.go_calls().broker,
        }
    }

    /// Calls no expectation matched (testify would fail the test).
    pub fn unexpected_calls(&self) -> Vec<String> {
        match &self.rust {
            Some(r) => {
                let mut u = r.controller.unexpected.lock().unwrap().clone();
                if let Some(b) = &r.mock_broker {
                    u.extend(b.unexpected.lock().unwrap().iter().cloned());
                }
                u
            }
            None => self.go_calls().unexpected,
        }
    }

    /// `n.Shutdown(context.Background())` (its error ignored, as upstream's
    /// `// nolint: errcheck`). Mock calls it makes are checked by
    /// [`Harness::unexpected_calls`] afterwards; on Go a testify panic during
    /// shutdown is reported there too.
    pub fn shutdown(&self) {
        self.shut_down.store(true, Ordering::SeqCst);
        match &self.rust {
            Some(r) => {
                let _ = r.node.shutdown(&cabby_server::rpc::Context::default());
            }
            None => {
                self.go_call::<Value>("ds.Harness.Shutdown", json!({}));
            }
        }
    }

    /// `DSHandler(n, brk, streamCtrl, nil, context.Background(), &server.DefaultHeadersExtractor{}, &conf, logger)`.
    pub fn handler(&self, conf: &rs::Config, streams: StreamsKind) -> Result<Handler, String> {
        if self.go.is_some() {
            #[derive(Deserialize)]
            struct R {
                handler: Option<i64>,
                nil_handler: bool,
                error: Option<String>,
            }
            let r: R =
                self.go_call("ds.Harness.Handler", json!({"Config": go_config(conf), "Streams": go_streams(&streams)}));
            if let Some(e) = r.error {
                assert!(r.nil_handler, "Go returned a handler together with error {e:?}");
                return Err(e);
            }
            return Ok(Handler { go: r.handler, rust: None, resolved: None, _shutdown: CancellationToken::new() });
        }
        let r = self.rust();
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let ctrl = match streams {
            StreamsKind::None => None,
            StreamsKind::Echo => Some(StreamsController::new(
                "",
                Box::new(|identifier: &str| {
                    Ok(SubscribeRequest { stream_name: identifier_stream_name(identifier)?, ..Default::default() })
                }),
            )),
            StreamsKind::RequireSigned => {
                let rec = resolved.clone();
                Some(StreamsController::new(
                    "",
                    Box::new(move |identifier: &str| {
                        let decoded: Result<HashMap<String, String>, _> = serde_json::from_str(identifier);
                        let (decoded, error) = match decoded {
                            Ok(d) => (Some(d), None),
                            Err(e) => (None, Some(e.to_string())),
                        };
                        let map = decoded.clone().unwrap_or_default();
                        rec.lock().unwrap().push(Resolved { identifier: identifier.to_owned(), decoded, error });
                        if map.get("signed_stream_name").is_none_or(String::is_empty) {
                            return Err(ControllerError::new("signed stream name is missing"));
                        }
                        Ok(SubscribeRequest {
                            stream_name: map.get("stream_name").cloned().unwrap_or_default(),
                            ..Default::default()
                        })
                    }),
                ))
            }
            StreamsKind::RejectNoauth => Some(StreamsController::new(
                "",
                Box::new(|identifier: &str| {
                    if identifier.contains("noauth") {
                        return Err(ControllerError::new("unauthenticated"));
                    }
                    Ok(SubscribeRequest { stream_name: identifier_stream_name(identifier)?, ..Default::default() })
                }),
            )),
            StreamsKind::Signed { secret, public } => {
                let mut sc = cabby_core::streams::Config::new();
                sc.secret = secret;
                sc.public = public;
                Some(StreamsController::for_config(&sc))
            }
        };
        let shutdown = CancellationToken::new();
        let h = rs::ds_handler(
            r.node.clone(),
            r.broker.clone(),
            ctrl.map(Arc::new),
            None,
            shutdown.clone(),
            Some(Arc::new(DefaultHeadersExtractor::default())),
            conf,
        )
        .map_err(|e| e.0)?;
        Ok(Handler { go: None, rust: Some(Arc::new(h)), resolved: Some(resolved), _shutdown: shutdown })
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // `defer n.Shutdown(ctx)`, unless the test shut the node down itself (a
        // second Shutdown fails upstream) or already failed.
        if !std::thread::panicking() && !self.shut_down.load(Ordering::SeqCst) {
            self.shutdown();
        }
    }
}

// ------------------------------------------------------------------ handler

/// A `DSHandler` built over a [`Harness`].
pub struct Handler {
    go: Option<i64>,
    rust: Option<Arc<rs::Handler>>,
    resolved: Option<Arc<Mutex<Vec<Resolved>>>>,
    _shutdown: CancellationToken,
}

/// A completed response.
pub type Recorded = ApiResponse;

/// How long a recorder-served request (they all complete without waiting) may take.
const SERVE_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct GoServe {
    done: bool,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    headers: HashMap<String, Vec<String>>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    panic: Option<String>,
}

impl Handler {
    /// `handler.ServeHTTP(w, req)` with `w := httptest.NewRecorder()`.
    pub fn serve(&self, req: &Request) -> Recorded {
        if let Some(id) = self.go {
            let deadline = Instant::now() + SERVE_DEADLINE;
            let serve: i64 = bridge::call(
                "ds.Handler.Serve",
                json!({"Handler": id, "Method": req.method, "Target": req.target, "Headers": req.headers}),
            );
            let r = loop {
                let r: GoServe = bridge::call("ds.Serve.Wait", json!({"Serve": serve, "TimeoutMs": ms(BRIDGE_SLICE)}));
                if r.done || Instant::now() >= deadline {
                    break r;
                }
            };
            assert!(r.done, "handler did not return within {SERVE_DEADLINE:?}");
            if let Some(p) = r.panic {
                panic!("handler panicked in Go: {p}");
            }
            return GoApiResponse::from_parts(r.status, r.headers, r.body);
        }
        let h = self.rust.clone().expect("Rust handler");
        let rec = Recorder::new();
        let w = OwnedRecorder::new(&rec);
        let (tx, rx) = channel();
        let r = req.to_rust();
        std::thread::spawn(move || {
            let done = CancellationToken::new();
            // The handler finishes the writer before it returns.
            h.serve(&r, Box::new(w), &done);
            let _ = tx.send(());
        });
        match rx.recv_timeout(SERVE_DEADLINE) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => panic!("handler did not return within {SERVE_DEADLINE:?}"),
            Err(RecvTimeoutError::Disconnected) => panic!("handler panicked"),
        }
        let headers = rec.headers();
        ApiResponse { status: rec.code(), headers, body: rec.body_bytes().unwrap_or_default(), body_read: 0 }
    }

    /// The identifiers the [`StreamsKind::RequireSigned`] resolver received.
    pub fn resolved(&self) -> Vec<Resolved> {
        match self.go {
            Some(id) => bridge::call("ds.Handler.Resolved", json!({"Handler": id})),
            None => self.resolved.as_ref().expect("resolver records").lock().unwrap().clone(),
        }
    }

    /// `mux.Handle("/ds/", handler); httptest.NewServer(mux)`.
    pub fn listen(&self) -> Server {
        match self.go {
            Some(id) => {
                #[derive(Deserialize)]
                struct R {
                    url: String,
                }
                let r: R = bridge::call("ds.Handler.Listen", json!({"Handler": id, "Pattern": "/ds/"}));
                Server { url: r.url, go: Some(id), rust: None }
            }
            None => serve_rust(self.rust.clone().expect("Rust handler"), "/ds/"),
        }
    }
}

/// A handler served on loopback (`httptest.Server`); closed on drop.
pub struct Server {
    url: String,
    go: Option<i64>,
    rust: Option<Arc<HttpServer>>,
}

impl Server {
    /// `ts.URL`.
    pub fn url(&self) -> &str {
        &self.url
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.url.trim_start_matches("http://").parse().expect("ip:port")
    }

    /// One complete request (`http.DefaultClient.Do(req)` and reading the body).
    pub fn request(&self, req: &Request) -> crate::raw_http::Response {
        let headers: Vec<(&str, &str)> = req.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        crate::raw_http::request(self.addr(), &req.method, &req.target, &headers, None).expect("HTTP request")
    }

    /// Starts a request on its own thread; [`PendingResponse::wait`] collects it.
    pub fn request_async(&self, req: &Request) -> PendingResponse {
        let (tx, rx) = channel();
        let addr = self.addr();
        let req = req.clone();
        std::thread::spawn(move || {
            let headers: Vec<(&str, &str)> = req.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let _ = tx.send(crate::raw_http::request(addr, &req.method, &req.target, &headers, None));
        });
        PendingResponse { rx }
    }
}

/// A request running on its own (detached) thread.
pub struct PendingResponse {
    rx: std::sync::mpsc::Receiver<anyhow::Result<crate::raw_http::Response>>,
}

impl PendingResponse {
    pub fn wait(self, timeout: Duration) -> crate::raw_http::Response {
        match self.rx.recv_timeout(timeout) {
            Ok(r) => r.expect("HTTP request"),
            Err(_) => panic!("no response within {timeout:?}"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(id) = self.go
            && !std::thread::panicking()
        {
            bridge::call::<_, Value>("ds.Handler.Close", json!({"Handler": id}));
        }
        if let Some(server) = &self.rust {
            server.shutdown();
        }
    }
}

// ------------------------------------------------------------------ loopback server for the Rust handler

/// `httptest.NewServer(mux)` for the Rust handler: Cabby's own HTTP server
/// (`cabby_server::server`) on an ephemeral loopback port, with the handler
/// mounted at `<prefix>*` as the CLI mounts `ds::handler` (`ds::endpoint`);
/// other paths get the server's 404, as the mux's.
fn serve_rust(handler: Arc<rs::Handler>, prefix: &'static str) -> Server {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).expect("HTTP server");
    server.setup_handler(&format!("{prefix}*"), rs::endpoint(handler));
    server.start().expect("HTTP server starts");
    let url = format!("http://{}", server.local_addr().expect("bound address"));
    Server { url, go: None, rust: Some(server) }
}
