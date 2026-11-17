//! Facade for upstream package `broker`.
//!
//! Stateful upstream objects (trackers, brokers) live in the bridge process
//! behind numeric handles; each facade method forwards one call to that object.
//! Rust brokers take an explicit [`Clock`]; the tests pass a
//! [`ManualClock`](crate::clock::ManualClock). The Go reference always uses
//! wall-clock time, so tests that advance the clock are not observable on Go.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cabby_core::broker::{
    Broadcaster, Broker, BrokerError, Cacheable, Clock, Config, ConfigError, Entry, Memory, Memstream, Nats,
    NatsConfig, Presenter, StreamsTracker,
};
use cabby_core::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::go_types::*;
use crate::{Backend, backend, bridge};

fn go_err(e: Option<String>) -> Result<(), BrokerError> {
    e.map_or(Ok(()), |message| Err(BrokerError { message }))
}

#[derive(Deserialize)]
struct GoReply {
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    presenter_calls: Value,
}

impl GoReply {
    fn into_result<T>(self, f: impl Fn(Value) -> T) -> Result<T, BrokerError> {
        match self.error {
            Some(message) => Err(BrokerError { message }),
            None => Ok(f(self.result)),
        }
    }
}

fn bytes_from_go(v: Value) -> Option<Vec<u8>> {
    // Go encodes []byte as base64; nil is null.
    v.as_str().map(|s| decode_base64(s).expect("base64 from Go"))
}

fn decode_base64(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes().filter(|&c| c != b'=') {
        let v = ALPHABET.iter().position(|&a| a == c)? as u32;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

fn release(kind: &str, handle: u64) {
    if !std::thread::panicking() {
        let _: Value = bridge::call("broker.Release", json!({"kind": kind, "id": handle}));
    }
}

// ------------------------------------------------------------------ config

/// `broker.Config` as encoding/json writes it (exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    adapter: String,
    #[serde(rename = "HistoryTTL")]
    history_ttl: i64,
    history_limit: usize,
    #[serde(rename = "SessionsTTL")]
    sessions_ttl: i64,
    #[serde(rename = "PresenceTTL")]
    presence_ttl: i64,
}

impl From<&Config> for GoConfig {
    fn from(c: &Config) -> Self {
        GoConfig {
            adapter: c.adapter.clone(),
            history_ttl: c.history_ttl,
            history_limit: c.history_limit,
            sessions_ttl: c.sessions_ttl,
            presence_ttl: c.presence_ttl,
        }
    }
}

impl From<GoConfig> for Config {
    fn from(g: GoConfig) -> Self {
        Config {
            adapter: g.adapter,
            history_ttl: g.history_ttl,
            history_limit: g.history_limit,
            sessions_ttl: g.sessions_ttl,
            presence_ttl: g.presence_ttl,
        }
    }
}

/// `broker.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoConfig>("broker.NewConfig", json!({})).into(),
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(conf: &Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("broker.Config.ToToml", json!({"Config": GoConfig::from(conf)})),
    }
}

#[derive(Deserialize)]
struct GoDecoded {
    config: GoConfig,
    error: Option<String>,
}

/// `toml.Decode(s, &conf)`.
pub fn config_merge_toml(conf: &mut Config, toml: &str) -> Result<(), ConfigError> {
    match backend() {
        Backend::Rust => conf.merge_toml(toml),
        Backend::Go => {
            let d: GoDecoded =
                bridge::call("broker.Config.DecodeToml", json!({"Config": GoConfig::from(&*conf), "Toml": toml}));
            *conf = d.config.into();
            d.error.map_or(Ok(()), |message| Err(ConfigError { message }))
        }
    }
}

// ----------------------------------------------------------------- tracker

/// `broker.NewStreamsTracker()` under test.
pub struct Tracker {
    rust: Option<StreamsTracker>,
    go: u64,
}

#[derive(Deserialize)]
struct GoTrackerReply {
    result: bool,
    /// How many times the Go callback ran.
    calls: usize,
}

/// `broker.NewStreamsTracker()`.
pub fn new_streams_tracker() -> Tracker {
    match backend() {
        Backend::Rust => Tracker { rust: Some(StreamsTracker::new()), go: 0 },
        Backend::Go => Tracker { rust: None, go: bridge::call("broker.NewStreamsTracker", json!({})) },
    }
}

impl Tracker {
    fn go_call(&self, method: &str, name: &str, callback: &mut dyn FnMut()) -> bool {
        let r: GoTrackerReply =
            bridge::call("broker.StreamsTracker", json!({"id": self.go, "method": method, "name": name}));
        // The Go callback only counts its invocations; run the test's callback
        // once per invocation, so extra calls in Go show up in the test's counts.
        for _ in 0..r.calls {
            callback();
        }
        r.result
    }

    /// `tracker.Add(name, onNew)`.
    pub fn add(&self, name: &str, on_new: &mut dyn FnMut()) -> bool {
        match &self.rust {
            Some(t) => t.add(name, on_new),
            None => self.go_call("Add", name, on_new),
        }
    }

    /// `tracker.Has(name)`.
    pub fn has(&self, name: &str) -> bool {
        match &self.rust {
            Some(t) => t.has(name),
            None => self.go_call("Has", name, &mut || {}),
        }
    }

    /// `tracker.Remove(name, onLast)`.
    pub fn remove(&self, name: &str, on_last: &mut dyn FnMut()) -> bool {
        match &self.rust {
            Some(t) => t.remove(name, on_last),
            None => self.go_call("Remove", name, on_last),
        }
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        if self.rust.is_none() {
            release("tracker", self.go);
        }
    }
}

// --------------------------------------------------------------- presenter

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresenterMethod {
    HandleJoin,
    HandleLeave,
}

/// `&MockPresenter{}` with `On("HandleJoin", Anything, Anything)` and
/// `On("HandleLeave", Anything, Anything)`: accepts every call and records it.
#[derive(Default)]
pub struct MockPresenter {
    calls: Mutex<Vec<(PresenterMethod, String, PresenceEvent)>>,
}

impl MockPresenter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn assert_called(&self, method: PresenterMethod, stream: &str, msg: &PresenceEvent) {
        let calls = self.calls.lock().unwrap();
        assert!(
            calls.iter().any(|(m, s, e)| *m == method && s == stream && e == msg),
            "expected {method:?}({stream:?}, {msg:?}); calls: {calls:?}"
        );
    }

    pub fn assert_number_of_calls(&self, method: PresenterMethod, n: usize) {
        let calls = self.calls.lock().unwrap();
        let got = calls.iter().filter(|(m, ..)| *m == method).count();
        assert_eq!(n, got, "number of {method:?} calls; calls: {calls:?}");
    }

    fn record(&self, method: PresenterMethod, stream: &str, msg: &PresenceEvent) {
        self.calls.lock().unwrap().push((method, stream.into(), msg.clone()));
    }

    fn replay(&self, calls: &Value) {
        for c in calls.as_array().into_iter().flatten() {
            let method = match c["method"].as_str() {
                Some("HandleJoin") => PresenterMethod::HandleJoin,
                Some("HandleLeave") => PresenterMethod::HandleLeave,
                other => panic!("unknown presenter call {other:?}"),
            };
            self.record(method, c["stream"].as_str().unwrap_or_default(), &presence_event_from_go(&c["event"]));
        }
    }
}

impl Presenter for MockPresenter {
    fn handle_join(&self, stream: &str, msg: &PresenceEvent) {
        self.record(PresenterMethod::HandleJoin, stream, msg);
    }
    fn handle_leave(&self, stream: &str, msg: &PresenceEvent) {
        self.record(PresenterMethod::HandleLeave, stream, msg);
    }
}

// ------------------------------------------------------------------ memory

/// `broker.NewMemoryBroker(nil, presenter, &config)` under test.
pub struct MemoryBroker {
    rust: Option<Memory>,
    go: u64,
    presenter: Option<Arc<MockPresenter>>,
}

/// `broker.NewMemoryBroker(nil, presenter, &config)`, with `clock` for Rust.
///
/// Go: the broker gets a no-op broadcaster instead of nil, because
/// [`MemoryBroker::add`] goes through `HandleBroadcast`.
pub fn new_memory_broker(
    presenter: Option<&Arc<MockPresenter>>,
    config: &Config,
    clock: Arc<dyn Clock>,
) -> MemoryBroker {
    match backend() {
        Backend::Rust => MemoryBroker {
            rust: Some(Memory::new(None, presenter.map(|p| p.clone() as Arc<dyn Presenter>), config, clock)),
            go: 0,
            presenter: presenter.cloned(),
        },
        Backend::Go => MemoryBroker {
            rust: None,
            go: bridge::call(
                "broker.NewMemoryBroker",
                json!({"Config": GoConfig::from(config), "Presenter": presenter.is_some()}),
            ),
            presenter: presenter.cloned(),
        },
    }
}

/// A `Cacheable` holding fixed bytes (upstream `TestCacheable`).
pub struct TestCacheable(pub String);

impl Cacheable for TestCacheable {
    fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError> {
        Ok(self.0.clone().into_bytes())
    }
}

fn messages(v: Value) -> Vec<StreamMessage> {
    v.as_array().map(|a| a.iter().map(stream_message_from_go).collect()).unwrap_or_default()
}

fn opt_messages(v: Value) -> Option<Vec<StreamMessage>> {
    (!v.is_null()).then(|| messages(v))
}

fn opt_event(v: Value) -> Option<PresenceEvent> {
    (!v.is_null()).then(|| presence_event_from_go(&v))
}

impl MemoryBroker {
    fn go(&self, method: &str, args: Value) -> GoReply {
        let reply: GoReply = bridge::call("broker.Memory", json!({"id": self.go, "method": method, "args": args}));
        if let Some(p) = &self.presenter {
            p.replay(&reply.presenter_calls);
        }
        reply
    }

    fn rust(&self) -> Option<&Memory> {
        self.rust.as_ref()
    }

    /// `broker.add(stream, data)` (unexported). Go: through `HandleBroadcast`,
    /// which stores via `add` and reports its offset in the message.
    pub fn add(&self, stream: &str, data: &str) -> u64 {
        match self.rust() {
            Some(b) => b.add(stream, data),
            None => {
                let r = self.go("Add", json!({"stream": stream, "data": data}));
                r.into_result(|v| v.as_u64().expect("offset")).expect("HandleBroadcast")
            }
        }
    }

    /// `broker.expire()` (unexported, driven by an internal goroutine upstream).
    pub fn expire(&self) {
        match self.rust() {
            Some(b) => b.expire(),
            None => unimplemented!("not_observable: Memory.expire is unexported and Go expiry uses wall-clock time"),
        }
    }

    /// `broker.GetEpoch()` (upstream tests also read the `epoch` field).
    pub fn epoch(&self) -> String {
        match self.rust() {
            Some(b) => b.epoch(),
            None => self.go("GetEpoch", json!({})).into_result(|v| v.as_str().unwrap_or_default().to_owned()).unwrap(),
        }
    }

    pub fn set_epoch(&self, epoch: &str) {
        match self.rust() {
            Some(b) => b.set_epoch(epoch),
            None => self.go("SetEpoch", json!({"epoch": epoch})).into_result(|_| ()).unwrap(),
        }
    }

    pub fn store(&self, stream: &str, data: &[u8], offset: u64, ts: Option<SystemTime>) -> Result<u64, BrokerError> {
        match self.rust() {
            Some(b) => b.store(stream, data, offset, ts),
            None => {
                let ts = ts.map(|t| t.duration_since(UNIX_EPOCH).expect("after epoch").as_nanos() as i64);
                self.go(
                    "Store",
                    json!({"stream": stream, "data": String::from_utf8(data.to_vec()).expect("UTF-8"),
                           "offset": offset, "ts_unix_nanos": ts}),
                )
                .into_result(|v| v.as_u64().unwrap_or(0))
            }
        }
    }

    pub fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        match self.rust() {
            Some(b) => b.history_from(stream, epoch, offset),
            None => self
                .go("HistoryFrom", json!({"stream": stream, "epoch": epoch, "offset": offset}))
                .into_result(messages),
        }
    }

    pub fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        match self.rust() {
            Some(b) => b.history_since(stream, ts),
            None => self.go("HistorySince", json!({"stream": stream, "ts": ts})).into_result(opt_messages),
        }
    }

    pub fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        match self.rust() {
            Some(b) => b.commit_session(sid, session),
            None => {
                let data = session.to_cache_entry()?;
                let data = String::from_utf8(data).expect("UTF-8 cache entry");
                go_err(self.go("CommitSession", json!({"sid": sid, "data": data})).error)
            }
        }
    }

    pub fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        match self.rust() {
            Some(b) => b.restore_session(sid),
            None => self.go("RestoreSession", json!({"sid": sid})).into_result(bytes_from_go),
        }
    }

    pub fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        match self.rust() {
            Some(b) => b.touch_session(sid),
            None => go_err(self.go("TouchSession", json!({"sid": sid})).error),
        }
    }

    pub fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        match self.rust() {
            Some(b) => b.presence_add(stream, sid, pid, info),
            None => self
                .go("PresenceAdd", json!({"stream": stream, "sid": sid, "pid": pid, "info": info}))
                .into_result(opt_event),
        }
    }

    pub fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        match self.rust() {
            Some(b) => b.presence_remove(stream, sid),
            None => self.go("PresenceRemove", json!({"stream": stream, "sid": sid})).into_result(opt_event),
        }
    }

    /// `broker.PresenceInfo(stream)` (default options).
    pub fn presence_info(&self, stream: &str) -> Result<PresenceInfo, BrokerError> {
        match self.rust() {
            Some(b) => b.presence_info(stream, None),
            None => self.go("PresenceInfo", json!({"stream": stream})).into_result(|v| presence_info_from_go(&v)),
        }
    }

    pub fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        match self.rust() {
            Some(b) => b.touch_presence(sid),
            None => go_err(self.go("TouchPresence", json!({"sid": sid})).error),
        }
    }

    pub fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        match self.rust() {
            Some(b) => b.peak(stream),
            None => self
                .go("Peak", json!({"stream": stream}))
                .into_result(|v| (!v.is_null()).then(|| stream_message_from_go(&v))),
        }
    }
}

impl Drop for MemoryBroker {
    fn drop(&mut self) {
        if self.rust.is_none() {
            release("memory", self.go);
        }
    }
}

/// `&memstream{ttl: ttl, limit: limit}` (unexported upstream type).
pub fn new_memstream(ttl: i64, limit: usize, clock: Arc<dyn Clock>) -> Memstream {
    match backend() {
        Backend::Rust => Memstream::new(ttl, limit, clock),
        Backend::Go => unimplemented!("not_observable: memstream is an unexported type"),
    }
}

/// Re-exported for memstream callbacks.
pub type MemstreamEntry = Entry;

// -------------------------------------------------------------------- NATS

/// Stands in for upstream `pubsub.NewLegacySubscriber(FakeBroadastHandler{})`
/// (a subscriber whose handler ignores everything) until `pubsub` is ported.
struct FakeLegacySubscriber;

impl Broadcaster for FakeLegacySubscriber {
    fn broadcast(&self, _msg: &StreamMessage) {}
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
    fn subscribe(&self, _stream: &str) {}
    fn unsubscribe(&self, _stream: &str) {}
}

/// The broadcaster a NATS broker test passes to `NewNATSBroker`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsBroadcaster {
    /// `nil`.
    None,
    /// `pubsub.NewLegacySubscriber(FakeBroadastHandler{})`.
    FakeLegacySubscriber,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoNatsConfig {
    servers: String,
    dont_randomize_servers: bool,
    max_reconnect_attempts: i64,
}

/// `natsconfig.NewNATSConfig()` (upstream package `nats`).
pub fn new_nats_config() -> NatsConfig {
    match backend() {
        Backend::Rust => NatsConfig::new(),
        Backend::Go => {
            let g: GoNatsConfig = bridge::call("broker.NewNATSConfig", json!({}));
            NatsConfig {
                servers: g.servers,
                dont_randomize_servers: g.dont_randomize_servers,
                max_reconnect_attempts: g.max_reconnect_attempts,
            }
        }
    }
}

/// `broker.NewNATSBroker(broadcaster, &config, &nconfig, logger)` under test.
pub struct NatsBroker {
    rust: Option<Nats>,
    go: u64,
}

/// `broker.NewNATSBroker(broadcaster, &config, &nconfig, slog.Default())`.
pub fn new_nats_broker(broadcaster: NatsBroadcaster, config: &Config, nats: &NatsConfig) -> NatsBroker {
    match backend() {
        Backend::Rust => {
            let b: Option<Arc<dyn Broadcaster>> = match broadcaster {
                NatsBroadcaster::None => None,
                NatsBroadcaster::FakeLegacySubscriber => Some(Arc::new(FakeLegacySubscriber)),
            };
            NatsBroker { rust: Some(Nats::new(b, config, nats)), go: 0 }
        }
        Backend::Go => NatsBroker {
            rust: None,
            go: bridge::call(
                "broker.NewNATSBroker",
                json!({
                    "Config": GoConfig::from(config),
                    "NATS": GoNatsConfig {
                        servers: nats.servers.clone(),
                        dont_randomize_servers: nats.dont_randomize_servers,
                        max_reconnect_attempts: nats.max_reconnect_attempts,
                    },
                    "Broadcaster": broadcaster == NatsBroadcaster::FakeLegacySubscriber,
                }),
            ),
        },
    }
}

impl NatsBroker {
    fn go(&self, method: &str, args: Value) -> GoReply {
        bridge::call("broker.NATS", json!({"id": self.go, "method": method, "args": args}))
    }

    /// `broker.Start(nil)`.
    pub fn start(&self) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.start(),
            None => go_err(self.go("Start", json!({})).error),
        }
    }

    /// `broker.Shutdown(context.Background())`.
    pub fn shutdown(&self) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.shutdown(),
            None => go_err(self.go("Shutdown", json!({})).error),
        }
    }

    pub fn ready(&self, timeout: Duration) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.ready(timeout),
            None => go_err(self.go("Ready", json!({"timeout_ms": timeout.as_millis() as u64})).error),
        }
    }

    pub fn reset(&self) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.reset(),
            None => go_err(self.go("Reset", json!({})).error),
        }
    }

    pub fn subscribe(&self, stream: &str) -> String {
        match &self.rust {
            Some(b) => b.subscribe(stream),
            None => self.go("Subscribe", json!({"stream": stream})).result.as_str().unwrap_or_default().to_owned(),
        }
    }

    pub fn unsubscribe(&self, stream: &str) -> String {
        match &self.rust {
            Some(b) => b.unsubscribe(stream),
            None => self.go("Unsubscribe", json!({"stream": stream})).result.as_str().unwrap_or_default().to_owned(),
        }
    }

    pub fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.handle_broadcast(msg),
            None => go_err(self.go("HandleBroadcast", json!({"msg": stream_message_to_go(msg)})).error),
        }
    }

    pub fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        match &self.rust {
            Some(b) => b.history_since(stream, ts),
            None => self.go("HistorySince", json!({"stream": stream, "ts": ts})).into_result(opt_messages),
        }
    }

    pub fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        match &self.rust {
            Some(b) => b.history_from(stream, epoch, offset),
            None => self
                .go("HistoryFrom", json!({"stream": stream, "epoch": epoch, "offset": offset}))
                .into_result(messages),
        }
    }

    pub fn epoch(&self) -> String {
        match &self.rust {
            Some(b) => b.epoch(),
            None => self.go("Epoch", json!({})).result.as_str().unwrap_or_default().to_owned(),
        }
    }

    pub fn set_epoch(&self, epoch: &str) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.set_epoch(epoch),
            None => go_err(self.go("SetEpoch", json!({"epoch": epoch})).error),
        }
    }

    /// `broker.local.GetEpoch()` (unexported field upstream).
    ///
    /// Go: observed through the exported `HistoryFrom`, which delegates to
    /// `n.local.HistoryFrom`; that checks the epoch first and rejects any other
    /// epoch with `unknown epoch: <given>, current: <local epoch>`. The facade
    /// reads the local epoch from that rejection, then checks that the local
    /// broker accepts it (no "unknown epoch" error for it).
    pub fn local_epoch(&self) -> String {
        const PROBE_STREAM: &str = "cabby-local-epoch-probe";
        const SENTINEL: &str = "cabby-sentinel-epoch";
        match &self.rust {
            Some(b) => b.local_epoch(),
            None => {
                let rejected = self.history_from(PROBE_STREAM, SENTINEL, 0).expect_err("sentinel epoch accepted");
                let prefix = format!("unknown epoch: {SENTINEL}, current: ");
                let current = rejected
                    .message
                    .strip_prefix(&prefix)
                    .unwrap_or_else(|| panic!("unexpected HistoryFrom rejection {:?}", rejected.message))
                    .to_owned();
                if let Err(e) = self.history_from(PROBE_STREAM, &current, 0) {
                    assert!(!e.message.starts_with("unknown epoch"), "local broker rejects its own epoch: {e}");
                }
                current
            }
        }
    }

    pub fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.commit_session(sid, session),
            None => {
                let data = String::from_utf8(session.to_cache_entry()?).expect("UTF-8 cache entry");
                go_err(self.go("CommitSession", json!({"sid": sid, "data": data})).error)
            }
        }
    }

    pub fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        match &self.rust {
            Some(b) => b.restore_session(sid),
            None => self.go("RestoreSession", json!({"sid": sid})).into_result(bytes_from_go),
        }
    }

    pub fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        match &self.rust {
            Some(b) => b.touch_session(sid),
            None => go_err(self.go("TouchSession", json!({"sid": sid})).error),
        }
    }
}

impl Drop for NatsBroker {
    fn drop(&mut self) {
        if self.rust.is_none() {
            release("nats", self.go);
        }
    }
}

/// Upstream test helper `consumerSequence(js, stream, offset).read(n)`: stream
/// sequence numbers of the next `n` messages of the broker's JetStream stream
/// for `stream`, read from `offset`. This is NATS-side test tooling (a Go
/// JetStream client in the bridge), independent of the backend under test; the
/// stream name is the broker's wire contract (`_ac_<stream>`).
pub fn consumer_sequence(servers: &str, stream: &str, offset: u64, n: usize) -> Result<Vec<u64>, String> {
    let r: GoReply = bridge::call(
        "broker.test.ConsumerSequence",
        json!({"servers": servers, "stream": stream, "offset": offset, "n": n}),
    );
    match r.error {
        Some(e) => Err(e),
        None => Ok(serde_json::from_value(r.result).expect("sequence numbers")),
    }
}
