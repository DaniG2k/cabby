//! Facade for upstream package `node`.
//!
//! The Go arms drive real Go objects through the bridge (`bridge/node.go`):
//! nodes wired like upstream's `NewMockNode` / `setupIntegrationNode`, sessions
//! built like `NewMockSession` (or by `node.NewSession` when timers must run)
//! over `mocks.NewMockConnection()`, memory and NATS brokers, disconnect
//! queues, subscription states and the configs, kept behind integer handles.
//! testify mocks (`mocks.Controller`, `mocks.Broker`) are programmed from the
//! same [`Expect`]ations on both backends, with canned `Run` callbacks. Only
//! exported Go API is called; where upstream reaches into unexported state the
//! Go arm uses the exported operation with the same observable effect, and
//! the method documents the adaptation. What has no exported path stays
//! behind accessors that report `not_observable` under Go ([`TestNode::rust_with`],
//! [`TestSession::rust_with`], [`TestQueue::rust_with`], [`TestNode::timers`]).
//!
//! Rust nodes built here run on a [`ManualTimers`] clock (session timers and the
//! disconnect queue throttle), which tests advance explicitly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cabby_core::broker::{Broker, BrokerError};
use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, Message, MessageData, Outcome, PresenceEvent, Reply,
    SessionEnv, StreamMessage, StringMap,
};
use cabby_core::encoders::{FrameType, SentFrame};
use cabby_server::metrics::{Instrumenter, Metrics};
use cabby_server::node::{self as rs, Disconnector, LegacySubscriber, Node, NodeOption, Session, SessionOption};
pub use cabby_server::node::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_INTERNAL_SERVER_ERR, CLOSE_NORMAL_CLOSURE, Config,
    DISCONNECT_MODE_ALWAYS, DISCONNECT_MODE_AUTO, DISCONNECT_MODE_NEVER, DisconnectQueueConfig,
    METRICS_DISCONNECT_DISCARDED, NodeError,
};
use cabby_server::rpc::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::go_types::{
    command_result_from_go, command_result_to_go, connect_result_from_go, connect_result_to_go, env_to_go,
    opt_env_from_go, presence_event_from_go, presence_event_to_go, stream_message_to_go,
};
pub use super::node_doubles::*;
use crate::sync::Watched;
use crate::timers::ManualTimers;
use crate::{Backend, backend, bridge};

fn not_observable(reason: &str) -> ! {
    unimplemented!("not_observable: {reason}")
}

#[derive(Deserialize)]
struct GoErr {
    error: Option<String>,
}

impl GoErr {
    fn into_result(self) -> Result<(), NodeError> {
        self.error.map_or(Ok(()), |e| Err(NodeError(e)))
    }
}

// ------------------------------------------------------------------ config

/// `node.Config` as encoding/json writes it (exported Go field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    disconnect_mode: String,
    shutdown_disconnect_pool_size: i64,
    ping_interval: i64,
    stats_refresh_interval: i64,
    hub_gopool_size: i64,
    ping_timestamp_precision: String,
    pong_timeout: i64,
    enable_native_ping: bool,
    shutdown_timeout: i64,
    shutdown_delay: i64,
}

impl From<&Config> for GoConfig {
    fn from(c: &Config) -> Self {
        GoConfig {
            disconnect_mode: c.disconnect_mode.clone(),
            shutdown_disconnect_pool_size: c.shutdown_disconnect_pool_size,
            ping_interval: c.ping_interval,
            stats_refresh_interval: c.stats_refresh_interval,
            hub_gopool_size: c.hub_gopool_size,
            ping_timestamp_precision: c.ping_timestamp_precision.clone(),
            pong_timeout: c.pong_timeout,
            enable_native_ping: c.enable_native_ping,
            shutdown_timeout: c.shutdown_timeout,
            shutdown_delay: c.shutdown_delay,
        }
    }
}

impl From<GoConfig> for Config {
    fn from(g: GoConfig) -> Self {
        Config {
            disconnect_mode: g.disconnect_mode,
            shutdown_disconnect_pool_size: g.shutdown_disconnect_pool_size,
            ping_interval: g.ping_interval,
            stats_refresh_interval: g.stats_refresh_interval,
            hub_gopool_size: g.hub_gopool_size,
            ping_timestamp_precision: g.ping_timestamp_precision,
            pong_timeout: g.pong_timeout,
            enable_native_ping: g.enable_native_ping,
            shutdown_timeout: g.shutdown_timeout,
            shutdown_delay: g.shutdown_delay,
        }
    }
}

/// `node.DisconnectQueueConfig` as encoding/json writes it.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoQueueConfig {
    rate: i64,
    workers_num: i64,
    backlog: i64,
    max_backlog: i64,
    shutdown_timeout: i64,
}

impl From<&DisconnectQueueConfig> for GoQueueConfig {
    fn from(c: &DisconnectQueueConfig) -> Self {
        GoQueueConfig {
            rate: c.rate,
            workers_num: c.workers_num,
            backlog: c.backlog,
            max_backlog: c.max_backlog,
            shutdown_timeout: c.shutdown_timeout,
        }
    }
}

impl From<GoQueueConfig> for DisconnectQueueConfig {
    fn from(g: GoQueueConfig) -> Self {
        DisconnectQueueConfig {
            rate: g.rate,
            workers_num: g.workers_num,
            backlog: g.backlog,
            max_backlog: g.max_backlog,
            shutdown_timeout: g.shutdown_timeout,
        }
    }
}

#[derive(Deserialize)]
struct GoDecoded<C> {
    config: C,
    error: Option<String>,
}

/// `node.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoConfig>("node.NewConfig", json!({})).into(),
    }
}

/// `config.ToToml()`.
pub fn config_to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("node.Config.ToToml", json!({"Config": GoConfig::from(c)})),
    }
}

/// `toml.Decode(s, &conf)`.
pub fn config_merge_toml(c: &mut Config, toml: &str) -> Result<(), String> {
    match backend() {
        Backend::Rust => c.merge_toml(toml).map_err(|e| e.0),
        Backend::Go => {
            let r: GoDecoded<GoConfig> =
                bridge::call("node.Config.DecodeToml", json!({"Config": GoConfig::from(&*c), "Toml": toml}));
            *c = r.config.into();
            r.error.map_or(Ok(()), Err)
        }
    }
}

/// `node.NewDisconnectQueueConfig()`.
pub fn new_disconnect_queue_config() -> DisconnectQueueConfig {
    match backend() {
        Backend::Rust => DisconnectQueueConfig::new(),
        Backend::Go => bridge::call::<_, GoQueueConfig>("node.NewDisconnectQueueConfig", json!({})).into(),
    }
}

/// `config.ToToml()`.
pub fn disconnect_queue_config_to_toml(c: &DisconnectQueueConfig) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("node.DisconnectQueueConfig.ToToml", json!({"Config": GoQueueConfig::from(c)})),
    }
}

/// `toml.Decode(s, &conf)`.
pub fn disconnect_queue_config_merge_toml(c: &mut DisconnectQueueConfig, toml: &str) -> Result<(), String> {
    match backend() {
        Backend::Rust => c.merge_toml(toml).map_err(|e| e.0),
        Backend::Go => {
            let r: GoDecoded<GoQueueConfig> = bridge::call(
                "node.DisconnectQueueConfig.DecodeToml",
                json!({"Config": GoQueueConfig::from(&*c), "Toml": toml}),
            );
            *c = r.config.into();
            r.error.map_or(Ok(()), Err)
        }
    }
}

// ------------------------------------------------------ subscription state

/// `node.NewSubscriptionState()` under test.
pub enum SubscriptionState {
    Rust(rs::SubscriptionState),
    Go(u64),
}

pub fn new_subscription_state() -> SubscriptionState {
    match backend() {
        Backend::Rust => SubscriptionState::Rust(rs::SubscriptionState::new()),
        Backend::Go => SubscriptionState::Go(bridge::call("node.NewSubscriptionState", json!({}))),
    }
}

#[derive(Deserialize)]
struct GoStateReply {
    #[serde(default)]
    result: Value,
}

impl SubscriptionState {
    fn go(&self, id: u64, method: &str, channel: &str, stream: &str) -> Value {
        bridge::call::<_, GoStateReply>(
            "node.SubscriptionState",
            json!({"id": id, "method": method, "channel": channel, "stream": stream}),
        )
        .result
    }

    fn go_strings(v: Value) -> Option<Vec<String>> {
        (!v.is_null()).then(|| serde_json::from_value(v).expect("string list"))
    }

    pub fn has_channel(&self, id: &str) -> bool {
        match self {
            Self::Rust(s) => s.has_channel(id),
            Self::Go(h) => self.go(*h, "HasChannel", id, "").as_bool().expect("bool"),
        }
    }

    pub fn add_channel(&self, id: &str) {
        match self {
            Self::Rust(s) => s.add_channel(id),
            Self::Go(h) => {
                self.go(*h, "AddChannel", id, "");
            }
        }
    }

    pub fn remove_channel(&self, id: &str) {
        match self {
            Self::Rust(s) => s.remove_channel(id),
            Self::Go(h) => {
                self.go(*h, "RemoveChannel", id, "");
            }
        }
    }

    pub fn channels(&self) -> Vec<String> {
        match self {
            Self::Rust(s) => s.channels(),
            Self::Go(h) => Self::go_strings(self.go(*h, "Channels", "", "")).unwrap_or_default(),
        }
    }

    pub fn add_channel_stream(&self, id: &str, stream: &str) {
        match self {
            Self::Rust(s) => s.add_channel_stream(id, stream),
            Self::Go(h) => {
                self.go(*h, "AddChannelStream", id, stream);
            }
        }
    }

    pub fn remove_channel_stream(&self, id: &str, stream: &str) {
        match self {
            Self::Rust(s) => s.remove_channel_stream(id, stream),
            Self::Go(h) => {
                self.go(*h, "RemoveChannelStream", id, stream);
            }
        }
    }

    pub fn remove_channel_streams(&self, id: &str) -> Option<Vec<String>> {
        match self {
            Self::Rust(s) => s.remove_channel_streams(id),
            Self::Go(h) => Self::go_strings(self.go(*h, "RemoveChannelStreams", id, "")),
        }
    }

    pub fn streams_for(&self, id: &str) -> Option<Vec<String>> {
        match self {
            Self::Rust(s) => s.streams_for(id),
            Self::Go(h) => Self::go_strings(self.go(*h, "StreamsFor", id, "")),
        }
    }
}

// ------------------------------------------------------------- testify mocks

/// What a mocked call returns (testify `Return(...)`), in the bridge JSON shape
/// of the method's Go result.
#[derive(Debug, Clone)]
pub enum Ret {
    /// `Return(nil)` of an error-only method.
    Unit,
    /// An error (with a nil result).
    Err(String),
    /// The Go result: a string, `[]StreamMessage`, a cache entry (as text), a
    /// `*PresenceEvent`, `*ConnectResult` or `*CommandResult`; `null` is nil.
    Json(Value),
    /// `Return(func(name string) string { return name })`.
    EchoFirst,
    /// TestIntegrationRestore's Perform responder: transmissions
    /// `city:<cstate city>` and `locale:<istate locale of the identifier>`.
    EchoStates,
}

/// testify `Run(...)` callbacks the translated tests use.
#[derive(Debug, Clone)]
pub enum Run {
    /// `node.Broadcast(args.Get(0).(*common.StreamMessage))`.
    Broadcast,
    /// `node.HandleJoin(stream, event)`.
    HandleJoin(String, PresenceEvent),
    /// `node.HandleLeave(stream, event)`.
    HandleLeave(String, PresenceEvent),
    /// `t.Error(message)`: the call must not happen.
    Forbidden(String),
    /// Upstream's blocking Disconnect: inflight/peak/calls bookkeeping around a
    /// release barrier.
    InflightGate,
    /// Counts calls.
    Count,
}

/// `mock.On(method, args...).Return(ret).Run(run)`. Controller expectations
/// omit the context argument.
#[derive(Debug, Clone)]
pub struct Expect {
    method: String,
    args: Vec<Arg>,
    ret: Ret,
    run: Option<Run>,
}

impl Expect {
    pub fn new(method: &str, args: Vec<Arg>, ret: Ret) -> Self {
        Self { method: method.to_owned(), args, ret, run: None }
    }

    pub fn run(mut self, run: Run) -> Self {
        self.run = Some(run);
        self
    }

    fn to_go(&self) -> Value {
        let args: Vec<Value> = self
            .args
            .iter()
            .map(|a| match a {
                Arg::Any => json!({"any": true}),
                Arg::Eq(v) => json!({"eq": v}),
            })
            .collect();
        let ret = match &self.ret {
            Ret::Unit => json!({"kind": "unit"}),
            Ret::Err(e) => json!({"kind": "err", "error": e}),
            Ret::Json(v) => json!({"kind": "json", "value": v}),
            Ret::EchoFirst => json!({"kind": "echo_first"}),
            Ret::EchoStates => json!({"kind": "echo_states"}),
        };
        let run = self.run.as_ref().map(|r| match r {
            Run::Broadcast => json!({"kind": "broadcast"}),
            Run::HandleJoin(s, e) => json!({"kind": "handle_join", "stream": s, "event": presence_event_to_go(e)}),
            Run::HandleLeave(s, e) => json!({"kind": "handle_leave", "stream": s, "event": presence_event_to_go(e)}),
            Run::Forbidden(m) => json!({"kind": "forbidden", "message": m}),
            Run::InflightGate => json!({"kind": "inflight_gate"}),
            Run::Count => json!({"kind": "count"}),
        });
        json!({"method": self.method, "args": args, "ret": ret, "run": run})
    }
}

pub fn ret_string(s: &str) -> Ret {
    Ret::Json(json!(s))
}

pub fn ret_messages(m: &[StreamMessage]) -> Ret {
    Ret::Json(Value::Array(m.iter().map(stream_message_to_go).collect()))
}

pub fn ret_nil() -> Ret {
    Ret::Json(Value::Null)
}

pub fn ret_cache(entry: &[u8]) -> Ret {
    Ret::Json(json!(String::from_utf8(entry.to_vec()).expect("a JSON cache entry")))
}

pub fn ret_presence(e: &PresenceEvent) -> Ret {
    Ret::Json(presence_event_to_go(e))
}

pub fn ret_connect(r: &ConnectResult) -> Ret {
    Ret::Json(connect_result_to_go(r))
}

pub fn ret_command(r: &CommandResult) -> Ret {
    Ret::Json(command_result_to_go(r))
}

#[derive(Debug, Default)]
struct GateCounters {
    inflight: i32,
    peak: i32,
    calls: i32,
    arrived: i32,
}

struct MockInner {
    node: Mutex<Option<Arc<Node>>>,
    gate: Watched<GateCounters>,
    release: BlockingGate,
    violations: Mutex<Vec<String>>,
}

impl Default for MockInner {
    fn default() -> Self {
        Self {
            node: Mutex::new(None),
            gate: Watched::new(GateCounters::default()),
            release: BlockingGate::new(),
            violations: Mutex::new(Vec::new()),
        }
    }
}

impl MockInner {
    fn node(&self) -> Arc<Node> {
        self.node.lock().unwrap().clone().expect("the mock is bound to a node")
    }

    fn run(&self, run: &Option<Run>, a: &[Value]) {
        match run {
            None => {}
            Some(Run::Broadcast) => self.node().broadcast(&stream_message_arg(&a[0])),
            Some(Run::HandleJoin(s, e)) => self.node().handle_join(s, e),
            Some(Run::HandleLeave(s, e)) => self.node().handle_leave(s, e),
            Some(Run::Forbidden(m)) => self.violations.lock().unwrap().push(m.clone()),
            Some(Run::InflightGate) => {
                self.gate.update(|g| {
                    g.arrived += 1;
                    g.inflight += 1;
                    if g.inflight > g.peak {
                        g.peak = g.inflight;
                    }
                });
                self.release.pass();
                self.gate.update(|g| {
                    g.inflight -= 1;
                    g.calls += 1;
                });
            }
            Some(Run::Count) => self.gate.update(|g| g.calls += 1),
        }
    }
}

fn broker_err(r: &Ret) -> Result<(), BrokerError> {
    match r {
        Ret::Err(e) => Err(BrokerError { message: e.clone() }),
        _ => Ok(()),
    }
}

fn controller_err(r: &Ret) -> Option<ControllerError> {
    match r {
        Ret::Err(e) => Some(ControllerError::new(e.clone())),
        _ => None,
    }
}

fn json_of(r: &Ret) -> Value {
    match r {
        Ret::Json(v) => v.clone(),
        _ => Value::Null,
    }
}

fn messages_of(v: &Value) -> Vec<StreamMessage> {
    v.as_array().map(|a| a.iter().map(stream_message_arg).collect()).unwrap_or_default()
}

enum MockImp {
    Rust { broker: Option<Arc<MockBroker>>, controller: Option<Arc<MockNodeController>>, inner: Arc<MockInner> },
    Go(u64),
}

/// A testify mock (`&mocks.Controller{}` or `&mocks.Broker{}`) programmed with
/// [`Expect`]ations. Under Go the bridge builds the same testify mock and runs
/// the same canned callbacks.
pub struct TestMock {
    imp: MockImp,
}

/// `&mocks.Controller{}`.
pub fn new_mock_controller() -> TestMock {
    new_test_mock(false)
}

/// `&mocks.Broker{}`.
pub fn new_mock_broker() -> TestMock {
    new_test_mock(true)
}

fn new_test_mock(broker: bool) -> TestMock {
    match backend() {
        Backend::Rust => TestMock {
            imp: MockImp::Rust {
                broker: broker.then(MockBroker::new),
                controller: (!broker).then(MockNodeController::new),
                inner: Arc::new(MockInner::default()),
            },
        },
        Backend::Go => TestMock {
            imp: MockImp::Go(bridge::call(
                "node.NewMock",
                json!({"node": 0, "kind": if broker { "broker" } else { "controller" }}),
            )),
        },
    }
}

#[derive(Deserialize)]
struct GoGate {
    inflight: i32,
    peak: i32,
    calls: i32,
    arrived: i32,
}

impl TestMock {
    fn go(&self, method: &str, args: Value) -> Value {
        let MockImp::Go(id) = &self.imp else { unreachable!() };
        let mut a = args;
        a["id"] = json!(id);
        a["method"] = json!(method);
        bridge::call("node.Mock", a)
    }

    fn go_handle(&self) -> u64 {
        match &self.imp {
            MockImp::Go(id) => *id,
            MockImp::Rust { .. } => unreachable!("a Rust mock passed to the Go bridge"),
        }
    }

    fn bind(&self, node: &Arc<Node>) {
        if let MockImp::Rust { inner, .. } = &self.imp {
            *inner.node.lock().unwrap() = Some(node.clone());
        }
    }

    fn rust_controller(&self) -> Arc<dyn Controller> {
        match &self.imp {
            MockImp::Rust { controller: Some(c), .. } => c.clone(),
            _ => panic!("not a Rust controller mock"),
        }
    }

    fn rust_broker(&self) -> Arc<dyn Broker> {
        match &self.imp {
            MockImp::Rust { broker: Some(b), .. } => b.clone(),
            _ => panic!("not a Rust broker mock"),
        }
    }

    /// `mock.On(...).Return(...).Run(...)`.
    pub fn on(&self, e: Expect) {
        match &self.imp {
            MockImp::Go(_) => {
                self.go("On", json!({"expect": e.to_go()}));
            }
            MockImp::Rust { broker: Some(b), inner, .. } => Self::on_broker(&b.mock, inner.clone(), e),
            MockImp::Rust { controller: Some(c), inner, .. } => Self::on_controller(&c.mock, inner.clone(), e),
            MockImp::Rust { .. } => unreachable!(),
        }
    }

    fn reg<R: std::any::Any + Send>(
        mock: &Mock,
        inner: Arc<MockInner>,
        e: Expect,
        conv: impl Fn(&Ret, &[Value]) -> R + Send + Sync + 'static,
    ) {
        let (ret, run) = (e.ret.clone(), e.run.clone());
        mock.on(&e.method, e.args.clone(), move |a| {
            inner.run(&run, a);
            conv(&ret, a)
        });
    }

    fn on_broker(mock: &Mock, inner: Arc<MockInner>, e: Expect) {
        match e.method.as_str() {
            "Subscribe" | "Unsubscribe" => Self::reg(mock, inner, e, |r, a| match r {
                Ret::EchoFirst => a[0].as_str().expect("a stream").to_owned(),
                r => json_of(r).as_str().expect("a string result").to_owned(),
            }),
            "HistorySince" => Self::reg(mock, inner, e, |r, _| {
                broker_err(r).map(|()| Some(json_of(r)).filter(|v| !v.is_null()).map(|v| messages_of(&v)))
            }),
            "HistoryFrom" => Self::reg(mock, inner, e, |r, _| broker_err(r).map(|()| messages_of(&json_of(r)))),
            "RestoreSession" => Self::reg(mock, inner, e, |r, _| {
                broker_err(r).map(|()| json_of(r).as_str().map(|s| s.as_bytes().to_vec()))
            }),
            "PresenceAdd" | "PresenceRemove" => Self::reg(mock, inner, e, |r, _| {
                broker_err(r).map(|()| Some(json_of(r)).filter(|v| !v.is_null()).map(|v| presence_event_from_go(&v)))
            }),
            "CommitSession" | "TouchPresence" | "TouchSession" | "HandleBroadcast" => {
                Self::reg(mock, inner, e, |r, _| broker_err(r))
            }
            other => panic!("unsupported broker method {other}"),
        }
    }

    fn on_controller(mock: &Mock, inner: Arc<MockInner>, e: Expect) {
        match e.method.as_str() {
            "Start" | "Shutdown" | "Disconnect" => {
                Self::reg(mock, inner, e, |r, _| controller_err(r).map_or(Ok(()), Err))
            }
            "Authenticate" => Self::reg(mock, inner, e, |r, _| Outcome {
                result: Some(json_of(r)).filter(|v| !v.is_null()).map(|v| connect_result_from_go(&v)),
                error: controller_err(r),
            }),
            "Subscribe" | "Unsubscribe" | "Perform" => Self::reg(mock, inner, e, |r, a| match r {
                Ret::EchoStates => Outcome {
                    result: Some(CommandResult {
                        status: cabby_core::common::Status::Success,
                        transmissions: vec![
                            format!("city:{}", env_arg_connection_state(&a[1], "city")),
                            format!(
                                "locale:{}",
                                env_arg_channel_state(&a[1], a[3].as_str().unwrap_or_default(), "locale")
                            ),
                        ],
                        ..Default::default()
                    }),
                    error: None,
                },
                r => Outcome {
                    result: Some(json_of(r)).filter(|v| !v.is_null()).map(|v| command_result_from_go(&v)),
                    error: controller_err(r),
                },
            }),
            other => panic!("unsupported controller method {other}"),
        }
    }

    /// Every call the mock received (controller calls without the context).
    pub fn calls(&self) -> Vec<Call> {
        match &self.imp {
            MockImp::Rust { broker: Some(b), .. } => b.mock.calls(),
            MockImp::Rust { controller: Some(c), .. } => c.mock.calls(),
            MockImp::Rust { .. } => unreachable!(),
            MockImp::Go(_) => {
                #[derive(Deserialize)]
                struct C {
                    method: String,
                    args: Vec<Value>,
                }
                let calls: Vec<C> = serde_json::from_value(self.go("Calls", json!({}))).expect("calls");
                calls.into_iter().map(|c| Call { method: c.method, args: c.args }).collect()
            }
        }
    }

    /// testify `AssertCalled`.
    pub fn assert_called(&self, method: &str, args: &[Value]) {
        let calls = self.calls();
        assert!(
            calls.iter().any(|c| c.method == method && c.args == args),
            "mock: expected call {method}({args:?}) was not made; calls: {calls:?}"
        );
    }

    /// testify `AssertNumberOfCalls`.
    pub fn assert_number_of_calls(&self, method: &str, n: usize) {
        let got = self.calls().iter().filter(|c| c.method == method).count();
        assert_eq!(n, got, "mock: expected {n} call(s) of {method}, got {got}");
    }

    /// No [`Run::Forbidden`] expectation was called (upstream `t.Error`).
    pub fn assert_no_forbidden_calls(&self) {
        let v: Vec<String> = match &self.imp {
            MockImp::Rust { inner, .. } => inner.violations.lock().unwrap().clone(),
            MockImp::Go(_) => serde_json::from_value(self.go("Violations", json!({}))).expect("violations"),
        };
        assert!(v.is_empty(), "{v:?}");
    }

    fn gate(&self) -> GoGate {
        match &self.imp {
            MockImp::Rust { inner, .. } => {
                inner.gate.read(|g| GoGate { inflight: g.inflight, peak: g.peak, calls: g.calls, arrived: g.arrived })
            }
            MockImp::Go(_) => serde_json::from_value(self.go("Gate", json!({}))).expect("gate"),
        }
    }

    /// [`Run::InflightGate`] calls currently held.
    pub fn inflight(&self) -> i32 {
        self.gate().inflight
    }

    pub fn peak(&self) -> i32 {
        self.gate().peak
    }

    /// Completed [`Run::InflightGate`] / [`Run::Count`] calls.
    pub fn completed_calls(&self) -> i32 {
        self.gate().calls
    }

    /// Calls that reached the [`Run::InflightGate`] barrier.
    pub fn arrived(&self) -> i32 {
        self.gate().arrived
    }

    /// `close(release)`.
    pub fn release(&self) {
        match &self.imp {
            MockImp::Rust { inner, .. } => inner.release.open(),
            MockImp::Go(_) => {
                self.go("Release", json!({}));
            }
        }
    }

    fn await_counter(&self, name: &str, n: i32, deadline: Duration) {
        let ok = match &self.imp {
            MockImp::Rust { inner, .. } => inner.gate.wait_until(deadline, |g| match name {
                "inflight" => g.inflight >= n,
                _ => g.calls >= n,
            }),
            MockImp::Go(_) => self
                .go("AwaitGate", json!({"name": name, "n": n, "timeout_ms": deadline.as_millis() as u64}))
                .as_bool()
                .expect("bool"),
        };
        assert!(ok, "{name} >= {n} expected within {deadline:?}");
    }

    /// Upstream `require.Eventually(inflight == n, deadline, 1ms)`. Rust waits
    /// on the counter itself; Go probes the bridge's atomics (no notification).
    pub fn await_inflight(&self, n: i32, deadline: Duration) {
        self.await_counter("inflight", n, deadline);
    }

    /// Upstream `require.Eventually(calls == n, deadline, 1ms)`.
    pub fn await_completed_calls(&self, n: i32, deadline: Duration) {
        self.await_counter("calls", n, deadline);
    }
}

// -------------------------------------------------------------------- node

/// What a Rust test node holds besides the node itself (upstream test helpers
/// keep these as locals or reach them through unexported fields).
pub struct RustNode {
    pub node: Arc<Node>,
    pub timers: ManualTimers,
    /// Whether `timers` drives the node (false for system-clock nodes).
    pub manual: bool,
    /// The registry passed with `WithInstrumenter`.
    pub metrics: Option<Arc<Metrics>>,
    /// The disconnect queue `NewMockNode` installs.
    pub queue: Option<Arc<rs::DisconnectQueue>>,
}

enum NodeImp {
    Rust(RustNode),
    Go { handle: u64, queue: Option<u64> },
}

/// A node under test.
pub struct TestNode {
    imp: NodeImp,
}

/// A session's identity (upstream compares `*Session` pointers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionId(u64);

/// Upstream `NewMockNode()` (node/node_mocks_test.go): `mocks.NewMockController()`,
/// `HubGopoolSize = 2`, `metrics.NewMetrics(nil, 10)`, the legacy broker over a
/// legacy subscriber, and a disconnect queue with `Rate = 1`.
pub fn new_mock_node() -> TestNode {
    match backend() {
        Backend::Rust => {
            let timers = ManualTimers::new();
            let mut config = Config::new();
            config.hub_gopool_size = 2;
            let metrics = Arc::new(Metrics::new(Vec::new(), 10));
            let node = Node::new(
                config,
                vec![
                    NodeOption::Instrumenter(metrics.clone() as Arc<dyn Instrumenter>),
                    NodeOption::Controller(MockController::new()),
                    NodeOption::Clock(Arc::new(timers.clone())),
                ],
            );
            let subscriber = Arc::new(LegacySubscriber::new(node.clone()));
            node.set_broker(Arc::new(cabby_core::broker::Legacy::new(subscriber)));
            let mut dconfig = DisconnectQueueConfig::new();
            dconfig.rate = 1;
            let queue = Arc::new(rs::DisconnectQueue::new(node.clone(), &dconfig));
            node.set_disconnector(queue.clone());
            TestNode {
                imp: NodeImp::Rust(RustNode { node, timers, manual: true, metrics: Some(metrics), queue: Some(queue) }),
            }
        }
        Backend::Go => {
            #[derive(Deserialize)]
            struct R {
                node: u64,
                queue: u64,
            }
            let r: R = bridge::call("node.NewMockNode", json!({}));
            TestNode { imp: NodeImp::Go { handle: r.node, queue: Some(r.queue) } }
        }
    }
}

/// `NewNode(&config, WithController(controller), WithInstrumenter(metrics.NewMetrics(nil, 10)))`
/// with a testify controller (`&mocks.Controller{}`), on a controlled clock.
pub fn new_node_with_controller(config: Config, controller: &TestMock) -> TestNode {
    let timers = ManualTimers::new();
    node_with_controller(config, controller, Arc::new(timers.clone()), timers, true)
}

/// [`new_node_with_controller`] on the operating system clock, for cases whose
/// timing is enforced by an external service (NATS TTLs run on the server's
/// clock).
pub fn new_node_with_controller_on_system_clock(config: Config, controller: &TestMock) -> TestNode {
    node_with_controller(config, controller, Arc::new(rs::SystemClock), ManualTimers::new(), false)
}

fn node_with_controller(
    config: Config,
    controller: &TestMock,
    clock: Arc<dyn rs::Clock>,
    timers: ManualTimers,
    manual: bool,
) -> TestNode {
    match backend() {
        Backend::Rust => {
            let metrics = Arc::new(Metrics::new(Vec::new(), 10));
            let node = Node::new(
                config,
                vec![
                    NodeOption::Controller(controller.rust_controller()),
                    NodeOption::Instrumenter(metrics.clone() as Arc<dyn Instrumenter>),
                    NodeOption::Clock(clock),
                ],
            );
            controller.bind(&node);
            TestNode { imp: NodeImp::Rust(RustNode { node, timers, manual, metrics: Some(metrics), queue: None }) }
        }
        Backend::Go => {
            let handle: u64 = bridge::call(
                "node.NewNode",
                json!({"config": GoConfig::from(&config), "controller": controller.go_handle()}),
            );
            TestNode { imp: NodeImp::Go { handle, queue: None } }
        }
    }
}

fn message_to_go(m: &Message) -> Value {
    let mut v = json!({"command": m.command, "identifier": m.identifier});
    match &m.data {
        None => {}
        Some(MessageData::Json(d)) => v["data"] = d.clone(),
        Some(MessageData::Typed(_)) => panic!("typed message data cannot cross the bridge"),
    }
    let mut history = json!({});
    if m.history.since != 0 {
        history["since"] = json!(m.history.since);
    }
    // Go distinguishes a nil Streams map; Cabby's empty map stands for nil.
    if !m.history.streams.is_empty() {
        history["streams"] = Value::Object(
            m.history.streams.iter().map(|(k, p)| (k.clone(), json!({"epoch": p.epoch, "offset": p.offset}))).collect(),
        );
    }
    v["history"] = history;
    if let Some(p) = &m.presence {
        v["presence"] = presence_event_to_go(p);
    }
    v
}

#[derive(Deserialize)]
struct GoCommand {
    #[serde(default)]
    result: Value,
    error: Option<String>,
}

impl GoCommand {
    fn into_result(self) -> Result<Option<CommandResult>, NodeError> {
        match self.error {
            Some(e) => Err(NodeError(e)),
            None => Ok((!self.result.is_null()).then(|| command_result_from_go(&self.result))),
        }
    }
}

/// Time as a disconnect queue test sees it: a controlled clock on Rust, wall
/// clock on Go.
pub enum Pace<'a> {
    Manual(&'a ManualTimers),
    Wall(std::time::Instant),
}

impl Pace<'_> {
    /// Blocks until `n` workers wait for a throttle tick (Rust); nothing on Go.
    pub fn workers_waiting(&self, n: usize) {
        if let Pace::Manual(t) = self {
            t.await_ticker_waiters(n, DEADLINE);
        }
    }

    /// One throttle period passes (Rust); the Go ticker runs on its own.
    pub fn tick(&self, period: Duration) {
        if let Pace::Manual(t) = self {
            t.advance(period);
        }
    }

    /// `d` passes: period by period on Rust, a real wait on Go (elapsed time
    /// is the behavior under test there, as in upstream's `time.Sleep`).
    pub fn elapse(&self, d: Duration, period: Duration) {
        match self {
            Pace::Manual(t) => {
                let mut left = d;
                while left >= period {
                    t.advance(period);
                    left -= period;
                }
            }
            Pace::Wall(_) => std::thread::sleep(d),
        }
    }

    /// Time since the pace was taken (virtual on Rust).
    pub fn now(&self) -> Duration {
        match self {
            Pace::Manual(t) => t.elapsed(),
            Pace::Wall(start) => start.elapsed(),
        }
    }
}

impl TestNode {
    /// The Rust node and its helpers; not observable on Go (`reason`).
    pub fn rust_parts(&self, reason: &str) -> &RustNode {
        match &self.imp {
            NodeImp::Rust(r) => r,
            NodeImp::Go { .. } => not_observable(reason),
        }
    }

    /// The Rust node, for operations without an exported Go path.
    pub fn rust_with(&self, reason: &str) -> &Arc<Node> {
        &self.rust_parts(reason).node
    }

    /// The controlled clock of a Rust node.
    pub fn timers(&self) -> &ManualTimers {
        &self.rust_parts("the Go node runs on wall-clock time").timers
    }

    /// The controlled clock when it drives the node.
    pub fn manual_timers(&self) -> Option<&ManualTimers> {
        match &self.imp {
            NodeImp::Rust(r) if r.manual => Some(&r.timers),
            _ => None,
        }
    }

    pub fn pace(&self) -> Pace<'_> {
        match self.manual_timers() {
            Some(t) => Pace::Manual(t),
            None => Pace::Wall(std::time::Instant::now()),
        }
    }

    fn go_node(&self, method: &str, args: Value) -> Value {
        let NodeImp::Go { handle, .. } = &self.imp else { unreachable!() };
        let mut a = args;
        a["id"] = json!(handle);
        a["method"] = json!(method);
        bridge::call("node.Node", a)
    }

    fn go_err(&self, method: &str, args: Value) -> Result<(), NodeError> {
        serde_json::from_value::<GoErr>(self.go_node(method, args)).expect("error reply").into_result()
    }

    fn go_command(&self, method: &str, s: &TestSession, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        let v = self.go_node(method, json!({"session": s.go_handle(), "msg": message_to_go(msg)}));
        serde_json::from_value::<GoCommand>(v).expect("command reply").into_result()
    }

    fn go_session_err(&self, method: &str, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        self.go_err(method, json!({"session": s.go_handle(), "msg": message_to_go(msg)}))
    }

    /// Runs the hub in the background until the guard drops (upstream
    /// `go node.hub.Run(); defer node.hub.Shutdown()`). The Go arm calls the
    /// exported `node.Start()`, which runs the hub (and the stats collector).
    pub fn run_hub(&self) -> HubGuard<'_> {
        match &self.imp {
            NodeImp::Rust(r) => {
                let node = r.node.clone();
                std::thread::spawn(move || node.hub().run());
            }
            NodeImp::Go { .. } => {
                self.go_err("Start", json!({})).expect("start");
            }
        }
        HubGuard { node: self }
    }

    /// `node.Start()`.
    pub fn start(&self) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.start(),
            NodeImp::Go { .. } => self.go_err("Start", json!({})),
        }
    }

    /// `node.Shutdown(context.Background())`.
    pub fn shutdown(&self) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.shutdown(&background()),
            NodeImp::Go { .. } => self.go_err("Shutdown", json!({})),
        }
    }

    /// `node.Size()` (the hub size; upstream tests read `node.hub.Size()`).
    pub fn size(&self) -> usize {
        match &self.imp {
            NodeImp::Rust(r) => r.node.size(),
            NodeImp::Go { .. } => self.go_node("Size", json!({})).as_u64().expect("size") as usize,
        }
    }

    /// `node.Authenticate(session)`.
    pub fn authenticate(&self, s: &TestSession) -> Result<Option<ConnectResult>, NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.authenticate(s.rust_session(), &[]),
            NodeImp::Go { .. } => {
                let v = self.go_node("Authenticate", json!({"session": s.go_handle()}));
                match v["error"].as_str() {
                    Some(e) => Err(NodeError(e.to_owned())),
                    None => Ok((!v["result"].is_null()).then(|| connect_result_from_go(&v["result"]))),
                }
            }
        }
    }

    /// `node.Authenticated(session, ids)`.
    pub fn authenticated(&self, s: &TestSession, ids: &str) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.authenticated(s.rust_session(), ids),
            NodeImp::Go { .. } => {
                self.go_node("Authenticated", json!({"session": s.go_handle(), "ids": ids}));
            }
        }
    }

    pub fn subscribe(&self, s: &TestSession, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.subscribe(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_command("Subscribe", s, msg),
        }
    }

    pub fn unsubscribe(&self, s: &TestSession, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.unsubscribe(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_command("Unsubscribe", s, msg),
        }
    }

    pub fn perform(&self, s: &TestSession, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.perform(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_command("Perform", s, msg),
        }
    }

    pub fn history(&self, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.history(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_session_err("History", s, msg),
        }
    }

    pub fn whisper(&self, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.whisper(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_session_err("Whisper", s, msg),
        }
    }

    pub fn presence(&self, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.presence(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_session_err("Presence", s, msg),
        }
    }

    pub fn presence_join(&self, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.presence_join(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_session_err("PresenceJoin", s, msg),
        }
    }

    pub fn presence_leave(&self, s: &TestSession, msg: &Message) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.presence_leave(s.rust_session(), msg),
            NodeImp::Go { .. } => self.go_session_err("PresenceLeave", s, msg),
        }
    }

    /// `node.HandleBroadcast(raw)`.
    pub fn handle_broadcast(&self, raw: &[u8]) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.handle_broadcast(raw),
            NodeImp::Go { .. } => {
                self.go_err("HandleBroadcast", json!({"data": String::from_utf8(raw.to_vec()).expect("UTF-8")}))
            }
        }
    }

    /// `node.HandlePubSub(raw)`.
    pub fn handle_pub_sub(&self, raw: &[u8]) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.handle_pub_sub(raw),
            NodeImp::Go { .. } => {
                self.go_node("HandlePubSub", json!({"data": String::from_utf8(raw.to_vec()).expect("UTF-8")}));
            }
        }
    }

    /// `node.hub.BroadcastMessage(msg)`. The Go arm calls the exported
    /// `node.Broadcast(msg)`, which is exactly that (plus a metrics counter).
    pub fn hub_broadcast_message(&self, msg: &StreamMessage) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.hub().broadcast_message(msg.clone()),
            NodeImp::Go { .. } => {
                self.go_node("Broadcast", json!({"msg": stream_message_to_go(msg)}));
            }
        }
    }

    /// `node.hub.Broadcast(stream, data)`: upstream builds
    /// `&StreamMessage{Stream, Data}`; the Go arm passes it to `node.Broadcast`.
    pub fn hub_broadcast(&self, stream: &str, data: &str) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.hub().broadcast(stream, data),
            NodeImp::Go { .. } => {
                self.go_node("Broadcast", json!({"msg": {"stream": stream, "data": data}}));
            }
        }
    }

    /// `node.hub.AddSession(session)`, also marking the session connected on
    /// both backends. Go has no exported hub-only call: it uses
    /// `node.Authenticated(s, s.GetIdentifiers())` (identifiers unchanged),
    /// which does both; the Rust arm does the same so both backends exercise
    /// the same (connected) disconnect path.
    pub fn hub_add_session(&self, s: &TestSession) {
        match &self.imp {
            NodeImp::Rust(r) => {
                r.node.hub().add_session(s.rust_session().clone());
                s.rust_session().set_connected(true);
            }
            NodeImp::Go { .. } => self.authenticated(s, &s.get_identifiers()),
        }
    }

    /// `node.hub.RemoveSession(session)` (cleanup only; no-op on Go, where
    /// bridge objects live until the bridge exits).
    pub fn hub_remove_session(&self, s: &TestSession) {
        if let NodeImp::Rust(r) = &self.imp {
            r.node.hub().remove_session(s.rust_session());
        }
    }

    /// `node.hub.SubscribeSession(session, stream, identifier)`. No exported Go
    /// call does only this: the Go arm runs the node's own command flow
    /// (`Subscribe`, or `Perform` when the session already has the channel)
    /// with a controller answering `Streams: [stream]`, no transmissions and no
    /// Disconnect interest. Besides the hub subscription this calls the broker's
    /// `Subscribe(stream)` and records the channel/stream in the session's
    /// subscriptions.
    pub fn hub_subscribe_session(&self, s: &TestSession, stream: &str, identifier: &str) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.hub().subscribe_session(s.rust_session(), stream, identifier),
            NodeImp::Go { .. } => self
                .go_err("HubSubscribe", json!({"session": s.go_handle(), "stream": stream, "identifier": identifier}))
                .expect("hub subscription"),
        }
    }

    /// `node.hub.FindByIdentifier(ids)`; Go: the exported `node.LookupSession`,
    /// which returns exactly that.
    pub fn hub_find_by_identifier(&self, ids: &str) -> Option<SessionId> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.hub().find_by_identifier(ids).map(|s| SessionId(Arc::as_ptr(&s) as u64)),
            NodeImp::Go { .. } => self.lookup_session(ids),
        }
    }

    /// `node.LookupSession(ids)`.
    pub fn lookup_session(&self, ids: &str) -> Option<SessionId> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.lookup_session(ids).map(|s| SessionId(Arc::as_ptr(&s) as u64)),
            NodeImp::Go { .. } => match self.go_node("LookupSession", json!({"ids": ids})) {
                Value::Null => None,
                Value::Number(n) => Some(SessionId(n.as_u64().expect("a handle"))),
                other => panic!("LookupSession returned a session the bridge does not know: {other}"),
            },
        }
    }

    /// `node.broker.Subscribe(stream)` on the broker the node was built with.
    pub fn broker_subscribe(&self, stream: &str) -> String {
        match &self.imp {
            NodeImp::Rust(r) => r.node.broker().subscribe(stream),
            NodeImp::Go { .. } => self.go_node("BrokerSubscribe", json!({"stream": stream})).as_str().unwrap().into(),
        }
    }

    /// `node.Disconnect(session)`.
    pub fn disconnect(&self, s: &TestSession) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.disconnect(s.rust_session()),
            NodeImp::Go { .. } => self.go_err("Disconnect", json!({"session": s.go_handle()})),
        }
    }

    /// `node.SetBroker(mock)`.
    pub fn set_mock_broker(&self, broker: &TestMock) {
        match &self.imp {
            NodeImp::Rust(r) => {
                broker.bind(&r.node);
                r.node.set_broker(broker.rust_broker());
            }
            NodeImp::Go { .. } => {
                self.go_node("SetBroker", json!({"broker": broker.go_handle()}));
            }
        }
    }

    /// `node.SetBroker(broker)`.
    pub fn set_broker(&self, broker: &TestBroker) {
        match (&self.imp, &broker.imp) {
            (NodeImp::Rust(r), BrokerImp::Rust { broker, .. }) => r.node.set_broker(broker.clone()),
            (NodeImp::Go { .. }, BrokerImp::Go(h)) => {
                self.go_node("SetBroker", json!({"broker": h}));
            }
            _ => unreachable!("node and broker of different backends"),
        }
    }

    /// `node.controller = controller` (upstream assigns the unexported field;
    /// the Go arm applies the exported option `node.WithController(c)(n)`).
    pub fn set_controller(&self, controller: &TestMock) {
        match &self.imp {
            NodeImp::Rust(r) => {
                controller.bind(&r.node);
                r.node.set_controller(controller.rust_controller());
            }
            NodeImp::Go { .. } => {
                self.go_node("SetController", json!({"controller": controller.go_handle()}));
            }
        }
    }

    /// `node.SetDisconnector(NewNoopDisconnector())`.
    pub fn set_noop_disconnector(&self) {
        match &self.imp {
            NodeImp::Rust(r) => r.node.set_disconnector(Arc::new(rs::NoopDisconnectQueue)),
            NodeImp::Go { .. } => {
                self.go_node("SetDisconnector", json!({"kind": "noop"}));
            }
        }
    }

    /// `node.SetDisconnector(&blockingDisconnector{...})` (node_test.go).
    pub fn set_blocking_disconnector(&self) -> TestBlockingDisconnector {
        match &self.imp {
            NodeImp::Rust(r) => {
                let d = Arc::new(BlockingDisconnector::default());
                r.node.set_disconnector(d.clone());
                TestBlockingDisconnector::Rust(d)
            }
            NodeImp::Go { .. } => TestBlockingDisconnector::Go(
                self.go_node("SetDisconnector", json!({"kind": "blocking"})).as_u64().expect("handle"),
            ),
        }
    }

    /// `node.metrics.(*metrics.Metrics).Counter(name).Value()`.
    pub fn counter_value(&self, name: &str) -> u64 {
        match &self.imp {
            NodeImp::Rust(r) => r
                .metrics
                .as_ref()
                .expect("node built with metrics")
                .counter(name)
                .unwrap_or_else(|| panic!("counter {name} is not registered"))
                .value(),
            NodeImp::Go { .. } => self.go_node("Counter", json!({"name": name})).as_u64().expect("counter value"),
        }
    }

    /// The disconnect queue `NewMockNode` installed (upstream `node.disconnector`).
    pub fn mock_queue(&self) -> TestQueue<'_> {
        match &self.imp {
            NodeImp::Rust(r) => {
                TestQueue { node: self, imp: QueueImp::Rust(r.queue.clone().expect("a NewMockNode node")) }
            }
            NodeImp::Go { queue, .. } => {
                TestQueue { node: self, imp: QueueImp::Go(queue.expect("a NewMockNode node")) }
            }
        }
    }
}

/// Stops the hub when dropped (upstream `defer node.hub.Shutdown()`).
pub struct HubGuard<'a> {
    node: &'a TestNode,
}

impl Drop for HubGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        if let NodeImp::Rust(r) = &self.node.imp {
            r.node.hub().shutdown();
        }
        // Go: the hub is part of the bridge-held node; the bridge process ends with the test binary.
    }
}

// ------------------------------------------------- blocking disconnector

/// Upstream `blockingDisconnector` (node_test.go): a `NoopDisconnectQueue`
/// whose `Enqueue` signals `enqueued` and blocks until `release`.
///
/// Upstream closes `enqueued` on every call, so a second Enqueue panics; here
/// calls are counted and the test asserts exactly one.
pub struct BlockingDisconnector {
    noop: rs::NoopDisconnectQueue,
    enqueued: Watched<usize>,
    release: BlockingGate,
}

impl Default for BlockingDisconnector {
    fn default() -> Self {
        Self { noop: rs::NoopDisconnectQueue, enqueued: Watched::new(0), release: BlockingGate::new() }
    }
}

impl Disconnector for BlockingDisconnector {
    fn run(&self) -> Result<(), NodeError> {
        self.noop.run()
    }
    fn shutdown(&self, ctx: &Context) -> Result<(), NodeError> {
        self.noop.shutdown(ctx)
    }
    fn enqueue(&self, _session: Arc<Session>) -> Result<(), NodeError> {
        self.enqueued.update(|e| *e += 1);
        self.release.pass();
        Ok(())
    }
    fn size(&self) -> usize {
        self.noop.size()
    }
}

pub enum TestBlockingDisconnector {
    Rust(Arc<BlockingDisconnector>),
    Go(u64),
}

impl TestBlockingDisconnector {
    /// `select { case <-d.enqueued: ...; case <-time.After(deadline): ... }`.
    pub fn await_enqueued(&self, deadline: Duration) -> bool {
        match self {
            Self::Rust(d) => d.enqueued.wait_until(deadline, |e| *e > 0),
            Self::Go(h) => bridge::call::<_, Value>(
                "node.BlockingDisconnector",
                json!({"id": h, "method": "AwaitEnqueued", "timeout_ms": deadline.as_millis() as u64}),
            )
            .as_bool()
            .expect("bool"),
        }
    }

    /// Enqueue calls so far.
    pub fn enqueues(&self) -> usize {
        match self {
            Self::Rust(d) => d.enqueued.read(|e| *e),
            Self::Go(h) => {
                bridge::call::<_, Value>("node.BlockingDisconnector", json!({"id": h, "method": "Enqueues"}))
                    .as_u64()
                    .expect("count") as usize
            }
        }
    }

    /// `close(d.release)`.
    pub fn release(&self) {
        match self {
            Self::Rust(d) => d.release.open(),
            Self::Go(h) => {
                bridge::call::<_, Value>("node.BlockingDisconnector", json!({"id": h, "method": "Release"}));
            }
        }
    }
}

// ----------------------------------------------------------------- session

/// Session construction options the translated helpers accept.
#[derive(Debug, Clone)]
pub enum SessionOpt {
    /// `WithKeepaliveIntervals(sessionsTTL, presenceTTL)`.
    KeepaliveIntervals(Duration, Duration),
    /// `WithPrevSID(sid)`.
    PrevSid(String),
    /// `WithMaxPendingSize(n)`; also used where upstream stores
    /// `session.maxPendingSize` right after creating the session.
    MaxPendingSize(u64),
    /// The connection's writes block until [`TestSession::release_writes`]
    /// (replaces upstream tests locking the unexported `session.wmu`).
    GatedWrites,
}

enum SessionImp {
    Rust { session: Arc<Session>, conn: Arc<MockConnection>, write_gate: Option<BlockingGate> },
    Go { handle: u64 },
}

/// A session under test with its mock connection.
pub struct TestSession {
    imp: SessionImp,
}

fn rust_session_options(opts: &[SessionOpt]) -> Vec<SessionOption> {
    opts.iter()
        .filter_map(|o| match o {
            SessionOpt::KeepaliveIntervals(a, b) => {
                Some(SessionOption::KeepaliveIntervals { sessions_ttl: *a, presence_ttl: *b })
            }
            SessionOpt::PrevSid(s) => Some(SessionOption::PrevSid(s.clone())),
            SessionOpt::MaxPendingSize(n) => Some(SessionOption::MaxPendingSize(*n)),
            SessionOpt::GatedWrites => None,
        })
        .collect()
}

fn gate_of(opts: &[SessionOpt]) -> Option<BlockingGate> {
    opts.iter().any(|o| matches!(o, SessionOpt::GatedWrites)).then(BlockingGate::new)
}

/// Upstream `NewMockSession(uid, node, opts...)` (node/node_mocks_test.go): a built
/// session over a mock connection, bound to the node as executor and broker
/// user, identifiers equal to `uid`, writer running, timers *not* started.
///
/// The Go arm repeats the helper with exported API: `BuildSession`,
/// `WithExecutor(node)`, `SetID`, `Log`, `SetIdentifiers`, the options and
/// `go SendMessages()`. It cannot bind the unexported `session.broker`, which
/// only the (never started) timers use.
pub fn new_mock_session(uid: &str, node: &TestNode, opts: Vec<SessionOpt>) -> TestSession {
    session_of(uid, node, None, opts, "mock")
}

/// Upstream `NewMockSessionWithEnv(uid, node, url, headers, opts...)`.
pub fn new_mock_session_with_env(
    uid: &str,
    node: &TestNode,
    url: &str,
    headers: Option<StringMap>,
    opts: Vec<SessionOpt>,
) -> TestSession {
    session_of(uid, node, Some((url, headers)), opts, "mock")
}

/// A mock session whose keepalive timers run (upstream: `NewMockSessionWithEnv`
/// with pings disabled, then the unexported `startTimers()`). The Go arm uses
/// the exported `node.NewSession(..., WithPingInterval(0), opts...)`, which
/// starts the timers itself (at creation rather than at the upstream call).
pub fn new_session_with_timers(
    uid: &str,
    node: &TestNode,
    url: &str,
    headers: Option<StringMap>,
    opts: Vec<SessionOpt>,
) -> TestSession {
    let s = session_of(uid, node, Some((url, headers)), opts, "timers");
    if let SessionImp::Rust { session, .. } = &s.imp {
        // do not send pings
        session.set_ping_interval(Duration::ZERO);
        session.start_timers();
    }
    s
}

fn session_of(
    uid: &str,
    node: &TestNode,
    env: Option<(&str, Option<StringMap>)>,
    opts: Vec<SessionOpt>,
    mode: &str,
) -> TestSession {
    match &node.imp {
        NodeImp::Rust(r) => {
            let write_gate = gate_of(&opts);
            let conn = match &write_gate {
                Some(g) => MockConnection::gated(g.clone()),
                None => MockConnection::new(),
            };
            let session = Session::build(conn.clone(), SessionEnv::new("/cable-test", Some(HashMap::new())));
            session.set_executor(r.node.clone());
            session.set_broker(r.node.broker());
            session.set_id(uid);
            session.set_identifiers(uid);
            session.apply(rust_session_options(&opts));
            let writer = session.clone();
            std::thread::spawn(move || writer.send_messages());
            if let Some((url, headers)) = env {
                session.set_env(SessionEnv::new(url, headers));
            }
            TestSession { imp: SessionImp::Rust { session, conn, write_gate } }
        }
        NodeImp::Go { handle, .. } => {
            let mut a = json!({"node": handle, "mode": mode, "uid": uid});
            for o in &opts {
                match o {
                    SessionOpt::MaxPendingSize(n) => a["max_pending_size"] = json!(n),
                    SessionOpt::GatedWrites => a["gated"] = json!(true),
                    SessionOpt::KeepaliveIntervals(s, p) => {
                        a["sessions_ttl_ms"] = json!(s.as_millis() as u64);
                        a["presence_ttl_ms"] = json!(p.as_millis() as u64);
                    }
                    SessionOpt::PrevSid(sid) => a["prev_sid"] = json!(sid),
                }
            }
            match env {
                Some((url, headers)) => {
                    a["url"] = json!(url);
                    a["headers"] = json!(headers);
                }
                None => {
                    a["url"] = json!("/cable-test");
                    a["headers"] = json!({});
                }
            }
            TestSession { imp: SessionImp::Go { handle: bridge::call("node.NewSession", a) } }
        }
    }
}

/// A zero-value `node.Session{}` with fresh subscriptions and `env =
/// NewSessionEnv("/cable", nil)`, as upstream cache tests build it. Rust uses
/// `Session::build` (also a mock connection, no writer); Go uses the exported
/// `node.BuildSession`.
pub fn bare_session() -> TestSession {
    match backend() {
        Backend::Rust => {
            let conn = MockConnection::new();
            let session = Session::build(conn.clone(), SessionEnv::new("/cable", None));
            TestSession { imp: SessionImp::Rust { session, conn, write_gate: None } }
        }
        Backend::Go => {
            let handle: u64 = bridge::call("node.BuildSession", json!({"url": "/cable", "headers": null}));
            TestSession { imp: SessionImp::Go { handle } }
        }
    }
}

#[derive(Deserialize)]
struct GoRead {
    msg: Option<String>,
    error: Option<String>,
}

impl TestSession {
    /// The Rust session; not observable on Go (`reason`).
    pub fn rust_with(&self, reason: &str) -> &Arc<Session> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session,
            SessionImp::Go { .. } => not_observable(reason),
        }
    }

    fn rust_session(&self) -> &Arc<Session> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session,
            SessionImp::Go { .. } => unreachable!("a Go session passed to a Rust node"),
        }
    }

    /// The session's identity.
    pub fn id_ref(&self) -> SessionId {
        match &self.imp {
            SessionImp::Rust { session, .. } => SessionId(Arc::as_ptr(session) as u64),
            SessionImp::Go { handle } => SessionId(*handle),
        }
    }

    fn go_handle(&self) -> u64 {
        match &self.imp {
            SessionImp::Go { handle } => *handle,
            SessionImp::Rust { .. } => unreachable!("a Rust session passed to the Go bridge"),
        }
    }

    fn go(&self, method: &str, args: Value) -> Value {
        let mut a = args;
        a["id"] = json!(self.go_handle());
        a["method"] = json!(method);
        bridge::call("node.Session", a)
    }

    /// `session.conn.Read()`: the next written frame (an empty frame for a
    /// close), or an error after the mock's 100ms timeout.
    pub fn read(&self) -> Result<Vec<u8>, String> {
        match &self.imp {
            SessionImp::Rust { conn, .. } => cabby_server::node::Connection::read(conn.as_ref()).map_err(|e| e.0),
            SessionImp::Go { .. } => {
                let r: GoRead = serde_json::from_value(self.go("Read", json!({}))).expect("read reply");
                match r.error {
                    Some(e) => Err(e),
                    None => Ok(r.msg.unwrap_or_default().into_bytes()),
                }
            }
        }
    }

    /// [`TestSession::read`] as text.
    pub fn read_string(&self) -> Result<String, String> {
        self.read().map(|b| String::from_utf8(b).expect("UTF-8 frame"))
    }

    fn simple(&self, method: &str, args: Value) -> Value {
        self.go(method, args)
    }

    pub fn get_id(&self) -> String {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_id(),
            SessionImp::Go { .. } => self.simple("GetID", json!({})).as_str().expect("id").to_owned(),
        }
    }

    pub fn is_connected(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_connected(),
            SessionImp::Go { .. } => self.simple("IsConnected", json!({})).as_bool().expect("bool"),
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_closed(),
            SessionImp::Go { .. } => self.simple("IsClosed", json!({})).as_bool().expect("bool"),
        }
    }

    pub fn get_identifiers(&self) -> String {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_identifiers(),
            SessionImp::Go { .. } => self.simple("GetIdentifiers", json!({})).as_str().expect("ids").to_owned(),
        }
    }

    pub fn set_identifiers(&self, ids: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_identifiers(ids),
            SessionImp::Go { .. } => {
                self.simple("SetIdentifiers", json!({"ids": ids}));
            }
        }
    }

    /// `session.GetEnv()` (a snapshot).
    pub fn get_env(&self) -> SessionEnv {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_env(),
            SessionImp::Go { .. } => opt_env_from_go(&self.simple("GetEnv", json!({}))).expect("a session env"),
        }
    }

    /// `session.SetEnv(&env)`.
    pub fn set_env(&self, env: &SessionEnv) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_env(env.clone()),
            SessionImp::Go { .. } => {
                self.simple("SetEnv", json!({"env": env_to_go(env)}));
            }
        }
    }

    /// `session.MergeEnv(&env)`.
    pub fn merge_env(&self, env: &SessionEnv) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.merge_env(env),
            SessionImp::Go { .. } => {
                self.simple("MergeEnv", json!({"env": env_to_go(env)}));
            }
        }
    }

    /// `session.env.MergeChannelState(id, &state)` (Go: on `GetEnv()`).
    pub fn merge_channel_state(&self, id: &str, state: &StringMap) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.update_env(&mut |e| e.merge_channel_state(id, state)),
            SessionImp::Go { .. } => {
                self.simple("MergeChannelState", json!({"channel": id, "state": state}));
            }
        }
    }

    /// `session.env.MergeConnectionState(&state)` (Go: on `GetEnv()`).
    pub fn merge_connection_state(&self, state: &StringMap) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.update_env(&mut |e| e.merge_connection_state(state)),
            SessionImp::Go { .. } => {
                self.simple("MergeConnectionState", json!({"state": state}));
            }
        }
    }

    /// `session.env.RemoveChannelState(id)` (Go: on `GetEnv()`).
    pub fn remove_channel_state(&self, id: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.update_env(&mut |e| e.remove_channel_state(id)),
            SessionImp::Go { .. } => {
                self.simple("RemoveChannelState", json!({"channel": id}));
            }
        }
    }

    pub fn mark_disconnectable(&self, val: bool) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.mark_disconnectable(val),
            SessionImp::Go { .. } => {
                self.simple("MarkDisconnectable", json!({"val": val}));
            }
        }
    }

    pub fn is_disconnectable(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_disconnectable(),
            SessionImp::Go { .. } => self.simple("IsDisconnectable", json!({})).as_bool().expect("bool"),
        }
    }

    /// `session.Send(&common.Reply{Type: kind, Message: message})`.
    pub fn send_reply(&self, kind: &str, message: Value) {
        match &self.imp {
            SessionImp::Rust { session, .. } => {
                session.send(&Reply { kind: kind.to_owned(), message: Some(message), ..Default::default() })
            }
            SessionImp::Go { .. } => {
                self.simple("Send", json!({"reply": {"type": kind, "message": message}}));
            }
        }
    }

    /// `session.sendFrame(&ws.SentFrame{FrameType: ws.TextFrame, Payload: text})`.
    /// Go: the exported `SendJSONTransmission(text)`, which with the default
    /// JSON encoder queues exactly that frame.
    pub fn send_text_frame(&self, text: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.send_frame(text_frame(text)),
            SessionImp::Go { .. } => {
                self.simple("SendJSONTransmission", json!({"data": text}));
            }
        }
    }

    /// `session.connected.Store(true)` (unexported). Go: the exported
    /// `node.Authenticated(s, s.GetIdentifiers())`, which sets it (and adds the
    /// session to the hub).
    pub fn mark_connected(&self, node: &TestNode) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_connected(true),
            SessionImp::Go { .. } => node.authenticated(self, &self.get_identifiers()),
        }
    }

    /// `session.Disconnect(reason, code)`.
    pub fn disconnect(&self, reason: &str, code: i32) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.disconnect(reason, code),
            SessionImp::Go { .. } => {
                self.simple("Disconnect", json!({"reason": reason, "code": code}));
            }
        }
    }

    /// `session.DisconnectNow(reason, code)`.
    pub fn disconnect_now(&self, reason: &str, code: i32) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.disconnect_now(reason, code),
            SessionImp::Go { .. } => {
                self.simple("DisconnectNow", json!({"reason": reason, "code": code}));
            }
        }
    }

    /// `go session.DisconnectNow(reason, code)`.
    pub fn disconnect_now_in_background(&self, reason: &str, code: i32) {
        match &self.imp {
            SessionImp::Rust { session, .. } => {
                let (s, reason) = (session.clone(), reason.to_owned());
                std::thread::spawn(move || s.disconnect_now(&reason, code));
            }
            SessionImp::Go { .. } => {
                self.simple("DisconnectNowAsync", json!({"reason": reason, "code": code}));
            }
        }
    }

    /// `session.resumeInterval = d` (unexported). Go: the exported option
    /// `WithKeepaliveIntervals(d, 0)(s)`, which also sets the presence
    /// interval, 0 for mock sessions anyway.
    pub fn set_resume_interval(&self, d: Duration) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_resume_interval(d),
            SessionImp::Go { .. } => {
                self.simple("SetResumeInterval", json!({"timeout_ms": d.as_millis() as u64}));
            }
        }
    }

    /// `session.closeCtx.Err() != nil` (unexported). Go: the context is the one
    /// every controller call receives; a probe controller reads it from a
    /// side-effect-free `node.Subscribe` on the closed session.
    pub fn close_context_cancelled(&self, node: &TestNode) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.close_context().is_cancelled(),
            SessionImp::Go { .. } => {
                node.go_node("CloseContextErr", json!({"session": self.go_handle()})).as_bool().expect("bool")
            }
        }
    }

    /// The session's subscriptions (upstream reads the unexported
    /// `session.subscriptions`). Go: the `subs` of the exported `ToCacheEntry()`
    /// (`SubscriptionState.ToMap()`).
    pub fn subscription_map(&self) -> HashMap<String, Vec<String>> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().to_map(),
            SessionImp::Go { .. } => {
                let v = self.simple("Subscriptions", json!({}));
                if v.is_null() { HashMap::new() } else { serde_json::from_value(v).expect("subscriptions") }
            }
        }
    }

    /// `session.subscriptions.HasChannel(id)`.
    pub fn has_channel(&self, id: &str) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().has_channel(id),
            SessionImp::Go { .. } => self.subscription_map().contains_key(id),
        }
    }

    /// `session.subscriptions.Channels()`.
    pub fn channels(&self) -> Vec<String> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().channels(),
            SessionImp::Go { .. } => self.subscription_map().into_keys().collect(),
        }
    }

    /// `session.subscriptions.StreamsFor(id)`.
    pub fn streams_for(&self, id: &str) -> Option<Vec<String>> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().streams_for(id),
            SessionImp::Go { .. } => self.subscription_map().remove(id),
        }
    }

    /// `session.subscriptions.AddChannel(id)`. Go: the exported
    /// `RestoreFromCache` applies a cache entry whose `subs` gain the channel
    /// (it calls `AddChannel`/`AddChannelStream` and re-merges identical states).
    pub fn add_channel(&self, id: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().add_channel(id),
            SessionImp::Go { .. } => {
                self.simple("AddChannel", json!({"channel": id}));
            }
        }
    }

    /// `session.subscriptions.AddChannelStream(id, stream)` (Go: see [`TestSession::add_channel`]).
    pub fn add_channel_stream(&self, id: &str, stream: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.subscriptions().add_channel_stream(id, stream),
            SessionImp::Go { .. } => {
                self.simple("AddChannelStream", json!({"channel": id, "stream": stream}));
            }
        }
    }

    /// `session.ToCacheEntry()`.
    pub fn to_cache_entry(&self) -> Result<Vec<u8>, String> {
        match &self.imp {
            SessionImp::Rust { session, .. } => {
                cabby_core::broker::Cacheable::to_cache_entry(session.as_ref()).map_err(|e| e.message)
            }
            SessionImp::Go { .. } => {
                let r: GoRead = serde_json::from_value(self.simple("ToCacheEntry", json!({}))).expect("cache reply");
                match r.error {
                    Some(e) => Err(e),
                    None => Ok(r.msg.unwrap_or_default().into_bytes()),
                }
            }
        }
    }

    /// `session.RestoreFromCache(cached)`.
    pub fn restore_from_cache(&self, cached: &[u8]) -> Result<(), String> {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.restore_from_cache(cached).map_err(|e| e.0),
            SessionImp::Go { .. } => {
                let data = String::from_utf8(cached.to_vec()).expect("JSON cache entry");
                let v = self.simple("RestoreFromCache", json!({"data": data}));
                serde_json::from_value::<GoErr>(v).expect("error reply").into_result().map_err(|e| e.0)
            }
        }
    }

    /// Blocks until `n` writes wait at the connection's write gate.
    pub fn await_writes_held(&self, n: usize) {
        match &self.imp {
            SessionImp::Rust { write_gate, .. } => {
                write_gate.as_ref().expect("a session with GatedWrites").await_held(n, DEADLINE)
            }
            SessionImp::Go { .. } => {
                let ok = self.simple("AwaitWritesHeld", json!({"n": n, "timeout_ms": DEADLINE.as_millis() as u64}));
                assert_eq!(Some(true), ok.as_bool(), "{n} held write(s) expected within {DEADLINE:?}");
            }
        }
    }

    /// Opens the connection's write gate for good.
    pub fn release_writes(&self) {
        match &self.imp {
            SessionImp::Rust { write_gate, .. } => write_gate.as_ref().expect("a session with GatedWrites").open(),
            SessionImp::Go { .. } => {
                self.simple("ReleaseWrites", json!({}));
            }
        }
    }
}

fn text_frame(payload: &str) -> SentFrame {
    SentFrame { frame_type: FrameType::Text, payload: payload.as_bytes().to_vec(), ..Default::default() }
}

/// Runs `f` on a detached thread and waits for it with a deadline, so a hung
/// call fails the test instead of hanging it (the thread is never joined).
fn within<T: Send + 'static>(deadline: Duration, f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(deadline).ok()
}

/// `MergeEnv(env)` for every env concurrently (upstream: one goroutine each,
/// then `wg.Wait()`); `false` if they did not all return within `deadline`.
pub fn merge_env_concurrently(s: &TestSession, envs: &[SessionEnv], deadline: Duration) -> bool {
    match &s.imp {
        SessionImp::Rust { session, .. } => {
            let (tx, rx) = std::sync::mpsc::channel();
            for env in envs {
                let (session, env, tx) = (session.clone(), env.clone(), tx.clone());
                std::thread::spawn(move || {
                    session.merge_env(&env);
                    let _ = tx.send(());
                });
            }
            (0..envs.len()).all(|_| rx.recv_timeout(deadline).is_ok())
        }
        SessionImp::Go { handle } => {
            bridge::call::<_, Value>(
                "node.Session.MergeEnvConcurrently",
                json!({"id": handle, "envs": envs.iter().map(env_to_go).collect::<Vec<_>>()}),
            );
            true
        }
    }
}

/// Upstream TestSendRaceConditions' scaffolding: 10 mock sessions "123" of
/// `node`, each with a reader draining its connection forever and four
/// concurrent `sendFrame` calls from nested goroutines ("hi!"/"bye",
/// "bye"/"why"). `true` when every send returned (`wg.Wait()`) within
/// `deadline`. Threads are detached so a hang fails instead of hanging.
pub fn send_race_conditions(node: &TestNode, deadline: Duration) -> bool {
    match &node.imp {
        NodeImp::Rust(_) => {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut expected = 0;
            for _ in 1..=10 {
                let session = new_mock_session("123", node, vec![]);
                let SessionImp::Rust { session, conn, .. } = session.imp else { unreachable!() };
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Acquire) {
                        let _ = cabby_server::node::Connection::read(conn.as_ref());
                    }
                });
                for pair in [["hi!", "bye"], ["bye", "why"]] {
                    expected += 2;
                    let (session, tx) = (session.clone(), tx.clone());
                    std::thread::spawn(move || {
                        for payload in pair {
                            let (session, tx) = (session.clone(), tx.clone());
                            std::thread::spawn(move || {
                                session.send_frame(text_frame(payload));
                                let _ = tx.send(());
                            });
                        }
                    });
                }
            }
            let ok = (0..expected).all(|_| rx.recv_timeout(deadline).is_ok());
            stop.store(true, std::sync::atomic::Ordering::Release);
            ok
        }
        NodeImp::Go { handle, .. } => bridge::call::<_, Value>(
            "node.SendRaceConditions",
            json!({"node": handle, "timeout_ms": deadline.as_millis() as u64}),
        )
        .as_bool()
        .expect("bool"),
    }
}

/// Upstream TestAuthenticatedRacesWithDisconnect's scaffolding: `n` mock
/// sessions `s<i>`; for each, `node.Authenticated(s, s.GetID())` and
/// `s.Disconnect("Ping failed", CloseAbnormalClosure)` start together (a shared
/// start signal), and the call returns when all have returned (`wg.Wait()`),
/// bounded by `deadline`.
pub fn authenticated_races_with_disconnect(node: &TestNode, n: usize, deadline: Duration) -> Vec<TestSession> {
    match &node.imp {
        NodeImp::Rust(r) => {
            let sessions: Vec<TestSession> = (0..n).map(|i| new_mock_session(&format!("s{i}"), node, vec![])).collect();
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            for s in &sessions {
                let start = Arc::new(std::sync::Barrier::new(2));
                let (s1, s2) = (s.rust_session().clone(), s.rust_session().clone());
                let (b1, b2) = (start.clone(), start);
                let (node, tx1, tx2) = (r.node.clone(), tx.clone(), tx.clone());
                std::thread::spawn(move || {
                    b1.wait();
                    node.authenticated(&s1, &s1.get_id());
                    let _ = tx1.send(());
                });
                std::thread::spawn(move || {
                    b2.wait();
                    // Like a failed ping write while the Connect call completes
                    s2.disconnect("Ping failed", CLOSE_ABNORMAL_CLOSURE);
                    let _ = tx2.send(());
                });
            }
            for i in 0..2 * n {
                assert!(rx.recv_timeout(deadline).is_ok(), "only {i} of {} calls returned", 2 * n);
            }
            sessions
        }
        NodeImp::Go { handle, .. } => {
            let handles: Vec<u64> =
                bridge::call("node.AuthenticatedRacesWithDisconnect", json!({"node": handle, "n": n}));
            handles.into_iter().map(|handle| TestSession { imp: SessionImp::Go { handle } }).collect()
        }
    }
}

// ------------------------------------------------------------------- queue

enum QueueImp {
    Rust(Arc<rs::DisconnectQueue>),
    Go(u64),
}

/// A disconnect queue under test, with the node it belongs to (upstream `q.node`).
pub struct TestQueue<'a> {
    node: &'a TestNode,
    imp: QueueImp,
}

/// `NewDisconnectQueue(node, &config, slog.Default())`.
pub fn new_disconnect_queue<'a>(node: &'a TestNode, config: &DisconnectQueueConfig) -> TestQueue<'a> {
    let imp = match &node.imp {
        NodeImp::Rust(r) => QueueImp::Rust(Arc::new(rs::DisconnectQueue::new(r.node.clone(), config))),
        NodeImp::Go { handle, .. } => QueueImp::Go(bridge::call(
            "node.NewDisconnectQueue",
            json!({"node": handle, "config": GoQueueConfig::from(config)}),
        )),
    };
    TestQueue { node, imp }
}

/// A background context (`context.Background()`).
pub fn background() -> Context {
    Context::default()
}

#[derive(Deserialize)]
struct GoSize {
    size: usize,
}

/// Outcome of [`TestQueue::shutdown_while_enqueueing`].
#[derive(Debug, PartialEq)]
pub enum ShutdownRace {
    Returned(Result<(), NodeError>),
    /// Shutdown did not return within 5s.
    Blocked,
}

impl TestQueue<'_> {
    pub fn node(&self) -> &TestNode {
        self.node
    }

    /// The Rust queue (not observable on Go: `reason`).
    pub fn rust_with(&self, reason: &str) -> &Arc<rs::DisconnectQueue> {
        match &self.imp {
            QueueImp::Rust(q) => q,
            QueueImp::Go(_) => not_observable(reason),
        }
    }

    fn go(&self, method: &str, args: Value) -> Value {
        let QueueImp::Go(id) = &self.imp else { unreachable!() };
        let mut a = args;
        a["id"] = json!(id);
        a["method"] = json!(method);
        bridge::call("node.DisconnectQueue", a)
    }

    pub fn enqueue(&self, s: &TestSession) -> Result<(), NodeError> {
        match &self.imp {
            QueueImp::Rust(q) => q.enqueue(s.rust_session().clone()),
            QueueImp::Go(_) => serde_json::from_value::<GoErr>(self.go("Enqueue", json!({"session": s.go_handle()})))
                .expect("error reply")
                .into_result(),
        }
    }

    pub fn size(&self) -> usize {
        match &self.imp {
            QueueImp::Rust(q) => q.size(),
            QueueImp::Go(_) => serde_json::from_value::<GoSize>(self.go("Size", json!({}))).expect("size").size,
        }
    }

    /// `q.Shutdown(context.Background())`.
    pub fn shutdown(&self) -> Result<(), NodeError> {
        match &self.imp {
            QueueImp::Rust(q) => q.shutdown(&background()),
            QueueImp::Go(_) => serde_json::from_value::<GoErr>(self.go("Shutdown", json!({"timeout_ms": null})))
                .expect("error reply")
                .into_result(),
        }
    }

    /// `q.Shutdown(ctx)` with a context whose deadline has already passed.
    pub fn shutdown_expired(&self) -> Result<(), NodeError> {
        match &self.imp {
            QueueImp::Rust(q) => {
                let ctx = Context { deadline: Some(tokio::time::Instant::now()), ..Default::default() };
                ctx.cancel.cancel();
                q.shutdown(&ctx)
            }
            QueueImp::Go(_) => serde_json::from_value::<GoErr>(self.go("Shutdown", json!({"timeout_ms": 0})))
                .expect("error reply")
                .into_result(),
        }
    }

    /// `go q.Run()`.
    pub fn run_in_background(&self) {
        match &self.imp {
            QueueImp::Rust(q) => {
                let q = q.clone();
                std::thread::spawn(move || {
                    let _ = q.run();
                });
            }
            QueueImp::Go(_) => {
                self.go("Run", json!({}));
            }
        }
    }

    /// Go only: `start := time.Now(); go q.Run(); require.Eventually(calls ==
    /// n, timeout, 1ms)`, measured inside the bridge. `None` if not reached.
    pub fn run_until_calls(&self, controller: &TestMock, n: usize, timeout: Duration) -> Option<Duration> {
        #[derive(Deserialize)]
        struct R {
            reached: bool,
            elapsed_ms: u64,
        }
        let r: R = serde_json::from_value(self.go(
            "RunUntilCalls",
            json!({"controller": controller.go_handle(), "n": n, "timeout_ms": timeout.as_millis() as u64}),
        ))
        .expect("reply");
        r.reached.then(|| Duration::from_millis(r.elapsed_ms))
    }

    /// Upstream's concurrent shutdown scaffolding: eight callers keep
    /// enqueueing their own session (`c-<j>`) while `q.Shutdown(ctx)` runs;
    /// `Blocked` if it does not return within 5s. Rust threads are detached, so
    /// a blocked shutdown fails the test instead of hanging it.
    pub fn shutdown_while_enqueueing(&self) -> ShutdownRace {
        match &self.imp {
            QueueImp::Rust(q) => {
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                for j in 0..8 {
                    let s = new_mock_session(&format!("c-{j}"), self.node, vec![]).rust_session().clone();
                    let (q, stop) = (q.clone(), stop.clone());
                    std::thread::spawn(move || {
                        while !stop.load(std::sync::atomic::Ordering::Acquire) {
                            let _ = q.enqueue(s.clone());
                        }
                    });
                }
                let q2 = q.clone();
                let r = within(Duration::from_secs(5), move || q2.shutdown(&background()));
                stop.store(true, std::sync::atomic::Ordering::Release);
                r.map_or(ShutdownRace::Blocked, ShutdownRace::Returned)
            }
            QueueImp::Go(_) => {
                let NodeImp::Go { handle, .. } = &self.node.imp else { unreachable!() };
                let v = self.go("ShutdownWhileEnqueueing", json!({"session": handle}));
                if v["blocked"].as_bool().expect("bool") {
                    ShutdownRace::Blocked
                } else {
                    ShutdownRace::Returned(serde_json::from_value::<GoErr>(v).expect("reply").into_result())
                }
            }
        }
    }

    /// Waits until a running queue's workers have taken every queued session
    /// (upstream spins on `q.Size() > 0`). Rust: the worker then blocks on the
    /// controlled throttle ticker, which is the observable readiness point.
    /// Go: a bounded probe of the exported `Size()` (no change notification).
    pub fn await_drained(&self, workers_waiting: usize) {
        match &self.imp {
            QueueImp::Rust(_) => self.node.timers().await_ticker_waiters(workers_waiting, DEADLINE),
            QueueImp::Go(_) => {
                let end = std::time::Instant::now() + DEADLINE;
                while self.size() > 0 {
                    assert!(std::time::Instant::now() < end, "queue not drained within {DEADLINE:?}");
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }
}

// ------------------------------------------------------------------ broker

enum BrokerImp {
    Rust {
        broker: Arc<dyn Broker>,
        memory: Option<Arc<cabby_core::broker::Memory>>,
        nats: Option<Arc<cabby_core::broker::Nats>>,
    },
    Go(u64),
}

/// A real broker bound to a node (upstream integration tests).
pub struct TestBroker {
    imp: BrokerImp,
}

fn broker_config_to_go(c: &cabby_core::broker::Config) -> Value {
    json!({
        "Adapter": c.adapter,
        "HistoryTTL": c.history_ttl,
        "HistoryLimit": c.history_limit,
        "SessionsTTL": c.sessions_ttl,
        "PresenceTTL": c.presence_ttl,
    })
}

/// Upstream `broker.NewMemoryBroker(pubsub.NewLegacySubscriber(node), node, &config)`;
/// the Rust broker runs on the node's controlled clock.
pub fn new_memory_broker_for(node: &TestNode, config: &cabby_core::broker::Config) -> TestBroker {
    match &node.imp {
        NodeImp::Rust(r) => {
            let subscriber = Arc::new(LegacySubscriber::new(r.node.clone()));
            let memory = Arc::new(cabby_core::broker::Memory::new(
                Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>),
                Some(r.node.clone() as Arc<dyn cabby_core::broker::Presenter>),
                config,
                Arc::new(r.timers.clone()),
            ));
            TestBroker { imp: BrokerImp::Rust { broker: memory.clone(), memory: Some(memory), nats: None } }
        }
        NodeImp::Go { handle, .. } => TestBroker {
            imp: BrokerImp::Go(bridge::call(
                "node.NewBroker",
                json!({"node": handle, "kind": "memory", "config": broker_config_to_go(config)}),
            )),
        },
    }
}

/// Upstream `broker.NewNATSBroker(pubsub.NewLegacySubscriber(node), &config, &natsConfig)`
/// with `natsConfig.Servers = servers`.
pub fn new_nats_broker_for(node: &TestNode, config: &cabby_core::broker::Config, servers: &str) -> TestBroker {
    match &node.imp {
        NodeImp::Rust(r) => {
            let mut nconf = cabby_core::broker::NatsConfig::new();
            nconf.servers = servers.to_owned();
            let subscriber = Arc::new(LegacySubscriber::new(r.node.clone()));
            let nats = Arc::new(cabby_core::broker::Nats::new(
                Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>),
                config,
                &nconf,
            ));
            TestBroker { imp: BrokerImp::Rust { broker: nats.clone(), memory: None, nats: Some(nats) } }
        }
        NodeImp::Go { handle, .. } => TestBroker {
            imp: BrokerImp::Go(bridge::call(
                "node.NewBroker",
                json!({"node": handle, "kind": "nats", "config": broker_config_to_go(config), "servers": servers}),
            )),
        },
    }
}

impl TestBroker {
    fn go_err(&self, method: &str, args: Value) -> Result<(), String> {
        let BrokerImp::Go(h) = &self.imp else { unreachable!() };
        let mut a = args;
        a["id"] = json!(h);
        a["method"] = json!(method);
        let v: Value = bridge::call("node.Broker", a);
        v["error"].as_str().map_or(Ok(()), |e| Err(e.to_owned()))
    }

    /// `broker.Start(nil)`.
    pub fn start(&self) -> Result<(), String> {
        match &self.imp {
            BrokerImp::Rust { broker, .. } => broker.start().map_err(|e| e.message),
            BrokerImp::Go(_) => self.go_err("Start", json!({})),
        }
    }

    /// `broker.SetEpoch(epoch)`.
    pub fn set_epoch(&self, epoch: &str) -> Result<(), String> {
        match &self.imp {
            BrokerImp::Rust { memory: Some(m), .. } => {
                m.set_epoch(epoch);
                Ok(())
            }
            BrokerImp::Rust { nats: Some(n), .. } => n.set_epoch(epoch).map_err(|e| e.message),
            BrokerImp::Rust { .. } => unreachable!(),
            BrokerImp::Go(_) => self.go_err("SetEpoch", json!({"epoch": epoch})),
        }
    }

    /// `broker.Reset()` (NATS).
    pub fn reset(&self) -> Result<(), String> {
        match &self.imp {
            BrokerImp::Rust { nats: Some(n), .. } => n.reset().map_err(|e| e.message),
            BrokerImp::Rust { .. } => unreachable!("reset is a NATS broker method"),
            BrokerImp::Go(_) => self.go_err("Reset", json!({})),
        }
    }

    /// One expiration pass of a Rust memory broker (upstream's background
    /// loop runs it every second); nothing for other brokers.
    pub fn expire(&self) {
        if let BrokerImp::Rust { memory: Some(m), .. } = &self.imp {
            m.expire();
        }
    }
}
