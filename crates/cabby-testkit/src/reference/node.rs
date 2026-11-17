//! Facade for upstream package `node`.
//!
//! The Go arms drive real Go objects through the bridge (`bridge/node.go`):
//! `node.NewNode` wired like upstream's `NewMockNode`, sessions from
//! `node.NewSession`/`node.BuildSession` over `mocks.NewMockConnection()`,
//! `node.NewDisconnectQueue`, `node.NewSubscriptionState` and the configs,
//! kept behind integer handles. Only exported Go API is called.
//!
//! Most upstream node tests also reach into unexported state (`node.hub`,
//! `session.subscriptions`, `session.conn`, private fields and methods) or use
//! testify mocks with `Run` callbacks into the node. Those operations are only
//! available on the Rust backend: their accessors ([`TestNode::rust`],
//! [`TestNode::hub`], [`TestSession::rust`], [`TestSession::subscriptions`], ...)
//! report `not_observable` under Go, so a case runs against Go up to its first
//! unobservable step.
//!
//! Rust nodes built here run on a [`ManualTimers`] clock (session timers and the
//! disconnect queue throttle), which tests advance explicitly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cabby_core::broker::Broker;
use cabby_core::common::{ConnectResult, Controller, Reply, SessionEnv, StringMap};
use cabby_server::metrics::{Instrumenter, Metrics};
use cabby_server::node::{self as rs, Disconnector, Hub, LegacySubscriber, Node, NodeOption, Session, SessionOption};
pub use cabby_server::node::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_INTERNAL_SERVER_ERR, CLOSE_NORMAL_CLOSURE, Config,
    DISCONNECT_MODE_ALWAYS, DISCONNECT_MODE_AUTO, DISCONNECT_MODE_NEVER, DisconnectQueueConfig,
    METRICS_DISCONNECT_DISCARDED, NodeError,
};
use cabby_server::rpc::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::go_types::{connect_result_from_go, env_to_go, opt_env_from_go};
pub use super::node_doubles::*;
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

// -------------------------------------------------------------------- node

/// What a Rust test node holds besides the node itself (upstream test helpers
/// keep these as locals or reach them through unexported fields).
pub struct RustNode {
    pub node: Arc<Node>,
    pub timers: ManualTimers,
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
            TestNode { imp: NodeImp::Rust(RustNode { node, timers, metrics: Some(metrics), queue: Some(queue) }) }
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

/// `NewNode(&config, WithInstrumenter(metrics.NewMetrics(nil, 10)), WithController(controller))`
/// with a testify-style controller. Rust only: the testify mocks script `Run`
/// callbacks the bridge cannot reproduce.
pub fn new_node_with_controller(config: Config, controller: Arc<dyn Controller>) -> TestNode {
    let timers = ManualTimers::new();
    node_with_controller(config, controller, Arc::new(timers.clone()), timers)
}

/// [`new_node_with_controller`] on the operating system clock, for cases whose
/// timing is enforced by an external service (NATS TTLs run on the server's
/// clock). [`TestNode::timers`] of such a node is not connected to it.
pub fn new_node_with_controller_on_system_clock(config: Config, controller: Arc<dyn Controller>) -> TestNode {
    node_with_controller(config, controller, Arc::new(rs::SystemClock), ManualTimers::new())
}

fn node_with_controller(
    config: Config,
    controller: Arc<dyn Controller>,
    clock: Arc<dyn rs::Clock>,
    timers: ManualTimers,
) -> TestNode {
    if backend() == Backend::Go {
        not_observable("the case scripts the node's controller with a testify mock (Run callbacks into the test)");
    }
    let metrics = Arc::new(Metrics::new(Vec::new(), 10));
    let node = Node::new(
        config,
        vec![
            NodeOption::Controller(controller),
            NodeOption::Instrumenter(metrics.clone() as Arc<dyn Instrumenter>),
            NodeOption::Clock(clock),
        ],
    );
    TestNode { imp: NodeImp::Rust(RustNode { node, timers, metrics: Some(metrics), queue: None }) }
}

impl TestNode {
    /// The Rust node and its helpers; not observable on Go (`reason`).
    pub fn rust_parts(&self, reason: &str) -> &RustNode {
        match &self.imp {
            NodeImp::Rust(r) => r,
            NodeImp::Go { .. } => not_observable(reason),
        }
    }

    /// The Rust node, for operations that need unexported Go state or testify
    /// callbacks.
    pub fn rust(&self) -> &Arc<Node> {
        &self.rust_parts("the case drives node internals (unexported fields, testify callbacks)").node
    }

    /// `node.hub` (unexported upstream).
    pub fn hub(&self) -> &Hub {
        self.rust_parts("node.hub is unexported").node.hub()
    }

    /// The controlled clock of a Rust node.
    pub fn timers(&self) -> &ManualTimers {
        &self.rust_parts("the Go node runs on wall-clock time").timers
    }

    fn go_node(&self, method: &str, args: Value) -> Value {
        let NodeImp::Go { handle, .. } = &self.imp else { unreachable!() };
        let mut a = args;
        a["id"] = json!(handle);
        a["method"] = json!(method);
        bridge::call("node.Node", a)
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
                self.go_node("Start", json!({}));
            }
        }
        HubGuard { node: self }
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
            NodeImp::Rust(r) => r.node.authenticate(s.rust(), &[]),
            NodeImp::Go { .. } => {
                let v = self.go_node("Authenticate", json!({"session": s.go_handle()}));
                match v["error"].as_str() {
                    Some(e) => Err(NodeError(e.to_owned())),
                    None => Ok((!v["result"].is_null()).then(|| connect_result_from_go(&v["result"]))),
                }
            }
        }
    }

    /// `node.Disconnect(session)`.
    pub fn disconnect(&self, s: &TestSession) -> Result<(), NodeError> {
        match &self.imp {
            NodeImp::Rust(r) => r.node.disconnect(s.rust()),
            NodeImp::Go { .. } => {
                let v = self.go_node("Disconnect", json!({"session": s.go_handle()}));
                serde_json::from_value::<GoErr>(v).expect("error reply").into_result()
            }
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
            NodeImp::Rust(r) => TestQueue {
                node: self,
                imp: QueueImp::Rust(r.queue.clone().expect("a NewMockNode node")),
                ran: Mutex::new(false),
            },
            NodeImp::Go { queue, .. } => {
                TestQueue { node: self, imp: QueueImp::Go(queue.expect("a NewMockNode node")), ran: Mutex::new(false) }
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
/// The Go arm uses the exported `node.NewSession` (ping interval 0, so no timer
/// is scheduled) followed by `SetIdentifiers(uid)`.
pub fn new_mock_session(uid: &str, node: &TestNode, opts: Vec<SessionOpt>) -> TestSession {
    mock_session(uid, node, None, opts)
}

/// Upstream `NewMockSessionWithEnv(uid, node, url, headers, opts...)`.
pub fn new_mock_session_with_env(
    uid: &str,
    node: &TestNode,
    url: &str,
    headers: Option<StringMap>,
    opts: Vec<SessionOpt>,
) -> TestSession {
    mock_session(uid, node, Some((url, headers)), opts)
}

fn mock_session(
    uid: &str,
    node: &TestNode,
    env: Option<(&str, Option<StringMap>)>,
    opts: Vec<SessionOpt>,
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
            let mut max_pending = Value::Null;
            let mut gated = false;
            for o in &opts {
                match o {
                    SessionOpt::MaxPendingSize(n) => max_pending = json!(n),
                    SessionOpt::GatedWrites => gated = true,
                    other => not_observable(&format!(
                        "NewMockSession applies {other:?} without starting timers, which node.NewSession cannot do"
                    )),
                }
            }
            let (url, headers) = match env {
                Some((url, headers)) => (url.to_owned(), json!(headers)),
                None => ("/cable-test".to_owned(), json!({})),
            };
            let handle: u64 = bridge::call(
                "node.NewSession",
                json!({"node": handle, "uid": uid, "url": url, "headers": headers,
                       "max_pending_size": max_pending, "gated": gated}),
            );
            TestSession { imp: SessionImp::Go { handle } }
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

    /// The Rust session, for unexported operations.
    pub fn rust(&self) -> &Arc<Session> {
        self.rust_with("the case uses unexported Session state or methods")
    }

    /// `session.subscriptions` (unexported upstream).
    pub fn subscriptions(&self) -> &rs::SubscriptionState {
        self.rust_with("Session.subscriptions is unexported").subscriptions()
    }

    /// `session.conn` as the mock connection (unexported upstream).
    pub fn conn(&self) -> &Arc<MockConnection> {
        match &self.imp {
            SessionImp::Rust { conn, .. } => conn,
            SessionImp::Go { .. } => not_observable("Session.conn is unexported"),
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

    pub fn get_id(&self) -> String {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_id(),
            SessionImp::Go { .. } => self.go("GetID", json!({})).as_str().expect("id").to_owned(),
        }
    }

    pub fn is_connected(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_connected(),
            SessionImp::Go { .. } => self.go("IsConnected", json!({})).as_bool().expect("bool"),
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_closed(),
            SessionImp::Go { .. } => self.go("IsClosed", json!({})).as_bool().expect("bool"),
        }
    }

    pub fn get_identifiers(&self) -> String {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_identifiers(),
            SessionImp::Go { .. } => self.go("GetIdentifiers", json!({})).as_str().expect("ids").to_owned(),
        }
    }

    pub fn set_identifiers(&self, ids: &str) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_identifiers(ids),
            SessionImp::Go { .. } => {
                self.go("SetIdentifiers", json!({"ids": ids}));
            }
        }
    }

    /// `session.GetEnv()` (a snapshot).
    pub fn get_env(&self) -> SessionEnv {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.get_env(),
            SessionImp::Go { .. } => opt_env_from_go(&self.go("GetEnv", json!({}))).expect("a session env"),
        }
    }

    /// `session.SetEnv(&env)`.
    pub fn set_env(&self, env: &SessionEnv) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.set_env(env.clone()),
            SessionImp::Go { .. } => {
                self.go("SetEnv", json!({"env": env_to_go(env)}));
            }
        }
    }

    /// `session.MergeEnv(&env)`.
    pub fn merge_env(&self, env: &SessionEnv) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.merge_env(env),
            SessionImp::Go { .. } => {
                self.go("MergeEnv", json!({"env": env_to_go(env)}));
            }
        }
    }

    pub fn mark_disconnectable(&self, val: bool) {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.mark_disconnectable(val),
            SessionImp::Go { .. } => {
                self.go("MarkDisconnectable", json!({"val": val}));
            }
        }
    }

    pub fn is_disconnectable(&self) -> bool {
        match &self.imp {
            SessionImp::Rust { session, .. } => session.is_disconnectable(),
            SessionImp::Go { .. } => self.go("IsDisconnectable", json!({})).as_bool().expect("bool"),
        }
    }

    /// `session.Send(&common.Reply{Type: kind, Message: message})`.
    pub fn send_reply(&self, kind: &str, message: Value) {
        match &self.imp {
            SessionImp::Rust { session, .. } => {
                session.send(&Reply { kind: kind.to_owned(), message: Some(message), ..Default::default() })
            }
            SessionImp::Go { .. } => {
                self.go("Send", json!({"reply": {"type": kind, "message": message}}));
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
                let r: GoRead = serde_json::from_value(self.go("ToCacheEntry", json!({}))).expect("cache reply");
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
                let v = self.go("RestoreFromCache", json!({"data": data}));
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
                let ok = self.go("AwaitWritesHeld", json!({"n": n, "timeout_ms": DEADLINE.as_millis() as u64}));
                assert_eq!(Some(true), ok.as_bool(), "{n} held write(s) expected within {DEADLINE:?}");
            }
        }
    }

    /// Opens the connection's write gate for good.
    pub fn release_writes(&self) {
        match &self.imp {
            SessionImp::Rust { write_gate, .. } => write_gate.as_ref().expect("a session with GatedWrites").open(),
            SessionImp::Go { .. } => {
                self.go("ReleaseWrites", json!({}));
            }
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
    ran: Mutex<bool>,
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
    TestQueue { node, imp, ran: Mutex::new(false) }
}

/// A background context (`context.Background()`).
pub fn background() -> Context {
    Context::default()
}

#[derive(Deserialize)]
struct GoSize {
    size: usize,
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
            QueueImp::Rust(q) => q.enqueue(s.rust().clone()),
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
        *self.ran.lock().unwrap() = true;
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

/// Upstream `broker.NewMemoryBroker(subscriber, node, &config)` on the node's
/// controlled clock, with `pubsub.NewLegacySubscriber(node)`. Rust only.
pub fn new_memory_broker_for(node: &TestNode, config: &cabby_core::broker::Config) -> Arc<cabby_core::broker::Memory> {
    let r = node.rust_parts("broker integration cases use unexported session timers (startTimers) and fields");
    let subscriber = Arc::new(LegacySubscriber::new(r.node.clone()));
    Arc::new(cabby_core::broker::Memory::new(
        Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>),
        Some(r.node.clone() as Arc<dyn cabby_core::broker::Presenter>),
        config,
        Arc::new(r.timers.clone()),
    ))
}

/// Upstream `broker.NewNATSBroker(pubsub.NewLegacySubscriber(node), &config, &natsConfig)`.
/// Rust only.
pub fn new_nats_broker_for(
    node: &TestNode,
    config: &cabby_core::broker::Config,
    nats: &cabby_core::broker::NatsConfig,
) -> Arc<cabby_core::broker::Nats> {
    let r = node.rust_parts("broker integration cases use unexported session timers (startTimers) and fields");
    let subscriber = Arc::new(LegacySubscriber::new(r.node.clone()));
    Arc::new(cabby_core::broker::Nats::new(Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>), config, nats))
}

/// Sets a broker on a Rust node (upstream `node.SetBroker`).
pub fn set_broker(node: &TestNode, broker: Arc<dyn Broker>) {
    node.rust().set_broker(broker);
}

/// Sets a disconnector on a Rust node (upstream `node.SetDisconnector`).
pub fn set_disconnector(node: &TestNode, d: Arc<dyn Disconnector>) {
    node.rust().set_disconnector(d);
}
