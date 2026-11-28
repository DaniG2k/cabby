//! Test doubles for the node translations (Rust backend).
//!
//! * [`MockConnection`]: upstream `mocks.MockConnection` (a 10-frame buffered
//!   channel; `read` gives up after 100ms), plus an optional write gate that
//!   replaces upstream tests' `session.wmu.Lock()`.
//! * [`MockController`]: upstream `mocks.MockController` (the scripted
//!   behavior keyed by URL/channel/data that `NewMockNode` uses).
//! * [`Mock`], [`MockBroker`], [`MockNodeController`]: testify mocks
//!   (`mocks.Broker`, `mocks.Controller`) with `On(...).Return(...).Run(...)`:
//!   expectations are matched in registration order, every call is recorded,
//!   and an unexpected call panics like testify does.
//! * [`BlockingGate`]: holds threads until the test releases them.
//!
//! These are doubles of upstream test helpers, not of production behavior.

use std::any::Any;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::broker::{Broker, BrokerError, Cacheable, PresenceInfoOptions};
use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, Outcome, PresenceEvent, PresenceInfo,
    RemoteCommandMessage, SessionEnv, Status, StreamMessage,
};
use cabby_server::node::{Connection, ConnectionError};
use serde::Serialize;
use serde_json::{Value, json};

use super::go_types::{env_to_go, opt_env_to_go, stream_message_to_go};
use crate::sync::Watched;

/// Upstream `MockConnection.Read` timeout.
pub const MOCK_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// Deadline for gate and state waits in node tests.
pub const DEADLINE: Duration = Duration::from_secs(10);

// ------------------------------------------------------------------ gate

#[derive(Debug, Default)]
struct GateState {
    open: bool,
    held: usize,
    arrived: usize,
    passed: usize,
}

/// A barrier that holds calling threads until the test opens it.
#[derive(Clone)]
pub struct BlockingGate {
    state: Arc<Watched<GateState>>,
}

impl Default for BlockingGate {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BlockingGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.read(|s| f.debug_struct("BlockingGate").field("state", s).finish())
    }
}

impl BlockingGate {
    /// A closed gate.
    pub fn new() -> Self {
        Self { state: Arc::new(Watched::new(GateState::default())) }
    }

    /// An open gate (calls pass immediately but are still counted).
    pub fn opened() -> Self {
        let g = Self::new();
        g.open();
        g
    }

    pub fn open(&self) {
        self.state.update(|s| s.open = true);
    }

    pub fn held(&self) -> usize {
        self.state.read(|s| s.held)
    }

    pub fn arrived(&self) -> usize {
        self.state.read(|s| s.arrived)
    }

    pub fn passed(&self) -> usize {
        self.state.read(|s| s.passed)
    }

    /// Holds the caller until the gate opens; panics after a generous deadline.
    pub fn pass(&self) {
        self.state.update(|s| {
            s.arrived += 1;
            s.held += 1;
        });
        let opened = self.state.wait_until(Duration::from_secs(120), |s| s.open);
        self.state.update(|s| {
            s.held -= 1;
            s.passed += 1;
        });
        assert!(opened, "blocking gate: never opened");
    }

    /// Blocks until exactly `n` callers are held; panics after `deadline`.
    pub fn await_held(&self, n: usize, deadline: Duration) {
        assert!(self.state.wait_until(deadline, |s| s.held == n), "gate: {n} held call(s) expected, {self:?}");
    }

    /// Blocks until `n` calls have passed; panics after `deadline`.
    pub fn await_passed(&self, n: usize, deadline: Duration) {
        assert!(self.state.wait_until(deadline, |s| s.passed >= n), "gate: {n} passed call(s) expected, {self:?}");
    }
}

// ------------------------------------------------------------ connection

/// Upstream `mocks.MockConnection`: writes go to a 10-frame buffered channel
/// (blocking when full), `close` writes an empty frame, `read` returns the next
/// frame or fails after [`MOCK_READ_TIMEOUT`].
pub struct MockConnection {
    tx: SyncSender<Vec<u8>>,
    rx: Mutex<Receiver<Vec<u8>>>,
    /// When set, every write waits at this gate before reaching the channel.
    write_gate: Option<BlockingGate>,
}

impl MockConnection {
    pub fn new() -> Arc<Self> {
        Self::build(None)
    }

    /// Writes block at `gate` until it opens (stands in for upstream tests
    /// holding the session's write mutex).
    pub fn gated(gate: BlockingGate) -> Arc<Self> {
        Self::build(Some(gate))
    }

    fn build(write_gate: Option<BlockingGate>) -> Arc<Self> {
        let (tx, rx) = sync_channel(10);
        Arc::new(Self { tx, rx: Mutex::new(rx), write_gate })
    }

    fn push(&self, msg: &[u8]) {
        if let Some(g) = &self.write_gate {
            g.pass();
        }
        // The receiver lives as long as `self`.
        let _ = self.tx.send(msg.to_vec());
    }

    /// Upstream `ReadIndifinitely`, bounded by `deadline`.
    pub fn read_within(&self, deadline: Duration) -> Result<Vec<u8>, ConnectionError> {
        self.rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .recv_timeout(deadline)
            .map_err(|_| ConnectionError("connection hasn't received any messages".into()))
    }
}

impl Connection for MockConnection {
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        self.push(msg);
        Ok(())
    }

    fn write_binary(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        self.push(msg);
        Ok(())
    }

    fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }

    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        self.read_within(MOCK_READ_TIMEOUT)
    }

    fn close(&self, _code: i32, _reason: &str) {
        let _ = self.tx.send(Vec::new());
    }
}

// ------------------------------------------------------- mock controller

/// Upstream `mocks.MockController` (`mocks.NewMockController()`).
#[derive(Debug, Default)]
pub struct MockController {
    pub started: Mutex<bool>,
}

impl MockController {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { started: Mutex::new(true) })
    }

    /// Upstream `NewMockResult(sid)`: success with one transmission equal to the sid.
    fn mock_result(sid: &str) -> CommandResult {
        CommandResult {
            status: Status::Success,
            transmissions: vec![sid.to_owned()],
            disconnect: false,
            stop_all_streams: false,
            ..Default::default()
        }
    }
}

fn ok<T>(r: T) -> Outcome<T> {
    Outcome { result: Some(r), error: None }
}

impl Controller for MockController {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        *self.started.lock().unwrap() = false;
        Ok(())
    }

    /// `/failure`: FAILURE with "unauthorized"; `/error`: ERROR plus an error;
    /// otherwise identifiers from the `id` header, "welcome", and the
    /// `x-session-test` header as connection state `_s_`.
    fn authenticate(&self, _sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        let env = env.expect("mocks.MockController dereferences env");
        if env.url == "/failure" {
            return ok(ConnectResult {
                status: Status::Failure,
                transmissions: vec!["unauthorized".into()],
                ..Default::default()
            });
        }
        if env.url == "/error" {
            return Outcome {
                result: Some(ConnectResult { status: Status::Error, ..Default::default() }),
                error: Some(ControllerError::new("Unknown")),
            };
        }
        // Upstream dereferences `*env.Headers` (a nil map reads as empty).
        let header = |k: &str| env.headers.as_ref().and_then(|h| h.get(k)).cloned().unwrap_or_default();
        let mut res =
            ConnectResult { identifier: header("id"), transmissions: vec!["welcome".into()], ..Default::default() };
        if !header("x-session-test").is_empty() {
            res.cstate = Some([("_s_".to_owned(), header("x-session-test"))].into_iter().collect());
        }
        ok(res)
    }

    fn subscribe(&self, sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        if channel == "error" {
            return Outcome { result: None, error: Some(ControllerError::new("Subscription Failure")) };
        }
        let mut res = Self::mock_result(sid);
        if channel == "failure" {
            // Upstream's mock rejects with streams to start (ignored on a rejection).
            res.status = Status::Failure;
            res.stop_all_streams = true;
            res.streams = vec!["bad_stream".into()];
            return ok(res);
        }
        if channel == "disconnect" {
            res.disconnect = true;
            return ok(res);
        }
        if channel == "with_stream" {
            res.streams = vec!["stream".into()];
        }
        ok(res)
    }

    fn unsubscribe(&self, sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        if channel == "failure" {
            return Outcome { result: None, error: Some(ControllerError::new("Unsubscription Failure")) };
        }
        let mut res = Self::mock_result(sid);
        res.transmissions = Vec::new(); // upstream: nil
        ok(res)
    }

    fn perform(
        &self,
        sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        if channel == "failure" {
            return Outcome { result: None, error: Some(ControllerError::new("Perform Failure")) };
        }
        let mut res = Self::mock_result(sid);
        res.transmissions = vec![data.to_owned()];
        if data == "session" {
            res.cstate = Some([("_s_".to_owned(), "performed".to_owned())].into_iter().collect());
        }
        if data == "stop_stream" {
            res.stopped_streams = vec![data.to_owned()];
            res.transmissions = Vec::new();
        }
        if data == "stop_and_start_streams" {
            res.stop_all_streams = true;
            res.streams = vec!["all".into()];
            res.transmissions = Vec::new();
        }
        if data == "channel_state" {
            res.istate = Some([("_c_".to_owned(), "performed".to_owned())].into_iter().collect());
        }
        ok(res)
    }

    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        Ok(())
    }
}

// ------------------------------------------------------------ testify mock

/// An expectation argument (testify `mock.Anything` or an exact value).
#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    Any,
    Eq(Value),
}

/// `mock.Anything`.
pub fn any() -> Arg {
    Arg::Any
}

/// An exact argument (compared as JSON).
pub fn eq(v: impl Serialize) -> Arg {
    Arg::Eq(serde_json::to_value(v).expect("serializable mock argument"))
}

type Responder = Arc<dyn Fn(&[Value]) -> Box<dyn Any + Send> + Send + Sync>;

struct Expectation {
    method: String,
    args: Vec<Arg>,
    respond: Responder,
}

/// A recorded call.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub method: String,
    pub args: Vec<Value>,
}

/// The testify `mock.Mock` engine: `on` registers an expectation whose
/// responder plays both `Return` (its result) and `Run` (its side effects).
#[derive(Default)]
pub struct Mock {
    expectations: Mutex<Vec<Expectation>>,
    calls: Mutex<Vec<Call>>,
}

impl Mock {
    /// `On(method, args...).Return(...)`. `R` must be the method's return type.
    pub fn on<R: Any + Send>(
        &self,
        method: &str,
        args: Vec<Arg>,
        respond: impl Fn(&[Value]) -> R + Send + Sync + 'static,
    ) {
        self.expectations.lock().unwrap().push(Expectation {
            method: method.to_owned(),
            args,
            respond: Arc::new(move |a| Box::new(respond(a))),
        });
    }

    /// Records the call and answers with the first matching expectation (the
    /// responder runs without any mock lock held, so it may call back into the
    /// code under test). Unexpected calls panic.
    pub fn call<R: Any>(&self, method: &str, args: Vec<Value>) -> R {
        self.calls.lock().unwrap().push(Call { method: method.to_owned(), args: args.clone() });
        let respond = {
            let exps = self.expectations.lock().unwrap();
            exps.iter()
                .find(|e| {
                    e.method == method
                        && e.args.len() == args.len()
                        && e.args.iter().zip(&args).all(|(m, a)| match m {
                            Arg::Any => true,
                            Arg::Eq(v) => v == a,
                        })
                })
                .map(|e| e.respond.clone())
        };
        let respond = respond.unwrap_or_else(|| {
            panic!("mock: I don't know what to return because the method call was unexpected: {method}({args:?})")
        });
        *respond(&args).downcast::<R>().unwrap_or_else(|_| {
            panic!("mock: the {method} expectation returns the wrong type (want {})", std::any::type_name::<R>())
        })
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
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
}

/// testify `mocks.Broker`.
#[derive(Default)]
pub struct MockBroker {
    pub mock: Mock,
}

impl MockBroker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn on<R: Any + Send>(
        &self,
        method: &str,
        args: Vec<Arg>,
        respond: impl Fn(&[Value]) -> R + Send + Sync + 'static,
    ) {
        self.mock.on(method, args, respond);
    }
}

type BrokerResult<T> = Result<T, BrokerError>;

impl Broker for MockBroker {
    fn start(&self) -> BrokerResult<()> {
        self.mock.call("Start", vec![])
    }
    fn shutdown(&self) -> BrokerResult<()> {
        self.mock.call("Shutdown", vec![])
    }
    fn announce(&self) -> String {
        self.mock.call("Announce", vec![])
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> BrokerResult<()> {
        self.mock.call("HandleBroadcast", vec![stream_message_to_go(msg)])
    }
    fn handle_command(&self, msg: &RemoteCommandMessage) -> BrokerResult<()> {
        self.mock.call("HandleCommand", vec![json!({"command": msg.command, "payload": msg.payload})])
    }
    fn subscribe(&self, stream: &str) -> String {
        self.mock.call("Subscribe", vec![json!(stream)])
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.mock.call("Unsubscribe", vec![json!(stream)])
    }
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> BrokerResult<Vec<StreamMessage>> {
        self.mock.call("HistoryFrom", vec![json!(stream), json!(epoch), json!(offset)])
    }
    fn history_since(&self, stream: &str, ts: i64) -> BrokerResult<Option<Vec<StreamMessage>>> {
        self.mock.call("HistorySince", vec![json!(stream), json!(ts)])
    }
    fn commit_session(&self, sid: &str, _session: &dyn Cacheable) -> BrokerResult<()> {
        // The session argument is only ever matched with `mock.Anything`.
        self.mock.call("CommitSession", vec![json!(sid), Value::Null])
    }
    fn restore_session(&self, sid: &str) -> BrokerResult<Option<Vec<u8>>> {
        self.mock.call("RestoreSession", vec![json!(sid)])
    }
    fn touch_session(&self, sid: &str) -> BrokerResult<()> {
        self.mock.call("TouchSession", vec![json!(sid)])
    }
    fn delete_session(&self, sid: &str) -> BrokerResult<()> {
        self.mock.call("DeleteSession", vec![json!(sid)])
    }
    fn supports_presence(&self) -> bool {
        self.mock.call("SupportsPresence", vec![])
    }
    fn presence_add(&self, stream: &str, sid: &str, pid: &str, info: Value) -> BrokerResult<Option<PresenceEvent>> {
        self.mock.call("PresenceAdd", vec![json!(stream), json!(sid), json!(pid), info])
    }
    fn presence_remove(&self, stream: &str, sid: &str) -> BrokerResult<Option<PresenceEvent>> {
        self.mock.call("PresenceRemove", vec![json!(stream), json!(sid)])
    }
    fn presence_info(&self, stream: &str, _opts: Option<&PresenceInfoOptions>) -> BrokerResult<PresenceInfo> {
        self.mock.call("PresenceInfo", vec![json!(stream), Value::Null])
    }
    fn touch_presence(&self, sid: &str) -> BrokerResult<()> {
        self.mock.call("TouchPresence", vec![json!(sid)])
    }
    fn peak(&self, stream: &str) -> BrokerResult<Option<StreamMessage>> {
        self.mock.call("Peak", vec![json!(stream)])
    }
}

/// testify `mocks.Controller`. Arguments are recorded without the context:
/// `Authenticate [sid, env]`, `Subscribe/Unsubscribe [sid, env, ids, channel]`,
/// `Perform [sid, env, ids, channel, data]`, `Disconnect [sid, env, ids, subscriptions]`;
/// `env` is the bridge JSON shape of `common.SessionEnv` (`null` for nil).
#[derive(Default)]
pub struct MockNodeController {
    pub mock: Mock,
}

pub type ControllerOutcome<T> = Outcome<T>;

impl MockNodeController {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn on<R: Any + Send>(
        &self,
        method: &str,
        args: Vec<Arg>,
        respond: impl Fn(&[Value]) -> R + Send + Sync + 'static,
    ) {
        self.mock.on(method, args, respond);
    }
}

impl Controller for MockNodeController {
    fn start(&self) -> Result<(), ControllerError> {
        self.mock.call("Start", vec![])
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        self.mock.call("Shutdown", vec![])
    }
    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        self.mock.call("Authenticate", vec![json!(sid), opt_env_to_go(env)])
    }
    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.mock.call("Subscribe", vec![json!(sid), opt_env_to_go(env), json!(ids), json!(channel)])
    }
    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.mock.call("Unsubscribe", vec![json!(sid), opt_env_to_go(env), json!(ids), json!(channel)])
    }
    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        self.mock.call("Perform", vec![json!(sid), opt_env_to_go(env), json!(ids), json!(channel), json!(data)])
    }
    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        self.mock.call("Disconnect", vec![json!(sid), opt_env_to_go(env), json!(ids), json!(subscriptions)])
    }
}

/// Reads a connection or channel state field from a recorded `env` argument
/// (what upstream responders do with `env.GetConnectionStateField`).
pub fn env_arg_connection_state(env: &Value, field: &str) -> String {
    env["connection_state"][field].as_str().unwrap_or_default().to_owned()
}

pub fn env_arg_channel_state(env: &Value, id: &str, field: &str) -> String {
    env["channel_states"][id][field].as_str().unwrap_or_default().to_owned()
}

/// The JSON shape a [`SessionEnv`] has as a recorded mock argument.
pub fn env_arg(env: &SessionEnv) -> Value {
    env_to_go(env)
}

/// Decodes a recorded `StreamMessage` argument (e.g. of `HandleBroadcast`).
pub fn stream_message_arg(v: &Value) -> StreamMessage {
    super::go_types::stream_message_from_go(v)
}
