//! Facade for upstream package `router`.
//!
//! Upstream tests drive `RouterController` with testify `mocks.Controller`s. Here a
//! test describes the mocks' expectations and one call ([`Scenario`]); the Rust arm
//! runs it with [`MockController`]s, the Go arm runs it in the bridge with the
//! upstream mocks, and both report every call each mock received.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cabby_core::common::{CommandResult, ConnectResult, Outcome as CoreOutcome, SessionEnv};
use cabby_core::router::{Controller, ControllerError, RouterController};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::reference::common::{GoCommandResult, GoConnectResult, GoSessionEnv};
use crate::{Backend, backend, bridge};

/// One controller method call (the env is the scenario's env; context is implicit).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Call {
    pub method: String,
    pub sid: String,
    #[serde(rename = "ID")]
    pub id: String,
    pub channel: String,
    pub data: String,
    pub subscriptions: Vec<String>,
}

impl Call {
    pub fn start() -> Self {
        Call { method: "Start".into(), ..Call::default() }
    }
    pub fn shutdown() -> Self {
        Call { method: "Shutdown".into(), ..Call::default() }
    }
    pub fn authenticate(sid: &str) -> Self {
        Call { method: "Authenticate".into(), sid: sid.into(), ..Call::default() }
    }
    pub fn subscribe(sid: &str, id: &str, channel: &str) -> Self {
        Call { method: "Subscribe".into(), sid: sid.into(), id: id.into(), channel: channel.into(), ..Call::default() }
    }
    pub fn unsubscribe(sid: &str, id: &str, channel: &str) -> Self {
        Call { method: "Unsubscribe".into(), ..Self::subscribe(sid, id, channel) }
    }
    pub fn perform(sid: &str, id: &str, channel: &str, data: &str) -> Self {
        Call { method: "Perform".into(), data: data.into(), ..Self::subscribe(sid, id, channel) }
    }
    pub fn disconnect(sid: &str, id: &str, subscriptions: &[&str]) -> Self {
        Call {
            method: "Disconnect".into(),
            sid: sid.into(),
            id: id.into(),
            subscriptions: subscriptions.iter().map(|s| s.to_string()).collect(),
            ..Call::default()
        }
    }
}

/// What a mock returns for a matching call (`On(...).Return(...)`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Return {
    pub connect_result: Option<ConnectResult>,
    pub command_result: Option<CommandResult>,
    pub error: Option<String>,
}

impl Return {
    pub fn ok() -> Self {
        Return::default()
    }
    pub fn connect(r: ConnectResult) -> Self {
        Return { connect_result: Some(r), ..Return::default() }
    }
    pub fn command(r: CommandResult) -> Self {
        Return { command_result: Some(r), ..Return::default() }
    }
    pub fn error(msg: &str) -> Self {
        Return { error: Some(msg.into()), ..Return::default() }
    }
    /// `Return(nil, errors.New(msg))`.
    pub fn nil_with_error(msg: &str) -> Self {
        Self::error(msg)
    }
}

/// Which mock an expectation belongs to: `""` is the default controller, otherwise
/// the routed channel name.
pub type ControllerName = String;

pub struct Scenario {
    pub env: SessionEnv,
    /// Channels routed to their own mock controller, in registration order.
    pub routes: Vec<String>,
    pub expectations: Vec<(ControllerName, Call, Return)>,
    /// The call to make on the router (`None`: only build and route).
    pub call: Option<Call>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Outcome {
    /// `subject.Route(name, mock)` errors, one per route.
    pub route_errors: Vec<Option<String>>,
    pub connect_result: Option<ConnectResult>,
    pub command_result: Option<CommandResult>,
    pub error: Option<String>,
    /// Calls received by each mock (`""`: default controller), in order. Every call
    /// carried the scenario's env (checked when collecting).
    pub calls: BTreeMap<ControllerName, Vec<Call>>,
}

impl Outcome {
    /// testify `AssertCalled`: the mock received this call.
    pub fn called(&self, controller: &str, call: &Call) -> bool {
        self.calls.get(controller).is_some_and(|calls| calls.contains(call))
    }
}

pub fn run(s: &Scenario) -> Outcome {
    match backend() {
        Backend::Rust => run_rust(s),
        Backend::Go => run_go(s),
    }
}

// --- Rust arm ---------------------------------------------------------------------

/// A testify-style mock controller: answers calls from its expectations (the first
/// registered match) and records every call. A call without a matching expectation
/// panics, as testify does.
pub struct MockController {
    env: SessionEnv,
    expectations: Vec<(Call, Return)>,
    calls: Mutex<Vec<(Call, SessionEnv)>>,
}

impl MockController {
    pub fn new(env: SessionEnv, expectations: Vec<(Call, Return)>) -> Self {
        MockController { env, expectations, calls: Mutex::new(Vec::new()) }
    }

    fn answer(&self, call: Call, env: &SessionEnv) -> Return {
        self.calls.lock().unwrap().push((call.clone(), env.clone()));
        let uses_env = !matches!(call.method.as_str(), "Start" | "Shutdown");
        self.expectations
            .iter()
            .find(|(c, _)| *c == call && (!uses_env || *env == self.env))
            .map(|(_, r)| r.clone())
            .unwrap_or_else(|| panic!("mock: no expectation matches {call:?}"))
    }

    fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(c, env)| {
                if !matches!(c.method.as_str(), "Start" | "Shutdown") {
                    assert_eq!(*env, self.env, "mock call {c:?} received another env");
                }
                c.clone()
            })
            .collect()
    }
}

fn unit(r: Return) -> Result<(), ControllerError> {
    r.error.map_or(Ok(()), |e| Err(ControllerError::new(e)))
}

/// testify `Return(result, err)`: both values are returned, as in Go.
fn command(r: Return) -> CoreOutcome<CommandResult> {
    CoreOutcome { result: r.command_result, error: r.error.map(ControllerError::new) }
}

/// The router passes the caller's env through; the mocks expect one.
fn env_or_default(env: Option<&SessionEnv>) -> SessionEnv {
    env.cloned().unwrap_or_default()
}

impl Controller for MockController {
    fn start(&self) -> Result<(), ControllerError> {
        unit(self.answer(Call::start(), &SessionEnv::default()))
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        unit(self.answer(Call::shutdown(), &SessionEnv::default()))
    }

    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> CoreOutcome<ConnectResult> {
        let r = self.answer(Call::authenticate(sid), &env_or_default(env));
        CoreOutcome { result: r.connect_result, error: r.error.map(ControllerError::new) }
    }

    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, id: &str, channel: &str) -> CoreOutcome<CommandResult> {
        command(self.answer(Call::subscribe(sid, id, channel), &env_or_default(env)))
    }

    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, id: &str, channel: &str) -> CoreOutcome<CommandResult> {
        command(self.answer(Call::unsubscribe(sid, id, channel), &env_or_default(env)))
    }

    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        id: &str,
        channel: &str,
        data: &str,
    ) -> CoreOutcome<CommandResult> {
        command(self.answer(Call::perform(sid, id, channel, data), &env_or_default(env)))
    }

    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        id: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        let subs: Vec<&str> = subscriptions.iter().map(String::as_str).collect();
        unit(self.answer(Call::disconnect(sid, id, &subs), &env_or_default(env)))
    }
}

fn run_rust(s: &Scenario) -> Outcome {
    let mock = |name: &str| {
        let exp = s.expectations.iter().filter(|(c, ..)| c == name).map(|(_, call, r)| (call.clone(), r.clone()));
        Arc::new(MockController::new(s.env.clone(), exp.collect()))
    };
    let mut mocks = vec![(String::new(), mock(""))];
    let mut subject = RouterController::new(mocks[0].1.clone());
    let mut out = Outcome::default();
    for name in &s.routes {
        let m = mock(name);
        out.route_errors.push(subject.route(name, m.clone()).err().map(|e| e.message));
        mocks.push((name.clone(), m));
    }
    let env = Some(&s.env);
    let Some(c) = &s.call else {
        out.calls = mocks.into_iter().map(|(n, m)| (n, m.calls())).collect();
        return out;
    };
    let err = match c.method.as_str() {
        "Start" => subject.start().err(),
        "Shutdown" => subject.shutdown().err(),
        "Authenticate" => {
            let res = subject.authenticate(&c.sid, env);
            out.connect_result = res.result;
            res.error
        }
        "Subscribe" | "Unsubscribe" | "Perform" => {
            let res = match c.method.as_str() {
                "Subscribe" => subject.subscribe(&c.sid, env, &c.id, &c.channel),
                "Unsubscribe" => subject.unsubscribe(&c.sid, env, &c.id, &c.channel),
                _ => subject.perform(&c.sid, env, &c.id, &c.channel, &c.data),
            };
            out.command_result = res.result;
            res.error
        }
        "Disconnect" => subject.disconnect(&c.sid, env, &c.id, &c.subscriptions).err(),
        other => panic!("unknown method {other}"),
    };
    out.error = err.map(|e| e.message);
    out.calls = mocks.into_iter().map(|(n, m)| (n, m.calls())).collect();
    out
}

// --- Go arm -----------------------------------------------------------------------

fn run_go(s: &Scenario) -> Outcome {
    let expectations: Vec<Value> = s
        .expectations
        .iter()
        .map(|(name, call, r)| {
            json!({
                "Controller": name,
                "Call": call,
                "ConnectResult": r.connect_result.as_ref().map(GoConnectResult::from),
                "CommandResult": r.command_result.as_ref().map(GoCommandResult::from),
                "Error": r.error,
            })
        })
        .collect();
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Out {
        route_errors: Vec<Option<String>>,
        connect_result: Option<GoConnectResult>,
        command_result: Option<GoCommandResult>,
        error: Option<String>,
        calls: BTreeMap<String, Vec<BTreeMap<String, Value>>>,
    }
    let out: Out = bridge::call(
        "router.Scenario",
        json!({
            "Env": GoSessionEnv::from(&s.env),
            "Routes": s.routes,
            "Expectations": expectations,
            "Call": s.call,
        }),
    );
    let str_at = |rec: &BTreeMap<String, Value>, i: &str| rec.get(i).and_then(Value::as_str).unwrap_or("").to_owned();
    let calls = out
        .calls
        .into_iter()
        .map(|(name, recs)| {
            let calls = recs
                .into_iter()
                .map(|rec| {
                    if let Some(env) = rec.get("Env") {
                        let env: SessionEnv =
                            serde_json::from_value::<GoSessionEnv>(env.clone()).expect("Go session env").into();
                        assert_eq!(env, s.env, "mock call received another env");
                    }
                    let method = str_at(&rec, "Method");
                    // Positional testify arguments: 0 ctx, 1 sid, 2 env, 3 ids, 4 channel/subscriptions, 5 data.
                    let mut call = Call { method: method.clone(), sid: str_at(&rec, "1"), ..Call::default() };
                    match method.as_str() {
                        "Subscribe" | "Unsubscribe" | "Perform" => {
                            call.id = str_at(&rec, "3");
                            call.channel = str_at(&rec, "4");
                            call.data = str_at(&rec, "5");
                        }
                        "Disconnect" => {
                            call.id = str_at(&rec, "3");
                            call.subscriptions = serde_json::from_value(rec.get("4").cloned().unwrap_or(json!([])))
                                .expect("subscriptions");
                        }
                        _ => {}
                    }
                    call
                })
                .collect();
            (name, calls)
        })
        .collect();
    Outcome {
        route_errors: out.route_errors,
        connect_result: out.connect_result.map(Into::into),
        command_result: out.command_result.map(Into::into),
        error: out.error,
        calls,
    }
}
