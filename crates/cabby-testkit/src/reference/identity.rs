//! Facade for upstream package `identity`.
//!
//! Upstream tests use testify mocks of `node.Controller` and `identity.Identifier`.
//! [`MockController`] and [`MockIdentifier`] are their data-driven equivalents:
//! expectations are `(call, return)` pairs. Under the Rust backend they implement
//! the Cabby traits directly; under Go the facade ships the same expectations to
//! the bridge, which wraps them in Go doubles of the upstream interfaces and
//! reports the calls they received, which are then recorded on these mocks.

use std::sync::{Arc, Mutex};

use cabby_core::common::{CommandResult, ConnectResult, Controller, ControllerError, Outcome, SessionEnv};
use cabby_core::identity::{
    self as rs, ConfigError, IdentifierPipeline, JwtAlgo, JwtConfig, JwtIdentifier, PublicIdentifier,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::go_types::*;
use crate::{Backend, backend, bridge};

// ------------------------------------------------------------------ mocks

/// A call received by a controller (upstream methods minus the `context.Context`).
#[derive(Debug, Clone, PartialEq)]
pub enum ControllerCall {
    Start,
    Shutdown,
    Authenticate { sid: String, env: Option<SessionEnv> },
    Subscribe { sid: String, env: Option<SessionEnv>, ids: String, channel: String },
    Unsubscribe { sid: String, env: Option<SessionEnv>, ids: String, channel: String },
    Perform { sid: String, env: Option<SessionEnv>, ids: String, channel: String, data: String },
    Disconnect { sid: String, env: Option<SessionEnv>, ids: String, subscriptions: Vec<String> },
}

/// What a mocked controller method returns.
#[derive(Debug, Clone, PartialEq)]
pub enum ControllerReturn {
    Unit(Result<(), ControllerError>),
    Connect(Outcome<ConnectResult>),
    Command(Outcome<CommandResult>),
}

impl ControllerCall {
    fn method(&self) -> &'static str {
        match self {
            Self::Start => "Start",
            Self::Shutdown => "Shutdown",
            Self::Authenticate { .. } => "Authenticate",
            Self::Subscribe { .. } => "Subscribe",
            Self::Unsubscribe { .. } => "Unsubscribe",
            Self::Perform { .. } => "Perform",
            Self::Disconnect { .. } => "Disconnect",
        }
    }

    fn to_go(&self) -> Value {
        let args = match self {
            Self::Start | Self::Shutdown => json!({}),
            Self::Authenticate { sid, env } => json!({"sid": sid, "env": opt_env_to_go(env.as_ref())}),
            Self::Subscribe { sid, env, ids, channel } | Self::Unsubscribe { sid, env, ids, channel } => {
                json!({"sid": sid, "env": opt_env_to_go(env.as_ref()), "ids": ids, "channel": channel})
            }
            Self::Perform { sid, env, ids, channel, data } => json!({
                "sid": sid, "env": opt_env_to_go(env.as_ref()), "ids": ids, "channel": channel, "data": data,
            }),
            Self::Disconnect { sid, env, ids, subscriptions } => json!({
                "sid": sid, "env": opt_env_to_go(env.as_ref()), "ids": ids, "subscriptions": subscriptions,
            }),
        };
        json!({"method": self.method(), "args": args})
    }

    fn from_go(v: &Value) -> Self {
        let a = &v["args"];
        let s = |k: &str| a[k].as_str().unwrap_or_default().to_owned();
        let env = opt_env_from_go(&a["env"]);
        match v["method"].as_str().unwrap_or_default() {
            "Start" => Self::Start,
            "Shutdown" => Self::Shutdown,
            "Authenticate" => Self::Authenticate { sid: s("sid"), env },
            "Subscribe" => Self::Subscribe { sid: s("sid"), env, ids: s("ids"), channel: s("channel") },
            "Unsubscribe" => Self::Unsubscribe { sid: s("sid"), env, ids: s("ids"), channel: s("channel") },
            "Perform" => Self::Perform { sid: s("sid"), env, ids: s("ids"), channel: s("channel"), data: s("data") },
            "Disconnect" => Self::Disconnect {
                sid: s("sid"),
                env,
                ids: s("ids"),
                subscriptions: serde_json::from_value(a["subscriptions"].clone()).unwrap_or_default(),
            },
            other => panic!("unknown controller method {other}"),
        }
    }
}

impl ControllerReturn {
    fn to_go(&self) -> Value {
        match self {
            Self::Unit(r) => json!({"result": null, "error": error_to_go(r)}),
            Self::Connect(o) => outcome_to_go(o, connect_result_to_go),
            Self::Command(o) => outcome_to_go(o, command_result_to_go),
        }
    }
}

/// testify-style mock of `node.Controller`: unexpected calls panic.
#[derive(Default)]
pub struct MockController {
    expectations: Mutex<Vec<(ControllerCall, ControllerReturn)>>,
    calls: Mutex<Vec<ControllerCall>>,
}

impl MockController {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `mock.On(method, args...).Return(ret)`.
    pub fn on(&self, call: ControllerCall, ret: ControllerReturn) {
        self.expectations.lock().unwrap().push((call, ret));
    }

    pub fn assert_called(&self, call: &ControllerCall) {
        let calls = self.calls.lock().unwrap();
        assert!(calls.contains(call), "expected call {call:?}; calls: {calls:?}");
    }

    pub fn assert_not_called(&self, call: &ControllerCall) {
        let calls = self.calls.lock().unwrap();
        assert!(!calls.contains(call), "unexpected call {call:?}");
    }

    /// Records a call and returns the first matching expectation's value.
    fn answer(&self, call: ControllerCall) -> ControllerReturn {
        let ret = self.expectations.lock().unwrap().iter().find(|(c, _)| *c == call).map(|(_, r)| r.clone());
        self.calls.lock().unwrap().push(call.clone());
        ret.unwrap_or_else(|| panic!("mock: unexpected controller call {call:?}"))
    }

    fn expectations_to_go(&self) -> Value {
        let exps = self.expectations.lock().unwrap();
        Value::Array(
            exps.iter()
                .map(|(c, r)| {
                    let mut v = c.to_go();
                    v["returns"] = r.to_go();
                    v
                })
                .collect(),
        )
    }

    fn replay(&self, calls: &Value) {
        for c in calls.as_array().into_iter().flatten() {
            self.answer(ControllerCall::from_go(c));
        }
    }
}

fn unit(r: ControllerReturn) -> Result<(), ControllerError> {
    match r {
        ControllerReturn::Unit(r) => r,
        other => panic!("mock: expected a unit return, got {other:?}"),
    }
}

fn connect(r: ControllerReturn) -> Outcome<ConnectResult> {
    match r {
        ControllerReturn::Connect(o) => o,
        other => panic!("mock: expected a connect return, got {other:?}"),
    }
}

fn command(r: ControllerReturn) -> Outcome<CommandResult> {
    match r {
        ControllerReturn::Command(o) => o,
        other => panic!("mock: expected a command return, got {other:?}"),
    }
}

fn owned(env: Option<&SessionEnv>) -> Option<SessionEnv> {
    env.cloned()
}

impl Controller for MockController {
    fn start(&self) -> Result<(), ControllerError> {
        unit(self.answer(ControllerCall::Start))
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        unit(self.answer(ControllerCall::Shutdown))
    }
    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        connect(self.answer(ControllerCall::Authenticate { sid: sid.into(), env: owned(env) }))
    }
    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        command(self.answer(ControllerCall::Subscribe {
            sid: sid.into(),
            env: owned(env),
            ids: ids.into(),
            channel: channel.into(),
        }))
    }
    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        command(self.answer(ControllerCall::Unsubscribe {
            sid: sid.into(),
            env: owned(env),
            ids: ids.into(),
            channel: channel.into(),
        }))
    }
    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        command(self.answer(ControllerCall::Perform {
            sid: sid.into(),
            env: owned(env),
            ids: ids.into(),
            channel: channel.into(),
            data: data.into(),
        }))
    }
    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        unit(self.answer(ControllerCall::Disconnect {
            sid: sid.into(),
            env: owned(env),
            ids: ids.into(),
            subscriptions: subscriptions.to_vec(),
        }))
    }
}

/// An `Identify(sid, env)` expectation and its return value.
type IdentifyExpectation = ((String, SessionEnv), Outcome<ConnectResult>);

/// testify-style mock of `identity.Identifier`: unexpected calls panic.
#[derive(Default)]
pub struct MockIdentifier {
    expectations: Mutex<Vec<IdentifyExpectation>>,
    calls: Mutex<Vec<(String, SessionEnv)>>,
}

impl MockIdentifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `mock.On("Identify", sid, env).Return(result, err)`.
    pub fn on(&self, sid: &str, env: &SessionEnv, ret: Outcome<ConnectResult>) {
        self.expectations.lock().unwrap().push(((sid.into(), env.clone()), ret));
    }

    pub fn assert_called(&self, sid: &str, env: &SessionEnv) {
        let calls = self.calls.lock().unwrap();
        assert!(calls.contains(&(sid.into(), env.clone())), "expected Identify({sid:?}, ..)");
    }

    fn answer(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
        let key = (sid.to_owned(), env.clone());
        let ret = self.expectations.lock().unwrap().iter().find(|(c, _)| *c == key).map(|(_, r)| r.clone());
        self.calls.lock().unwrap().push(key);
        ret.unwrap_or_else(|| panic!("mock: unexpected Identify({sid:?}, {env:?})"))
    }

    fn expectations_to_go(&self) -> Value {
        let exps = self.expectations.lock().unwrap();
        Value::Array(
            exps.iter()
                .map(|((sid, env), r)| {
                    json!({"method": "Identify", "args": {"sid": sid, "env": env_to_go(env)},
                           "returns": outcome_to_go(r, connect_result_to_go)})
                })
                .collect(),
        )
    }

    fn replay(&self, calls: &Value) {
        for c in calls.as_array().into_iter().flatten() {
            let env = opt_env_from_go(&c["args"]["env"]).expect("Identify env");
            self.answer(c["args"]["sid"].as_str().unwrap_or_default(), &env);
        }
    }
}

impl rs::Identifier for MockIdentifier {
    fn identify(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
        self.answer(sid, env)
    }
}

// ------------------------------------------------------- IdentifiableController

#[derive(Deserialize)]
struct GoControllerReply {
    #[serde(flatten)]
    outcome: GoOutcome,
    #[serde(default)]
    controller_calls: Value,
    #[serde(default)]
    identifier_calls: Value,
}

/// `identity.NewIdentifiableController(controller, identifier)`.
pub struct IdentifiableController {
    controller: Arc<MockController>,
    identifier: Arc<MockIdentifier>,
    rust: rs::IdentifiableController,
}

pub fn new_identifiable_controller(
    controller: &Arc<MockController>,
    identifier: &Arc<MockIdentifier>,
) -> IdentifiableController {
    IdentifiableController {
        controller: controller.clone(),
        identifier: identifier.clone(),
        rust: rs::IdentifiableController::new(controller.clone(), identifier.clone()),
    }
}

impl IdentifiableController {
    fn go(&self, call: ControllerCall) -> GoOutcome {
        let reply: GoControllerReply = bridge::call(
            "identity.IdentifiableController",
            json!({
                "controller": self.controller.expectations_to_go(),
                "identifier": self.identifier.expectations_to_go(),
                "call": call.to_go(),
            }),
        );
        self.controller.replay(&reply.controller_calls);
        self.identifier.replay(&reply.identifier_calls);
        reply.outcome
    }

    fn go_unit(&self, call: ControllerCall) -> Result<(), ControllerError> {
        match self.go(call).error {
            None => Ok(()),
            Some(e) => Err(ControllerError::new(e)),
        }
    }

    pub fn start(&self) -> Result<(), ControllerError> {
        match backend() {
            Backend::Rust => self.rust.start(),
            Backend::Go => self.go_unit(ControllerCall::Start),
        }
    }

    pub fn shutdown(&self) -> Result<(), ControllerError> {
        match backend() {
            Backend::Rust => self.rust.shutdown(),
            Backend::Go => self.go_unit(ControllerCall::Shutdown),
        }
    }

    pub fn authenticate(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
        match backend() {
            Backend::Rust => self.rust.authenticate(sid, Some(env)),
            Backend::Go => self
                .go(ControllerCall::Authenticate { sid: sid.into(), env: Some(env.clone()) })
                .map(connect_result_from_go),
        }
    }

    pub fn subscribe(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str) -> Outcome<CommandResult> {
        match backend() {
            Backend::Rust => self.rust.subscribe(sid, Some(env), ids, channel),
            Backend::Go => self
                .go(ControllerCall::Subscribe {
                    sid: sid.into(),
                    env: Some(env.clone()),
                    ids: ids.into(),
                    channel: channel.into(),
                })
                .map(command_result_from_go),
        }
    }

    pub fn unsubscribe(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str) -> Outcome<CommandResult> {
        match backend() {
            Backend::Rust => self.rust.unsubscribe(sid, Some(env), ids, channel),
            Backend::Go => self
                .go(ControllerCall::Unsubscribe {
                    sid: sid.into(),
                    env: Some(env.clone()),
                    ids: ids.into(),
                    channel: channel.into(),
                })
                .map(command_result_from_go),
        }
    }

    pub fn perform(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str, data: &str) -> Outcome<CommandResult> {
        match backend() {
            Backend::Rust => self.rust.perform(sid, Some(env), ids, channel, data),
            Backend::Go => self
                .go(ControllerCall::Perform {
                    sid: sid.into(),
                    env: Some(env.clone()),
                    ids: ids.into(),
                    channel: channel.into(),
                    data: data.into(),
                })
                .map(command_result_from_go),
        }
    }

    pub fn disconnect(
        &self,
        sid: &str,
        env: &SessionEnv,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        match backend() {
            Backend::Rust => self.rust.disconnect(sid, Some(env), ids, subscriptions),
            Backend::Go => self.go_unit(ControllerCall::Disconnect {
                sid: sid.into(),
                env: Some(env.clone()),
                ids: ids.into(),
                subscriptions: subscriptions.to_vec(),
            }),
        }
    }
}

// ------------------------------------------------------- IdentifierPipeline

/// `identity.NewIdentifierPipeline(mocked, identity.NewPublicIdentifier()).Identify(sid, env)`.
pub fn pipeline_with_public_identify(
    mocked: &Arc<MockIdentifier>,
    sid: &str,
    env: &SessionEnv,
) -> Outcome<ConnectResult> {
    match backend() {
        Backend::Rust => {
            let pipe = IdentifierPipeline::new(vec![mocked.clone(), Arc::new(PublicIdentifier::new())]);
            rs::Identifier::identify(&pipe, sid, env)
        }
        Backend::Go => {
            let reply: GoControllerReply = bridge::call(
                "identity.IdentifierPipeline.MockThenPublic",
                json!({"identifier": mocked.expectations_to_go(), "sid": sid, "env": env_to_go(env)}),
            );
            mocked.replay(&reply.identifier_calls);
            reply.outcome.map(connect_result_from_go)
        }
    }
}

// ------------------------------------------------------------------ JWT

fn jwt_config_to_go(c: &JwtConfig) -> Value {
    let algo = match c.algo {
        JwtAlgo::Hs256 => "HS256",
    };
    json!({"Secret": c.secret, "Param": c.param, "Algo": algo, "Force": c.force})
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoJwtConfig {
    secret: String,
    param: String,
    algo: Option<GoSigningMethod>,
    force: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoSigningMethod {
    name: String,
}

impl From<GoJwtConfig> for JwtConfig {
    fn from(g: GoJwtConfig) -> Self {
        let algo = match g.algo.map(|a| a.name).as_deref() {
            Some("HS256") => JwtAlgo::Hs256,
            other => panic!("unsupported Go JWT algorithm {other:?}"),
        };
        JwtConfig { secret: g.secret, param: g.param, algo, force: g.force }
    }
}

/// `identity.NewJWTConfig(secret)`.
pub fn new_jwt_config(secret: &str) -> JwtConfig {
    match backend() {
        Backend::Rust => JwtConfig::new(secret),
        Backend::Go => bridge::call::<_, GoJwtConfig>("identity.NewJWTConfig", json!({"Secret": secret})).into(),
    }
}

/// `conf.ToToml()`.
pub fn jwt_config_to_toml(conf: &JwtConfig) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("identity.JWTConfig.ToToml", json!({"Config": jwt_config_to_go(conf)})),
    }
}

#[derive(Deserialize)]
struct GoDecoded<C> {
    config: C,
    error: Option<String>,
}

/// `toml.Decode(s, &conf)` (BurntSushi/toml over the upstream struct tags).
pub fn jwt_config_merge_toml(conf: &mut JwtConfig, toml: &str) -> Result<(), ConfigError> {
    match backend() {
        Backend::Rust => conf.merge_toml(toml),
        Backend::Go => {
            let d: GoDecoded<GoJwtConfig> =
                bridge::call("identity.JWTConfig.DecodeToml", json!({"Config": jwt_config_to_go(conf), "Toml": toml}));
            *conf = d.config.into();
            d.error.map_or(Ok(()), |message| Err(ConfigError { message }))
        }
    }
}

/// `identity.NewJWTIdentifier(&conf, logger).Identify(sid, env)`.
pub fn jwt_identify(conf: &JwtConfig, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult> {
    match backend() {
        Backend::Rust => rs::Identifier::identify(&JwtIdentifier::new(conf), sid, env),
        Backend::Go => bridge::call::<_, GoOutcome>(
            "identity.JWTIdentifier.Identify",
            json!({"Config": jwt_config_to_go(conf), "Sid": sid, "Env": env_to_go(env)}),
        )
        .map(connect_result_from_go),
    }
}
