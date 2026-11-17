//! Facade for upstream package `streams`.
//!
//! Controllers are stateless, so the Go arm rebuilds the controller from its
//! constructor arguments on every call.

use cabby_core::common::{CommandResult, Controller, Outcome, SessionEnv};
use cabby_core::streams::{Config, ConfigError, StreamsController, SubscribeRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::go_types::*;
use crate::{Backend, backend, bridge};

/// `streams.Config` as encoding/json writes it (exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    secret: String,
    public: bool,
    whisper: bool,
    presence: bool,
    #[serde(rename = "PubSubChannel")]
    pubsub_channel: String,
    turbo: bool,
    turbo_secret: String,
    cable_ready: bool,
    cable_ready_secret: String,
}

impl From<&Config> for GoConfig {
    fn from(c: &Config) -> Self {
        GoConfig {
            secret: c.secret.clone(),
            public: c.public,
            whisper: c.whisper,
            presence: c.presence,
            pubsub_channel: c.pubsub_channel.clone(),
            turbo: c.turbo,
            turbo_secret: c.turbo_secret.clone(),
            cable_ready: c.cable_ready,
            cable_ready_secret: c.cable_ready_secret.clone(),
        }
    }
}

impl From<GoConfig> for Config {
    fn from(g: GoConfig) -> Self {
        Config {
            secret: g.secret,
            public: g.public,
            whisper: g.whisper,
            presence: g.presence,
            pubsub_channel: g.pubsub_channel,
            turbo: g.turbo,
            turbo_secret: g.turbo_secret,
            cable_ready: g.cable_ready,
            cable_ready_secret: g.cable_ready_secret,
        }
    }
}

/// `streams.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoConfig>("streams.NewConfig", json!({})).into(),
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(conf: &Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("streams.Config.ToToml", json!({"Config": GoConfig::from(conf)})),
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
                bridge::call("streams.Config.DecodeToml", json!({"Config": GoConfig::from(&*conf), "Toml": toml}));
            *conf = d.config.into();
            d.error.map_or(Ok(()), |message| Err(ConfigError { message }))
        }
    }
}

/// How a controller was built (replayed on the Go side for each call).
#[derive(Clone)]
enum Ctor {
    /// `NewController(key, resolver)` with a resolver returning a fixed request.
    Fixed {
        key: String,
        request: SubscribeRequest,
    },
    Streams(Config),
    Turbo(String),
    CableReady(String),
}

impl Ctor {
    fn to_go(&self) -> Value {
        match self {
            Ctor::Fixed { key, request } => {
                // `whisper`/`presence` are unexported upstream and cannot be set
                // from outside the package.
                assert!(!request.whisper && !request.presence, "Go cannot set SubscribeRequest.whisper/presence");
                json!({"kind": "fixed", "key": key,
                       "request": {"stream_name": request.stream_name, "signed_stream_name": request.signed_stream_name}})
            }
            Ctor::Streams(c) => json!({"kind": "streams", "config": GoConfig::from(c)}),
            Ctor::Turbo(key) => json!({"kind": "turbo", "key": key}),
            Ctor::CableReady(key) => json!({"kind": "cable_ready", "key": key}),
        }
    }
}

/// A `streams.Controller` under test.
pub struct Subject {
    ctor: Ctor,
    rust: Option<StreamsController>,
}

fn build(ctor: Ctor) -> Subject {
    let rust = match backend() {
        Backend::Go => None,
        Backend::Rust => Some(match &ctor {
            Ctor::Fixed { key, request } => {
                let request = request.clone();
                StreamsController::new(key, Box::new(move |_| Ok(request.clone())))
            }
            Ctor::Streams(c) => StreamsController::for_config(c),
            Ctor::Turbo(key) => StreamsController::turbo(key),
            Ctor::CableReady(key) => StreamsController::cable_ready(key),
        }),
    };
    Subject { ctor, rust }
}

/// `streams.NewController(key, func(string) (*SubscribeRequest, error) { return &request, nil })`.
pub fn new_controller_with_fixed_request(key: &str, request: SubscribeRequest) -> Subject {
    build(Ctor::Fixed { key: key.into(), request })
}

/// `streams.NewStreamsController(&conf)`.
pub fn new_streams_controller(conf: &Config) -> Subject {
    build(Ctor::Streams(conf.clone()))
}

/// `streams.NewTurboController(key)`.
pub fn new_turbo_controller(key: &str) -> Subject {
    build(Ctor::Turbo(key.into()))
}

/// `streams.NewCableReadyController(key)`.
pub fn new_cable_ready_controller(key: &str) -> Subject {
    build(Ctor::CableReady(key.into()))
}

impl Subject {
    fn go(
        &self,
        method: &str,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        identifier: &str,
    ) -> Outcome<CommandResult> {
        bridge::call::<_, GoOutcome>(
            "streams.Controller",
            json!({"ctor": self.ctor.to_go(), "method": method, "sid": sid, "env": opt_env_to_go(env),
                   "ids": ids, "identifier": identifier}),
        )
        .map(command_result_from_go)
    }

    pub fn subscribe(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        identifier: &str,
    ) -> Outcome<CommandResult> {
        match &self.rust {
            Some(c) => c.subscribe(sid, env, ids, identifier),
            None => self.go("Subscribe", sid, env, ids, identifier),
        }
    }

    pub fn unsubscribe(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        identifier: &str,
    ) -> Outcome<CommandResult> {
        match &self.rust {
            Some(c) => c.unsubscribe(sid, env, ids, identifier),
            None => self.go("Unsubscribe", sid, env, ids, identifier),
        }
    }
}
