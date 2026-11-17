//! Direct stream subscriptions without channels (upstream package `streams`):
//! signed/public pub/sub streams, Turbo Streams and CableReady.

use crate::common::{CommandResult, ConnectResult, Controller, ControllerError, Outcome, SessionEnv};
use crate::pending;

/// Invalid TOML or a type mismatch while decoding a config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ConfigError {
    pub message: String,
}

/// Streams settings (upstream `streams.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    /// Key used to sign and verify streams.
    pub secret: String,
    /// Allow public (unsigned) streams.
    pub public: bool,
    pub whisper: bool,
    pub presence: bool,
    /// Channel name used for direct pub/sub.
    pub pubsub_channel: String,
    pub turbo: bool,
    pub turbo_secret: String,
    pub cable_ready: bool,
    pub cable_ready_secret: String,
}

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        pending!("streams::new_config")
    }

    pub fn turbo_secret(&self) -> String {
        pending!("streams::config_get_turbo_secret")
    }

    pub fn cable_ready_secret(&self) -> String {
        pending!("streams::config_get_cable_ready_secret")
    }

    pub fn to_toml(&self) -> String {
        pending!("streams::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, _toml: &str) -> Result<(), ConfigError> {
        pending!("streams::config_merge_toml")
    }
}

/// A resolved subscription request (upstream `streams.SubscribeRequest`;
/// `whisper` and `presence` are unexported upstream and set by resolvers).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscribeRequest {
    pub stream_name: String,
    pub signed_stream_name: String,
    pub whisper: bool,
    pub presence: bool,
}

impl SubscribeRequest {
    pub fn is_present(&self) -> bool {
        pending!("streams::subscribe_request_is_present")
    }
}

/// Turns a channel identifier into a subscription request (upstream `StreamResolver`).
pub type StreamResolver = Box<dyn Fn(&str) -> Result<SubscribeRequest, ControllerError> + Send + Sync>;

/// Controller for stream subscriptions (upstream `streams.Controller`).
pub struct StreamsController {
    _key: String,
    _resolver: StreamResolver,
}

impl StreamsController {
    /// Upstream `NewController(key, resolver)`: no key means signed streams are
    /// rejected.
    pub fn new(key: &str, resolver: StreamResolver) -> Self {
        Self { _key: key.to_owned(), _resolver: resolver }
    }

    /// Upstream `NewStreamsController(conf)`.
    pub fn for_config(_conf: &Config) -> Self {
        pending!("streams::new_streams_controller")
    }

    /// Upstream `NewTurboController(key)`.
    pub fn turbo(_key: &str) -> Self {
        pending!("streams::new_turbo_controller")
    }

    /// Upstream `NewCableReadyController(key)`.
    pub fn cable_ready(_key: &str) -> Self {
        pending!("streams::new_cable_ready_controller")
    }

    /// Resolves and verifies the stream named by `identifier`.
    pub fn verified_stream(&self, _identifier: &str) -> Result<(SubscribeRequest, String), ControllerError> {
        pending!("streams::controller_verified_stream")
    }
}

impl Controller for StreamsController {
    fn start(&self) -> Result<(), ControllerError> {
        pending!("streams::controller_start")
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        pending!("streams::controller_shutdown")
    }

    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        pending!("streams::controller_authenticate")
    }

    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        pending!("streams::controller_subscribe")
    }

    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        pending!("streams::controller_unsubscribe")
    }

    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> Outcome<CommandResult> {
        pending!("streams::controller_perform")
    }

    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        pending!("streams::controller_disconnect")
    }
}
