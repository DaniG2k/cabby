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

crate::toml_section!(Config, "streams.Config" {
    "secret" => secret,
    "public" => public,
    "whisper" => whisper,
    "presence" => presence,
    "pubsub_channel" => pubsub_channel,
    "turbo" => turbo,
    "turbo_secret" => turbo_secret,
    "cable_ready" => cable_ready,
    "cable_ready_secret" => cable_ready_secret,
});

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self { pubsub_channel: "$pubsub".to_owned(), presence: true, ..Default::default() }
    }

    pub fn turbo_secret(&self) -> String {
        if self.turbo_secret.is_empty() { self.secret.clone() } else { self.turbo_secret.clone() }
    }

    pub fn cable_ready_secret(&self) -> String {
        if self.cable_ready_secret.is_empty() { self.secret.clone() } else { self.cable_ready_secret.clone() }
    }

    pub fn to_toml(&self) -> String {
        fn flag(out: &mut String, comment: &str, key: &str, on: bool) {
            out.push_str(comment);
            out.push_str(if on { "" } else { "# " });
            out.push_str(key);
            out.push_str(" = true\n");
        }
        fn secret(out: &mut String, comment: &str, key: &str, value: &str) {
            out.push_str(comment);
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }

        let mut out = String::new();
        secret(&mut out, "# Secret key used to sign and verify pub/sub streams\n", "secret", &self.secret);
        flag(&mut out, "# Enable public (unsigned) streams\n", "public", self.public);
        flag(&mut out, "# Enable whispering support for pub/sub streams\n", "whisper", self.whisper);
        flag(&mut out, "# Enable presence support for pub/sub streams\n", "presence", self.presence);
        out.push_str("# Name of the channel used for pub/sub\n");
        out.push_str(&format!("pubsub_channel = \"{}\"\n", self.pubsub_channel));
        flag(&mut out, "# Enable Turbo Streams support\n", "turbo", self.turbo);
        secret(&mut out, "# Custom secret key used to verify Turbo Streams\n", "turbo_secret", &self.turbo_secret);
        flag(&mut out, "# Enable CableReady support\n", "cable_ready", self.cable_ready);
        secret(
            &mut out,
            "# Custom secret key used to verify CableReady streams\n",
            "cable_ready_secret",
            &self.cable_ready_secret,
        );
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), ConfigError> {
        crate::toml_config::decode(self, toml).map_err(|e| ConfigError { message: e.0 })?;
        Ok(())
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
