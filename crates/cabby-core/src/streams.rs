//! Direct stream subscriptions without channels (upstream package `streams`):
//! signed/public pub/sub streams, Turbo Streams and CableReady.

use crate::common::{
    self, CommandResult, ConnectResult, Controller, ControllerError, Outcome, SessionEnv, Status, StringMap,
};
use crate::gojson;
use crate::utils::MessageVerifier;

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
        !self.stream_name.is_empty() || !self.signed_stream_name.is_empty()
    }
}

/// Turns a channel identifier into a subscription request (upstream `StreamResolver`).
pub type StreamResolver = Box<dyn Fn(&str) -> Result<SubscribeRequest, ControllerError> + Send + Sync>;

/// Controller for stream subscriptions (upstream `streams.Controller`).
pub struct StreamsController {
    /// `None`: no key, signed streams are rejected (the verifier never runs with
    /// an empty key).
    verifier: Option<MessageVerifier>,
    resolver: StreamResolver,
}

/// Go `json.Unmarshal(identifier, &T)` for a struct of string fields.
fn decode<const N: usize>(
    identifier: &str,
    type_name: &'static str,
    fields: &'static [&'static str; N],
) -> Result<[String; N], ControllerError> {
    gojson::decode_string_fields(identifier, type_name, fields).map_err(ControllerError::new)
}

impl StreamsController {
    /// Upstream `NewController(key, resolver)`: no key means signed streams are
    /// rejected.
    pub fn new(key: &str, resolver: StreamResolver) -> Self {
        let verifier = (!key.is_empty()).then(|| MessageVerifier::new(key));
        Self { verifier, resolver }
    }

    /// Upstream `NewStreamsController(conf)`: `{"stream_name": ...}` (public, if
    /// allowed) or `{"signed_stream_name": ...}` identifiers.
    pub fn for_config(conf: &Config) -> Self {
        let allow_public = conf.public;
        let whispers = conf.whisper;
        let presence = conf.presence;

        let resolver = move |identifier: &str| {
            let [stream_name, signed_stream_name] =
                decode(identifier, "streams.SubscribeRequest", &["stream_name", "signed_stream_name"])?;
            let public_stream = !stream_name.is_empty() && signed_stream_name.is_empty();

            if !allow_public && public_stream {
                return Err(ControllerError::new("public streams are not allowed"));
            }

            Ok(SubscribeRequest {
                stream_name,
                signed_stream_name,
                whisper: whispers || public_stream,
                presence: presence || public_stream,
            })
        };

        Self::new(&conf.secret, Box::new(resolver))
    }

    /// Upstream `NewTurboController(key)`: `{"signed_stream_name": ...}` identifiers.
    pub fn turbo(key: &str) -> Self {
        let resolver = |identifier: &str| {
            let [signed_stream_name] = decode(identifier, "streams.TurboMessage", &["signed_stream_name"])?;
            Ok(SubscribeRequest { signed_stream_name, ..Default::default() })
        };
        Self::new(key, Box::new(resolver))
    }

    /// Upstream `NewCableReadyController(key)`: `{"identifier": ...}` identifiers.
    pub fn cable_ready(key: &str) -> Self {
        let resolver = |identifier: &str| {
            let [signed_stream_name] = decode(identifier, "streams.CableReadyMesssage", &["identifier"])?;
            Ok(SubscribeRequest { signed_stream_name, ..Default::default() })
        };
        Self::new(key, Box::new(resolver))
    }

    /// Resolves and verifies the stream named by `identifier`.
    pub fn verified_stream(&self, identifier: &str) -> Result<(SubscribeRequest, String), ControllerError> {
        let request = (self.resolver)(identifier)
            .map_err(|e| ControllerError::new(format!("invalid identifier, cause: {}", e.message)))?;

        if !request.is_present() {
            return Err(ControllerError::new("malformed identifier: no stream name or signed stream"));
        }

        if request.signed_stream_name.is_empty() {
            let stream = request.stream_name.clone();
            return Ok((request, stream));
        }

        let Some(verifier) = &self.verifier else {
            return Err(ControllerError::new("verification failed: no secret configured"));
        };

        match verifier.verified(&request.signed_stream_name) {
            Ok(serde_json::Value::String(stream)) => Ok((request, stream)),
            Ok(_) => Err(ControllerError::new("verification failed: stream name is not a string")),
            Err(_) => Err(ControllerError::new("verification failed")),
        }
    }
}

impl Controller for StreamsController {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        Outcome { result: None, error: None }
    }

    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, identifier: &str) -> Outcome<CommandResult> {
        let (request, stream) = match self.verified_stream(identifier) {
            Ok(verified) => verified,
            Err(err) => {
                return Outcome {
                    result: Some(CommandResult {
                        status: Status::Failure,
                        transmissions: vec![common::rejection_message(identifier)],
                        ..Default::default()
                    }),
                    error: Some(err),
                };
            }
        };

        let mut state: Option<StringMap> = None;
        if request.whisper {
            state.get_or_insert_with(StringMap::new).insert(common::WHISPER_STREAM_STATE.to_owned(), stream.clone());
        }
        if request.presence {
            state.get_or_insert_with(StringMap::new).insert(common::PRESENCE_STREAM_STATE.to_owned(), stream.clone());
        }

        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                transmissions: vec![common::confirmation_message(identifier)],
                streams: vec![stream],
                disconnect_interest: -1,
                istate: state,
                ..Default::default()
            }),
            error: None,
        }
    }

    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                transmissions: Vec::new(),
                streams: Vec::new(),
                stop_all_streams: true,
                ..Default::default()
            }),
            error: None,
        }
    }

    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> Outcome<CommandResult> {
        Outcome { result: None, error: None }
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
