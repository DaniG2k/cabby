//! Connection identification without RPC (upstream package `identity`).
//!
//! An [`Identifier`] decides a connection from its session env alone (public
//! mode, JWT). [`IdentifiableController`] runs it before delegating to the
//! wrapped controller.

use std::sync::Arc;

use crate::common::{CommandResult, ConnectResult, Controller, ControllerError, Outcome, SessionEnv};
use crate::pending;

/// Identifies a connection (upstream `identity.Identifier`). Returning neither a
/// result nor an error means "not identified here, pass through".
pub trait Identifier: Send + Sync {
    fn identify(&self, sid: &str, env: &SessionEnv) -> Outcome<ConnectResult>;
}

/// Tries identifiers in order; the first result or error wins (upstream
/// `IdentifierPipeline`).
pub struct IdentifierPipeline {
    _identifiers: Vec<Arc<dyn Identifier>>,
}

impl IdentifierPipeline {
    pub fn new(identifiers: Vec<Arc<dyn Identifier>>) -> Self {
        Self { _identifiers: identifiers }
    }
}

impl Identifier for IdentifierPipeline {
    fn identify(&self, _sid: &str, _env: &SessionEnv) -> Outcome<ConnectResult> {
        pending!("identity::identifier_pipeline_identify")
    }
}

/// Identifies every client by its session id (upstream `PublicIdentifier`).
#[derive(Debug, Clone, Default)]
pub struct PublicIdentifier;

impl PublicIdentifier {
    pub fn new() -> Self {
        Self
    }
}

impl Identifier for PublicIdentifier {
    fn identify(&self, _sid: &str, _env: &SessionEnv) -> Outcome<ConnectResult> {
        pending!("identity::public_identifier_identify")
    }
}

/// Wraps a controller with an identifier (upstream `IdentifiableController`).
pub struct IdentifiableController {
    _controller: Arc<dyn Controller>,
    _identifier: Arc<dyn Identifier>,
}

impl IdentifiableController {
    pub fn new(controller: Arc<dyn Controller>, identifier: Arc<dyn Identifier>) -> Self {
        Self { _controller: controller, _identifier: identifier }
    }
}

impl Controller for IdentifiableController {
    fn start(&self) -> Result<(), ControllerError> {
        pending!("identity::identifiable_controller_start")
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        pending!("identity::identifiable_controller_shutdown")
    }

    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        pending!("identity::identifiable_controller_authenticate")
    }

    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        pending!("identity::identifiable_controller_subscribe")
    }

    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        pending!("identity::identifiable_controller_unsubscribe")
    }

    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> Outcome<CommandResult> {
        pending!("identity::identifiable_controller_perform")
    }

    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        pending!("identity::identifiable_controller_disconnect")
    }
}

/// JWT signing algorithm (upstream `jwt.SigningMethod`; only the default is configurable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JwtAlgo {
    #[default]
    Hs256,
}

/// Invalid TOML or a type mismatch while decoding a config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ConfigError {
    pub message: String,
}

/// JWT identification settings (upstream `identity.JWTConfig`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtConfig {
    pub secret: String,
    /// URL query parameter carrying the token; the header is `x-<param>`.
    pub param: String,
    pub algo: JwtAlgo,
    /// Reject connections without a token.
    pub force: bool,
}

impl JwtConfig {
    /// Upstream `NewJWTConfig(secret)`.
    pub fn new(_secret: &str) -> Self {
        pending!("identity::new_jwt_config")
    }

    pub fn enabled(&self) -> bool {
        pending!("identity::jwt_config_enabled")
    }

    pub fn header_key(&self) -> String {
        pending!("identity::jwt_config_header_key")
    }

    pub fn to_toml(&self) -> String {
        pending!("identity::jwt_config_to_toml")
    }

    /// Decodes TOML over the current values, keeping fields the document omits
    /// (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, _toml: &str) -> Result<(), ConfigError> {
        pending!("identity::jwt_config_merge_toml")
    }
}

/// Identifies connections by a JWT in a header or query parameter (upstream
/// `JWTIdentifier`).
pub struct JwtIdentifier {
    _config: JwtConfig,
}

impl JwtIdentifier {
    pub fn new(config: &JwtConfig) -> Self {
        Self { _config: config.clone() }
    }
}

impl Identifier for JwtIdentifier {
    fn identify(&self, _sid: &str, _env: &SessionEnv) -> Outcome<ConnectResult> {
        pending!("identity::jwt_identifier_identify")
    }
}
