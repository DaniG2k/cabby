//! Channel-based routing of RPC commands to controllers (upstream package `router`).

use std::sync::Arc;

use crate::common::{CommandResult, ConnectResult, SessionEnv};
use crate::pending;

/// A controller failure (upstream Go `error` values).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ControllerError(pub String);

/// The RPC application interface (upstream `node.Controller`).
///
/// Result-returning methods yield `Ok(None)` where upstream returns a nil result
/// without an error. Upstream's `context.Context` argument is omitted.
// TODO(merge): this mirrors `node.Controller`; unify with the `node` module.
pub trait Controller: Send + Sync {
    fn start(&self) -> Result<(), ControllerError>;
    fn shutdown(&self) -> Result<(), ControllerError>;
    fn authenticate(&self, sid: &str, env: &SessionEnv) -> Result<Option<ConnectResult>, ControllerError>;
    fn subscribe(
        &self,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> Result<Option<CommandResult>, ControllerError>;
    fn unsubscribe(
        &self,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> Result<Option<CommandResult>, ControllerError>;
    fn perform(
        &self,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
        data: &str,
    ) -> Result<Option<CommandResult>, ControllerError>;
    fn disconnect(
        &self,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError>;
}

/// Routes commands to per-channel controllers by the identifier's `channel` field,
/// falling back to the default controller (upstream `RouterController`).
pub struct RouterController {
    _default: Arc<dyn Controller>,
}

impl RouterController {
    pub fn new(default: Arc<dyn Controller>) -> Self {
        RouterController { _default: default }
    }

    /// Registers a controller for a channel name.
    pub fn route(&mut self, _channel: &str, _handler: Arc<dyn Controller>) -> Result<(), ControllerError> {
        pending!("router::route")
    }

    pub fn empty(&self) -> bool {
        pending!("router::empty")
    }

    pub fn routes(&self) -> Vec<String> {
        pending!("router::routes")
    }
}

impl Controller for RouterController {
    fn start(&self) -> Result<(), ControllerError> {
        pending!("router::start")
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        pending!("router::shutdown")
    }

    fn authenticate(&self, _sid: &str, _env: &SessionEnv) -> Result<Option<ConnectResult>, ControllerError> {
        pending!("router::authenticate")
    }

    fn subscribe(
        &self,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
    ) -> Result<Option<CommandResult>, ControllerError> {
        pending!("router::subscribe")
    }

    fn unsubscribe(
        &self,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
    ) -> Result<Option<CommandResult>, ControllerError> {
        pending!("router::unsubscribe")
    }

    fn perform(
        &self,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
        _data: &str,
    ) -> Result<Option<CommandResult>, ControllerError> {
        pending!("router::perform")
    }

    fn disconnect(
        &self,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        pending!("router::disconnect")
    }
}
