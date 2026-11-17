//! Conversion between session data and RPC messages (upstream package `protocol`).

use crate::common::{CommandResult, ConnectResult, SessionEnv};
use crate::pending;
use crate::protos;

/// The application answered with status ERROR (upstream: `Application error: <msg>`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Application error: {message}")]
pub struct ApplicationError {
    pub message: String,
}

/// A parsed RPC response. Upstream returns the result *and* an error for ERROR
/// responses, so both are kept.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed<T> {
    pub result: T,
    pub error: Option<ApplicationError>,
}

pub fn new_connect_message(_env: &SessionEnv) -> protos::ConnectionRequest {
    pending!("protocol::new_connect_message")
}

pub fn new_command_message(
    _env: &SessionEnv,
    _command: &str,
    _channel: &str,
    _identifiers: &str,
    _data: &str,
) -> protos::CommandMessage {
    pending!("protocol::new_command_message")
}

pub fn new_disconnect_message(
    _env: &SessionEnv,
    _identifiers: &str,
    _subscriptions: &[String],
) -> protos::DisconnectRequest {
    pending!("protocol::new_disconnect_message")
}

pub fn parse_connect_response(_response: &protos::ConnectionResponse) -> Parsed<ConnectResult> {
    pending!("protocol::parse_connect_response")
}

pub fn parse_command_response(_response: &protos::CommandResponse) -> Parsed<CommandResult> {
    pending!("protocol::parse_command_response")
}

pub fn parse_disconnect_response(_response: &protos::DisconnectResponse) -> Result<(), ApplicationError> {
    pending!("protocol::parse_disconnect_response")
}
