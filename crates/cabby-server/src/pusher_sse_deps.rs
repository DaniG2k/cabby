//! Interfaces from upstream packages that `pusher` depends on but that are ported
//! elsewhere (`node`, `broker`, `broadcast`, `encoders`). These are placeholders
//! sized to what the pusher tests need; unify them with the real ports.
//! `MemoryBroker` already delegates to `cabby_core::broker::Memory`. (`sse` and
//! `ds` use the canonical node, encoder and server types.)

use std::sync::Arc;

use cabby_core::common::{
    CommandResult, ConnectResult, EncodedMessage, Message, PresenceEvent, PresenceInfo, SentFrame, SessionEnv,
};

/// An error value (Go `error`); the text is what Go's `err.Error()` would print.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// A Go `(result, error)` pair where both may be set (e.g. a rejection result plus
/// a decoding error).
#[derive(Debug, Clone, Default)]
pub struct CallResult<T> {
    pub result: Option<T>,
    pub error: Option<Error>,
}

/// Upstream `node.Controller`: the application side of a session (RPC or a
/// built-in controller such as Pusher's).
pub trait Controller: Send + Sync {
    fn start(&self) -> Result<(), Error>;
    fn shutdown(&self) -> Result<(), Error>;
    fn authenticate(&self, sid: &str, env: &SessionEnv) -> CallResult<ConnectResult>;
    fn subscribe(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str) -> CallResult<CommandResult>;
    fn unsubscribe(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str) -> CallResult<CommandResult>;
    fn perform(&self, sid: &str, env: &SessionEnv, ids: &str, channel: &str, data: &str) -> CallResult<CommandResult>;
    fn disconnect(&self, sid: &str, env: &SessionEnv, ids: &str, subscriptions: &[String]) -> Result<(), Error>;
}

/// The presence subset of upstream `broker.Broker` used by pusher.
pub trait Broker: Send + Sync {
    fn supports_presence(&self) -> bool;
    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: serde_json::Value,
    ) -> Result<Option<PresenceEvent>, Error>;
    fn presence_info(&self, stream: &str) -> Result<PresenceInfo, Error>;
}

/// Upstream `broker.Memory` (in-memory broker with presence support): the
/// canonical [`cabby_core::broker::Memory`] behind pusher's presence subset.
pub struct MemoryBroker {
    inner: cabby_core::broker::Memory,
}

fn broker_error(e: cabby_core::broker::BrokerError) -> Error {
    Error(e.message)
}

impl MemoryBroker {
    #[allow(clippy::new_without_default)]
    /// `broker.NewMemoryBroker(nil, nil, &broker.NewConfig())`.
    pub fn new() -> Self {
        Self {
            inner: cabby_core::broker::Memory::new(
                None,
                None,
                &cabby_core::broker::Config::new(),
                Arc::new(cabby_core::broker::SystemClock),
            ),
        }
    }

    /// The canonical broker, for callers that need the full `broker.Broker` API.
    pub fn inner(&self) -> &cabby_core::broker::Memory {
        &self.inner
    }

    pub fn start(&self) -> Result<(), Error> {
        cabby_core::broker::Broker::start(&self.inner).map_err(broker_error)
    }

    pub fn shutdown(&self) -> Result<(), Error> {
        cabby_core::broker::Broker::shutdown(&self.inner).map_err(broker_error)
    }
}

impl Broker for MemoryBroker {
    fn supports_presence(&self) -> bool {
        cabby_core::broker::Broker::supports_presence(&self.inner)
    }

    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: serde_json::Value,
    ) -> Result<Option<PresenceEvent>, Error> {
        cabby_core::broker::Broker::presence_add(&self.inner, stream, sid, pid, info).map_err(broker_error)
    }

    fn presence_info(&self, stream: &str) -> Result<PresenceInfo, Error> {
        cabby_core::broker::Broker::presence_info(&self.inner, stream, None).map_err(broker_error)
    }
}

/// Upstream `broadcast.Handler`: receives raw broadcast payloads.
pub trait BroadcastHandler: Send + Sync {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), Error>;
}

/// Upstream `encoders.Encoder`.
pub trait Encoder: Send + Sync {
    fn id(&self) -> String;
    fn encode(&self, msg: &EncodedMessage) -> Result<Option<SentFrame>, Error>;
    fn encode_transmission(&self, raw: &str) -> Result<Option<SentFrame>, Error>;
    fn decode(&self, raw: &[u8]) -> Result<Message, Error>;
}

/// The session operations an executor uses (subset of upstream `node.Session`).
pub trait ExecutorSession {
    fn id(&self) -> String;
    fn identifiers(&self) -> String;
    fn env(&self) -> SessionEnv;
    /// `s.GetEnv().MergeChannelState(identifier, &state)`.
    fn merge_channel_state(&mut self, identifier: &str, state: &cabby_core::common::StringMap);
    /// Encodes a raw transmission with the session's encoder and writes it.
    fn send_json_transmission(&mut self, msg: &str);
}

/// Upstream `node.Executor`: handles decoded client commands for a session.
pub trait Executor: Send + Sync {
    fn handle_command(&self, session: &mut dyn ExecutorSession, msg: &Message) -> Result<(), Error>;
    fn disconnect(&self, session: &mut dyn ExecutorSession) -> Result<(), Error>;
}
