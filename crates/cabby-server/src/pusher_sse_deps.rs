//! Interfaces from upstream packages that `sse` and `ds` depend on but that are
//! ported elsewhere (`node`, `encoders`, `server`, and Go's `net/http`). These are
//! placeholders sized to what the sse/ds tests need; unify them with the real ports.
//! (`pusher` uses the canonical types.)

use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::{CommandResult, ConnectResult, EncodedMessage, Message, SentFrame, SessionEnv, StreamMessage};
use cabby_core::pending;

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

/// Upstream `encoders.Encoder`.
pub trait Encoder: Send + Sync {
    fn id(&self) -> String;
    fn encode(&self, msg: &EncodedMessage) -> Result<Option<SentFrame>, Error>;
    fn encode_transmission(&self, raw: &str) -> Result<Option<SentFrame>, Error>;
    fn decode(&self, raw: &[u8]) -> Result<Message, Error>;
}

/// Upstream `encoders.CachedEncodedMessage`: a message whose encodings are cached
/// per encoder ID.
pub struct CachedEncodedMessage {
    _target: EncodedMessage,
}

impl CachedEncodedMessage {
    pub fn new(msg: EncodedMessage) -> Self {
        Self { _target: msg }
    }

    /// Returns the cached encoding for `id`, computing it with `encode` on a miss.
    pub fn fetch(
        &self,
        _id: &str,
        _encode: &dyn Fn(&EncodedMessage) -> Result<Option<SentFrame>, Error>,
    ) -> Result<Option<SentFrame>, Error> {
        pending!("encoders::cached_encoded_message_fetch")
    }
}

/// Go `http.ResponseWriter` plus `http.Flusher`.
pub trait ResponseWriter: Send {
    fn headers_mut(&mut self) -> &mut http::HeaderMap;
    /// Sends the status line (Go `WriteHeader`; only the first call has effect).
    fn write_header(&mut self, status: u16);
    /// Writes body bytes (implicitly `write_header(200)` first).
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize>;
    fn flush(&mut self);
}

/// Upstream `server.DefaultHeadersExtractor`: which request headers and cookies are
/// passed to the application.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadersExtractor {
    pub headers: Vec<String>,
    pub cookies: Vec<String>,
    pub auth_header: String,
}

/// Node settings used by the tests (subset of upstream `node.Config` plus the
/// disconnector choice).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeOptions {
    /// Upstream `HubGopoolSize`.
    pub hub_pool_size: usize,
    /// Server ping interval (upstream `PingInterval`, default 3s).
    pub ping_interval: Duration,
    /// `Some(rate)`: a disconnect queue with that rate (`node.NewDisconnectQueue`);
    /// `None`: disconnect immediately.
    pub disconnect_rate: Option<i64>,
}

/// Upstream `node.Node` with the legacy broker (`broker.NewLegacyBroker`).
pub struct Node {
    _private: (),
}

impl Node {
    pub fn new(_options: NodeOptions, _controller: Arc<dyn Controller>) -> Self {
        pending!("node::new_node")
    }

    pub fn start(&self) -> Result<(), Error> {
        pending!("node::start")
    }

    pub fn shutdown(&self) -> Result<(), Error> {
        pending!("node::shutdown")
    }

    /// Delivers a stream message to subscribed sessions.
    pub fn broadcast(&self, _msg: &StreamMessage) {
        pending!("node::broadcast")
    }

    /// Number of registered sessions.
    pub fn size(&self) -> usize {
        pending!("node::size")
    }
}
