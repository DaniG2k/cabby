//! Server-sent events transport (upstream package `sse`).

use std::sync::Arc;
use std::time::Instant;

use cabby_core::common::{EncodedMessage, Message, SentFrame};
use cabby_core::pending;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::pusher_sse_deps::{Encoder as NodeEncoder, Error, HeadersExtractor, Node, ResponseWriter};

/// Default ping interval in seconds (upstream `DefaultPingInterval`).
pub const DEFAULT_PING_INTERVAL: i64 = 15;

/// Keepalive comment line (upstream `CommentPing`).
pub const COMMENT_PING: &str = ": ping";

/// `retry:` value telling a client not to reconnect (one year in ms, upstream
/// `retryNoReconnect`).
pub const RETRY_NO_RECONNECT: i64 = 31_536_000_000;

/// SSE settings (upstream `sse.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    /// Endpoint path.
    pub path: String,
    /// Send pings as `: ping` comments instead of ping events.
    pub comment_pings: bool,
    /// Seconds; 0: the global ping interval (if changed) or [`DEFAULT_PING_INTERVAL`].
    pub ping_interval: i64,
    /// Not configurable through TOML (upstream tag `toml:"-"`).
    #[serde(skip)]
    pub allowed_origins: String,
}

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self { enabled: false, path: "/events".to_owned(), ..Default::default() }
    }

    /// Renders the settings as an annotated TOML section body (upstream `ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Enable Server-sent events support\n");
        out.push_str(if self.enabled { "enabled = true\n" } else { "# enabled = true\n" });
        out.push_str("# Server-sent events endpoint path\n");
        out.push_str(&format!("path = \"{}\"\n", self.path));
        out.push_str("# Send pings as SSE comments (\": ping\") instead of ping events (always enabled in raw mode)\n");
        out.push_str(if self.comment_pings { "comment_pings = true\n" } else { "# comment_pings = true\n" });
        out.push_str("# Keepalive interval for comment-based pings (seconds)\n");
        out.push_str(&format!("# Defaults to the global ping_interval (if changed) or {DEFAULT_PING_INTERVAL}\n"));
        if self.ping_interval > 0 {
            out.push_str(&format!("ping_interval = {}\n", self.ping_interval));
        } else {
            out.push_str(&format!("# ping_interval = {DEFAULT_PING_INTERVAL}\n"));
        }
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), String> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| e.0)?;
        Ok(())
    }
}

cabby_core::toml_section!(Config, "sse.Config" {
    "enabled" => enabled,
    "path" => path,
    "comment_pings" => comment_pings,
    "ping_interval" => ping_interval,
});

/// Encodes server messages as SSE events (upstream `sse.Encoder`). Decoding client
/// messages is unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Encoder {
    /// Send only data messages, no protocol events.
    pub raw_data: bool,
    /// Send only the `message` field of replies as data.
    pub unwrap_data: bool,
    /// Send pings as comments.
    pub comment_pings: bool,
}

impl NodeEncoder for Encoder {
    fn id(&self) -> String {
        pending!("sse::encoder_id")
    }

    fn encode(&self, _msg: &EncodedMessage) -> Result<Option<SentFrame>, Error> {
        pending!("sse::encode")
    }

    fn encode_transmission(&self, _raw: &str) -> Result<Option<SentFrame>, Error> {
        pending!("sse::encode_transmission")
    }

    fn decode(&self, _raw: &[u8]) -> Result<Message, Error> {
        pending!("sse::decode")
    }
}

/// A server-to-client event stream over an HTTP response (upstream `sse.Connection`).
/// Messages written before [`Connection::established`] are buffered.
pub struct Connection {
    _writer: Box<dyn ResponseWriter>,
}

impl Connection {
    pub fn new(writer: Box<dyn ResponseWriter>) -> Self {
        Self { _writer: writer }
    }

    /// Unsupported for SSE.
    pub fn read(&self) -> Result<Vec<u8>, Error> {
        pending!("sse::connection_read")
    }

    pub fn write(&self, _msg: &[u8], _deadline: Instant) -> Result<(), Error> {
        pending!("sse::connection_write")
    }

    /// Unsupported for SSE.
    pub fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), Error> {
        pending!("sse::connection_write_binary")
    }

    pub fn write_ping(&self, _deadline: Instant) -> Result<(), Error> {
        pending!("sse::connection_write_ping")
    }

    /// Cancelled when the connection is closed (upstream `Context()`).
    pub fn context(&self) -> CancellationToken {
        pending!("sse::connection_context")
    }

    pub fn close(&self, _code: i32, _reason: &str) {
        pending!("sse::connection_close")
    }

    /// Whether the connection was closed (upstream unexported `done` field).
    pub fn is_done(&self) -> bool {
        pending!("sse::connection_done")
    }

    /// Marks the response as started and flushes buffered messages.
    pub fn established(&self) {
        pending!("sse::connection_established")
    }

    /// The underlying network connection, if any (upstream `Descriptor()`).
    pub fn descriptor(&self) -> Option<std::net::TcpStream> {
        pending!("sse::connection_descriptor")
    }
}

/// A request as seen by the handler (method, URI, headers, body). HTTP/1.x requests
/// get a `Connection: keep-alive` response header.
pub type Request = http::Request<Vec<u8>>;

/// The SSE endpoint (upstream `SSEHandler`).
pub struct Handler {
    _node: Arc<Node>,
    _shutdown: CancellationToken,
    _extractor: HeadersExtractor,
    _config: Config,
}

/// Builds the SSE endpoint. `shutdown` is cancelled on server shutdown.
pub fn sse_handler(
    node: Arc<Node>,
    shutdown: CancellationToken,
    extractor: HeadersExtractor,
    config: &Config,
) -> Handler {
    Handler { _node: node, _shutdown: shutdown, _extractor: extractor, _config: config.clone() }
}

impl Handler {
    /// Serves one request until it completes, the client goes away (`request_done`
    /// is cancelled) or the session is closed.
    pub fn serve(&self, _req: Request, _w: &mut dyn ResponseWriter, _request_done: &CancellationToken) {
        pending!("sse::handler_serve")
    }
}
