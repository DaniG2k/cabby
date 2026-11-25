//! WebSocket transport (upstream package `ws`).
//!
//! * [`websocket_handler`]: the endpoint mounted on the HTTP server. It validates
//!   the handshake in gorilla/websocket's order and with its status codes (400,
//!   405, 403 for a rejected origin), negotiates a subprotocol (server preference
//!   order, as gorilla), answers 101 with `X-AnyCable-Version`, and hands the
//!   upgraded socket to a session handler.
//! * [`WsConnection`]: the node's [`Connection`](crate::node::Connection) over a
//!   WebSocket, one Tokio task per connection (see `connection.rs`).
//!
//! # WebSocket library
//!
//! The handshake is ours (it runs inside the hyper service, so the HTTP side
//! stays one server); reading and control frames use **tungstenite**
//! (`tokio-tungstenite` for the async stream), and data frames, which a server
//! neither masks nor compresses, are framed by `connection.rs` and written past it.
//! Reasons:
//!
//! * **Compliance.** tungstenite passes the Autobahn test suite and is the most
//!   widely used Rust WebSocket implementation; it handles fragmentation, control
//!   frames interleaved with fragments, close handshakes, UTF-8 validation and
//!   message/frame size limits. tungstenite 0.30 was already in the workspace (the
//!   testkit's clients).
//! * **Memory per connection.** Its read buffer is configurable: `ws.read_buffer_size`
//!   (1 KiB by default, as upstream) is what an idle connection starts with. Frames
//!   are not buffered for writing: a connection holds a write buffer only for the
//!   pongs and the close frame tungstenite writes (control frames, up to 127 bytes
//!   each), so `ws.write_buffer_size` is accepted but no longer sizes anything that
//!   matters. There is no per-connection task besides ours.
//! * **No copies of shared frames.** Payloads are `Bytes`: the hub's `SentFrame`
//!   payload is shared by every session's queue, with the frame header next to it,
//!   and a batch of them is written with one vectored write. It is neither cloned,
//!   re-validated nor copied into a write buffer per session.
//!
//! `fastwebsockets` was the alternative: faster raw framing, but its reads are not
//! cancel-safe (our io loop multiplexes reads and writes on one task with `select!`,
//! which tungstenite's incremental parser supports), it leaves fragment reassembly
//! and UTF-8 checks to helpers, and it is far less widely deployed. Neither crate implements permessage-deflate, so
//! `ws.enable_compression` is accepted but not negotiated (proposed deviation).

use cabby_core::toml_config::string_list;

mod connection;
mod handler;
#[cfg(test)]
mod tests;

pub use cabby_core::encoders::{FrameType, SentFrame};
pub use cabby_core::logger::TomlError;
pub(crate) use connection::close_without_waiting;
pub use connection::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_INTERNAL_SERVER_ERR, CLOSE_INVALID_FRAME_PAYLOAD_DATA,
    CLOSE_MESSAGE_TOO_BIG, CLOSE_NO_STATUS_RECEIVED, CLOSE_NORMAL_CLOSURE, CLOSE_PROTOCOL_ERROR, CLOSE_TRY_AGAIN_LATER,
    InboundHandler, MAX_QUEUED, ReadPermit, WsConnection, is_close_error,
};
pub use handler::{SessionCallback, SessionHandler, SharedHeadersExtractor, WebsocketHandler, websocket_handler};

/// Origin policy built from the comma-separated `allowed_origins` setting.
#[derive(Debug, Clone)]
pub struct OriginCheck {
    /// `None`: any origin (an empty setting).
    hosts: Option<Vec<String>>,
}

/// Builds the origin policy (upstream `ws.CheckOrigin`).
pub fn check_origin(origins: &str) -> OriginCheck {
    OriginCheck { hosts: (!origins.is_empty()).then(|| origins.split(',').map(str::to_owned).collect()) }
}

impl OriginCheck {
    /// Decides a handshake given its `Origin` header (`None` when absent).
    pub fn allows(&self, origin: Option<&str>) -> bool {
        let Some(hosts) = &self.hosts else {
            return true;
        };
        let hosts: Vec<&str> = hosts.iter().map(String::as_str).collect();
        crate::server::origin_allowed(origin.unwrap_or(""), &hosts)
    }
}

/// WebSocket connection settings (upstream `ws.Config`, TOML table `[ws]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub paths: Vec<String>,
    pub read_buffer_size: i64,
    pub write_buffer_size: i64,
    pub max_message_size: i64,
    pub write_timeout: i64,
    pub max_pending_size: u64,
    pub enable_compression: bool,
    /// Not part of the TOML section (upstream tag `toml:"-"`).
    pub allowed_origins: String,
}

cabby_core::toml_section!(Config, "ws.Config" {
    "paths" => paths,
    "read_buffer_size" => read_buffer_size,
    "write_buffer_size" => write_buffer_size,
    "max_message_size" => max_message_size,
    "write_timeout" => write_timeout,
    "max_pending_size" => max_pending_size,
    "enable_compression" => enable_compression,
});

impl Config {
    /// Defaults (upstream `ws.NewConfig`).
    pub fn new() -> Self {
        Self {
            paths: vec!["/cable".to_owned()],
            read_buffer_size: 1024,
            write_buffer_size: 1024,
            max_message_size: 65536,
            // 1 MB
            max_pending_size: 1024 * 1024,
            write_timeout: 2,
            ..Default::default()
        }
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# WebSocket endpoint paths\n");
        out.push_str(&format!("paths = {}\n", string_list(&self.paths)));
        out.push_str("# Read buffer size\n");
        out.push_str(&format!("read_buffer_size = {}\n", self.read_buffer_size));
        out.push_str("# Write buffer size\n");
        out.push_str(&format!("write_buffer_size = {}\n", self.write_buffer_size));
        out.push_str("# Maximum message size\n");
        out.push_str(&format!("max_message_size = {}\n", self.max_message_size));
        out.push_str("# Write timeout (seconds)\n");
        out.push_str(&format!("write_timeout = {}\n", self.write_timeout));
        out.push_str("# Maximum pending size\n");
        if self.max_pending_size > 0 {
            out.push_str(&format!("max_pending_size = {}\n", self.max_pending_size));
        } else {
            out.push_str("# max_pending_size = 1048576\n");
        }
        if self.enable_compression {
            out.push_str("# Enable compression (per-message deflate)\n");
            out.push_str("enable_compression = true\n");
            out.push_str("# enable_compression = true\n");
        }
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| TomlError(e.0))?;
        Ok(())
    }
}
