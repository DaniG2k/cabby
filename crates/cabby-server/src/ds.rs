//! Durable Streams read API (upstream package `ds`, see `ds/DESIGN.md` upstream):
//! catch-up reads, long-poll and SSE over the broker's stream history.
//!
//! Status: interfaces only; behavior is `pending!`.
// TODO(merge): the encoders implement the placeholder `pusher_sse_deps::Encoder`
// (nil frames are `Ok(None)`), like `sse::Encoder`; unify with
// `cabby_core::encoders::Encoder` once that trait can express nil frames.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use cabby_core::broker::Broker;
use cabby_core::common::{EncodedMessage, Message, SentFrame, StreamMessage};
use cabby_core::pending;
use cabby_core::streams::StreamsController;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::metrics::Instrumenter;
use crate::node::{Connection, ConnectionError, Node, Session};
use crate::pusher_sse_deps::{Encoder as NodeEncoder, Error, ResponseWriter};
use crate::server::{DefaultHeadersExtractor, HttpRequest};

/// Response header with the next offset to read from (opaque to clients).
pub const STREAM_OFFSET_HEADER: &str = "Stream-Next-Offset";
/// Response header with the CDN-collapsing cursor.
pub const STREAM_CURSOR_HEADER: &str = "Stream-Cursor";
/// Response header present when the response ends at the current end of the stream.
pub const STREAM_UP_TO_DATE_HEADER: &str = "Stream-Up-To-Date";
/// Request header for writer coordination (write side, unsupported).
pub const STREAM_SEQ_HEADER: &str = "Stream-Seq";
/// Request header for the stream TTL (write side, unsupported).
pub const STREAM_TTL_HEADER: &str = "Stream-TTL";
/// Request header for the stream expiry (write side, unsupported).
pub const STREAM_EXPIRES_AT_HEADER: &str = "Stream-Expires-At";

pub const OFFSET_QUERY_PARAM: &str = "offset";
/// Values: [`LONG_POLL_MODE`], [`SSE_MODE`].
pub const LIVE_QUERY_PARAM: &str = "live";
pub const CURSOR_QUERY_PARAM: &str = "cursor";
/// Signed stream name query parameter (stream authorization).
pub const SIGNED_STREAM_PARAM: &str = "signed";
/// Signed stream name header (stream authorization).
pub const SIGNED_STREAM_HEADER: &str = "x-signed";

pub const LONG_POLL_MODE: &str = "long-poll";
pub const SSE_MODE: &str = "sse";

/// Reserved offset: read from the start of the stream.
pub const START_OFFSET: &str = "-1";
/// Reserved offset: read from the current tail.
pub const NOW_OFFSET: &str = "now";

// Metric names (upstream unexported constants).
pub const METRICS_REQUESTS_TOTAL: &str = "ds_requests_total";
pub const METRICS_POLL_TOTAL: &str = "ds_poll_requests_total";
pub const METRICS_SSE_TOTAL: &str = "ds_sse_requests_total";
pub const METRICS_POLL_NUM: &str = "ds_poll_clients_num";
pub const METRICS_SSE_NUM: &str = "ds_sse_clients_num";

/// A DS operation failed (Go `error`), compared by message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DsError(pub String);

/// Durable Streams settings (upstream `ds.Config`, TOML table `[ds]`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    /// Mount path of the HTTP handler.
    pub path: String,
    /// Skip client authentication (stream authorization still applies).
    pub skip_auth: bool,
    /// Long-poll timeout in seconds.
    pub poll_interval: i64,
    /// Maximum lifetime of an SSE connection in seconds.
    pub sse_ttl: i64,
    /// Comment-ping interval for `live=sse` in seconds; 0: the global ping interval
    /// (if changed) or `sse::DEFAULT_PING_INTERVAL`.
    pub sse_ping_interval: i64,
    /// Not configurable through TOML (upstream tag `toml:"-"`).
    #[serde(skip)]
    pub allowed_origins: String,
}

cabby_core::toml_section!(Config, "ds.Config" {
    "enabled" => enabled,
    "path" => path,
    "skip_auth" => skip_auth,
    "poll_interval" => poll_interval,
    "sse_ttl" => sse_ttl,
    "sse_ping_interval" => sse_ping_interval,
});

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self { enabled: false, path: "/ds".to_owned(), poll_interval: 10, sse_ttl: 60, ..Default::default() }
    }

    /// Renders the settings as an annotated TOML section body (upstream `ToToml`).
    pub fn to_toml(&self) -> String {
        let ping = crate::sse::DEFAULT_PING_INTERVAL;
        let mut out = String::new();
        out.push_str("# Enable Durable Streams support\n");
        out.push_str(if self.enabled { "enabled = true\n" } else { "# enabled = true\n" });
        out.push_str("# Skip authentication (only authorize per-stream access)\n");
        out.push_str(if self.skip_auth { "skip_auth = true\n" } else { "# skip_auth = true\n" });
        out.push_str("# Durable Streams mount path\n");
        out.push_str(&format!("path = \"{}\"\n", self.path));
        out.push_str("# Poll interval for live=poll mode (in seconds)\n");
        out.push_str(&format!("poll_interval = {}\n", self.poll_interval));
        out.push_str("# Max time to live for a SSE connection (in seconds)\n");
        out.push_str(&format!("sse_ttl = {}\n", self.sse_ttl));
        out.push_str("# Keepalive interval for comment-based pings in live=sse mode (in seconds)\n");
        out.push_str(&format!("# Defaults to the global ping_interval (if changed) or {ping}\n"));
        if self.sse_ping_interval > 0 {
            out.push_str(&format!("sse_ping_interval = {}\n", self.sse_ping_interval));
        } else {
            out.push_str(&format!("# sse_ping_interval = {ping}\n"));
        }
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), DsError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| DsError(e.0))?;
        Ok(())
    }
}

// ------------------------------------------------------------------ offsets

/// Encodes an offset and a broker epoch into one opaque offset (`<offset>::<epoch>`,
/// `"0"` without an epoch).
pub fn encode_offset(_offset: u64, _epoch: &str) -> String {
    pending!("ds::encode_offset")
}

/// Decodes an opaque offset into `(offset, epoch)`; the reserved offsets `""`, `"0"`,
/// `"-1"` and `"now"` decode as `(0, "")`.
pub fn decode_offset(_offset: &str) -> Result<(u64, String), DsError> {
    pending!("ds::decode_offset")
}

/// The messages' data as a JSON array (raw bytes, in order).
pub fn encode_json_batch(_batch: &[StreamMessage]) -> Vec<u8> {
    pending!("ds::encode_json_batch")
}

/// A batch as an SSE `data` event.
pub fn encode_sse_data_event(_messages: &[StreamMessage]) -> Vec<u8> {
    pending!("ds::encode_sse_data_event")
}

/// An SSE `control` event with the next offset, the cursor and the up-to-date flag.
pub fn encode_sse_control_event(_offset: &str, _cursor: &str, _up_to_date: bool) -> Vec<u8> {
    pending!("ds::encode_sse_control_event")
}

// ------------------------------------------------------------------ request parameters

/// A parsed DS read request (upstream `StreamParams`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamParams {
    /// The stream name in AnyCable (currently the path).
    pub name: String,
    pub signed_name: String,
    pub cursor: String,
    pub offset: u64,
    pub epoch: String,
    /// `""`, [`LONG_POLL_MODE`] or [`SSE_MODE`].
    pub live_mode: String,
    pub path: String,
    pub raw_offset: String,
}

/// Parses and validates a request against the mount path (upstream `StreamParamsFromReq`).
pub fn stream_params_from_req(_req: &HttpRequest, _c: &Config) -> Result<StreamParams, DsError> {
    pending!("ds::stream_params_from_req")
}

impl StreamParams {
    /// The `$pubsub` subscribe command authorizing and subscribing to the stream.
    pub fn to_subscribe_command(&self) -> Message {
        pending!("ds::stream_params_to_subscribe_command")
    }
}

/// `Cache-Control` of successful catch-up and long-poll reads (DS spec §10.1,
/// upstream unexported `readCacheControl`).
pub fn read_cache_control(_c: &Config, _req: &HttpRequest, _sp: &StreamParams) -> String {
    pending!("ds::read_cache_control")
}

/// Whether an `If-None-Match` value matches `etag` (upstream unexported `etagMatches`).
pub fn etag_matches(_if_none_match: &str, _etag: &str) -> bool {
    pending!("ds::etag_matches")
}

// ------------------------------------------------------------------ stream

/// A durable stream connection: the node session plus the request (upstream `Stream`).
pub struct Stream {
    pub session: Option<Arc<Session>>,
    pub params: StreamParams,
    pub conn: Option<Arc<dyn Connection>>,
    _next_cursor: Mutex<Option<String>>,
}

impl Stream {
    /// `&Stream{Params: params}`.
    pub fn new(params: StreamParams) -> Self {
        Self { session: None, params, conn: None, _next_cursor: Mutex::new(None) }
    }

    /// The CDN-collapsing cursor (DS spec §8.1): the current 20-second interval since
    /// 2024-10-09, strictly greater than the client's cursor; memoized per stream.
    pub fn next_cursor(&self) -> String {
        pending!("ds::stream_next_cursor")
    }
}

// ------------------------------------------------------------------ encoders

/// Encoder of one-shot (catch-up) sessions: encodes nothing (upstream `NoopEncoder`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopEncoder;

impl NodeEncoder for NoopEncoder {
    fn id(&self) -> String {
        pending!("ds::noop_encoder_id")
    }
    fn encode(&self, _msg: &EncodedMessage) -> Result<Option<SentFrame>, Error> {
        pending!("ds::noop_encoder_encode")
    }
    fn encode_transmission(&self, _raw: &str) -> Result<Option<SentFrame>, Error> {
        pending!("ds::noop_encoder_encode_transmission")
    }
    fn decode(&self, _raw: &[u8]) -> Result<Message, Error> {
        pending!("ds::noop_encoder_decode")
    }
}

/// Long-poll encoder: data replies become `<offset>\n<json>` (upstream `PollEncoder`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollEncoder;

impl NodeEncoder for PollEncoder {
    fn id(&self) -> String {
        pending!("ds::poll_encoder_id")
    }
    fn encode(&self, _msg: &EncodedMessage) -> Result<Option<SentFrame>, Error> {
        pending!("ds::poll_encoder_encode")
    }
    fn encode_transmission(&self, _raw: &str) -> Result<Option<SentFrame>, Error> {
        pending!("ds::poll_encoder_encode_transmission")
    }
    fn decode(&self, _raw: &[u8]) -> Result<Message, Error> {
        pending!("ds::poll_encoder_decode")
    }
}

/// SSE encoder: data replies become a `data` event plus a `control` event; pings
/// become comments (upstream `SSEEncoder`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SseEncoder {
    pub cursor: String,
    pub up_to_date: bool,
}

impl NodeEncoder for SseEncoder {
    fn id(&self) -> String {
        pending!("ds::sse_encoder_id")
    }
    fn encode(&self, _msg: &EncodedMessage) -> Result<Option<SentFrame>, Error> {
        pending!("ds::sse_encoder_encode")
    }
    fn encode_transmission(&self, _raw: &str) -> Result<Option<SentFrame>, Error> {
        pending!("ds::sse_encoder_encode_transmission")
    }
    fn decode(&self, _raw: &[u8]) -> Result<Message, Error> {
        pending!("ds::sse_encoder_decode")
    }
}

// ------------------------------------------------------------------ poll connection

/// Builds the `ETag` of a data response from its next offset.
pub type ETagFn = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Long-poll connection: turns the first encoded message into a response body and
/// headers, then is done (upstream `PollConnection`).
pub struct PollConnection {
    _writer: Mutex<Box<dyn ResponseWriter>>,
    /// `Cache-Control` of data responses (upstream field `CacheControl`).
    pub cache_control: Mutex<String>,
    /// `ETag` of data responses (upstream field `ETag`).
    pub etag: Mutex<Option<ETagFn>>,
}

impl PollConnection {
    /// Upstream `NewPollConnection(w)`.
    pub fn new(writer: Box<dyn ResponseWriter>) -> Self {
        Self { _writer: Mutex::new(writer), cache_control: Mutex::new(String::new()), etag: Mutex::new(None) }
    }

    /// Cancelled once a response was written or the connection closed.
    pub fn context(&self) -> CancellationToken {
        pending!("ds::poll_connection_context")
    }

    /// Whether a response was written or the connection closed (upstream unexported
    /// `done` field).
    pub fn is_done(&self) -> bool {
        pending!("ds::poll_connection_done")
    }

    /// Always `None` (upstream `Descriptor()`).
    pub fn descriptor(&self) -> Option<std::net::TcpStream> {
        pending!("ds::poll_connection_descriptor")
    }
}

impl Connection for PollConnection {
    fn write(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        pending!("ds::poll_connection_write")
    }
    fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        pending!("ds::poll_connection_write_binary")
    }
    fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        pending!("ds::poll_connection_write_ping")
    }
    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        pending!("ds::poll_connection_read")
    }
    /// Writes the status for `code` (WebSocket close codes map to HTTP statuses).
    fn close(&self, _code: i32, _reason: &str) {
        pending!("ds::poll_connection_close")
    }
}

// ------------------------------------------------------------------ handler

/// The Durable Streams HTTP endpoint (upstream `DSHandler`).
pub struct Handler {
    _node: Arc<Node>,
    _broker: Arc<dyn Broker>,
    _streams: Arc<StreamsController>,
    _metrics: Option<Arc<dyn Instrumenter>>,
    _shutdown: CancellationToken,
    _extractor: DefaultHeadersExtractor,
    _config: Config,
}

/// Builds the endpoint; fails without a streams controller. `shutdown` is cancelled
/// on server shutdown.
pub fn ds_handler(
    _node: Arc<Node>,
    _broker: Arc<dyn Broker>,
    _streams: Option<Arc<StreamsController>>,
    _metrics: Option<Arc<dyn Instrumenter>>,
    _shutdown: CancellationToken,
    _extractor: DefaultHeadersExtractor,
    _config: &Config,
) -> Result<Handler, DsError> {
    pending!("ds::ds_handler")
}

impl Handler {
    /// Serves one request until it completes, the client goes away (`request_done`
    /// is cancelled), the TTL/poll interval elapses or the server shuts down.
    pub fn serve(&self, _req: &HttpRequest, _w: &mut dyn ResponseWriter, _request_done: &CancellationToken) {
        pending!("ds::handler_serve")
    }
}
