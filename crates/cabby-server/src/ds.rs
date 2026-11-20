//! Durable Streams read API (upstream package `ds`, see `ds/DESIGN.md` upstream):
//! catch-up reads, long-poll and SSE over the broker's stream history.
//!
//! * Catch-up (`GET /ds/<stream>?offset=...`): history from the broker as a JSON
//!   array, with `Stream-Next-Offset`, `Stream-Cursor`, `Stream-Up-To-Date`,
//!   `Cache-Control` and an `ETag` (`304 Not Modified` on a match).
//! * Long-poll (`live=long-poll`): history if there is any, otherwise the session
//!   subscribes and the first live message is the response ([`PollConnection`]);
//!   `204` after `poll_interval`, `410` on shutdown.
//! * SSE (`live=sse`): the history as a `data` event plus a `control` event, then
//!   live messages ([`SseEncoder`] over an [`sse::Connection`]) for up to `sse_ttl`.
//! * `HEAD`: the tail offset.
//!
//! Requests are authenticated like any session (unless `skip_auth`), and the
//! stream is authorized with the streams controller (signed stream names).
//!
//! As in [`crate::sse`], setup runs on a blocking thread and the live wait is
//! async. Long-poll sessions have no pings: upstream's long-poll encoder encodes
//! pings as nothing, so they would never reach the client.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use cabby_core::broker::Broker;
use cabby_core::common::{Message, PING_TYPE, Status, StreamMessage};
use cabby_core::encoders::{EncodeError, EncodedMessageJson, Encoder as NodeEncoder, SentFrame, no_frame};
use cabby_core::logger::{Attr, Level, LogValue, Logger};
use cabby_core::streams::StreamsController;
use cabby_core::{log_debug, log_error};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::metrics::Instrumenter;
use crate::node::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_INTERNAL_SERVER_ERR, CLOSE_NORMAL_CLOSURE, Connection,
    ConnectionError, Node, Session, SessionOption,
};
use crate::server::{
    self, HeadersExtractor, HttpRequest, RequestInfo, ResponseFuture, ResponseWriter, SharedResponseWriter,
    lock_writer, query_param, shared_writer, write_cors_headers,
};
use crate::sse;

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

/// Records of the handler before a session exists (upstream `l.With("transport", "ds")`).
fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("transport", LogValue::String("ds".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

/// The session's logger (upstream `session.Log.With("ds", path)`, where
/// `session.Log` is the node's logger with the session id).
fn session_logger(sid: &str, path: &str) -> Logger {
    Logger::default().with("context", "node").with("sid", sid).with("ds", path)
}

fn attr(key: &str, value: impl std::fmt::Display) -> Attr {
    Attr::new(key, LogValue::String(value.to_string()))
}

// ------------------------------------------------------------------ offsets

/// Separates the offset and the epoch in an encoded offset.
const OFFSET_SEPARATOR: &str = "::";

/// Encodes an offset and a broker epoch into one opaque offset (`<offset>::<epoch>`,
/// `"0"` without an epoch).
pub fn encode_offset(offset: u64, epoch: &str) -> String {
    // Empty stream.
    if epoch.is_empty() {
        return "0".to_owned();
    }
    format!("{offset}{OFFSET_SEPARATOR}{epoch}")
}

/// Go `strconv.ParseUint(s, 10, 64)` with its error text.
fn parse_uint(s: &str) -> Result<u64, DsError> {
    let syntax = || DsError(format!("strconv.ParseUint: parsing {s:?}: invalid syntax"));
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(syntax());
    }
    s.parse().map_err(|_| DsError(format!("strconv.ParseUint: parsing {s:?}: value out of range")))
}

/// Decodes an opaque offset into `(offset, epoch)`; the reserved offsets `""`, `"0"`,
/// `"-1"` and `"now"` decode as `(0, "")`.
pub fn decode_offset(offset: &str) -> Result<(u64, String), DsError> {
    if offset.is_empty() || offset == "0" || offset == START_OFFSET || offset == NOW_OFFSET {
        return Ok((0, String::new()));
    }

    let mut parts = offset.split(OFFSET_SEPARATOR);
    let number = parse_uint(parts.next().unwrap_or_default())?;
    let epoch = parts.next().unwrap_or_default().to_owned();
    Ok((number, epoch))
}

/// The messages' data as a JSON array (raw bytes, in order).
pub fn encode_json_batch(batch: &[StreamMessage]) -> Vec<u8> {
    let len = batch.iter().map(|m| m.data.len() + 1).sum::<usize>() + 2;
    let mut buf = Vec::with_capacity(len);
    buf.push(b'[');
    for (i, msg) in batch.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(msg.data.as_bytes());
    }
    buf.push(b']');
    buf
}

/// A batch as an SSE `data` event.
pub fn encode_sse_data_event(messages: &[StreamMessage]) -> Vec<u8> {
    let mut buf = b"event: data\ndata:".to_vec();
    buf.extend_from_slice(&encode_json_batch(messages));
    buf
}

/// The `control` event payload: Go marshals a `map[string]interface{}`, so the
/// keys are sorted.
fn control_json(offset: &str, cursor: &str, up_to_date: bool) -> String {
    let mut control = std::collections::BTreeMap::new();
    control.insert("streamNextOffset", serde_json::Value::from(offset));
    if !cursor.is_empty() {
        control.insert("streamCursor", serde_json::Value::from(cursor));
    }
    if up_to_date {
        control.insert("upToDate", serde_json::Value::Bool(true));
    }
    cabby_core::gojson::to_string(&control)
}

/// An SSE `control` event with the next offset, the cursor and the up-to-date flag.
pub fn encode_sse_control_event(offset: &str, cursor: &str, up_to_date: bool) -> Vec<u8> {
    format!("event: control\ndata:{}", control_json(offset, cursor, up_to_date)).into_bytes()
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

/// The decoded path of a request target (Go `r.URL.Path`): `%XX` escapes are
/// decoded; a target with an invalid escape keeps its raw path.
fn url_path(target: &str) -> String {
    let mut path = target.split(['?', '#']).next().unwrap_or_default();
    // An absolute URL (`http://host/path`).
    if let Some(rest) = path.split_once("://").map(|(_, r)| r) {
        path = rest.find('/').map_or("", |i| &rest[i..]);
    }
    if !path.contains('%') {
        return path.to_owned();
    }
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok());
            match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                Some(b) => {
                    out.push(b);
                    i += 3;
                    continue;
                }
                None => return path.to_owned(),
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses and validates a request against the mount path (upstream `StreamParamsFromReq`).
pub fn stream_params_from_req(req: &HttpRequest, c: &Config) -> Result<StreamParams, DsError> {
    // The stream name is the URL path below the mount path.
    let path = url_path(&req.target);
    let stream_path = path.strip_prefix(c.path.as_str()).unwrap_or(&path);
    let stream_path = stream_path.strip_prefix('/').unwrap_or(stream_path);

    if stream_path.is_empty() {
        return Err(DsError("stream is missing in the URL".to_owned()));
    }

    let mut offset_str = query_param(&req.target, OFFSET_QUERY_PARAM);
    let live_mode = query_param(&req.target, LIVE_QUERY_PARAM);
    let cursor = query_param(&req.target, CURSOR_QUERY_PARAM);

    if live_mode != LONG_POLL_MODE && live_mode != SSE_MODE && !live_mode.is_empty() {
        return Err(DsError("invalid live mode".to_owned()));
    }

    if offset_str.is_empty() && !live_mode.is_empty() {
        return Err(DsError("offset is missing for live mode".to_owned()));
    }

    // Catch-up reads start from the beginning by default.
    if offset_str.is_empty() {
        offset_str = START_OFFSET.to_owned();
    }

    let (offset, epoch) = decode_offset(&offset_str)?;

    let mut signed_name = query_param(&req.target, SIGNED_STREAM_PARAM);
    if signed_name.is_empty() {
        signed_name = req.header(SIGNED_STREAM_HEADER).to_owned();
    }

    Ok(StreamParams {
        name: stream_path.to_owned(),
        path: stream_path.to_owned(),
        signed_name,
        cursor,
        offset,
        epoch,
        raw_offset: offset_str,
        live_mode,
    })
}

/// The `$pubsub` subscription identifier of a stream (upstream unexported
/// `subscribeIdentifier`).
#[derive(Serialize)]
struct SubscribeIdentifier<'a> {
    channel: &'a str,
    stream_name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    signed_stream_name: &'a str,
}

/// Upstream `pubsubChannel`.
const PUBSUB_CHANNEL: &str = "$pubsub";

impl StreamParams {
    /// The `$pubsub` subscribe command authorizing and subscribing to the stream.
    pub fn to_subscribe_command(&self) -> Message {
        let identifier = SubscribeIdentifier {
            channel: PUBSUB_CHANNEL,
            stream_name: &self.name,
            signed_stream_name: &self.signed_name,
        };
        Message {
            command: "subscribe".to_owned(),
            identifier: cabby_core::gojson::to_string(&identifier),
            ..Default::default()
        }
    }
}

/// `Cache-Control` of successful catch-up and long-poll reads (DS spec §10.1,
/// upstream unexported `readCacheControl`).
pub fn read_cache_control(c: &Config, req: &HttpRequest, sp: &StreamParams) -> String {
    // The tail offset changes with each append, so it must not be cached.
    if sp.raw_offset == NOW_OFFSET {
        return "no-store".to_owned();
    }

    let visibility = if is_shared_cacheable(c, req) { "public" } else { "private" };

    format!("{visibility}, max-age=60, stale-while-revalidate=300")
}

/// Whether access is fully determined by the URL, so shared caches (CDNs) may store
/// the response: authentication is skipped and no signed name comes in a header
/// (upstream unexported `isSharedCacheable`).
fn is_shared_cacheable(c: &Config, req: &HttpRequest) -> bool {
    if !c.skip_auth {
        return false;
    }
    !query_param(&req.target, SIGNED_STREAM_PARAM).is_empty() || req.header(SIGNED_STREAM_HEADER).is_empty()
}

/// `"<base64(path)>:<requested offset>:<next offset>"` (upstream unexported `streamETag`).
fn stream_etag(sp: &StreamParams, next_offset: &str) -> String {
    let path = base64::engine::general_purpose::STANDARD.encode(sp.path.as_bytes());
    format!("\"{path}:{}:{next_offset}\"", sp.raw_offset)
}

/// Whether an `If-None-Match` value matches `etag` (upstream unexported `etagMatches`).
pub fn etag_matches(if_none_match: &str, etag: &str) -> bool {
    if if_none_match.is_empty() {
        return false;
    }
    if_none_match.split(',').map(str::trim).any(|c| c == "*" || c.strip_prefix("W/").unwrap_or(c) == etag)
}

/// The history to return for the requested offset (upstream unexported
/// `fetchHistory`): everything for the start offsets, nothing for `now`,
/// otherwise the messages after the offset in its epoch.
fn fetch_history(brk: &dyn Broker, sp: &StreamParams) -> Result<Vec<StreamMessage>, DsError> {
    let broker_error = |e: cabby_core::broker::BrokerError| DsError(e.message);

    if sp.raw_offset == START_OFFSET || sp.raw_offset == "0" {
        return brk.history_since(&sp.path, 0).map(Option::unwrap_or_default).map_err(broker_error);
    }

    if sp.raw_offset == NOW_OFFSET {
        return Ok(Vec::new());
    }

    if sp.epoch.is_empty() {
        return Err(DsError("no epoch in the offset".to_owned()));
    }

    brk.history_from(&sp.path, &sp.epoch, sp.offset).map_err(broker_error)
}

// ------------------------------------------------------------------ stream

/// Cursor epoch for CDN collapsing (2024-10-09 00:00:00 UTC, upstream `cursorEpoch`).
const CURSOR_EPOCH_UNIX: u64 = 1_728_432_000;

/// Cursor interval in seconds (upstream `cursorInterval`).
const CURSOR_INTERVAL: u64 = 20;

/// A durable stream connection: the node session plus the request (upstream `Stream`).
pub struct Stream {
    pub session: Option<Arc<Session>>,
    pub params: StreamParams,
    pub conn: Option<Arc<dyn Connection>>,
    next_cursor: Mutex<Option<String>>,
}

impl Stream {
    /// `&Stream{Params: params}`.
    pub fn new(params: StreamParams) -> Self {
        Self { session: None, params, conn: None, next_cursor: Mutex::new(None) }
    }

    /// The CDN-collapsing cursor (DS spec §8.1): the current 20-second interval since
    /// 2024-10-09, strictly greater than the client's cursor; memoized per stream.
    pub fn next_cursor(&self) -> String {
        let mut next = self.next_cursor.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = next.as_ref() {
            return c.clone();
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let since_epoch = now.as_secs_f64() - CURSOR_EPOCH_UNIX as f64;
        let mut current = (since_epoch / CURSOR_INTERVAL as f64) as i64;

        // A client cursor at or past the current interval: return a strictly
        // greater one (1 to 60 intervals ahead) to keep cursors progressing.
        if !self.params.cursor.is_empty()
            && let Ok(client) = self.params.cursor.parse::<i64>()
            && client >= current
        {
            let jitter = (now.as_nanos() % 60) as i64;
            current = client.saturating_add(1).saturating_add(jitter);
        }

        let cursor = current.to_string();
        *next = Some(cursor.clone());
        cursor
    }
}

// ------------------------------------------------------------------ encoders

const DS_NOOP_ENCODER_ID: &str = "dsnull";
const DS_POLL_ENCODER_ID: &str = "dspoll";
const DS_SSE_ENCODER_ID: &str = "dsse";

fn unsupported() -> EncodeError {
    EncodeError("unsupported".to_owned())
}

/// The data reply with offset information a DS encoder encodes, if `msg` is one:
/// other messages (typed replies, pings, disconnects) and replies without an
/// offset and epoch are skipped.
fn data_reply(msg: &dyn EncodedMessageJson) -> Option<&cabby_core::common::Reply> {
    let reply = msg.as_reply().filter(|r| r.kind.is_empty())?;
    (reply.offset != 0 && !reply.epoch.is_empty()).then_some(reply)
}

/// Encoder of one-shot (catch-up) sessions: encodes nothing (upstream `NoopEncoder`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopEncoder;

impl NodeEncoder for NoopEncoder {
    fn id(&self) -> String {
        DS_NOOP_ENCODER_ID.to_owned()
    }

    fn encode(&self, _msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        Err(no_frame())
    }

    fn encode_transmission(&self, _raw: &str) -> Result<SentFrame, EncodeError> {
        Err(no_frame())
    }

    fn decode(&self, _raw: &[u8]) -> Result<Message, EncodeError> {
        Err(unsupported())
    }

    fn encode_optional(&self, _msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
    }

    fn encode_transmission_optional(&self, _raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
    }
}

/// Long-poll encoder: data replies become `<offset>\n<json>` (upstream `PollEncoder`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollEncoder;

impl NodeEncoder for PollEncoder {
    fn id(&self) -> String {
        DS_POLL_ENCODER_ID.to_owned()
    }

    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        self.encode_optional(msg)?.ok_or_else(no_frame)
    }

    fn encode_transmission(&self, _raw: &str) -> Result<SentFrame, EncodeError> {
        Err(no_frame())
    }

    fn decode(&self, _raw: &[u8]) -> Result<Message, EncodeError> {
        Err(unsupported())
    }

    fn encode_optional(&self, msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        let Some(reply) = data_reply(msg) else { return Ok(None) };
        let mut payload = encode_offset(reply.offset, &reply.epoch).into_bytes();
        payload.push(b'\n');
        cabby_core::gojson::write(&mut payload, &reply.message);
        Ok(Some(SentFrame::text(payload)))
    }

    fn encode_transmission_optional(&self, _raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
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
        DS_SSE_ENCODER_ID.to_owned()
    }

    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        self.encode_optional(msg)?.ok_or_else(no_frame)
    }

    fn encode_transmission(&self, _raw: &str) -> Result<SentFrame, EncodeError> {
        Err(no_frame())
    }

    fn decode(&self, _raw: &[u8]) -> Result<Message, EncodeError> {
        Err(unsupported())
    }

    fn encode_optional(&self, msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        if msg.get_type() == PING_TYPE {
            return Ok(Some(SentFrame::text(sse::COMMENT_PING.as_bytes().to_vec())));
        }

        let Some(reply) = data_reply(msg) else { return Ok(None) };

        // A data event (an array, as catch-up data), then a control event.
        let mut payload = b"event: data\ndata:[".to_vec();
        cabby_core::gojson::write(&mut payload, &reply.message);
        payload.extend_from_slice(b"]\n\nevent: control\ndata:");
        let offset = encode_offset(reply.offset, &reply.epoch);
        payload.extend_from_slice(control_json(&offset, &self.cursor, self.up_to_date).as_bytes());

        Ok(Some(SentFrame::text(payload)))
    }

    fn encode_transmission_optional(&self, _raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
    }
}

// ------------------------------------------------------------------ poll connection

/// Builds the `ETag` of a data response from its next offset.
pub type ETagFn = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Long-poll connection: turns the first encoded message into a response body and
/// headers, then is done (upstream `PollConnection`). Closing it sends the status
/// matching the close code.
pub struct PollConnection {
    writer: SharedResponseWriter,
    done: Mutex<bool>,
    ctx: CancellationToken,
    /// `Cache-Control` of data responses (upstream field `CacheControl`).
    pub cache_control: Mutex<String>,
    /// `ETag` of data responses (upstream field `ETag`).
    pub etag: Mutex<Option<ETagFn>>,
}

fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn set_header(h: &mut http::HeaderMap, name: &'static str, value: &str) {
    if let Ok(v) = http::HeaderValue::from_str(value) {
        h.insert(name, v);
    }
}

/// The HTTP status for a close code: WebSocket close codes the node uses map to
/// statuses, anything else is a status already (upstream unexported `wsCodeToHTTP`).
fn ws_code_to_http(code: i32, reason: &str) -> u16 {
    match code {
        CLOSE_NORMAL_CLOSURE if reason == "Auth Failed" => 401,
        CLOSE_NORMAL_CLOSURE => 200,
        CLOSE_GOING_AWAY => 410,
        CLOSE_ABNORMAL_CLOSURE => 400,
        CLOSE_INTERNAL_SERVER_ERR => 500,
        other => u16::try_from(other).unwrap_or(500),
    }
}

impl PollConnection {
    /// Upstream `NewPollConnection(w)`.
    pub fn new(writer: Box<dyn ResponseWriter>) -> Self {
        Self::with_writer(shared_writer(writer))
    }

    /// A connection writing to a writer the handler shares.
    pub fn with_writer(writer: SharedResponseWriter) -> Self {
        Self {
            writer,
            done: Mutex::new(false),
            ctx: CancellationToken::new(),
            cache_control: Mutex::new(String::new()),
            etag: Mutex::new(None),
        }
    }

    /// Cancelled once a response was written or the connection closed.
    pub fn context(&self) -> CancellationToken {
        self.ctx.clone()
    }

    /// Whether a response was written or the connection closed (upstream unexported
    /// `done` field).
    pub fn is_done(&self) -> bool {
        *lock(&self.done)
    }

    /// Always `None` (upstream `Descriptor()`).
    pub fn descriptor(&self) -> Option<std::net::TcpStream> {
        None
    }
}

impl Connection for PollConnection {
    /// Writes `[<body>]` with the offset headers of an encoded `<offset>\n<body>`
    /// message (see [`PollEncoder`]); only the first write or close counts.
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let mut done = lock(&self.done);
        if *done {
            return Ok(());
        }
        *done = true;
        let _cancel = self.ctx.clone().drop_guard();

        let Some(pos) = msg.iter().position(|b| *b == b'\n') else {
            return Err(ConnectionError("invalid poll message format: missing newline separator".to_owned()));
        };
        let offset = String::from_utf8_lossy(&msg[..pos]).into_owned();
        let mut body = Vec::with_capacity(msg.len() + 1);
        body.push(b'[');
        body.extend_from_slice(&msg[pos + 1..]);
        body.push(b']');

        let cache_control = lock(&self.cache_control).clone();
        let etag = lock(&self.etag).clone();

        let mut w = lock_writer(&self.writer);
        let h = w.headers_mut();
        set_header(h, STREAM_OFFSET_HEADER, &offset);
        set_header(h, STREAM_UP_TO_DATE_HEADER, "true");
        if !cache_control.is_empty() {
            set_header(h, "cache-control", &cache_control);
        }
        if let Some(etag) = etag {
            set_header(h, "etag", &etag(&offset));
        }
        w.write(&body).map_err(|e| ConnectionError(e.to_string()))?;
        w.flush();
        Ok(())
    }

    fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        Err(ConnectionError("unsupported".to_owned()))
    }

    fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }

    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError("unsupported".to_owned()))
    }

    /// Writes the status for `code` (WebSocket close codes map to HTTP statuses).
    fn close(&self, code: i32, reason: &str) {
        let mut done = lock(&self.done);
        if *done {
            return;
        }
        *done = true;
        let _cancel = self.ctx.clone().drop_guard();

        let status = ws_code_to_http(code, reason);

        let mut w = lock_writer(&self.writer);
        // Only data responses may be cached (204 No Content, errors, etc. must not);
        // 304 Not Modified keeps the headers of the data response it stands for.
        if status != 200 && status != 304 {
            set_header(w.headers_mut(), "cache-control", "no-store");
        }
        w.write_header(status);
    }
}

// ------------------------------------------------------------------ handler

/// Headers extractor shared with the handler's threads.
pub type SharedHeadersExtractor = Arc<dyn HeadersExtractor + Send + Sync>;

/// The Durable Streams HTTP endpoint (upstream `DSHandler`).
pub struct Handler {
    node: Arc<Node>,
    broker: Arc<dyn Broker>,
    streams: Arc<StreamsController>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: Config,
    allowed_hosts: Vec<String>,
}

/// Builds the endpoint (upstream `DSHandler(n, brk, st, m, shutdownCtx,
/// headersExtractor, c, l)`); fails without a streams controller. `shutdown` is
/// cancelled on server shutdown (upstream `ShutdownCtx`).
pub fn ds_handler(
    node: Arc<Node>,
    broker: Arc<dyn Broker>,
    streams: Option<Arc<StreamsController>>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: &Config,
) -> Result<Handler, DsError> {
    let Some(streams) = streams else {
        return Err(DsError("streams controller is required".to_owned()));
    };

    let allowed_hosts = if config.allowed_origins.is_empty() {
        Vec::new()
    } else {
        config.allowed_origins.split(',').map(str::to_owned).collect()
    };

    if let Some(m) = &metrics {
        m.register_counter(METRICS_REQUESTS_TOTAL, "The total number of durable streams HEAD/GET requests");
        m.register_counter(METRICS_POLL_TOTAL, "The total number of durable streams long-poll requests");
        m.register_counter(METRICS_SSE_TOTAL, "The total number of durable streams SSE requests");
        m.register_gauge(METRICS_POLL_NUM, "The number of active durable streams long-poll clients");
        m.register_gauge(METRICS_SSE_NUM, "The number of active durable streams SSE clients");
    }

    Ok(Handler { node, broker, streams, metrics, shutdown, extractor, config: config.clone(), allowed_hosts })
}

/// The endpoint as a server handler: mount it at `<config.path>/*` (upstream
/// `wsServer.SetupHandler(config.DS.Path + "/*", dsHandler)`).
pub fn handler(
    node: Arc<Node>,
    broker: Arc<dyn Broker>,
    streams: Option<Arc<StreamsController>>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: &Config,
) -> Result<impl server::Handler, DsError> {
    Ok(endpoint(Arc::new(ds_handler(node, broker, streams, metrics, shutdown, extractor, config)?)))
}

/// A built endpoint as a server handler: what [`handler`] mounts (tests serve
/// a handler they also inspect through the server this way).
pub fn endpoint(handler: Arc<Handler>) -> impl server::Handler {
    Endpoint(handler)
}

/// The session of a request: disconnected from the node when dropped (upstream
/// `defer stream.Session.Disconnect("done", ws.CloseNormalClosure)`), after the
/// mode's gauge is decremented.
struct StreamGuard {
    session: Arc<Session>,
    gauge: Option<(Arc<dyn Instrumenter>, &'static str)>,
    log: Logger,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if let Some((m, name)) = self.gauge.take() {
            m.gauge_decrement(name);
        }
        log_debug!(self.log, "session completed");
        self.session.disconnect("done", CLOSE_NORMAL_CLOSURE);
    }
}

/// A live request waiting for data.
enum Live {
    LongPoll(Arc<PollConnection>),
    Sse(Arc<sse::Connection>),
}

struct Active {
    live: Live,
    guard: StreamGuard,
}

/// A session and its connections (upstream `NewDSSession`'s results).
struct DsSession {
    stream: Stream,
    session: Arc<Session>,
    poll: Arc<PollConnection>,
    sse: Option<Arc<sse::Connection>>,
}

/// Runs a blocking node call off the async executor.
async fn blocking(f: impl FnOnce() + Send + 'static) {
    let _ = tokio::task::spawn_blocking(f).await;
}

impl Handler {
    /// Serves one request until it completes, the client goes away (`request_done`
    /// is cancelled), the TTL/poll interval elapses or the server shuts down; the
    /// writer is finished on return. Blocks the calling thread (which must not be
    /// a Tokio worker); the server uses [`handler`] instead.
    pub fn serve(&self, req: &HttpRequest, w: Box<dyn ResponseWriter>, request_done: &CancellationToken) {
        let w = shared_writer(w);
        if let Some(active) = self.prepare(req, &w) {
            server::runtime_handle().block_on(self.wait(&active, request_done));
            drop(active);
        }
        lock_writer(&w).finish();
    }

    /// [`Handler::serve`] for the async server.
    async fn serve_async(
        self: Arc<Self>,
        req: HttpRequest,
        w: Box<dyn ResponseWriter>,
        request_done: CancellationToken,
    ) {
        let w = shared_writer(w);
        let (this, writer) = (self.clone(), w.clone());
        match tokio::task::spawn_blocking(move || this.prepare(&req, &writer)).await {
            Ok(Some(active)) => {
                self.wait(&active, &request_done).await;
                blocking(move || drop(active)).await;
            }
            Ok(None) => {}
            Err(_) => lock_writer(&w).write_header(500),
        }
        lock_writer(&w).finish();
    }

    /// Builds the session of a request and authenticates it (upstream `NewDSSession`).
    fn new_ds_session(
        &self,
        w: &SharedResponseWriter,
        info: &RequestInfo,
        sp: StreamParams,
    ) -> (Arc<PollConnection>, Result<DsSession, DsError>) {
        let stream = Stream::new(sp);
        let poll = Arc::new(PollConnection::with_writer(w.clone()));
        let mut conn: Arc<dyn Connection> = poll.clone();
        let mut sse_conn = None;

        let opts = match stream.params.live_mode.as_str() {
            // A one-off session that is not registered for broadcasts; it gives all
            // modes a common interface.
            "" => vec![SessionOption::Encoder(Arc::new(NoopEncoder)), SessionOption::Inactive],
            // Long-poll pings would be encoded as nothing (see the module docs).
            LONG_POLL_MODE => {
                vec![SessionOption::Encoder(Arc::new(PollEncoder)), SessionOption::PingInterval(Duration::ZERO)]
            }
            SSE_MODE => {
                let c = Arc::new(sse::Connection::with_writer(w.clone()));
                conn = c.clone();
                sse_conn = Some(c);
                let encoder = SseEncoder { cursor: stream.next_cursor(), up_to_date: false };
                let mut opts = vec![SessionOption::Encoder(Arc::new(encoder))];
                if self.config.sse_ping_interval > 0 {
                    opts.push(SessionOption::PingInterval(Duration::from_secs(self.config.sse_ping_interval as u64)));
                }
                opts
            }
            other => return (poll, Err(DsError(format!("unsupported live mode: {other}")))),
        };

        let session = Session::new(&self.node, conn.clone(), &info.url, info.headers.clone(), &info.uid, opts);

        if self.config.skip_auth {
            log_debug!(session_logger(&info.uid, &stream.params.path), "skip authentication");
            self.node.authenticated(&session, &format!("ds::{}", session.get_id()));
        } else {
            match self.node.authenticate(&session, &[]) {
                Err(e) => return (poll, Err(DsError(e.0))),
                Ok(Some(res)) if res.status != Status::Failure => {}
                // Rejected, or closed while authenticating (Go would get a nil result).
                Ok(_) => return (poll, Err(DsError("authentication rejected".to_owned()))),
            }
        }

        let mut stream = stream;
        stream.session = Some(session.clone());
        stream.conn = Some(conn);
        (poll.clone(), Ok(DsSession { stream, session, poll, sse: sse_conn }))
    }

    /// Everything up to the live wait. `None` when the response is complete.
    fn prepare(&self, req: &HttpRequest, w: &SharedResponseWriter) -> Option<Active> {
        {
            let mut w = lock_writer(w);
            write_cors_headers(w.headers_mut(), req.header("Origin"), &self.allowed_hosts);

            // Respond to preflight requests.
            if req.method == "OPTIONS" {
                w.write_header(200);
                return None;
            }

            set_header(w.headers_mut(), "x-anycable-version", &crate::telemetry::version());
        }
        let status = |code: u16| lock_writer(w).write_header(code);

        let params = match stream_params_from_req(req, &self.config) {
            Ok(p) => p,
            Err(e) => {
                log(Level::Debug, "invalid stream request", &[attr("err", e)]);
                status(400);
                return None;
            }
        };

        // Only GET and HEAD read data.
        if req.method != "GET" && req.method != "HEAD" {
            log(Level::Debug, "non-read operations are not supported", &[]);
            status(405);
            return None;
        }

        let extractor = self.extractor.as_deref().map(|e| e as &dyn HeadersExtractor);
        let Ok(info) = RequestInfo::new(req, extractor) else {
            status(400);
            return None;
        };

        // Authenticate and init the stream.
        let (poll, ds) = self.new_ds_session(w, &info, params);
        let ds = match ds {
            Ok(ds) => ds,
            Err(e) => {
                // Upstream logs through the handler's logger here, without `transport`.
                log_debug!(Logger::default(), "unauthenticated", "err" => e.to_string());
                poll.close(401, "Auth Failed");
                return None;
            }
        };
        let DsSession { stream, session, poll, sse: sse_conn } = ds;
        let sp = &stream.params;
        let sid = session.get_id();
        let slog = session_logger(&sid, &sp.path);

        // Stream access check: the verified stream (e.g. of a signed stream name)
        // must be the requested one, since the data is read by the URL's name.
        let identifier = sp.to_subscribe_command().identifier;
        let verified = match self.streams.verified_stream(&identifier) {
            Ok((_, verified)) if verified == sp.name => Ok(()),
            Ok((_, verified)) => Err(format!("stream mismatch: requested {:?}, authorized {verified:?}", sp.name)),
            Err(e) => Err(e.message),
        };
        if let Err(e) = verified {
            log_debug!(slog, "unauthorized", "err" => e);
            poll.close(401, "unauthorized");
            // Make sure it is removed from the node.
            session.disconnect("unauthorized", 401);
            return None;
        }

        log_debug!(slog, "session established", "mode" => &sp.live_mode, "offset" => &sp.raw_offset);
        let mut guard = StreamGuard { session: session.clone(), gauge: None, log: slog.clone() };

        let tail = match self.broker.peak(&sp.name) {
            Ok(tail) => tail.unwrap_or_default(),
            Err(e) => {
                log_error!(slog, "failed to get stream metadata", "err" => e.message);
                poll.close(500, "");
                return None;
            }
        };

        if req.method == "HEAD" {
            {
                let mut w = lock_writer(w);
                let h = w.headers_mut();
                set_header(h, "content-type", "application/json");
                set_header(h, "cache-control", "no-store");
                set_header(h, STREAM_OFFSET_HEADER, &encode_offset(tail.offset, &tail.epoch));
            }
            poll.close(200, "");
            return None;
        }

        if let Some(m) = &self.metrics {
            m.counter_increment(METRICS_REQUESTS_TOTAL);
        }

        let live = match sse_conn {
            None => self.prepare_http(req, w, &stream, &poll, tail, &mut guard)?,
            Some(conn) => self.prepare_sse(w, &stream, &session, &poll, conn, tail, &mut guard)?,
        };
        Some(Active { live, guard })
    }

    /// Catch-up and long-poll reads (upstream unexported `handleHTTP` up to its wait).
    fn prepare_http(
        &self,
        req: &HttpRequest,
        w: &SharedResponseWriter,
        s: &Stream,
        poll: &Arc<PollConnection>,
        mut tail: StreamMessage,
        guard: &mut StreamGuard,
    ) -> Option<Live> {
        let slog = guard.log.clone();
        {
            let mut w = lock_writer(w);
            let h = w.headers_mut();
            set_header(h, "content-type", "application/json");
            set_header(h, "x-content-type-options", "nosniff");
            set_header(h, STREAM_CURSOR_HEADER, &s.next_cursor());
        }

        let backlog = match fetch_history(self.broker.as_ref(), &s.params) {
            Ok(b) => b,
            Err(e) => {
                log_error!(slog, "failed to fetch history", "err" => e.to_string());
                // Either the offset is unreachable or the epoch has changed.
                poll.close(410, "");
                return None;
            }
        };

        if let Some(last) = backlog.last() {
            tail = last.clone();
        }

        let cache_control = read_cache_control(&self.config, req, &s.params);

        if !backlog.is_empty() || s.params.live_mode != LONG_POLL_MODE {
            let next_offset = encode_offset(tail.offset, &tail.epoch);
            {
                let mut w = lock_writer(w);
                let h = w.headers_mut();
                set_header(h, "cache-control", &cache_control);
                set_header(h, STREAM_OFFSET_HEADER, &next_offset);
                set_header(h, STREAM_UP_TO_DATE_HEADER, "true");
            }

            if s.params.raw_offset != NOW_OFFSET {
                let etag = stream_etag(&s.params, &next_offset);
                set_header(lock_writer(w).headers_mut(), "etag", &etag);

                if etag_matches(req.header("If-None-Match"), &etag) {
                    log_debug!(slog, "not modified");
                    poll.close(304, "");
                    return None;
                }
            }

            log_debug!(slog, "write history", "len" => backlog.len());
            poll.close(200, "");
            // Upstream ignores this error too.
            let _ = lock_writer(w).write(&encode_json_batch(&backlog));
            return None;
        }

        if let Some(m) = &self.metrics {
            m.counter_increment(METRICS_POLL_TOTAL);
            m.gauge_increment(METRICS_POLL_NUM);
            guard.gauge = Some((m.clone(), METRICS_POLL_NUM));
        }

        // Set before subscribing: the response may be written right away.
        *lock(&poll.cache_control) = cache_control;
        if s.params.raw_offset != NOW_OFFSET {
            let params = s.params.clone();
            *lock(&poll.etag) = Some(Arc::new(move |next: &str| stream_etag(&params, next)));
        }

        if let Err(e) = self.node.subscribe(&guard.session, &s.params.to_subscribe_command()) {
            log_error!(slog, "failed to subscribe", "err" => e.to_string());
            poll.close(500, "");
            return None;
        }

        Some(Live::LongPoll(poll.clone()))
    }

    /// SSE reads (upstream unexported `handleSSE` up to its wait).
    #[allow(clippy::too_many_arguments)]
    fn prepare_sse(
        &self,
        w: &SharedResponseWriter,
        s: &Stream,
        session: &Arc<Session>,
        poll: &Arc<PollConnection>,
        conn: Arc<sse::Connection>,
        mut tail: StreamMessage,
        guard: &mut StreamGuard,
    ) -> Option<Live> {
        let slog = guard.log.clone();
        {
            let mut w = lock_writer(w);
            let h = w.headers_mut();
            // HTTP/1.1 only (the server speaks nothing else).
            set_header(h, "connection", "keep-alive");
            set_header(h, "content-type", "text/event-stream");
            set_header(h, "cache-control", "private, no-cache, no-store, must-revalidate, max-age=0");
            set_header(h, "x-content-type-options", "nosniff");
            set_header(h, "x-accel-buffering", "no");
        }

        if let Some(m) = &self.metrics {
            m.counter_increment(METRICS_SSE_TOTAL);
            m.gauge_increment(METRICS_SSE_NUM);
            guard.gauge = Some((m.clone(), METRICS_SSE_NUM));
        }

        let backlog = match fetch_history(self.broker.as_ref(), &s.params) {
            Ok(b) => b,
            Err(e) => {
                log_error!(slog, "failed to fetch history", "err" => e.to_string());
                poll.close(410, "");
                return None;
            }
        };

        if let Some(last) = backlog.last() {
            tail = last.clone();
        }

        poll.close(200, "");
        lock_writer(w).flush();

        let offset = encode_offset(tail.offset, &tail.epoch);
        let cursor = s.next_cursor();

        if !backlog.is_empty() {
            log_debug!(slog, "write history", "len" => backlog.len());
            let mut w = lock_writer(w);
            let mut out = encode_sse_data_event(&backlog);
            out.extend_from_slice(b"\n\n");
            out.extend_from_slice(&encode_sse_control_event(&offset, &cursor, true));
            out.extend_from_slice(b"\n\n");
            // Upstream ignores these errors too.
            let _ = w.write(&out);
            w.flush();
        }

        conn.established();

        if let Err(e) = self.node.subscribe(session, &s.params.to_subscribe_command()) {
            log_error!(slog, "failed to subscribe", "err" => e.to_string());
            return None;
        }

        Some(Live::Sse(conn))
    }

    /// Waits for the live response to complete.
    async fn wait(&self, active: &Active, request_done: &CancellationToken) {
        let slog = &active.guard.log;
        match &active.live {
            Live::LongPoll(poll) => {
                let flushed = poll.context();
                let timeout = tokio::time::sleep(Duration::from_secs(self.config.poll_interval.max(0) as u64));
                tokio::pin!(timeout);
                let mut shutdown_received = false;
                loop {
                    tokio::select! {
                        () = self.shutdown.cancelled(), if !shutdown_received => {
                            shutdown_received = true;
                            log_debug!(slog, "server shutdown");
                            poll.close(410, "Server shutdown");
                        }
                        () = &mut timeout => {
                            poll.close(204, "No content");
                            return;
                        }
                        () = request_done.cancelled() => {
                            log_debug!(slog, "polling request terminated");
                            return;
                        }
                        () = flushed.cancelled() => {
                            log_debug!(slog, "polling request fulfilled");
                            return;
                        }
                    }
                }
            }
            Live::Sse(conn) => {
                let closed = conn.context();
                let ttl = if self.config.sse_ttl == 0 { 60 } else { self.config.sse_ttl.max(0) as u64 };
                tokio::select! {
                    () = self.shutdown.cancelled() => {
                        log_debug!(slog, "server shutdown");
                        conn.close(CLOSE_GOING_AWAY, "Server shutdown");
                    }
                    () = request_done.cancelled() => {
                        log_debug!(slog, "client disconnected");
                        // Closed synchronously, so no write happens after the handler returns.
                        conn.close(CLOSE_NORMAL_CLOSURE, "Client disconnected");
                    }
                    () = closed.cancelled() => {
                        log_debug!(slog, "connection closed");
                    }
                    () = tokio::time::sleep(Duration::from_secs(ttl)) => {
                        conn.close(200, "No content");
                    }
                }
            }
        }
        let _ = &active.guard;
    }
}

/// [`Handler`] on the HTTP server: the response streams through a
/// [`ChannelResponseWriter`](crate::server::ChannelResponseWriter), and the client
/// going away ends the request.
struct Endpoint(Arc<Handler>);

impl server::Handler for Endpoint {
    fn call(&self, req: server::Request) -> ResponseFuture {
        let handler = self.0.clone();
        let (writer, client_gone, response) = server::channel_response();
        let conn = server::conn_info(&req);
        let (parts, _body) = req.into_parts();
        let request = HttpRequest::from_parts(&parts, &conn);
        server::runtime_handle().spawn(handler.serve_async(request, Box::new(writer), client_gone));
        Box::pin(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node;

    #[test]
    fn decode_offset_errors_like_go() {
        assert_eq!(Err(DsError(r#"strconv.ParseUint: parsing "abc": invalid syntax"#.into())), decode_offset("abc"));
        assert_eq!(Err(DsError(r#"strconv.ParseUint: parsing "+1": invalid syntax"#.into())), decode_offset("+1::e"));
        assert_eq!(
            Err(DsError(r#"strconv.ParseUint: parsing "99999999999999999999": value out of range"#.into())),
            decode_offset("99999999999999999999")
        );
        assert_eq!(Ok((3, "e".to_owned())), decode_offset("3::e::x"));
    }

    #[test]
    fn control_events_have_sorted_keys() {
        assert_eq!(
            r#"event: control
data:{"streamCursor":"c","streamNextOffset":"1::e","upToDate":true}"#,
            String::from_utf8(encode_sse_control_event("1::e", "c", true)).unwrap()
        );
        assert_eq!(r#"{"streamNextOffset":"0"}"#, control_json("0", "", false));
    }

    #[test]
    fn subscribe_identifier_omits_an_empty_signed_name() {
        let sp = StreamParams { name: "a/b".into(), ..Default::default() };
        assert_eq!(r#"{"channel":"$pubsub","stream_name":"a/b"}"#, sp.to_subscribe_command().identifier);
        let sp = StreamParams { name: "a".into(), signed_name: "s".into(), ..Default::default() };
        assert_eq!(
            r#"{"channel":"$pubsub","stream_name":"a","signed_stream_name":"s"}"#,
            sp.to_subscribe_command().identifier
        );
    }

    #[test]
    fn stream_path_is_decoded_and_relative_to_the_mount_path() {
        let req = |target: &str| HttpRequest { method: "GET".into(), target: target.into(), ..Default::default() };
        let c = Config::new();
        assert_eq!("chat:1", stream_params_from_req(&req("/ds/chat:1"), &c).unwrap().path);
        assert_eq!("a/b c", stream_params_from_req(&req("/ds/a%2Fb%20c?offset=-1"), &c).unwrap().path);
        assert!(stream_params_from_req(&req("/ds/"), &c).is_err());
    }

    /// The mounted endpoint over a real socket: HEAD, catch-up, a long-poll
    /// answered by a broadcast.
    #[test]
    fn serves_on_the_http_server() {
        use std::io::{Read, Write};

        let mut nconf = node::Config::new();
        nconf.disconnect_mode = node::DISCONNECT_MODE_NEVER.into();
        let mut sconf = cabby_core::streams::Config::new();
        sconf.public = true;
        let streams_ctrl = Arc::new(StreamsController::for_config(&sconf));
        let streams = Some(streams_ctrl.clone());
        let node = Node::new(nconf, vec![node::NodeOption::Controller(streams_ctrl)]);
        let subscriber = Arc::new(node::LegacySubscriber::new(node.clone()));
        let memory = Arc::new(cabby_core::broker::Memory::new(
            Some(subscriber as Arc<dyn cabby_core::broker::Broadcaster>),
            None,
            &cabby_core::broker::Config::new(),
            Arc::new(cabby_core::broker::SystemClock),
        ));
        memory.set_epoch("ep");
        node.set_broker(memory.clone());
        memory.start().unwrap();
        node.start().unwrap();
        let msg = |data: &str| StreamMessage { stream: "s".into(), data: data.into(), ..Default::default() };
        memory.handle_broadcast(&msg(r#"{"n":1}"#)).unwrap();

        let mut conf = Config::new();
        conf.skip_auth = true;
        conf.poll_interval = 5;

        let srv = server::HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
        let ep = handler(node.clone(), memory.clone(), streams, None, srv.shutdown_token(), None, &conf).unwrap();
        srv.setup_handler("/ds/*", ep);
        srv.start().unwrap();
        let addr = srv.local_addr().unwrap();

        let request = move |method: &str, target: &str| {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            write!(s, "{method} {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
            let mut out = String::new();
            let _ = s.read_to_string(&mut out);
            out.to_lowercase()
        };

        let head = request("HEAD", "/ds/s");
        assert!(head.starts_with("http/1.1 200"), "{head}");
        assert!(head.contains("stream-next-offset: 1::ep"), "{head}");

        let get = request("GET", "/ds/s?offset=-1");
        assert!(get.starts_with("http/1.1 200"), "{get}");
        assert!(get.contains(r#"[{"n":1}]"#), "{get}");

        let pending = std::thread::spawn(move || request("GET", "/ds/s?offset=1::ep&live=long-poll"));
        let deadline = Instant::now() + Duration::from_secs(5);
        while node.size() != 1 {
            assert!(Instant::now() < deadline, "long-poll session not registered");
            std::thread::sleep(Duration::from_millis(10));
        }
        // Subscribed once the hub knows the stream.
        while node.hub().streams_size() != 1 {
            assert!(Instant::now() < deadline, "long-poll session not subscribed");
            std::thread::sleep(Duration::from_millis(10));
        }
        memory.handle_broadcast(&msg(r#"{"n":2}"#)).unwrap();
        let resp = pending.join().unwrap();
        assert!(resp.starts_with("http/1.1 200"), "{resp}");
        assert!(resp.contains("stream-next-offset: 2::ep"), "{resp}");
        assert!(resp.contains(r#"[{"n":2}]"#), "{resp}");
    }

    #[test]
    fn close_codes_map_to_statuses() {
        assert_eq!(401, ws_code_to_http(CLOSE_NORMAL_CLOSURE, "Auth Failed"));
        assert_eq!(200, ws_code_to_http(CLOSE_NORMAL_CLOSURE, "done"));
        assert_eq!(410, ws_code_to_http(CLOSE_GOING_AWAY, ""));
        assert_eq!(400, ws_code_to_http(CLOSE_ABNORMAL_CLOSURE, ""));
        assert_eq!(500, ws_code_to_http(CLOSE_INTERNAL_SERVER_ERR, ""));
        assert_eq!(204, ws_code_to_http(204, ""));
    }
}
