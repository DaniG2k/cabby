//! Durable Streams read API (upstream package `ds`, see `ds/DESIGN.md` upstream):
//! catch-up reads, long-poll and SSE over the broker's stream history.
//!
//! * Catch-up (`GET /ds/<stream>?offset=...`): history from the broker as a JSON
//!   array, with `Stream-Next-Offset`, `Stream-Cursor`, `Stream-Up-To-Date`,
//!   `Cache-Control` and an `ETag` (`304 Not Modified` on a match).
//! * Long-poll (`live=long-poll`): history if there is any, otherwise the session
//!   subscribes, reads the history again, and the first message the client does
//!   not have is the response ([`PollConnection`]); `204` after `poll_interval`,
//!   `410` on shutdown.
//! * SSE (`live=sse`): the session subscribes, then the history is written as a
//!   `data` event plus a `control` event, then live messages for up to `sse_ttl`:
//!   each message's data event is encoded once for all sessions of the stream, and
//!   each session writes it with a control event carrying its own cursor
//!   (`SseDataEncoder` over an `SseStream`, D-090).
//!
//! Live reads install the subscription before they read the history and hold live
//! messages until the history is written; messages the history delivered are
//! dropped (`Watermark`, D-089). A client that follows the offsets it gets
//! receives every message once, in order.
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
        out.push_str(&format!("path = {}\n", cabby_core::toml_config::toml_quote(&self.path)));
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

/// A batch as an SSE `data` event. A line terminator in the messages' data
/// starts another `data:` field, so the client gets the batch with its line
/// terminators as LF (D-099).
pub fn encode_sse_data_event(messages: &[StreamMessage]) -> Vec<u8> {
    let batch = encode_json_batch(messages);
    let mut buf = Vec::with_capacity(batch.len() + 17);
    buf.extend_from_slice(b"event: data\ndata:");
    sse::write_sse_data(&mut buf, &batch);
    buf
}

/// Appends the `control` event payload. Go marshals a `map[string]interface{}`,
/// so the keys are sorted: `streamCursor`, `streamNextOffset`, `upToDate`.
fn write_control_json(out: &mut Vec<u8>, offset: &str, cursor: &str, up_to_date: bool) {
    out.push(b'{');
    if !cursor.is_empty() {
        out.extend_from_slice(br#""streamCursor":"#);
        cabby_core::gojson::write(out, cursor);
        out.push(b',');
    }
    out.extend_from_slice(br#""streamNextOffset":"#);
    cabby_core::gojson::write(out, offset);
    if up_to_date {
        out.extend_from_slice(br#","upToDate":true"#);
    }
    out.push(b'}');
}

/// Appends an SSE `control` event (without the blank line that ends it).
fn write_control_event(out: &mut Vec<u8>, offset: &str, cursor: &str, up_to_date: bool) {
    out.extend_from_slice(b"event: control\ndata:");
    write_control_json(out, offset, cursor, up_to_date);
}

/// The `control` event payload.
#[cfg(test)]
fn control_json(offset: &str, cursor: &str, up_to_date: bool) -> String {
    let mut out = Vec::new();
    write_control_json(&mut out, offset, cursor, up_to_date);
    String::from_utf8(out).expect("JSON is UTF-8")
}

/// An SSE `control` event with the next offset, the cursor and the up-to-date flag.
pub fn encode_sse_control_event(offset: &str, cursor: &str, up_to_date: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    write_control_event(&mut out, offset, cursor, up_to_date);
    out
}

/// Appends the SSE `data` event of one live message (an array, as catch-up data),
/// without the blank line that ends it.
fn write_data_event(out: &mut Vec<u8>, reply: &cabby_core::common::Reply) {
    out.extend_from_slice(b"event: data\ndata:[");
    cabby_core::gojson::write(out, &reply.message);
    out.push(b']');
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
/// decoded; a target with an invalid escape keeps its raw path. A path that is
/// not valid UTF-8 once decoded names no stream (D-104).
fn url_path(target: &str) -> Result<String, DsError> {
    let mut path = target.split(['?', '#']).next().unwrap_or_default();
    // An absolute URL (`http://host/path`): the path follows the authority. In an
    // origin-form target (`/path`), a `://` is part of the path, as in Go.
    if !path.starts_with('/')
        && let Some((_, rest)) = path.split_once("://")
    {
        path = rest.find('/').map_or("", |i| &rest[i..]);
    }
    if !path.contains('%') {
        return Ok(path.to_owned());
    }
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .filter(|h| h.iter().all(u8::is_ascii_hexdigit))
                .and_then(|h| std::str::from_utf8(h).ok());
            match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                Some(b) => {
                    out.push(b);
                    i += 3;
                    continue;
                }
                None => return Ok(path.to_owned()),
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).map_err(|_| DsError("stream name is not valid UTF-8".to_owned()))
}

/// Parses and validates a request against the mount path (upstream `StreamParamsFromReq`).
pub fn stream_params_from_req(req: &HttpRequest, c: &Config) -> Result<StreamParams, DsError> {
    // The stream name is the URL path below the mount path.
    let path = url_path(&req.target)?;
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
const DS_SSE_DATA_ENCODER_ID: &str = "dssedata";

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

    #[inline]
    fn id_str(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(DS_NOOP_ENCODER_ID)
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

    #[inline]
    fn id_str(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(DS_POLL_ENCODER_ID)
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
///
/// Its frames carry its cursor and up-to-date flag, so its ID includes them: the
/// hub shares a frame only between sessions whose encoders write the same one
/// (D-090; upstream's ID is `dsse` for every cursor). The endpoint's live
/// sessions use `SseDataEncoder` and `SseStream` instead, which encode the
/// data once for every session of a stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SseEncoder {
    pub cursor: String,
    pub up_to_date: bool,
}

impl NodeEncoder for SseEncoder {
    fn id(&self) -> String {
        format!("{DS_SSE_ENCODER_ID}:{}:{}", u8::from(self.up_to_date), self.cursor)
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

        // A data event, then a control event.
        let mut payload = Vec::with_capacity(160);
        write_data_event(&mut payload, reply);
        payload.extend_from_slice(b"\n\n");
        write_control_event(&mut payload, &encode_offset(reply.offset, &reply.epoch), &self.cursor, self.up_to_date);

        Ok(Some(SentFrame::text(payload)))
    }

    fn encode_transmission_optional(&self, _raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
    }
}

/// Encoder of the endpoint's live SSE sessions: the part of an event that is the
/// same for every session, `<offset>\n<data event>`, so the hub encodes a message
/// once for all of them; [`SseStream`] writes it with the session's control event
/// (D-090). Pings become comments, as with [`SseEncoder`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SseDataEncoder;

impl NodeEncoder for SseDataEncoder {
    fn id(&self) -> String {
        DS_SSE_DATA_ENCODER_ID.to_owned()
    }

    #[inline]
    fn id_str(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(DS_SSE_DATA_ENCODER_ID)
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

        let mut payload = encode_offset(reply.offset, &reply.epoch).into_bytes();
        payload.push(b'\n');
        write_data_event(&mut payload, reply);
        Ok(Some(SentFrame::text(payload)))
    }

    fn encode_transmission_optional(&self, _raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        Ok(None)
    }
}

/// The offset and the rest of a `<offset>\n<rest>` frame ([`PollEncoder`],
/// [`SseDataEncoder`]); `None` for other frames (pings).
fn split_frame(frame: &[u8]) -> Option<(&str, &[u8])> {
    let pos = frame.iter().position(|b| *b == b'\n')?;
    let offset = std::str::from_utf8(&frame[..pos]).ok()?;
    Some((offset, &frame[pos + 1..]))
}

// ------------------------------------------------------------------ live delivery

/// What the client of a live request has, to compare live messages with (D-089).
///
/// The live subscription is installed before the history is read, so a live
/// message can also be in the history, and the stream's epoch can change in
/// between.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Watermark {
    /// The epoch and offset of the last message the client has: the last of the
    /// history, else its offset (the tail for `now`); `None` without one.
    position: Option<(String, u64)>,
    /// The epoch of `position` is not the one the stream had before the
    /// subscription: a live message of another epoch may be older or newer than
    /// the position. Cleared by the first message delivered.
    strict: bool,
}

/// What to do with a live message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admit {
    /// Write it: it follows the client's position.
    Deliver,
    /// Drop it: the client has it.
    Skip,
    /// End the response without it: it is of another epoch and may be older than
    /// the position. The client's next request reads from its position.
    End,
}

impl Watermark {
    /// Compares a live message with the position, which moves to it when it is
    /// delivered.
    fn admit(&mut self, epoch: &str, offset: u64) -> Admit {
        match &mut self.position {
            Some((e, o)) if *e == epoch => {
                if offset <= *o {
                    return Admit::Skip;
                }
                *o = offset;
            }
            Some(_) if self.strict => return Admit::End,
            // A later epoch (the stream's messages reach the hub in order), or the
            // client's first message.
            position => *position = Some((epoch.to_owned(), offset)),
        }
        self.strict = false;
        Admit::Deliver
    }
}

/// `(offset, epoch)` of an encoded `<offset>::<epoch>`.
fn frame_position(offset: &str) -> Option<(u64, &str)> {
    let (offset, epoch) = offset.split_once(OFFSET_SEPARATOR)?;
    Some((offset.parse().ok()?, epoch))
}

/// The live frames of a request: held from before the subscription until the
/// history is written, then compared with the watermark (D-089).
#[derive(Debug, Default)]
struct LiveGate {
    /// The frames written while held, in order.
    held: Option<Vec<Vec<u8>>>,
    mark: Watermark,
}

impl LiveGate {
    fn held() -> Self {
        Self { held: Some(Vec::new()), mark: Watermark::default() }
    }

    /// Keeps a copy of `frame` while held; false once open.
    fn keep(&mut self, frame: &[u8]) -> bool {
        match &mut self.held {
            Some(held) => {
                held.push(frame.to_vec());
                true
            }
            None => false,
        }
    }

    /// Opens at `mark`; returns the frames held, to be written now.
    fn open(&mut self, mark: Watermark) -> Vec<Vec<u8>> {
        self.mark = mark;
        self.held.take().unwrap_or_default()
    }

    /// What to do with a frame at `offset`; an offset without an epoch is not
    /// compared.
    fn admit(&mut self, offset: &str) -> Admit {
        match frame_position(offset) {
            Some((offset, epoch)) => self.mark.admit(epoch, offset),
            None => Admit::Deliver,
        }
    }
}

// ------------------------------------------------------------------ SSE stream

/// The connection of a live SSE session: writes each [`SseDataEncoder`] frame as
/// its data event and a control event with this session's cursor (D-090); other
/// frames (pings) as they are. Frames are held until [`SseStream::release`], and
/// the frames of messages the client has are dropped (D-089).
struct SseStream {
    conn: Arc<sse::Connection>,
    cursor: String,
    up_to_date: bool,
    gate: Mutex<LiveGate>,
}

impl SseStream {
    fn new(conn: Arc<sse::Connection>, cursor: String) -> Self {
        Self { conn, cursor, up_to_date: false, gate: Mutex::new(LiveGate::held()) }
    }

    /// Writes the frames held and then every frame as it comes, compared with
    /// `mark`: call it once the history is written.
    fn release(&self, mark: Watermark) {
        let mut gate = lock(&self.gate);
        for frame in gate.open(mark) {
            // As for live writes, a failed write ends the response.
            let _ = self.deliver(&mut gate, &frame, Instant::now());
        }
    }

    fn deliver(&self, gate: &mut LiveGate, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        let Some((offset, data)) = split_frame(msg) else { return self.conn.write(msg, deadline) };
        match gate.admit(offset) {
            Admit::Deliver => {}
            Admit::Skip => return Ok(()),
            Admit::End => {
                // The stream ends, as at its TTL; the client reconnects with its offset.
                self.conn.close(CLOSE_NORMAL_CLOSURE, "");
                return Ok(());
            }
        }
        let mut event = Vec::with_capacity(data.len() + 96);
        event.extend_from_slice(data);
        event.extend_from_slice(b"\n\n");
        write_control_event(&mut event, offset, &self.cursor, self.up_to_date);
        self.conn.write(&event, deadline)
    }
}

impl Connection for SseStream {
    fn write(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        let mut gate = lock(&self.gate);
        if gate.keep(msg) {
            return Ok(());
        }
        self.deliver(&mut gate, msg, deadline)
    }

    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        self.conn.write_binary(msg, deadline)
    }

    fn write_ping(&self, deadline: Instant) -> Result<(), ConnectionError> {
        self.conn.write_ping(deadline)
    }

    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        self.conn.read()
    }

    fn close(&self, code: i32, reason: &str) {
        self.conn.close(code, reason);
    }
}

// ------------------------------------------------------------------ poll connection

/// Builds the `ETag` of a data response from its next offset.
pub type ETagFn = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Long-poll connection: turns the first encoded message into a response body and
/// headers, then is done (upstream `PollConnection`). Closing it sends the status
/// matching the close code.
///
/// The endpoint holds its messages while it reads the history after subscribing,
/// and drops those the client has (D-089).
pub struct PollConnection {
    writer: SharedResponseWriter,
    state: Mutex<PollState>,
    ctx: CancellationToken,
    /// `Cache-Control` of data responses (upstream field `CacheControl`).
    pub cache_control: Mutex<String>,
    /// `ETag` of data responses (upstream field `ETag`).
    pub etag: Mutex<Option<ETagFn>>,
}

#[derive(Debug, Default)]
struct PollState {
    /// A response was written or the connection closed (upstream field `done`).
    done: bool,
    /// Open unless the endpoint holds the messages.
    gate: LiveGate,
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
            state: Mutex::new(PollState::default()),
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
        lock(&self.state).done
    }

    /// Always `None` (upstream `Descriptor()`).
    pub fn descriptor(&self) -> Option<std::net::TcpStream> {
        None
    }

    /// Holds the messages written from now on, until [`PollConnection::release`].
    fn hold(&self) {
        lock(&self.state).gate = LiveGate::held();
    }

    /// Answers with the first held message the client does not have, if any;
    /// later messages are compared with `mark` too.
    fn release(&self, mark: Watermark) {
        let mut st = lock(&self.state);
        for frame in st.gate.open(mark) {
            if st.done {
                break;
            }
            // As for live writes, the error ends the response.
            let _ = self.respond(&mut st, &frame);
        }
    }

    fn respond(&self, st: &mut PollState, msg: &[u8]) -> Result<(), ConnectionError> {
        let Some(pos) = msg.iter().position(|b| *b == b'\n') else {
            st.done = true;
            self.ctx.cancel();
            return Err(ConnectionError("invalid poll message format: missing newline separator".to_owned()));
        };
        let offset = String::from_utf8_lossy(&msg[..pos]).into_owned();
        match st.gate.admit(&offset) {
            Admit::Deliver => {}
            Admit::Skip => return Ok(()),
            // As when the poll interval elapses: the client polls again.
            Admit::End => {
                self.close_with(st, 204, "No content");
                return Ok(());
            }
        }
        st.done = true;
        let _cancel = self.ctx.clone().drop_guard();

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

    fn close_with(&self, st: &mut PollState, code: i32, reason: &str) {
        if st.done {
            return;
        }
        st.done = true;
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

impl Connection for PollConnection {
    /// Writes `[<body>]` with the offset headers of an encoded `<offset>\n<body>`
    /// message (see [`PollEncoder`]); only the first write or close counts.
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let mut st = lock(&self.state);
        if st.done || st.gate.keep(msg) {
            return Ok(());
        }
        self.respond(&mut st, msg)
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
        let mut st = lock(&self.state);
        self.close_with(&mut st, code, reason);
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
    sse: Option<(Arc<sse::Connection>, Arc<SseStream>)>,
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
                // The data is encoded once per message for every session of the
                // stream; the session's connection adds its cursor (D-090).
                let live = Arc::new(SseStream::new(c.clone(), stream.next_cursor()));
                conn = live.clone();
                sse_conn = Some((c, live));
                let mut opts = vec![SessionOption::Encoder(Arc::new(SseDataEncoder))];
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
                Ok(Some(res)) if res.status == Status::Success => {}
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

            set_header(w.headers_mut(), "x-anycable-version", crate::version::ANYCABLE_COMPAT);
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
            Some((conn, live)) => self.prepare_sse(w, &stream, &session, &poll, (conn, live), tail, &mut guard)?,
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
        tail: StreamMessage,
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

        let cache_control = read_cache_control(&self.config, req, &s.params);

        if !backlog.is_empty() || s.params.live_mode != LONG_POLL_MODE {
            self.write_history(req, w, s, poll, &tail, &backlog, &cache_control, &slog);
            return None;
        }

        if let Some(m) = &self.metrics {
            m.counter_increment(METRICS_POLL_TOTAL);
            m.gauge_increment(METRICS_POLL_NUM);
            guard.gauge = Some((m.clone(), METRICS_POLL_NUM));
        }

        // Set before the messages are released: the response may be written right away.
        *lock(&poll.cache_control) = cache_control.clone();
        if s.params.raw_offset != NOW_OFFSET {
            let params = s.params.clone();
            *lock(&poll.etag) = Some(Arc::new(move |next: &str| stream_etag(&params, next)));
        }

        // Live messages wait until the history is read again with the subscription
        // installed: what was published since the first read is in one or the
        // other (D-089).
        poll.hold();
        if let Err(e) = self.node.subscribe(&guard.session, &s.params.to_subscribe_command()) {
            log_error!(slog, "failed to subscribe", "err" => e.to_string());
            poll.close(500, "");
            return None;
        }

        let (backlog, mark) = match self.read_after_subscribe(&s.params, &tail) {
            Ok(read) => read,
            Err(e) => {
                log_error!(slog, "failed to fetch history", "err" => e.to_string());
                poll.close(410, "");
                return None;
            }
        };
        if !backlog.is_empty() {
            self.write_history(req, w, s, poll, &tail, &backlog, &cache_control, &slog);
            return None;
        }

        poll.release(mark);
        Some(Live::LongPoll(poll.clone()))
    }

    /// The data response of a catch-up or long-poll read (`304` when the client
    /// has it).
    #[allow(clippy::too_many_arguments)]
    fn write_history(
        &self,
        req: &HttpRequest,
        w: &SharedResponseWriter,
        s: &Stream,
        poll: &PollConnection,
        tail: &StreamMessage,
        backlog: &[StreamMessage],
        cache_control: &str,
        slog: &Logger,
    ) {
        let tail = backlog.last().unwrap_or(tail);
        let next_offset = encode_offset(tail.offset, &tail.epoch);
        {
            let mut w = lock_writer(w);
            let h = w.headers_mut();
            set_header(h, "cache-control", cache_control);
            set_header(h, STREAM_OFFSET_HEADER, &next_offset);
            set_header(h, STREAM_UP_TO_DATE_HEADER, "true");
        }

        if s.params.raw_offset != NOW_OFFSET {
            let etag = stream_etag(&s.params, &next_offset);
            set_header(lock_writer(w).headers_mut(), "etag", &etag);

            if etag_matches(req.header("If-None-Match"), &etag) {
                log_debug!(slog, "not modified");
                poll.close(304, "");
                return;
            }
        }

        log_debug!(slog, "write history", "len" => backlog.len());
        poll.close(200, "");
        // Upstream ignores this error too.
        let _ = lock_writer(w).write(&encode_json_batch(backlog));
    }

    /// The history of a live read, read once the subscription is installed, and
    /// what live messages are compared with (D-089). `tail` was read before the
    /// subscription: for `now`, the history is what follows it, and none if the
    /// epoch changed since; when the stream had no entries then, everything it
    /// holds now.
    fn read_after_subscribe(
        &self,
        sp: &StreamParams,
        tail: &StreamMessage,
    ) -> Result<(Vec<StreamMessage>, Watermark), DsError> {
        let (backlog, client) = if sp.raw_offset == NOW_OFFSET && tail.epoch.is_empty() {
            (self.read_since_empty(sp), None)
        } else if sp.raw_offset == NOW_OFFSET {
            let client = Some((tail.epoch.clone(), tail.offset));
            match self.broker.history_from(&sp.path, &tail.epoch, tail.offset) {
                Ok(backlog) => (backlog, client),
                Err(_) => return Ok((Vec::new(), Watermark { position: client, strict: true })),
            }
        } else {
            let client = (!sp.epoch.is_empty()).then(|| (sp.epoch.clone(), sp.offset));
            (fetch_history(self.broker.as_ref(), sp)?, client)
        };
        let position = backlog.last().map(|m| (m.epoch.clone(), m.offset)).or(client);
        // Another epoch than before the subscription: the epoch changed meanwhile.
        let strict = position.as_ref().is_some_and(|(epoch, _)| *epoch != tail.epoch);
        Ok((backlog, Watermark { position, strict }))
    }

    /// For `now` on a stream that had no entries at the tail read: what it holds
    /// now was published since. The tail is read again first, and the history only
    /// when there is one: a broker that reports no tail for a stream with entries
    /// (the NATS broker never does) keeps its history out of a `now` read. A failed
    /// read gives nothing, as before.
    fn read_since_empty(&self, sp: &StreamParams) -> Vec<StreamMessage> {
        match self.broker.peak(&sp.path) {
            Ok(Some(_)) => self.broker.history_since(&sp.path, 0).ok().flatten().unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// SSE reads (upstream unexported `handleSSE` up to its wait).
    #[allow(clippy::too_many_arguments)]
    fn prepare_sse(
        &self,
        w: &SharedResponseWriter,
        s: &Stream,
        session: &Arc<Session>,
        poll: &Arc<PollConnection>,
        (conn, live): (Arc<sse::Connection>, Arc<SseStream>),
        tail: StreamMessage,
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

        // Subscribed before the history is read; the session's connection holds
        // live messages until the history is written (D-089).
        let subscribed = match self.node.subscribe(session, &s.params.to_subscribe_command()) {
            Ok(_) => true,
            Err(e) => {
                log_error!(slog, "failed to subscribe", "err" => e.to_string());
                false
            }
        };

        let (backlog, mark) = match self.read_after_subscribe(&s.params, &tail) {
            Ok(read) => read,
            Err(e) => {
                log_error!(slog, "failed to fetch history", "err" => e.to_string());
                poll.close(410, "");
                return None;
            }
        };
        let tail = backlog.last().unwrap_or(&tail);

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

        if !subscribed {
            return None;
        }

        live.release(mark);
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
        let (writer, client_gone, response) = server::channel_response_for(&req);
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
    use cabby_core::common::{CommandResult, Outcome};

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

    /// The stream is the path below the mount path: a `://` in an origin-form
    /// target is part of it; in an absolute-form target, the scheme and the
    /// authority are not (Go `r.URL.Path`).
    #[test]
    fn a_scheme_is_stripped_from_absolute_targets_only() {
        let req = |target: &str| HttpRequest { method: "GET".into(), target: target.into(), ..Default::default() };
        let c = Config::new();
        for (target, stream) in [
            ("/ds/https://example.com/feed", "https://example.com/feed"),
            ("/ds/https://example.com/feed?offset=-1", "https://example.com/feed"),
            ("/ds/x://y/feed", "x://y/feed"),
            ("/ds/feed", "feed"),
            ("http://host/ds/a://b", "a://b"),
            ("http://host/ds/feed?offset=-1", "feed"),
            ("/ds/a%3A%2F%2Fb", "a://b"),
        ] {
            let sp = stream_params_from_req(&req(target), &c).unwrap();
            assert_eq!((stream, stream), (sp.name.as_str(), sp.path.as_str()), "{target}");
        }
    }

    /// A path that is not valid UTF-8 once decoded is refused (Go keeps the
    /// bytes as the stream name); the encoded U+FFFD is a valid name.
    #[test]
    fn deviation_d104_a_stream_name_must_be_valid_utf8() {
        let req = |target: &str| HttpRequest { method: "GET".into(), target: target.into(), ..Default::default() };
        let c = Config::new();
        for target in ["/ds/%ff", "/ds/%fe", "/ds/a%C3", "/ds/%ff?offset=-1&live=sse"] {
            let err = stream_params_from_req(&req(target), &c).unwrap_err();
            assert_eq!("stream name is not valid UTF-8", err.0, "{target}");
        }
        assert_eq!("\u{FFFD}", stream_params_from_req(&req("/ds/%EF%BF%BD"), &c).unwrap().name);
        assert_eq!("café", stream_params_from_req(&req("/ds/caf%C3%A9"), &c).unwrap().name);
    }

    fn message(data: &str) -> StreamMessage {
        StreamMessage { stream: "s".into(), data: data.into(), ..Default::default() }
    }

    fn request(addr: std::net::SocketAddr, method: &str, target: &str) -> String {
        use std::io::{Read, Write};

        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        write!(s, "{method} {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
        let mut out = String::new();
        let _ = s.read_to_string(&mut out);
        out.to_lowercase()
    }

    /// The endpoint on an HTTP server, with public streams and one message in `s`.
    struct Served {
        node: Arc<Node>,
        memory: Arc<cabby_core::broker::Memory>,
        addr: std::net::SocketAddr,
        _server: Arc<server::HttpServer>,
    }

    impl Served {
        /// Connections are authenticated by `controller`, or not at all.
        fn new(controller: Option<Arc<dyn cabby_core::common::Controller>>) -> Self {
            let mut nconf = node::Config::new();
            nconf.disconnect_mode = node::DISCONNECT_MODE_NEVER.into();
            let mut sconf = cabby_core::streams::Config::new();
            sconf.public = true;
            let streams = Arc::new(StreamsController::for_config(&sconf));
            let mut conf = Config::new();
            conf.skip_auth = controller.is_none();
            conf.poll_interval = 5;
            let controller = controller.unwrap_or_else(|| streams.clone());
            let node = Node::new(nconf, vec![node::NodeOption::Controller(controller)]);
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
            memory.handle_broadcast(&message(r#"{"n":1}"#)).unwrap();

            let srv = server::HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
            let ep = handler(node.clone(), memory.clone(), Some(streams), None, srv.shutdown_token(), None, &conf);
            srv.setup_handler("/ds/*", ep.unwrap());
            srv.start().unwrap();
            let addr = srv.local_addr().unwrap();
            Self { node, memory, addr, _server: srv }
        }
    }

    /// Answers every connection with a status, and no error.
    struct Answering(Status);

    impl cabby_core::common::Controller for Answering {
        fn start(&self) -> Result<(), cabby_core::common::ControllerError> {
            Ok(())
        }
        fn shutdown(&self) -> Result<(), cabby_core::common::ControllerError> {
            Ok(())
        }
        fn authenticate(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
        ) -> Outcome<cabby_core::common::ConnectResult> {
            let result =
                cabby_core::common::ConnectResult { status: self.0, identifier: "ids".into(), ..Default::default() };
            Outcome { result: Some(result), error: None }
        }
        fn subscribe(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
        ) -> Outcome<CommandResult> {
            Outcome { result: None, error: None }
        }
        fn unsubscribe(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
        ) -> Outcome<CommandResult> {
            Outcome { result: None, error: None }
        }
        fn perform(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
            _data: &str,
        ) -> Outcome<CommandResult> {
            Outcome { result: None, error: None }
        }
        fn disconnect(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _subscriptions: &[String],
        ) -> Result<(), cabby_core::common::ControllerError> {
            Ok(())
        }
    }

    #[test]
    fn only_successful_authentication_is_served() {
        for (status, want) in [(Status::Success, "200"), (Status::Failure, "401"), (Status::Error, "401")] {
            let endpoint = Served::new(Some(Arc::new(Answering(status))));
            let get = request(endpoint.addr, "GET", "/ds/s?offset=-1");
            assert!(get.starts_with(&format!("http/1.1 {want}")), "{status:?}: {get}");
            assert_eq!(want == "200", get.contains(r#"[{"n":1}]"#), "{status:?}: {get}");
        }
    }

    /// The mounted endpoint over a real socket: HEAD, catch-up, a long-poll
    /// answered by a broadcast.
    #[test]
    fn serves_on_the_http_server() {
        let endpoint = Served::new(None);
        let (node, memory) = (&endpoint.node, &endpoint.memory);
        let addr = endpoint.addr;
        let request = move |method: &str, target: &str| request(addr, method, target);

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
        memory.handle_broadcast(&message(r#"{"n":2}"#)).unwrap();
        let resp = pending.join().unwrap();
        assert!(resp.starts_with("http/1.1 200"), "{resp}");
        assert!(resp.contains("stream-next-offset: 2::ep"), "{resp}");
        assert!(resp.contains(r#"[{"n":2}]"#), "{resp}");
    }

    /// The endpoint never reads a request body: every request is answered
    /// without waiting for the body its headers announce (no `100 Continue`, so
    /// the body was never polled): a preflight, a method that is not a read, a
    /// failed sign-in, and a read.
    #[test]
    fn deviation_d096_requests_are_answered_without_their_body() {
        use crate::server::tests::support::{connect, read_response};
        use std::io::Write;

        let answer = |endpoint: &Served, head: &str| {
            let mut stream = connect(endpoint.addr);
            write!(stream, "{head}\r\nHost: x\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n").unwrap();
            let sent = Instant::now();
            let resp = read_response(&mut stream);
            assert!(sent.elapsed() < Duration::from_secs(1), "{head}: answered after {:?}", sent.elapsed());
            resp
        };

        let refusing = Served::new(Some(Arc::new(Answering(Status::Failure))));
        for (head, status) in [
            ("OPTIONS /ds/s HTTP/1.1", 200),
            ("POST /ds/s HTTP/1.1", 405),
            ("PUT /ds/s HTTP/1.1", 405),
            ("GET /ds/s?offset=-1 HTTP/1.1", 401),
            ("GET /ds/s?offset=-1&live=sse HTTP/1.1", 401),
        ] {
            assert_eq!(status, answer(&refusing, head).status, "{head}");
        }

        let open = Served::new(None);
        let resp = answer(&open, "GET /ds/s?offset=-1 HTTP/1.1");
        assert_eq!((200, br#"[{"n":1}]"#.to_vec()), (resp.status, resp.body));
    }

    /// A line terminator in the data of SSE history starts another `data:`
    /// field: a client joining the fields gets the batch, which is the JSON of
    /// the catch-up response.
    #[test]
    fn deviation_d099_sse_history_keeps_line_terminators_in_data_fields() {
        let batch = [message("{\n  \"n\": 1\r\n}"), message("{\"n\":\r2}")];
        let event = String::from_utf8(encode_sse_data_event(&batch)).unwrap();
        assert_eq!("event: data\ndata:[{\ndata:   \"n\": 1\ndata: },{\"n\":\ndata: 2}]", event);

        let fields: Vec<&str> = event.split('\n').skip(1).map(|l| l.strip_prefix("data:").unwrap()).collect();
        let data: Vec<&str> = fields.iter().map(|v| v.strip_prefix(' ').unwrap_or(v)).collect();
        let joined: serde_json::Value = serde_json::from_str(&data.join("\n")).unwrap();
        let catch_up: serde_json::Value = serde_json::from_slice(&encode_json_batch(&batch)).unwrap();
        assert_eq!(serde_json::json!([{"n": 1}, {"n": 2}]), joined);
        assert_eq!(catch_up, joined);

        // Data without line terminators is written as before.
        assert_eq!(
            b"event: data\ndata:[{\"n\":1},2]".to_vec(),
            encode_sse_data_event(&[message(r#"{"n":1}"#), message("2")])
        );
    }

    /// Over a socket, with the request line as written:
    /// `/ds/https://example.com/feed` reads the stream `https://example.com/feed`,
    /// as its encoded form does, not `feed`.
    #[test]
    fn a_target_with_a_scheme_in_its_path_reads_that_stream() {
        let endpoint = Served::new(None);
        for (stream, data) in [("feed", r#"{"from":"feed"}"#), ("https://example.com/feed", r#"{"from":"url"}"#)] {
            let msg = StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() };
            endpoint.memory.handle_broadcast(&msg).unwrap();
        }
        for (target, body) in [
            ("/ds/https://example.com/feed?offset=-1", r#"[{"from":"url"}]"#),
            ("/ds/https%3A%2F%2Fexample.com%2Ffeed?offset=-1", r#"[{"from":"url"}]"#),
            ("/ds/feed?offset=-1", r#"[{"from":"feed"}]"#),
        ] {
            let resp = request(endpoint.addr, "GET", target);
            assert!(resp.starts_with("http/1.1 200"), "{target}: {resp}");
            assert!(resp.ends_with(body), "{target}: {resp}");
        }
    }

    /// Over a socket: a path that is not valid UTF-8 once decoded is answered
    /// 400 with no body, as the other invalid stream requests; the encoded
    /// U+FFFD reads its stream.
    #[test]
    fn deviation_d104_a_path_that_is_not_utf8_is_answered_400() {
        let endpoint = Served::new(None);
        let msg = StreamMessage { stream: "\u{FFFD}".into(), data: r#"{"n":9}"#.into(), ..Default::default() };
        endpoint.memory.handle_broadcast(&msg).unwrap();
        for target in ["/ds/%ff?offset=-1", "/ds/%fe?offset=-1"] {
            let resp = request(endpoint.addr, "GET", target);
            assert!(resp.starts_with("http/1.1 400"), "{target}: {resp}");
            assert!(resp.ends_with("\r\n\r\n"), "{target}: an empty body: {resp}");
        }
        let resp = request(endpoint.addr, "GET", "/ds/%EF%BF%BD?offset=-1");
        assert!(resp.starts_with("http/1.1 200") && resp.ends_with(r#"[{"n":9}]"#), "{resp}");
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

    // ------------------------------------------------------------ live requests

    use cabby_core::broker::{Broadcaster, BrokerError, Cacheable, Memory, PresenceInfoOptions};
    use cabby_core::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage};
    use std::sync::Condvar;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Bound for every wait.
    const WAIT: Duration = Duration::from_secs(5);

    /// A response as the client sees it.
    #[derive(Debug, Default, Clone)]
    struct RecState {
        status: Option<u16>,
        headers: http::HeaderMap,
        body: Vec<u8>,
        finished: bool,
    }

    impl RecState {
        fn header(&self, name: &str) -> &str {
            self.headers.get(name).map_or("", |v| v.to_str().unwrap())
        }
    }

    /// A recorded response; waiters are woken on every change.
    #[derive(Default)]
    struct Rec {
        state: Mutex<RecState>,
        changed: Condvar,
    }

    impl Rec {
        fn update(&self, f: impl FnOnce(&mut RecState)) {
            f(&mut lock(&self.state));
            self.changed.notify_all();
        }

        /// Waits up to `timeout` until `done` holds; the state then, if it does.
        fn wait_up_to(&self, timeout: Duration, done: impl Fn(&RecState) -> bool) -> Option<RecState> {
            let deadline = Instant::now() + timeout;
            let mut st = lock(&self.state);
            while !done(&st) {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return None;
                }
                st = self.changed.wait_timeout(st, left).unwrap_or_else(PoisonError::into_inner).0;
            }
            Some(st.clone())
        }

        /// Waits (bounded) until `done` holds.
        fn wait(&self, what: &str, done: impl Fn(&RecState) -> bool) -> RecState {
            self.wait_up_to(WAIT, done).unwrap_or_else(|| {
                let st = lock(&self.state);
                panic!("timed out waiting for {what}: {:?} {:?}", st.status, String::from_utf8_lossy(&st.body))
            })
        }
    }

    /// Writes into a [`Rec`]; the head goes out with the first write, flush or finish.
    struct RecWriter {
        rec: Arc<Rec>,
        headers: http::HeaderMap,
    }

    impl RecWriter {
        fn send(&self, status: u16, f: impl FnOnce(&mut RecState)) {
            self.rec.update(|st| {
                if st.status.is_none() {
                    st.status = Some(status);
                    st.headers = self.headers.clone();
                }
                f(st);
            });
        }
    }

    impl ResponseWriter for RecWriter {
        fn headers_mut(&mut self) -> &mut http::HeaderMap {
            &mut self.headers
        }
        fn write_header(&mut self, status: u16) {
            self.send(status, |_| {});
        }
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.send(200, |st| st.body.extend_from_slice(data));
            Ok(data.len())
        }
        fn flush(&mut self) {
            self.send(200, |_| {});
        }
        fn finish(&mut self) {
            self.send(200, |st| st.finished = true);
        }
    }

    /// Where a hook runs: after the n-th tail read, after the broker has the
    /// subscription (the hub gets it next), before or after the n-th history read
    /// (1-based).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum At {
        Peak(usize),
        Subscribe,
        BeforeRead(usize),
        AfterRead(usize),
    }

    type Hook = Box<dyn FnOnce(&Memory) + Send>;

    /// The memory broker with hooks at the steps of a live request (each runs once).
    struct Hooked {
        memory: Arc<Memory>,
        peaks: AtomicUsize,
        reads: AtomicUsize,
        /// Reports no tail for any stream, as the NATS broker does.
        no_tail: std::sync::atomic::AtomicBool,
        hooks: Mutex<Vec<(At, Hook)>>,
    }

    impl Hooked {
        fn on(&self, at: At, hook: impl FnOnce(&Memory) + Send + 'static) {
            lock(&self.hooks).push((at, Box::new(hook)));
        }

        fn run(&self, at: At) {
            let hook = {
                let mut hooks = lock(&self.hooks);
                hooks.iter().position(|(a, _)| *a == at).map(|i| hooks.remove(i).1)
            };
            if let Some(hook) = hook {
                hook(&self.memory);
            }
        }

        fn read<T>(&self, read: impl FnOnce() -> T) -> T {
            let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
            self.run(At::BeforeRead(n));
            let result = read();
            self.run(At::AfterRead(n));
            result
        }
    }

    impl Broker for Hooked {
        fn start(&self) -> Result<(), BrokerError> {
            self.memory.start()
        }
        fn shutdown(&self) -> Result<(), BrokerError> {
            self.memory.shutdown()
        }
        fn announce(&self) -> String {
            self.memory.announce()
        }
        fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
            self.memory.handle_broadcast(msg)
        }
        fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
            self.memory.handle_command(msg)
        }
        fn subscribe(&self, stream: &str) -> String {
            let id = self.memory.subscribe(stream);
            self.run(At::Subscribe);
            id
        }
        fn unsubscribe(&self, stream: &str) -> String {
            self.memory.unsubscribe(stream)
        }
        fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
            self.read(|| self.memory.history_from(stream, epoch, offset))
        }
        fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
            self.read(|| self.memory.history_since(stream, ts))
        }
        fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
            self.memory.commit_session(sid, session)
        }
        fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
            self.memory.restore_session(sid)
        }
        fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
            self.memory.touch_session(sid)
        }
        fn supports_presence(&self) -> bool {
            self.memory.supports_presence()
        }
        fn presence_add(
            &self,
            stream: &str,
            sid: &str,
            pid: &str,
            info: serde_json::Value,
        ) -> Result<Option<PresenceEvent>, BrokerError> {
            self.memory.presence_add(stream, sid, pid, info)
        }
        fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
            self.memory.presence_remove(stream, sid)
        }
        fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
            self.memory.presence_info(stream, opts)
        }
        fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
            self.memory.touch_presence(sid)
        }
        fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
            let tail = if self.no_tail.load(Ordering::SeqCst) { Ok(None) } else { self.memory.peak(stream) };
            self.run(At::Peak(self.peaks.fetch_add(1, Ordering::SeqCst) + 1));
            tail
        }
    }

    /// Publishes `{"n":<n>}` to `s`.
    fn publish(memory: &Memory, n: u64) {
        publish_in(memory, "s", n);
    }

    /// Publishes `{"n":<n>}` to `stream`.
    fn publish_in(memory: &Memory, stream: &str, n: u64) {
        let msg = StreamMessage { stream: stream.into(), data: format!(r#"{{"n":{n}}}"#), ..Default::default() };
        memory.handle_broadcast(&msg).unwrap();
    }

    /// Waits (bounded) until `cond` holds.
    fn wait_for(what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The endpoint over [`Hooked`], served without a socket: public streams,
    /// `{"n":1}` at `1::ep` in `s`.
    struct Live {
        node: Arc<Node>,
        memory: Arc<Memory>,
        broker: Arc<Hooked>,
        handler: Arc<Handler>,
    }

    /// A request served on its own thread.
    struct InFlight {
        rec: Arc<Rec>,
        done: CancellationToken,
        thread: std::thread::JoinHandle<()>,
    }

    impl Live {
        fn new() -> Self {
            let mut nconf = node::Config::new();
            nconf.disconnect_mode = node::DISCONNECT_MODE_NEVER.into();
            let mut sconf = cabby_core::streams::Config::new();
            sconf.public = true;
            let streams = Arc::new(StreamsController::for_config(&sconf));
            let mut conf = Config::new();
            conf.skip_auth = true;
            conf.poll_interval = 3;
            let node = Node::new(nconf, vec![node::NodeOption::Controller(streams.clone())]);
            let subscriber = Arc::new(node::LegacySubscriber::new(node.clone()));
            let memory = Arc::new(Memory::new(
                Some(subscriber as Arc<dyn Broadcaster>),
                None,
                &cabby_core::broker::Config::new(),
                Arc::new(cabby_core::broker::SystemClock),
            ));
            memory.set_epoch("ep");
            let broker = Arc::new(Hooked {
                memory: memory.clone(),
                peaks: AtomicUsize::new(0),
                reads: AtomicUsize::new(0),
                no_tail: Default::default(),
                hooks: Mutex::default(),
            });
            node.set_broker(broker.clone());
            memory.start().unwrap();
            node.start().unwrap();
            publish(&memory, 1);
            let handler =
                ds_handler(node.clone(), broker.clone(), Some(streams), None, CancellationToken::new(), None, &conf);
            Self { node, memory, broker, handler: Arc::new(handler.unwrap()) }
        }

        fn publish(&self, n: u64) {
            publish(&self.memory, n);
        }

        fn get(&self, target: &str) -> InFlight {
            let rec = Arc::new(Rec::default());
            let done = CancellationToken::new();
            let writer = RecWriter { rec: rec.clone(), headers: http::HeaderMap::new() };
            let req =
                HttpRequest { method: "GET".into(), target: target.into(), host: "x".into(), ..Default::default() };
            let (handler, token) = (self.handler.clone(), done.clone());
            let thread = std::thread::spawn(move || handler.serve(&req, Box::new(writer), &token));
            InFlight { rec, done, thread }
        }

        /// Waits (bounded) until a live session is subscribed in the hub (`s` has none).
        fn wait_subscribed(&self) {
            wait_for("a live subscription", || self.node.hub().streams_size() == 1);
        }
    }

    impl InFlight {
        /// The response of a request that ends by itself.
        fn response(self) -> RecState {
            let st = self.rec.wait("the end of the response", |st| st.finished);
            self.thread.join().unwrap();
            st
        }

        /// Ends the request as a client going away does.
        fn close(self) -> RecState {
            self.done.cancel();
            self.response()
        }

        /// Waits (bounded) until the SSE client has `n` messages.
        fn wait_sse(&self, n: usize) -> RecState {
            self.rec.wait(&format!("{n} SSE messages"), |st| sse_read(&st.body).0.len() >= n)
        }
    }

    /// The complete events of an SSE body as `(event, data)`, pings left out.
    fn sse_events(body: &[u8]) -> Vec<(String, String)> {
        let text = String::from_utf8_lossy(body);
        let mut events: Vec<&str> = text.split("\n\n").collect();
        events.pop(); // incomplete, or empty
        events
            .into_iter()
            .filter(|e| *e != sse::COMMENT_PING)
            .map(|e| {
                let (event, data) = e.split_once('\n').expect("an event and its data");
                (event.strip_prefix("event: ").unwrap().to_owned(), data.strip_prefix("data:").unwrap().to_owned())
            })
            .collect()
    }

    /// What a client following an SSE body got: the `n` of the messages and the
    /// offset of each control event, in order.
    fn sse_read(body: &[u8]) -> (Vec<u64>, Vec<String>) {
        let (mut ns, mut offsets) = (Vec::new(), Vec::new());
        for (event, data) in sse_events(body) {
            let v: serde_json::Value = serde_json::from_str(&data).unwrap();
            if event == "data" {
                ns.extend(v.as_array().unwrap().iter().map(|m| m["n"].as_u64().unwrap()));
            } else {
                offsets.push(v["streamNextOffset"].as_str().unwrap().to_owned());
            }
        }
        (ns, offsets)
    }

    /// The `n` of the messages in a long-poll or catch-up body.
    fn json_read(body: &[u8]) -> Vec<u64> {
        let v: Vec<serde_json::Value> = serde_json::from_slice(body).unwrap();
        v.iter().map(|m| m["n"].as_u64().unwrap()).collect()
    }

    fn offsets(range: std::ops::RangeInclusive<u64>, epoch: &str) -> Vec<String> {
        range.map(|n| encode_offset(n, epoch)).collect()
    }

    /// The data and control event of the live event for `{"n":<n>}`, if the body has it.
    fn live_event(body: &[u8], n: u64) -> Option<(String, String)> {
        let events = sse_events(body);
        let i = events.iter().position(|(e, d)| e == "data" && *d == format!(r#"[{{"n":{n}}}]"#))?;
        let (event, control) = events.get(i + 1)?;
        (event == "control" && !control.contains("upToDate")).then(|| (events[i].1.clone(), control.clone()))
    }

    // ------------------------------------------------------------ D-090

    /// D-090: SSE encoders with different cursors, fed one cached message in
    /// either order, write their own cursor and the same data.
    #[test]
    fn deviation_d090_sse_encoders_keep_their_cursor_for_a_shared_message() {
        use cabby_core::common::{EncodedMessage, Reply};
        use cabby_core::encoders::CachedEncodedMessage;

        let a = SseEncoder { cursor: "100".into(), up_to_date: false };
        let b = SseEncoder { cursor: "200".into(), up_to_date: true };
        for (first, second) in [(&a, &b), (&b, &a)] {
            let msg = CachedEncodedMessage::new(EncodedMessage::Reply(Reply {
                message: Some(serde_json::json!({"n": 1})),
                offset: 7,
                epoch: "ep".into(),
                stream_id: "s".into(),
                ..Default::default()
            }));
            for e in [first, second] {
                let frame = String::from_utf8(msg.fetch_with(e).unwrap().payload.to_vec()).unwrap();
                let (data, control) = frame.split_once("\n\n").unwrap();
                assert_eq!("event: data\ndata:[{\"n\":1}]", data);
                assert_eq!(
                    String::from_utf8(encode_sse_control_event("7::ep", &e.cursor, e.up_to_date)).unwrap(),
                    control
                );
            }
        }
    }

    /// D-090: the endpoint's live SSE sessions share one encoded frame per message
    /// (whichever session fetches it first), and each writes its own cursor.
    #[test]
    fn deviation_d090_live_sse_sessions_share_the_data_and_keep_their_cursor() {
        use cabby_core::common::{EncodedMessage, Reply};
        use cabby_core::encoders::CachedEncodedMessage;

        let session = |cursor: &str| {
            let rec = Arc::new(Rec::default());
            let conn =
                Arc::new(sse::Connection::new(Box::new(RecWriter { rec: rec.clone(), headers: Default::default() })));
            conn.established();
            let stream = SseStream::new(conn, cursor.to_owned());
            stream.release(Watermark::default());
            (stream, rec)
        };
        let (a, b) = (session("100"), session("200"));
        for (offset, (first, second)) in [(7, (&a, &b)), (8, (&b, &a))] {
            let msg = CachedEncodedMessage::new(EncodedMessage::Reply(Reply {
                message: Some(serde_json::json!({"n": 1})),
                offset,
                epoch: "ep".into(),
                stream_id: "s".into(),
                ..Default::default()
            }));
            let frames = [first, second].map(|_| msg.fetch_with(&SseDataEncoder).unwrap());
            assert!(Arc::ptr_eq(&frames[0], &frames[1]), "the frame is encoded once");
            for ((conn, rec), frame) in [first, second].into_iter().zip(&frames) {
                lock(&rec.state).body.clear();
                conn.write(&frame.payload, Instant::now()).unwrap();
                let body = String::from_utf8(lock(&rec.state).body.clone()).unwrap();
                let want = format!(
                    "event: data\ndata:[{{\"n\":1}}]\n\nevent: control\ndata:{{\"streamCursor\":\"{}\",\"streamNextOffset\":\"{offset}::ep\"}}\n\n",
                    conn.cursor
                );
                assert_eq!(want, body);
            }
        }

        // Pings are written as they are.
        let ping = SseDataEncoder.encode(&EncodedMessage::Ping(Default::default())).unwrap();
        lock(&a.1.state).body.clear();
        a.0.write(&ping.payload, Instant::now()).unwrap();
        assert_eq!(format!("{}\n\n", sse::COMMENT_PING), String::from_utf8(lock(&a.1.state).body.clone()).unwrap());
    }

    /// D-090: SSE clients of one stream with different cursors each get their own
    /// in the control events of the same broadcasts, and the same data events.
    #[test]
    fn deviation_d090_sse_clients_get_their_own_cursor() {
        let live = Live::new();
        let cursors = [1_000_000_000_000_u64, 2_000_000_000_000];
        let clients: Vec<_> =
            cursors.iter().map(|c| live.get(&format!("/ds/s?offset=1::ep&live=sse&cursor={c}"))).collect();

        // Broadcast until both clients got the same message live (history is
        // written per request).
        let mut n = 1;
        let deadline = Instant::now() + WAIT;
        let events = loop {
            assert!(Instant::now() < deadline, "the clients did not get a live broadcast");
            n += 1;
            live.publish(n);
            let live_event = |c: &InFlight| {
                let st = c.rec.wait_up_to(Duration::from_millis(100), |st| live_event(&st.body, n).is_some())?;
                live_event(&st.body, n)
            };
            if let [Some(a), Some(b)] = [live_event(&clients[0]), live_event(&clients[1])] {
                break [a, b];
            }
        };

        assert_eq!(events[0].0, events[1].0, "the data events differ");
        for (client, (_, control)) in cursors.iter().zip(&events) {
            let control: serde_json::Value = serde_json::from_str(control).unwrap();
            let cursor: u64 = control["streamCursor"].as_str().unwrap().parse().unwrap();
            // A client cursor ahead of the clock gets 1 to 60 intervals more.
            assert!(cursor > *client && cursor <= client + 60, "client {client} got cursor {cursor}");
            assert_eq!(encode_offset(n, "ep"), control["streamNextOffset"]);
        }
        for c in clients {
            c.close();
        }
    }

    /// D-089: how live messages compare with what the client has.
    #[test]
    fn deviation_d089_live_messages_follow_the_watermark() {
        let at = |epoch: &str, offset| Some((epoch.to_owned(), offset));

        let mut mark = Watermark { position: at("ep", 2), strict: false };
        assert_eq!(Admit::Skip, mark.admit("ep", 2));
        assert_eq!(Admit::Deliver, mark.admit("ep", 3));
        assert_eq!(Admit::Skip, mark.admit("ep", 3));
        // A later epoch.
        assert_eq!(Admit::Deliver, mark.admit("ep2", 1));
        assert_eq!(at("ep2", 1), mark.position);

        // The epoch changed around the subscription: another epoch ends the
        // response until a message of the position's epoch was delivered.
        let mut mark = Watermark { position: at("ep2", 2), strict: true };
        assert_eq!(Admit::Skip, mark.admit("ep2", 1));
        assert_eq!(Admit::End, mark.admit("ep", 3));
        assert_eq!(Admit::Deliver, mark.admit("ep2", 3));
        assert_eq!(Admit::Deliver, mark.admit("ep3", 1));

        // No position yet: the first message sets it.
        let mut mark = Watermark::default();
        assert_eq!(Admit::Deliver, mark.admit("ep", 5));
        assert_eq!(Admit::Skip, mark.admit("ep", 5));
    }

    // ------------------------------------------------------------ D-089: the gate
    //
    // The tests below write to the connections directly, before `release`, so
    // the hold does not depend on the hub's delivery thread being slower than
    // the history read.

    /// The frame `encoder` makes of `{"n":<n>}` at offset `n` of `epoch`.
    fn reply_frame(encoder: impl NodeEncoder, n: u64, epoch: &str) -> Vec<u8> {
        use cabby_core::common::{EncodedMessage, Reply};
        let msg = EncodedMessage::Reply(Reply {
            message: Some(serde_json::json!({"n": n})),
            offset: n,
            epoch: epoch.into(),
            stream_id: "s".into(),
            ..Default::default()
        });
        encoder.encode(&msg).unwrap().payload.to_vec()
    }

    fn mark(epoch: &str, offset: u64, strict: bool) -> Watermark {
        Watermark { position: Some((epoch.to_owned(), offset)), strict }
    }

    /// A live SSE session with cursor `c` on an established response.
    fn sse_session() -> (SseStream, Arc<Rec>, Arc<sse::Connection>) {
        let rec = Arc::new(Rec::default());
        let conn =
            Arc::new(sse::Connection::new(Box::new(RecWriter { rec: rec.clone(), headers: Default::default() })));
        conn.established();
        (SseStream::new(conn.clone(), "c".to_owned()), rec, conn)
    }

    fn body_text(rec: &Rec) -> String {
        String::from_utf8(lock(&rec.state).body.clone()).unwrap()
    }

    /// The data and control event an SSE session with cursor `c` writes for `n`.
    fn sse_event(n: u64) -> String {
        format!(
            "event: data\ndata:[{{\"n\":{n}}}]\n\nevent: control\ndata:{{\"streamCursor\":\"c\",\"streamNextOffset\":\"{n}::ep\"}}\n\n"
        )
    }

    /// D-089: frames written to an SSE session before `release` are held; on
    /// release, those at or below the mark are dropped, the later one and a
    /// ping are written once, in the order they came, and later frames are
    /// compared with the moved mark.
    #[test]
    fn deviation_d089_sse_holds_frames_until_release_and_drops_what_the_client_has() {
        use cabby_core::common::EncodedMessage;
        let (stream, rec, conn) = sse_session();
        let ping = SseDataEncoder.encode(&EncodedMessage::Ping(Default::default())).unwrap().payload.to_vec();
        let now = Instant::now();
        let held = [
            reply_frame(SseDataEncoder, 1, "ep"),
            reply_frame(SseDataEncoder, 2, "ep"),
            ping,
            reply_frame(SseDataEncoder, 3, "ep"),
            reply_frame(SseDataEncoder, 2, "ep"),
        ];
        for frame in &held {
            stream.write(frame, now).unwrap();
        }
        assert_eq!("", body_text(&rec), "nothing is written before the history");

        stream.release(mark("ep", 2, false));

        let released = format!("{}\n\n{}", sse::COMMENT_PING, sse_event(3));
        assert_eq!(released, body_text(&rec));
        stream.write(&reply_frame(SseDataEncoder, 3, "ep"), now).unwrap();
        stream.write(&reply_frame(SseDataEncoder, 4, "ep"), now).unwrap();
        assert_eq!(format!("{released}{}", sse_event(4)), body_text(&rec), "3 once, then 4");
        assert!(!conn.context().is_cancelled());
    }

    /// D-089: a held frame of another epoch than a strict mark's ends the SSE
    /// response on release, without being written.
    #[test]
    fn deviation_d089_sse_ends_on_another_epoch_with_a_strict_mark() {
        let (stream, rec, conn) = sse_session();
        stream.write(&reply_frame(SseDataEncoder, 5, "old"), Instant::now()).unwrap();
        assert!(!conn.context().is_cancelled());
        assert_eq!("", body_text(&rec));

        stream.release(mark("ep", 2, true));

        assert!(conn.context().is_cancelled(), "the response ends");
        assert_eq!("", body_text(&rec), "the frame is not written");
    }

    /// A long-poll connection that holds its frames.
    fn poll_session() -> (PollConnection, Arc<Rec>) {
        let rec = Arc::new(Rec::default());
        let poll = PollConnection::new(Box::new(RecWriter { rec: rec.clone(), headers: Default::default() }));
        poll.hold();
        (poll, rec)
    }

    /// D-089: frames written to a held long-poll before `release` (and a
    /// ping) answer nothing; on release, the response is the first frame
    /// above the mark, once, and later frames are not written.
    #[test]
    fn deviation_d089_long_poll_holds_frames_until_release_and_answers_with_the_first_the_client_lacks() {
        let (poll, rec) = poll_session();
        let now = Instant::now();
        for n in [1, 2] {
            poll.write(&reply_frame(PollEncoder, n, "ep"), now).unwrap();
        }
        poll.write_ping(now).unwrap();
        for n in [3, 4] {
            poll.write(&reply_frame(PollEncoder, n, "ep"), now).unwrap();
        }
        assert!(!poll.is_done(), "nothing is answered before the history");
        assert_eq!(None, lock(&rec.state).status);

        poll.release(mark("ep", 2, false));

        assert!(poll.is_done());
        assert_poll(&lock(&rec.state).clone(), &[3], "3::ep");
        poll.write(&reply_frame(PollEncoder, 5, "ep"), now).unwrap();
        assert_poll(&lock(&rec.state).clone(), &[3], "3::ep");
    }

    /// D-089: a held frame of another epoch than a strict mark's answers the
    /// long-poll with 204, as an elapsed poll interval does.
    #[test]
    fn deviation_d089_long_poll_ends_on_another_epoch_with_a_strict_mark() {
        let (poll, rec) = poll_session();
        poll.write(&reply_frame(PollEncoder, 5, "old"), Instant::now()).unwrap();
        assert!(!poll.is_done());

        poll.release(mark("ep", 2, true));

        assert!(poll.is_done());
        let st = lock(&rec.state).clone();
        assert_eq!((Some(204), true), (st.status, st.body.is_empty()), "{st:?}");
    }

    // ------------------------------------------------------------ D-089: SSE

    fn sse(offset: &str) -> String {
        format!("/ds/s?offset={offset}&live=sse")
    }

    fn long_poll(offset: &str) -> String {
        format!("/ds/s?offset={offset}&live=long-poll")
    }

    /// D-089: a message published while the subscription is installed and one
    /// published after the history read reach the SSE client once, in order.
    #[test]
    fn deviation_d089_sse_delivers_what_lands_between_catch_up_and_live() {
        let live = Live::new();
        live.broker.on(At::Subscribe, |m| publish(m, 2));
        live.broker.on(At::AfterRead(1), |m| publish(m, 3));
        let c = live.get(&sse("1::ep"));
        c.wait_sse(2);
        live.publish(4);
        c.wait_sse(3);
        assert_eq!((vec![2, 3, 4], offsets(2..=4, "ep")), sse_read(&c.close().body));
    }

    /// D-089: with nothing to catch up on, a message published after the history
    /// read reaches the SSE client.
    #[test]
    fn deviation_d089_sse_empty_history_then_a_message_before_live() {
        let live = Live::new();
        live.broker.on(At::AfterRead(1), |m| publish(m, 2));
        let c = live.get(&sse("1::ep"));
        c.wait_sse(1);
        live.publish(3);
        c.wait_sse(2);
        assert_eq!((vec![2, 3], offsets(2..=3, "ep")), sse_read(&c.close().body));
    }

    /// D-089: a message both in the history and in the live queue reaches the SSE
    /// client once, with the history.
    #[test]
    fn deviation_d089_sse_a_message_in_history_and_live_once() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(1), |m| publish(m, 2));
        let c = live.get(&sse("1::ep"));
        let st = c.wait_sse(1);
        assert!(live_event(&st.body, 2).is_none(), "not with the history: {:?}", String::from_utf8_lossy(&st.body));
        live.publish(3);
        c.wait_sse(2);
        assert_eq!((vec![2, 3], offsets(2..=3, "ep")), sse_read(&c.close().body));
    }

    /// D-089: `offset=now` delivers what is published after the tail is read,
    /// before and after the subscription is installed.
    #[test]
    fn deviation_d089_sse_now() {
        let live = Live::new();
        live.broker.on(At::Peak(1), |m| publish(m, 2));
        live.broker.on(At::AfterRead(1), |m| publish(m, 3));
        let c = live.get(&sse(NOW_OFFSET));
        c.wait_sse(2);
        live.publish(4);
        c.wait_sse(3);
        assert_eq!((vec![2, 3, 4], offsets(2..=4, "ep")), sse_read(&c.close().body));
    }

    /// D-089: when the epoch changes between the subscription and the history
    /// read, a live message of the previous epoch ends the stream after the
    /// history; the client resumes from the history's offset.
    #[test]
    fn deviation_d089_sse_epoch_change_before_the_history_read() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(1), |m| {
            publish(m, 2);
            m.set_epoch("ep2");
        });
        let st = live.get(&sse(START_OFFSET)).response();
        assert_eq!((vec![1, 2], offsets(2..=2, "ep2")), sse_read(&st.body));

        live.broker.on(At::AfterRead(2), |m| publish(m, 3));
        let c = live.get(&sse("2::ep2"));
        c.wait_sse(1);
        assert_eq!((vec![3], offsets(3..=3, "ep2")), sse_read(&c.close().body));
    }

    /// D-089: an offset of an epoch that ended before the history read is gone,
    /// as before.
    #[test]
    fn deviation_d089_sse_offset_of_a_previous_epoch_is_gone() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(1), |m| m.set_epoch("ep2"));
        assert_eq!(Some(410), live.get(&sse("1::ep")).response().status);
    }

    /// D-089: an epoch change after the history read is delivered live, as before.
    #[test]
    fn deviation_d089_sse_a_later_epoch_is_delivered() {
        let live = Live::new();
        live.broker.on(At::AfterRead(1), |m| {
            m.set_epoch("ep2");
            publish(m, 2);
        });
        let c = live.get(&sse("1::ep"));
        c.wait_sse(1);
        live.publish(3);
        c.wait_sse(2);
        assert_eq!((vec![2, 3], offsets(2..=3, "ep2")), sse_read(&c.close().body));
    }

    // ------------------------------------------------------------ D-089: long-poll

    /// A long-poll data response with these messages and next offset.
    fn assert_poll(st: &RecState, ns: &[u64], next: &str) {
        assert_eq!(Some(200), st.status, "{st:?}");
        assert_eq!(ns, json_read(&st.body));
        assert_eq!(next, st.header(STREAM_OFFSET_HEADER));
    }

    /// D-089: with nothing to catch up on, a long-poll returns a message published
    /// after the history read instead of timing out.
    #[test]
    fn deviation_d089_long_poll_returns_what_lands_before_live() {
        let live = Live::new();
        live.broker.on(At::AfterRead(1), |m| publish(m, 2));
        assert_poll(&live.get(&long_poll("1::ep")).response(), &[2], "2::ep");
    }

    /// D-089: a long-poll client following the offsets it gets receives every
    /// message once, in order.
    #[test]
    fn deviation_d089_long_poll_follows_the_offsets() {
        let live = Live::new();
        live.broker.on(At::AfterRead(1), |m| publish(m, 2));
        live.broker.on(At::Subscribe, |m| publish(m, 3));
        assert_poll(&live.get(&long_poll("1::ep")).response(), &[2, 3], "3::ep");
        // The next poll reads nothing (read 3), subscribes and reads again (read 4).
        live.broker.on(At::AfterRead(4), |m| publish(m, 4));
        assert_poll(&live.get(&long_poll("3::ep")).response(), &[4], "4::ep");
    }

    /// D-089: a message both in the history and in the live queue is returned once.
    #[test]
    fn deviation_d089_long_poll_a_message_in_history_and_live_once() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(2), |m| publish(m, 2));
        assert_poll(&live.get(&long_poll("1::ep")).response(), &[2], "2::ep");
        live.broker.on(At::AfterRead(4), |m| publish(m, 3));
        assert_poll(&live.get(&long_poll("2::ep")).response(), &[3], "3::ep");
    }

    /// D-089: `offset=now` returns a message published after the tail is read,
    /// with the headers of `now` reads (not cacheable, no `ETag`).
    #[test]
    fn deviation_d089_long_poll_now() {
        let live = Live::new();
        live.broker.on(At::Peak(1), |m| publish(m, 2));
        let st = live.get(&long_poll(NOW_OFFSET)).response();
        assert_poll(&st, &[2], "2::ep");
        assert_eq!("no-store", st.header("cache-control"));
        assert_eq!("", st.header("etag"));
    }

    /// D-089: when the epoch changes between the subscription and the history
    /// read, the long-poll is gone, as when it changes before.
    #[test]
    fn deviation_d089_long_poll_epoch_change_before_the_history_read() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(2), |m| m.set_epoch("ep2"));
        assert_eq!(Some(410), live.get(&long_poll("1::ep")).response().status);
    }

    /// D-089: `offset=now` when the epoch changed after the tail was read: a live
    /// message of another epoch ends the long-poll with `204` at once, and the
    /// client polls again.
    #[test]
    fn deviation_d089_long_poll_now_and_an_epoch_change() {
        let live = Live::new();
        live.broker.on(At::BeforeRead(1), |m| m.set_epoch("ep2"));
        live.broker.on(At::AfterRead(1), |m| publish(m, 2));
        let started = Instant::now();
        let st = live.get(&long_poll(NOW_OFFSET)).response();
        assert_eq!(Some(204), st.status, "{st:?}");
        assert!(started.elapsed() < Duration::from_secs(2), "answered at the poll interval");
    }

    /// D-089: an epoch change after the history read is delivered, as before.
    #[test]
    fn deviation_d089_long_poll_a_later_epoch_is_delivered() {
        let live = Live::new();
        live.broker.on(At::AfterRead(2), |m| {
            m.set_epoch("ep2");
            publish(m, 2);
        });
        assert_poll(&live.get(&long_poll("1::ep")).response(), &[2], "2::ep2");
    }

    // ------------------------------------------------------------ D-089: `now` on an empty stream

    /// D-089: `offset=now` on a stream without entries: a message published after
    /// the tail read and before the subscription, and one after, reach the SSE
    /// client once, in order.
    #[test]
    fn deviation_d089_sse_now_on_an_empty_stream() {
        let live = Live::new();
        live.broker.on(At::Peak(1), |m| publish_in(m, "e", 1));
        let c = live.get("/ds/e?offset=now&live=sse");
        c.wait_sse(1);
        publish_in(&live.memory, "e", 2);
        c.wait_sse(2);
        assert_eq!((vec![1, 2], offsets(1..=2, "ep")), sse_read(&c.close().body));
    }

    /// D-089: `offset=now` on a stream without entries and nothing published
    /// before the subscription: live messages, as before.
    #[test]
    fn deviation_d089_sse_now_on_an_empty_stream_without_a_gap() {
        let live = Live::new();
        let c = live.get("/ds/e?offset=now&live=sse");
        live.wait_subscribed();
        publish_in(&live.memory, "e", 1);
        c.wait_sse(1);
        publish_in(&live.memory, "e", 2);
        c.wait_sse(2);
        assert_eq!((vec![1, 2], offsets(1..=2, "ep")), sse_read(&c.close().body));
    }

    /// D-089: `offset=now` on a stream without entries: a long-poll returns a
    /// message published after the tail read and before the subscription, and the
    /// next poll the one after.
    #[test]
    fn deviation_d089_long_poll_now_on_an_empty_stream() {
        let live = Live::new();
        live.broker.on(At::Peak(1), |m| publish_in(m, "e", 1));
        let st = live.get("/ds/e?offset=now&live=long-poll").response();
        assert_poll(&st, &[1], "1::ep");
        assert_eq!("no-store", st.header("cache-control"));

        let next = live.get("/ds/e?offset=1::ep&live=long-poll");
        live.wait_subscribed();
        publish_in(&live.memory, "e", 2);
        assert_poll(&next.response(), &[2], "2::ep");
    }

    /// D-089: `offset=now` on a stream without entries and nothing published
    /// before the subscription: the first live message, as before.
    #[test]
    fn deviation_d089_long_poll_now_on_an_empty_stream_without_a_gap() {
        let live = Live::new();
        let pending = live.get("/ds/e?offset=now&live=long-poll");
        live.wait_subscribed();
        publish_in(&live.memory, "e", 1);
        assert_poll(&pending.response(), &[1], "1::ep");
    }

    /// D-089: `offset=now` with a broker that reports no tail for a stream with
    /// entries: its history is not read, and only later messages are delivered.
    #[test]
    fn deviation_d089_now_ignores_history_when_the_broker_reports_no_tail() {
        let live = Live::new();
        live.broker.no_tail.store(true, Ordering::SeqCst);
        let c = live.get(&sse(NOW_OFFSET));
        live.wait_subscribed();
        live.publish(2);
        c.wait_sse(1);
        assert_eq!((vec![2], offsets(2..=2, "ep")), sse_read(&c.close().body));
    }
}
