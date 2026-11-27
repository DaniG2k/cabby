//! Server-sent events transport (upstream package `sse`).
//!
//! * [`Encoder`]: server messages as SSE events (`event:`, `data:`, `id:`, `retry:`).
//! * [`Connection`]: the node's [`Connection`](crate::node::Connection) over an
//!   HTTP response; messages written before [`Connection::established`] are
//!   buffered.
//! * [`Handler`]: the endpoint. A GET subscribes to the channel or stream named in
//!   the query; a POST carries newline-separated JSON subscribe commands. The
//!   response streams until the client goes away, the session closes or the
//!   server shuts down. [`handler`] mounts it on the HTTP server.
//!
//! # Order of checks
//!
//! Everything the request line and headers decide is answered before the body
//! is read (D-096): the preflight, the method, the request id, the commands of a
//! GET, a POST body announced over [`DEFAULT_MAX_BODY_SIZE`], and the sign-in.
//! Only a signed-in POST reads its body, at most [`DEFAULT_MAX_BODY_SIZE`] bytes
//! within 5 s (D-097); other bodies are never read.
//!
//! # Execution
//!
//! Setting a session up calls the node synchronously (authentication may perform
//! an RPC call), so it runs on a blocking thread. Once the response is streaming,
//! the handler only waits for the request, the session or the server to end,
//! which is an async wait: an open SSE response holds no thread. Messages are
//! written by the session's writer (the node's task pool).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use cabby_core::common::{
    DisconnectMessage, HistoryPosition, HistoryRequest, Message, PING_TYPE, SERVER_RESTART_REASON, Status,
};
use cabby_core::encoders::{EncodeError, EncodedMessageJson, Encoder as NodeEncoder, SentFrame, no_frame};
use cabby_core::logger::{Attr, Level, LogValue};
use hyper::body::Incoming;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::metrics::Instrumenter;
use crate::node::{self, CLOSE_NORMAL_CLOSURE, ConnectionError, Node, Session, SessionOption};
use crate::server::{
    self, ConnInfo, HeadersExtractor, HttpRequest, RequestInfo, ResponseFuture, ResponseWriter, SharedResponseWriter,
    lock_writer, query_param, shared_writer, write_cors_headers,
};

/// Default ping interval in seconds (upstream `DefaultPingInterval`).
pub const DEFAULT_PING_INTERVAL: i64 = 15;

/// Keepalive comment line (upstream `CommentPing`).
pub const COMMENT_PING: &str = ": ping";

/// `retry:` value telling a client not to reconnect (one year in ms, upstream
/// `retryNoReconnect`).
pub const RETRY_NO_RECONNECT: i64 = 31_536_000_000;

/// Maximum POST body size (upstream `defaultMaxBodySize`, 64 kB).
pub const DEFAULT_MAX_BODY_SIZE: usize = 65_536;

/// How long reading a POST body may take, from when the session is signed in
/// (the server's [`server::BODY_READ_TIMEOUT`], D-097).
const BODY_READ_TIMEOUT: Duration = server::BODY_READ_TIMEOUT;

/// Gauge of open SSE responses (upstream `metricsClientsNum`).
pub const METRICS_CLIENTS_NUM: &str = "sse_clients_num";

const SSE_ENCODER_ID: &str = "sse";
const LAST_ID_DELIMITER: &str = "/";

const SIGNED_STREAM_PARAM: &str = "signed_stream";
const PUBLIC_STREAM_PARAM: &str = "stream";
const SIGNED_STREAM_CHANNEL: &str = "$pubsub";
const TURBO_STREAMS_PARAM: &str = "turbo_signed_stream_name";
const TURBO_STREAMS_CHANNEL: &str = "Turbo::StreamsChannel";
const HISTORY_SINCE_PARAM: &str = "history_since";

/// An SSE operation failed (Go `error`), compared by message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

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
        out.push_str(&format!("path = {}\n", cabby_core::toml_config::toml_quote(&self.path)));
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

fn log(level: Level, msg: &str, sid: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 2);
    all.push(Attr::new("sid", LogValue::String(sid.to_owned())));
    all.push(Attr::new("transport", LogValue::String("sse".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

fn error_attr(e: impl std::fmt::Display) -> Attr {
    Attr::new("error", LogValue::String(e.to_string()))
}

// ------------------------------------------------------------------ encoder

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

/// Formats a payload as one or more SSE `data:` fields: every line terminator
/// (CRLF, CR or LF) starts a new field (upstream `encodeSSEData`).
fn encode_sse_data(payload: &str) -> String {
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(b"data: ");
    write_sse_data(&mut out, payload.as_bytes());
    // Only whole lines of the payload were copied, so it is still UTF-8.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Appends the value of a `data:` field: every line terminator (CRLF, CR or LF)
/// in `data` starts a new `data: ` field, so an SSE client joins the lines with
/// LF and gets the data back with its terminators as LF.
pub(crate) fn write_sse_data(out: &mut Vec<u8>, data: &[u8]) {
    let mut rest = data;
    while let Some(pos) = rest.iter().position(|b| matches!(b, b'\r' | b'\n')) {
        out.extend_from_slice(&rest[..pos]);
        out.extend_from_slice(b"\ndata: ");
        let len = if rest[pos..].starts_with(b"\r\n") { 2 } else { 1 };
        rest = &rest[pos + len..];
    }
    out.extend_from_slice(rest);
}

/// Whether a value can be written as one SSE field (`event:`, `id:`): it has
/// no line terminator (D-098).
fn single_line(value: &str) -> bool {
    !value.contains(['\r', '\n'])
}

fn text_frame(payload: String) -> SentFrame {
    SentFrame::text(payload.into_bytes())
}

impl Encoder {
    /// Upstream `Encode`: `Ok(None)` is a nil frame (protocol events in raw mode).
    fn encode_message(&self, msg: &dyn EncodedMessageJson) -> Option<SentFrame> {
        let msg_type = msg.get_type();

        // Raw mode doesn't expose protocol events, so comments are used for keepalive.
        if msg_type == PING_TYPE && (self.raw_data || self.comment_pings) {
            return Some(text_frame(COMMENT_PING.to_owned()));
        }

        let reply = msg.as_reply();

        let mut payload = match reply {
            Some(r) if r.kind.is_empty() && self.unwrap_data => {
                let data = match &r.message {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    other => cabby_core::gojson::to_string(other),
                };
                encode_sse_data(&data)
            }
            _ => {
                let mut json = Vec::with_capacity(128);
                msg.write_json(&mut json);
                encode_sse_data(&String::from_utf8_lossy(&json))
            }
        };

        if !msg_type.is_empty() {
            if self.raw_data {
                return None;
            }
            // A type with a line terminator cannot be an event name: the event is
            // sent without one (D-098).
            if single_line(&msg_type) {
                payload = format!("event: {msg_type}\n{payload}");
            }
        }

        // Neither can such an id; the client keeps its last one (D-098).
        if let Some(r) = reply
            && r.offset > 0
            && !r.epoch.is_empty()
            && !r.stream_id.is_empty()
            && single_line(&r.epoch)
            && single_line(&r.stream_id)
        {
            payload.push_str(&format!(
                "\nid: {}{LAST_ID_DELIMITER}{}{LAST_ID_DELIMITER}{}",
                r.offset, r.epoch, r.stream_id
            ));
        }

        if msg_type == "disconnect"
            && let Some(d) = msg.as_disconnect()
            && !d.reconnect
        {
            payload.push_str(&format!("\nretry: {RETRY_NO_RECONNECT}"));
        }

        Some(text_frame(payload))
    }
}

impl NodeEncoder for Encoder {
    fn id(&self) -> String {
        self.id_str().into_owned()
    }

    #[inline]
    fn id_str(&self) -> std::borrow::Cow<'_, str> {
        let s = match (self.unwrap_data, self.raw_data, self.comment_pings) {
            (false, false, false) => SSE_ENCODER_ID,
            (true, false, false) => "sse:u",
            (false, true, false) => "sse:r",
            (false, false, true) => "sse:c",
            (true, true, false) => "sse:u:r",
            (true, false, true) => "sse:u:c",
            (false, true, true) => "sse:r:c",
            (true, true, true) => "sse:u:r:c",
        };
        std::borrow::Cow::Borrowed(s)
    }

    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        self.encode_optional(msg)?.ok_or_else(no_frame)
    }

    fn encode_transmission(&self, raw: &str) -> Result<SentFrame, EncodeError> {
        self.encode_transmission_optional(raw)?.ok_or_else(no_frame)
    }

    fn decode(&self, _raw: &[u8]) -> Result<Message, EncodeError> {
        Err(EncodeError("unsupported".to_owned()))
    }

    fn encode_optional(&self, msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        Ok(self.encode_message(msg))
    }

    /// The transmission is decoded as a `common.Reply` (so its `message` numbers
    /// are `float64`, as upstream) and encoded.
    fn encode_transmission_optional(&self, raw: &str) -> Result<Option<SentFrame>, EncodeError> {
        let reply = cabby_core::common::Reply::from_json(raw.as_bytes()).map_err(|e| EncodeError(e.0))?;
        Ok(self.encode_message(&reply))
    }
}

// ------------------------------------------------------------------ connection

#[derive(Default)]
struct ConnState {
    done: bool,
    established: bool,
    /// Messages written before the response started.
    backlog: Vec<u8>,
}

/// A server-to-client event stream over an HTTP response (upstream `sse.Connection`).
/// Messages written before [`Connection::established`] are buffered.
pub struct Connection {
    writer: SharedResponseWriter,
    state: Mutex<ConnState>,
    ctx: CancellationToken,
}

impl Connection {
    /// Upstream `NewConnection(w)`.
    pub fn new(writer: Box<dyn ResponseWriter>) -> Self {
        Self::with_writer(shared_writer(writer))
    }

    /// A connection writing to a writer the handler shares.
    pub fn with_writer(writer: SharedResponseWriter) -> Self {
        Self { writer, state: Mutex::new(ConnState::default()), ctx: CancellationToken::new() }
    }

    fn state(&self) -> MutexGuard<'_, ConnState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Unsupported for SSE.
    pub fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError("unsupported".to_owned()))
    }

    /// Writes one event (the message and a blank line) and flushes; buffered until
    /// the response is established, dropped once closed.
    pub fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let mut state = self.state();
        if state.done {
            return Ok(());
        }
        if !state.established {
            state.backlog.extend_from_slice(msg);
            state.backlog.extend_from_slice(b"\n\n");
            return Ok(());
        }
        let mut w = lock_writer(&self.writer);
        w.write(msg).map_err(|e| ConnectionError(e.to_string()))?;
        w.write(b"\n\n").map_err(|e| ConnectionError(e.to_string()))?;
        w.flush();
        Ok(())
    }

    /// Unsupported for SSE.
    pub fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        Err(ConnectionError("unsupported".to_owned()))
    }

    /// Pings are encoded messages; native pings are no-ops.
    pub fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }

    /// Cancelled when the connection is closed (upstream `Context()`).
    pub fn context(&self) -> CancellationToken {
        self.ctx.clone()
    }

    /// Marks the connection closed: later writes are dropped.
    pub fn close(&self, _code: i32, _reason: &str) {
        let mut state = self.state();
        if state.done {
            return;
        }
        state.done = true;
        self.ctx.cancel();
    }

    /// Whether the connection was closed (upstream unexported `done` field).
    pub fn is_done(&self) -> bool {
        self.state().done
    }

    /// Marks the response as started and flushes buffered messages.
    pub fn established(&self) {
        let mut state = self.state();
        state.established = true;
        if !state.backlog.is_empty() {
            let backlog = std::mem::take(&mut state.backlog);
            let mut w = lock_writer(&self.writer);
            // Upstream ignores this error too: the next write reports a broken response.
            let _ = w.write(&backlog);
            w.flush();
        }
    }

    /// The underlying network connection, if any (upstream `Descriptor()`): none.
    pub fn descriptor(&self) -> Option<std::net::TcpStream> {
        None
    }
}

impl node::Connection for Connection {
    fn write(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        Connection::write(self, msg, deadline)
    }

    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        Connection::write_binary(self, msg, deadline)
    }

    fn write_ping(&self, deadline: Instant) -> Result<(), ConnectionError> {
        Connection::write_ping(self, deadline)
    }

    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        Connection::read(self)
    }

    fn close(&self, code: i32, reason: &str) {
        Connection::close(self, code, reason);
    }
}

// ------------------------------------------------------------------ requests

/// A request as seen by the handler (method, URI, headers, body). HTTP/1.x requests
/// get a `Connection: keep-alive` response header. A [`ConnInfo`] extension, when
/// present, gives the peer address and TLS state.
pub type Request = http::Request<Vec<u8>>;

/// The request as the server's request inspection sees it.
fn http_request(req: &Request) -> HttpRequest {
    let (mut parts, ()) = http::Request::new(()).into_parts();
    parts.method = req.method().clone();
    parts.uri = req.uri().clone();
    parts.version = req.version();
    parts.headers = req.headers().clone();
    let conn = req.extensions().get::<ConnInfo>().copied().unwrap_or_default();
    HttpRequest::from_parts(&parts, &conn)
}

/// Go `r.URL.Query().Get(key)`.
fn param(req: &Request, key: &str) -> String {
    let target = req.uri().path_and_query().map_or("", |pq| pq.as_str());
    query_param(target, key)
}

/// Go `r.Header.Get(name)`: the value's bytes as they arrived. Values that are
/// not ASCII (an `EventSource` sends the last event id of a non-ASCII stream as
/// UTF-8) are kept; invalid UTF-8 is converted lossily.
fn header<'a>(req: &'a Request, name: &str) -> std::borrow::Cow<'a, str> {
    req.headers().get(name).map_or(std::borrow::Cow::Borrowed(""), |v| String::from_utf8_lossy(v.as_bytes()))
}

/// `map[string]string{...}` as Go's `utils.ToJSON` writes it (sorted keys).
fn identifier_json(pairs: &[(&str, &str)]) -> String {
    let map: std::collections::BTreeMap<&str, &str> = pairs.iter().copied().collect();
    cabby_core::gojson::to_string(&map)
}

/// The subscribe command a GET request names, if any (upstream
/// `subscribeCommandFromGetRequest`).
fn subscribe_command_from_get_request(req: &Request) -> Result<Option<Message>, Error> {
    let mut identifier = param(req, "identifier");

    if identifier.is_empty() {
        let channel = param(req, "channel");
        if !channel.is_empty() {
            identifier = identifier_json(&[("channel", &channel)]);
        }
    }

    // A public stream name.
    if identifier.is_empty() {
        let stream = param(req, PUBLIC_STREAM_PARAM);
        if !stream.is_empty() {
            identifier = identifier_json(&[("channel", SIGNED_STREAM_CHANNEL), ("stream_name", &stream)]);
        }
    }

    // A signed stream name.
    if identifier.is_empty() {
        let stream = param(req, SIGNED_STREAM_PARAM);
        if !stream.is_empty() {
            identifier = identifier_json(&[("channel", SIGNED_STREAM_CHANNEL), ("signed_stream_name", &stream)]);
        }
    }

    // A Turbo Streams name.
    if identifier.is_empty() {
        let stream = param(req, TURBO_STREAMS_PARAM);
        if !stream.is_empty() {
            identifier = identifier_json(&[("channel", TURBO_STREAMS_CHANNEL), ("signed_stream_name", &stream)]);
        }
    }

    if identifier.is_empty() {
        return Ok(None);
    }

    let mut msg = Message { command: "subscribe".to_owned(), identifier, ..Default::default() };

    let last_id = header(req, "last-event-id");
    if !last_id.is_empty() {
        let parts: Vec<&str> = last_id.splitn(3, LAST_ID_DELIMITER).collect();
        if let [offset, epoch, stream] = parts[..] {
            let offset: u64 = offset.parse().map_err(|e: std::num::ParseIntError| {
                Error(format!(
                    "failed to parse last event id: {last_id}, cause: {}",
                    go_parse_error("ParseUint", offset, &e)
                ))
            })?;
            let mut streams = std::collections::HashMap::new();
            streams.insert(stream.to_owned(), HistoryPosition { offset, epoch: epoch.to_owned() });
            msg.history = HistoryRequest { streams, ..Default::default() };
        }
    }

    let since = param(req, HISTORY_SINCE_PARAM);
    if !since.is_empty() {
        let since_int: i64 = since.parse().map_err(|e: std::num::ParseIntError| {
            Error(format!(
                "failed to parse history since value: {since}, cause: {}",
                go_parse_error("ParseInt", &since, &e)
            ))
        })?;
        msg.history.since = since_int;
    }

    Ok(Some(msg))
}

/// Go's `strconv` error text for a failed integer parse.
fn go_parse_error(func: &str, input: &str, e: &std::num::ParseIntError) -> String {
    let reason = match e.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => "value out of range",
        _ => "invalid syntax",
    };
    format!("strconv.{func}: parsing {input:?}: {reason}")
}

/// A body over [`DEFAULT_MAX_BODY_SIZE`] (Go `http.MaxBytesError`).
fn body_too_large() -> Error {
    Error(server::BodyError::TooLarge(DEFAULT_MAX_BODY_SIZE as u64).to_string())
}

/// The commands of a POST body: one JSON command per non-empty line (upstream
/// `subscribeCommandFromPostRequest`).
fn subscribe_commands_from_post_body(body: &[u8]) -> Result<Vec<Message>, Error> {
    if body.len() > DEFAULT_MAX_BODY_SIZE {
        return Err(body_too_large());
    }
    let mut cmds = Vec::new();
    for line in body.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let cmd = Message::from_json(line).map_err(|e| Error(format!("failed to parse command, cause: {e}")))?;
        cmds.push(cmd);
    }
    Ok(cmds)
}

/// What the request line and headers say about the subscribe commands (upstream
/// `subscribeCommandsFromRequest` without the body): the command of a GET, and
/// `None` for a POST, whose body has them. A POST whose `Content-Length` is over
/// [`DEFAULT_MAX_BODY_SIZE`] fails as reading its body would (D-096).
fn subscribe_commands_from_head(req: &Request) -> Result<Option<Vec<Message>>, Error> {
    if req.method() == http::Method::GET {
        return match subscribe_command_from_get_request(req)? {
            Some(cmd) => Ok(Some(vec![cmd])),
            None => Err(Error("no channel provided".to_owned())),
        };
    }
    let announced = req.headers().get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    if announced.is_some_and(|len| len > DEFAULT_MAX_BODY_SIZE as u64) {
        return Err(body_too_large());
    }
    Ok(None)
}

/// An authenticated session and its connection.
type SseSession = (Arc<Session>, Arc<Connection>);

/// Builds and authenticates the session of a request (upstream `NewSSESession`):
/// `Ok(None)` when authentication was rejected.
fn new_sse_session(
    n: &Arc<Node>,
    c: &Config,
    writer: &SharedResponseWriter,
    req: &Request,
    info: &RequestInfo,
) -> Result<Option<SseSession>, Error> {
    let conn = Arc::new(Connection::with_writer(writer.clone()));

    let unwrap_data = req.method() == http::Method::GET;
    let raw_param = param(req, "raw").to_lowercase();
    let raw_data = matches!(raw_param.as_str(), "true" | "1" | "t" | "y");

    let comment_pings = raw_data || c.comment_pings;

    let enc = Encoder { unwrap_data, raw_data, comment_pings: c.comment_pings };

    let mut opts = vec![SessionOption::Encoder(Arc::new(enc))];

    if comment_pings && c.ping_interval > 0 {
        opts.push(SessionOption::PingInterval(Duration::from_secs(c.ping_interval as u64)));
    }

    let session = Session::new(n, conn.clone(), &info.url, info.headers.clone(), &info.uid, opts);
    let res = n.authenticate(&session, &[]).map_err(|e| Error(e.0))?;

    match res {
        Some(res) if res.status == Status::Success => Ok(Some((session, conn))),
        // Rejected, or the session closed while authenticating (Go would get a
        // nil result here).
        _ => Ok(None),
    }
}

// ------------------------------------------------------------------ handler

/// Headers extractor shared with the handler's threads.
pub type SharedHeadersExtractor = Arc<dyn HeadersExtractor + Send + Sync>;

/// The SSE endpoint (upstream `SSEHandler`).
pub struct Handler {
    node: Arc<Node>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: Config,
    allowed_hosts: Vec<String>,
    /// How long reading a POST body may take ([`BODY_READ_TIMEOUT`]; shorter in tests).
    body_timeout: Duration,
}

/// Builds the SSE endpoint (upstream `SSEHandler(n, m, shutdownCtx,
/// headersExtractor, config, l)`). `shutdown` is cancelled on server shutdown
/// (upstream `ShutdownCtx`); open responses then get a `server_restart`
/// disconnect message.
pub fn sse_handler(
    node: Arc<Node>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: &Config,
) -> Handler {
    let allowed_hosts = if config.allowed_origins.is_empty() {
        Vec::new()
    } else {
        config.allowed_origins.split(',').map(str::to_owned).collect()
    };

    if let Some(m) = &metrics {
        m.register_gauge(METRICS_CLIENTS_NUM, "The number of active SSE clients");
    }

    Handler {
        node,
        metrics,
        shutdown,
        extractor,
        config: config.clone(),
        allowed_hosts,
        body_timeout: BODY_READ_TIMEOUT,
    }
}

/// The SSE endpoint as a server handler: mount it at `config.path` (upstream
/// `wsServer.SetupHandler(config.SSE.Path, sseHandler)`).
pub fn handler(
    node: Arc<Node>,
    metrics: Option<Arc<dyn Instrumenter>>,
    shutdown: CancellationToken,
    extractor: Option<SharedHeadersExtractor>,
    config: &Config,
) -> impl server::Handler {
    Endpoint(Arc::new(sse_handler(node, metrics, shutdown, extractor, config)))
}

/// A session being streamed: disconnected from the node when dropped (upstream
/// `defer session.Disconnect("Closed", ws.CloseNormalClosure)`).
struct Active {
    session: Arc<Session>,
    conn: Arc<Connection>,
    metrics: Option<Arc<dyn Instrumenter>>,
}

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(m) = &self.metrics {
            m.gauge_decrement(METRICS_CLIENTS_NUM);
        }
        self.session.disconnect("Closed", CLOSE_NORMAL_CLOSURE);
    }
}

/// A session that failed to start streaming: disconnected when dropped.
struct SessionGuard(Option<Arc<Session>>);

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            s.disconnect("Closed", CLOSE_NORMAL_CLOSURE);
        }
    }
}

/// A signed-in session whose subscriptions are still to be made: disconnected
/// when dropped (a blocking node call).
struct Started {
    session: Arc<Session>,
    conn: Arc<Connection>,
    sid: String,
    /// The commands of a GET; `None` for a POST, whose body has them.
    cmds: Option<Vec<Message>>,
    guard: SessionGuard,
}

/// Runs a blocking node call off the async executor.
async fn blocking(f: impl FnOnce() + Send + 'static) {
    let _ = tokio::task::spawn_blocking(f).await;
}

impl Handler {
    /// Serves one request until it completes, the client goes away (`request_done`
    /// is cancelled) or the session is closed; the writer is finished on return.
    /// Blocks the calling thread (which must not be a Tokio worker); the server
    /// uses [`handler`] instead.
    pub fn serve(&self, req: Request, w: Box<dyn ResponseWriter>, request_done: &CancellationToken) {
        let w = shared_writer(w);
        if let Some(active) = self.prepare(&req, &w) {
            server::runtime_handle().block_on(self.wait(&active, request_done));
            drop(active);
        }
        lock_writer(&w).finish();
    }

    /// [`Handler::serve`] for the async server: blocking node calls run on Tokio's
    /// blocking pool, the body read and the wait are async.
    async fn serve_async(
        self: Arc<Self>,
        req: Request,
        body: Incoming,
        w: Box<dyn ResponseWriter>,
        request_done: CancellationToken,
    ) {
        let w = shared_writer(w);
        match self.clone().prepare_async(req, body, w.clone()).await {
            Ok(Some(active)) => {
                self.wait(&active, &request_done).await;
                blocking(move || drop(active)).await;
            }
            Ok(None) => {}
            Err(_) => lock_writer(&w).write_header(500),
        }
        lock_writer(&w).finish();
    }

    /// Everything up to streaming for a request whose body is in memory: the
    /// checks of [`Handler::start`], the commands, the subscriptions. `None` when
    /// the response is complete.
    fn prepare(&self, req: &Request, w: &SharedResponseWriter) -> Option<Active> {
        let mut started = self.start(req, w)?;
        let cmds = match started.cmds.take() {
            Some(cmds) => cmds,
            None => self.body_commands(&started.sid, subscribe_commands_from_post_body(req.body()), w)?,
        };
        self.subscribe(started, &cmds, w)
    }

    /// [`Handler::prepare`] on the server: the body of a POST is read once the
    /// session is signed in, at most [`DEFAULT_MAX_BODY_SIZE`] bytes within the
    /// body timeout (D-097); other bodies, and those of requests answered before,
    /// are not read. `Err` when a blocking step panicked.
    async fn prepare_async(
        self: Arc<Self>,
        req: Request,
        body: Incoming,
        w: SharedResponseWriter,
    ) -> Result<Option<Active>, tokio::task::JoinError> {
        let (this, writer) = (self.clone(), w.clone());
        let started = tokio::task::spawn_blocking(move || this.start(&req, &writer)).await?;
        // The body of a request answered by now, or of a GET, is not read. It is
        // dropped only here, so a body that has arrived meanwhile is drained by
        // hyper and the connection stays open.
        let Some(mut started) = started else { return Ok(None) };
        let cmds = match started.cmds.take() {
            Some(cmds) => {
                drop(body);
                Ok(cmds)
            }
            None => self.read_commands(body).await,
        };
        // Leaving out the session (answered 400) disconnects it: a blocking call too.
        tokio::task::spawn_blocking(move || {
            let cmds = self.body_commands(&started.sid, cmds, &w)?;
            self.subscribe(started, &cmds, &w)
        })
        .await
    }

    /// The commands of a POST body, read with the size limit and the body timeout.
    async fn read_commands(&self, body: Incoming) -> Result<Vec<Message>, Error> {
        match server::read_body_within(body, Some(DEFAULT_MAX_BODY_SIZE as u64), self.body_timeout).await {
            Ok(body) => subscribe_commands_from_post_body(&body),
            Err(e) => Err(Error(e.to_string())),
        }
    }

    /// The commands of a POST body, or `None` once a failure to get them is
    /// answered (400).
    fn body_commands(
        &self,
        sid: &str,
        cmds: Result<Vec<Message>, Error>,
        w: &SharedResponseWriter,
    ) -> Option<Vec<Message>> {
        match cmds {
            Ok(cmds) => Some(cmds),
            Err(e) => {
                log(Level::Error, "failed to build subscribe command", sid, &[error_attr(e)]);
                lock_writer(w).write_header(400);
                None
            }
        }
    }

    /// The checks and the sign-in, from the request line and headers alone
    /// (D-096): preflight, method, response headers, request id, the commands of
    /// a GET (or the announced size of a POST body), authentication. `None` when
    /// the response is complete.
    fn start(&self, req: &Request, w: &SharedResponseWriter) -> Option<Started> {
        let method = req.method();
        {
            let mut w = lock_writer(w);
            write_cors_headers(w.headers_mut(), &header(req, "Origin"), &self.allowed_hosts);

            // Respond to preflight requests.
            if method == http::Method::OPTIONS {
                w.write_header(200);
                return None;
            }

            // SSE only supports GET and POST requests.
            if method != http::Method::GET && method != http::Method::POST {
                w.write_header(405);
                return None;
            }

            let h = w.headers_mut();
            let set = |h: &mut http::HeaderMap, name: &'static str, value: &str| {
                if let Ok(v) = http::HeaderValue::from_str(value) {
                    h.insert(name, v);
                }
            };
            set(h, "x-anycable-version", crate::version::ANYCABLE_COMPAT);
            if matches!(req.version(), http::Version::HTTP_10 | http::Version::HTTP_11 | http::Version::HTTP_09) {
                // An endpoint MUST NOT generate an HTTP/2 message containing
                // connection-specific header fields (RFC 7540).
                set(h, "connection", "keep-alive");
            }
            set(h, "content-type", "text/event-stream; charset=utf-8");
            set(h, "x-content-type-options", "nosniff");
            set(h, "x-accel-buffering", "no");
            set(h, "cache-control", "private, no-cache, no-store, must-revalidate, max-age=0");
            set(h, "pragma", "no-cache");
            set(h, "expire", "0");
        }

        let status = |code: u16| lock_writer(w).write_header(code);

        let extractor = self.extractor.as_deref().map(|e| e as &dyn HeadersExtractor);
        let info = match RequestInfo::new(&http_request(req), extractor) {
            Ok(info) => info,
            Err(_) => {
                status(400);
                return None;
            }
        };
        let sid = info.uid.clone();

        let cmds = match subscribe_commands_from_head(req) {
            Ok(cmds) => cmds,
            Err(e) => {
                log(Level::Error, "failed to build subscribe command", &sid, &[error_attr(e)]);
                status(400);
                return None;
            }
        };

        // Finally, a session can be established.
        let (session, conn) = match new_sse_session(&self.node, &self.config, w, req, &info) {
            Ok(Some(s)) => s,
            Ok(None) => {
                log(Level::Error, "authentication failed", &sid, &[]);
                status(401);
                return None;
            }
            Err(e) => {
                log(Level::Error, "failed to establish sesssion", &sid, &[error_attr(e)]);
                status(400);
                return None;
            }
        };

        // The session leaves the node when done, also when a subscription fails.
        let guard = SessionGuard(Some(session.clone()));
        Some(Started { session, conn, sid, cmds, guard })
    }

    /// Subscribes a started session and starts the response. `None` when the
    /// response is complete.
    fn subscribe(&self, started: Started, cmds: &[Message], w: &SharedResponseWriter) -> Option<Active> {
        let Started { session, conn, sid, mut guard, .. } = started;
        let status = |code: u16| lock_writer(w).write_header(code);

        for cmd in cmds {
            match self.node.subscribe(&session, cmd) {
                Ok(Some(res)) if res.status == Status::Success => {}
                Ok(Some(_)) => {
                    log(Level::Debug, "subscription rejected", &sid, &[]);
                    status(400);
                    return None;
                }
                Ok(None) => {
                    log(
                        Level::Error,
                        "failed to subscribe",
                        &sid,
                        &[Attr::new("error", LogValue::String("<nil>".into()))],
                    );
                    status(400);
                    return None;
                }
                Err(e) => {
                    log(Level::Error, "failed to subscribe", &sid, &[error_attr(e)]);
                    status(400);
                    return None;
                }
            }
        }

        {
            let mut w = lock_writer(w);
            w.write_header(200);
            w.flush();
        }

        conn.established();
        log(Level::Debug, "session established", &sid, &[]);

        if let Some(m) = &self.metrics {
            m.gauge_increment(METRICS_CLIENTS_NUM);
        }
        guard.0 = None;
        Some(Active { session, conn, metrics: self.metrics.clone() })
    }

    /// Streams until the request ends or the session closes; on server shutdown
    /// the client gets a `server_restart` disconnect message first.
    async fn wait(&self, active: &Active, request_done: &CancellationToken) {
        let conn_done = active.conn.context();
        let sid = active.session.get_id();
        let mut shutdown_received = false;
        loop {
            tokio::select! {
                () = self.shutdown.cancelled(), if !shutdown_received => {
                    shutdown_received = true;
                    log(Level::Debug, "server shutdown", &sid, &[]);
                    let session = active.session.clone();
                    blocking(move || {
                        session.disconnect_with_message(
                            &DisconnectMessage::new(SERVER_RESTART_REASON, true),
                            SERVER_RESTART_REASON,
                        );
                    })
                    .await;
                }
                () = request_done.cancelled() => {
                    log(Level::Debug, "request terminated", &sid, &[]);
                    let session = active.session.clone();
                    blocking(move || session.disconnect_now("Closed", CLOSE_NORMAL_CLOSURE)).await;
                    return;
                }
                () = conn_done.cancelled() => {
                    log(Level::Debug, "session completed", &sid, &[]);
                    return;
                }
            }
        }
    }
}

/// [`Handler`] on the HTTP server: the body of a POST (its commands) is read once
/// the request is signed in, up to [`DEFAULT_MAX_BODY_SIZE`] and within the body
/// timeout (a larger, unreadable or late body is answered 400, as upstream's
/// `MaxBytesReader` and read errors are); other bodies are never read. The
/// response streams through a
/// [`ChannelResponseWriter`](crate::server::ChannelResponseWriter), and the client
/// going away ends the request.
struct Endpoint(Arc<Handler>);

impl server::Handler for Endpoint {
    fn call(&self, req: server::Request) -> ResponseFuture {
        let handler = self.0.clone();
        let (writer, client_gone, response) = server::channel_response_for(&req);
        let conn = server::conn_info(&req);
        let (parts, body) = req.into_parts();
        let mut req = http::Request::from_parts(parts, Vec::new());
        req.extensions_mut().insert(conn);
        server::runtime_handle().spawn(handler.serve_async(req, body, Box::new(writer), client_gone));
        Box::pin(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_lines_split_on_every_terminator() {
        assert_eq!("data: a\ndata: b\ndata: c\ndata: d", encode_sse_data("a\r\nb\rc\nd"));
        assert_eq!("data: ", encode_sse_data(""));
        assert_eq!("data: x\ndata: ", encode_sse_data("x\r"));
    }

    #[test]
    fn get_identifiers_are_go_json_maps() {
        assert_eq!(r#"{"channel":"room_1"}"#, identifier_json(&[("channel", "room_1")]));
        assert_eq!(
            r#"{"channel":"$pubsub","stream_name":"chat_1"}"#,
            identifier_json(&[("channel", "$pubsub"), ("stream_name", "chat_1")])
        );
    }

    /// Accepts every subscription (`streams` is the stream of every
    /// subscription) and every connection, or none; counts sign-ins.
    struct App {
        sign_in: bool,
        sign_ins: std::sync::atomic::AtomicUsize,
    }

    impl App {
        fn new(sign_in: bool) -> Self {
            Self { sign_in, sign_ins: std::sync::atomic::AtomicUsize::new(0) }
        }
    }

    impl cabby_core::common::Controller for App {
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
        ) -> cabby_core::common::Outcome<cabby_core::common::ConnectResult> {
            self.sign_ins.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = if self.sign_in {
                cabby_core::common::ConnectResult {
                    identifier: "ids".into(),
                    transmissions: vec![r#"{"type":"welcome"}"#.into()],
                    ..Default::default()
                }
            } else {
                cabby_core::common::ConnectResult { status: Status::Failure, ..Default::default() }
            };
            cabby_core::common::Outcome { result: Some(result), error: None }
        }
        fn subscribe(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
        ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
            cabby_core::common::Outcome {
                result: Some(cabby_core::common::CommandResult {
                    streams: vec!["s1".into()],
                    transmissions: vec![r#"{"type":"confirm_subscription"}"#.into()],
                    ..Default::default()
                }),
                error: None,
            }
        }
        fn unsubscribe(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
        ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
            cabby_core::common::Outcome { result: None, error: None }
        }
        fn perform(
            &self,
            _sid: &str,
            _env: Option<&cabby_core::common::SessionEnv>,
            _ids: &str,
            _channel: &str,
            _data: &str,
        ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
            cabby_core::common::Outcome { result: None, error: None }
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

    /// The mounted endpoint over a real socket: headers on an early 400, then a
    /// streamed response with protocol events, a broadcast, and cleanup when the
    /// client goes away.
    #[test]
    fn serves_on_the_http_server() {
        use std::io::{BufRead, BufReader, Write};

        let node = Node::new(node::Config::new(), vec![node::NodeOption::Controller(Arc::new(App::new(true)))]);
        let subscriber = Arc::new(node::LegacySubscriber::new(node.clone()));
        node.set_broker(Arc::new(cabby_core::broker::Legacy::new(subscriber)));
        node.start().unwrap();

        let srv = server::HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
        srv.setup_handler("/events", handler(node.clone(), None, srv.shutdown_token(), None, &Config::new()));
        srv.start().unwrap();
        let addr = srv.local_addr().unwrap();

        let open = |target: &str| {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            write!(s, "GET {target} HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
            let mut r = BufReader::new(s);
            let mut head = Vec::new();
            loop {
                let mut line = String::new();
                r.read_line(&mut line).unwrap();
                if line.trim_end().is_empty() {
                    break;
                }
                head.push(line.trim_end().to_lowercase());
            }
            (head, r)
        };

        let (head, _) = open("/events");
        assert!(head[0].starts_with("http/1.1 400"), "{head:?}");
        assert!(head.contains(&"connection: keep-alive".to_owned()), "{head:?}");
        assert!(head.contains(&"content-type: text/event-stream; charset=utf-8".to_owned()), "{head:?}");

        let (head, mut r) = open("/events?stream=s1");
        assert!(head[0].starts_with("http/1.1 200"), "{head:?}");
        let mut read_until = |needle: &str| {
            let mut seen = String::new();
            while !seen.contains(needle) {
                let mut line = String::new();
                assert!(r.read_line(&mut line).unwrap() > 0, "EOF before {needle:?}: {seen}");
                seen.push_str(&line);
            }
        };
        read_until("event: welcome\ndata: {\"type\":\"welcome\"}");
        read_until("event: confirm_subscription");
        let deadline = Instant::now() + Duration::from_secs(5);
        while node.size() != 1 {
            assert!(Instant::now() < deadline, "session not registered");
            std::thread::sleep(Duration::from_millis(10));
        }
        node.broadcast(&cabby_core::common::StreamMessage {
            stream: "s1".into(),
            data: r#"{"a":1}"#.into(),
            ..Default::default()
        });
        read_until(r#"data: {"a":1}"#);
        drop(r);

        // The client is gone: the session leaves once the next write fails or the
        // server shuts down.
        srv.shutdown_token().cancel();
        let deadline = Instant::now() + Duration::from_secs(5);
        while node.size() != 0 {
            assert!(Instant::now() < deadline, "session not removed");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn get(target: &str, headers: &[(&str, &str)]) -> Request {
        let mut b = http::Request::builder().method("GET").uri(target);
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        b.body(Vec::new()).unwrap()
    }

    #[test]
    fn last_event_id_and_history_since() {
        let req = get("/?channel=room_1&history_since=1700", &[("Last-Event-ID", "12/ep/stream/x")]);
        let cmd = subscribe_command_from_get_request(&req).unwrap().unwrap();
        assert_eq!(r#"{"channel":"room_1"}"#, cmd.identifier);
        assert_eq!(1700, cmd.history.since);
        let pos = &cmd.history.streams["stream/x"];
        assert_eq!((12, "ep"), (pos.offset, pos.epoch.as_str()));

        let req = get("/?channel=room_1", &[("Last-Event-ID", "x/ep/stream")]);
        assert!(subscribe_command_from_get_request(&req).is_err());

        let req = get("/?channel=room_1", &[("Last-Event-ID", "2/ep")]);
        let cmd = subscribe_command_from_get_request(&req).unwrap().unwrap();
        assert!(cmd.history.streams.is_empty());

        let req = get("/?channel=room_1&channel=room_2", &[]);
        let cmd = subscribe_command_from_get_request(&req).unwrap().unwrap();
        assert_eq!(r#"{"channel":"room_1"}"#, cmd.identifier);

        let req = get("/", &[]);
        assert!(subscribe_command_from_get_request(&req).unwrap().is_none());
    }

    /// M6 (independent review): an `EventSource` sends the last event id of a
    /// non-ASCII stream as raw UTF-8; Go reads header values as bytes, so the
    /// history position is kept. Before, such a value was ignored.
    #[test]
    fn last_event_id_of_a_non_ascii_stream() {
        let mut req = get("/?stream=caf%C3%A9", &[]);
        req.headers_mut().insert("last-event-id", http::HeaderValue::from_bytes("1/EPOCH/café".as_bytes()).unwrap());
        let cmd = subscribe_command_from_get_request(&req).unwrap().unwrap();
        let pos = &cmd.history.streams["café"];
        assert_eq!((1, "EPOCH"), (pos.offset, pos.epoch.as_str()));
    }

    /// A type, stream or epoch with a line terminator cannot be written as one
    /// field: the event has no `event:` or no `id:` field, and its data (where
    /// the JSON escapes the terminator) stays one `data:` field.
    #[test]
    fn deviation_d098_event_names_and_ids_are_single_lines() {
        use cabby_core::common::Reply;
        let frame = |r: &Reply| String::from_utf8(Encoder::default().encode(r).unwrap().payload.to_vec()).unwrap();
        let data = |r: &Reply| {
            let mut json = Vec::new();
            r.write_json(&mut json);
            format!("data: {}", String::from_utf8(json).unwrap())
        };

        let typed = |kind: &str| Reply { kind: kind.into(), ..Default::default() };
        assert_eq!("event: confirm\ndata: {\"type\":\"confirm\"}", frame(&typed("confirm")));
        for kind in ["a\nb", "a\rb", "a\r\nb", "x\n\ndata: y"] {
            let r = typed(kind);
            assert_eq!(data(&r), frame(&r), "{kind:?}");
            assert!(!frame(&r).contains(['\r', '\n']), "{kind:?}");
        }

        let stream = |stream_id: &str, epoch: &str| Reply {
            identifier: "c".into(),
            message: Some(serde_json::json!(1)),
            stream_id: stream_id.into(),
            epoch: epoch.into(),
            offset: 2,
            ..Default::default()
        };
        let r = stream("s", "ep");
        assert_eq!(format!("{}\nid: 2/ep/s", data(&r)), frame(&r));
        for (stream_id, epoch) in [("s\nx", "ep"), ("s\r\ndata: x", "ep"), ("s", "e\rp")] {
            let r = stream(stream_id, epoch);
            assert_eq!(data(&r), frame(&r), "{stream_id:?} {epoch:?}");
            assert!(!frame(&r).contains(['\r', '\n']), "{stream_id:?} {epoch:?}");
        }
    }

    /// The endpoint on a server bound to port 0 over a node with an [`App`]; POST
    /// bodies may take `body_timeout`.
    struct Served {
        node: Arc<Node>,
        app: Arc<App>,
        srv: Arc<server::HttpServer>,
        addr: std::net::SocketAddr,
    }

    impl Served {
        fn new(app: App, config: &Config, body_timeout: Duration) -> Self {
            let app = Arc::new(app);
            let node = Node::new(node::Config::new(), vec![node::NodeOption::Controller(app.clone())]);
            let subscriber = Arc::new(node::LegacySubscriber::new(node.clone()));
            node.set_broker(Arc::new(cabby_core::broker::Legacy::new(subscriber)));
            node.start().unwrap();
            let srv = server::HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
            let mut h = sse_handler(node.clone(), None, srv.shutdown_token(), None, config);
            h.body_timeout = body_timeout;
            srv.setup_handler("/events", Endpoint(Arc::new(h)));
            let addr = crate::server::tests::support::start(&srv);
            Self { node, app, srv, addr }
        }

        fn signing_in(sign_in: bool) -> Self {
            Self::new(App::new(sign_in), &Config::new(), BODY_READ_TIMEOUT)
        }

        /// Sends a request head with `Host` and the given lines, and nothing else.
        fn send(&self, head: &str) -> (std::net::TcpStream, Instant) {
            use std::io::Write;
            let mut stream = crate::server::tests::support::connect(self.addr);
            write!(stream, "{head}\r\nHost: x\r\n\r\n").unwrap();
            (stream, Instant::now())
        }

        fn wait_for_sessions(&self, n: usize) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.node.size() != n {
                assert!(Instant::now() < deadline, "{} sessions, not {n}", self.node.size());
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for Served {
        fn drop(&mut self) {
            self.srv.shutdown();
        }
    }

    /// Well under [`BODY_READ_TIMEOUT`]: an answer that does not wait for a body.
    const PROMPTLY: Duration = Duration::from_secs(1);

    /// Reads the response to a request whose announced body was not sent: it is
    /// the final response (no `100 Continue`: the body was never polled), and it
    /// comes promptly.
    fn answered_without_the_body(
        stream: &mut std::net::TcpStream,
        sent: Instant,
    ) -> crate::server::tests::support::RawResponse {
        let resp = crate::server::tests::support::read_response(stream);
        assert!(sent.elapsed() < PROMPTLY, "answered after {:?}", sent.elapsed());
        assert_ne!(100, resp.status, "the body was polled");
        resp
    }

    /// Reads SSE lines until `needle` has been read.
    fn read_until(r: &mut impl std::io::BufRead, needle: &str) {
        let mut seen = String::new();
        while !seen.contains(needle) {
            let mut line = String::new();
            assert!(r.read_line(&mut line).unwrap() > 0, "EOF before {needle:?}: {seen}");
            seen.push_str(&line);
        }
    }

    /// A request with a method the endpoint rejects is answered without waiting
    /// for the body its headers announce.
    #[test]
    fn deviation_d096_a_rejected_method_is_answered_without_its_body() {
        let endpoint = Served::signing_in(true);
        for method in ["PUT", "DELETE", "PATCH"] {
            let (mut stream, sent) =
                endpoint.send(&format!("{method} /events HTTP/1.1\r\nContent-Length: 10\r\nExpect: 100-continue"));
            assert_eq!(405, answered_without_the_body(&mut stream, sent).status, "{method}");
        }
        assert_eq!(0, endpoint.app.sign_ins.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// The endpoint rejects no origin: a preflight from an origin that is not
    /// allowed is answered 200 without `Access-Control-Allow-Origin`, and without
    /// waiting for the body its headers announce.
    #[test]
    fn deviation_d096_a_preflight_is_answered_without_its_body() {
        let mut config = Config::new();
        config.allowed_origins = "example.com".into();
        let endpoint = Served::new(App::new(true), &config, BODY_READ_TIMEOUT);
        let (mut stream, sent) = endpoint.send(
            "OPTIONS /events HTTP/1.1\r\nOrigin: http://other.example\r\nContent-Length: 10\r\nExpect: 100-continue",
        );
        let resp = answered_without_the_body(&mut stream, sent);
        assert_eq!(200, resp.status);
        assert_eq!(None, resp.header("access-control-allow-origin"));
        assert_eq!(Some("Origin"), resp.header("vary"));
    }

    /// Sign-in needs no body: a failed one is answered 401 without waiting for
    /// the body, for a POST (whose body would have the commands) and a GET.
    #[test]
    fn deviation_d096_a_failed_sign_in_is_answered_without_its_body() {
        let endpoint = Served::signing_in(false);
        for target in ["POST /events", "GET /events?stream=s1"] {
            let (mut stream, sent) =
                endpoint.send(&format!("{target} HTTP/1.1\r\nContent-Length: 10\r\nExpect: 100-continue"));
            let resp = answered_without_the_body(&mut stream, sent);
            assert_eq!(401, resp.status, "{target}");
            assert!(resp.body.is_empty(), "{target}");
        }
        assert_eq!(2, endpoint.app.sign_ins.load(std::sync::atomic::Ordering::SeqCst));
        endpoint.wait_for_sessions(0);
    }

    /// Sign-in comes before the body is read: a POST with commands that cannot
    /// be parsed and a failed sign-in is answered 401 (Go: 400).
    #[test]
    fn deviation_d096_sign_in_is_checked_before_the_body() {
        use std::io::Write;
        let endpoint = Served::signing_in(false);
        let (mut stream, _) = endpoint.send("POST /events HTTP/1.1\r\nContent-Length: 5");
        stream.write_all(b"nope\n").unwrap();
        assert_eq!(401, crate::server::tests::support::read_response(&mut stream).status);
    }

    /// A POST body announced over the limit is answered 400 before sign-in, as Go
    /// answers it once its read of the body fails, and the body is not read.
    #[test]
    fn deviation_d096_a_body_announced_over_the_limit_is_answered_before_sign_in() {
        let endpoint = Served::signing_in(true);
        let (mut stream, sent) = endpoint.send(&format!(
            "POST /events HTTP/1.1\r\nContent-Length: {}\r\nExpect: 100-continue",
            DEFAULT_MAX_BODY_SIZE + 1
        ));
        assert_eq!(400, answered_without_the_body(&mut stream, sent).status);
        assert_eq!(0, endpoint.app.sign_ins.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A GET has its command in the query: its body is never read, and the
    /// response streams although the announced body never arrives.
    #[test]
    fn deviation_d096_a_get_does_not_read_its_body() {
        let endpoint = Served::signing_in(true);
        let (stream, sent) =
            endpoint.send("GET /events?stream=s1 HTTP/1.1\r\nContent-Length: 10\r\nExpect: 100-continue");
        let mut r = std::io::BufReader::new(stream);
        let mut status = String::new();
        std::io::BufRead::read_line(&mut r, &mut status).unwrap();
        assert!(status.starts_with("HTTP/1.1 200"), "{status}");
        read_until(&mut r, "event: confirm_subscription");
        assert!(sent.elapsed() < PROMPTLY, "streaming after {:?}", sent.elapsed());
        endpoint.wait_for_sessions(1);
    }

    /// A signed-in POST whose body stalls is answered 400 when the body timeout
    /// passes, and its session leaves the node.
    #[test]
    fn deviation_d097_a_stalled_body_is_answered_at_the_deadline() {
        use std::io::Write;
        let timeout = Duration::from_millis(300);
        let endpoint = Served::new(App::new(true), &Config::new(), timeout);
        let (mut stream, _) = endpoint.send("POST /events HTTP/1.1\r\nContent-Length: 100\r\nExpect: 100-continue");
        // Signed in: the body is read now.
        assert_eq!(100, crate::server::tests::support::read_response(&mut stream).status);
        let reading = Instant::now();
        stream.write_all(br#"{"command":"#).unwrap();
        let resp = crate::server::tests::support::read_response(&mut stream);
        let elapsed = reading.elapsed();
        assert_eq!(400, resp.status);
        assert!(elapsed >= timeout && elapsed < timeout + PROMPTLY, "answered after {elapsed:?}");
        assert_eq!(1, endpoint.app.sign_ins.load(std::sync::atomic::Ordering::SeqCst));
        endpoint.wait_for_sessions(0);
    }

    /// A body that arrives in parts, all within the body timeout, is read: the
    /// response streams.
    #[test]
    fn deviation_d097_a_slow_body_within_the_deadline_is_served() {
        use std::io::Write;
        let endpoint = Served::new(App::new(true), &Config::new(), Duration::from_millis(900));
        let body = "{\"command\":\"subscribe\",\"identifier\":\"chat_1\"}\n";
        let (mut stream, _) = endpoint.send(&format!("POST /events HTTP/1.1\r\nContent-Length: {}", body.len()));
        for part in [&body[..10], &body[10..30], &body[30..]] {
            std::thread::sleep(Duration::from_millis(150));
            stream.write_all(part.as_bytes()).unwrap();
        }
        let mut r = std::io::BufReader::new(stream);
        let mut status = String::new();
        std::io::BufRead::read_line(&mut r, &mut status).unwrap();
        assert!(status.starts_with("HTTP/1.1 200"), "{status}");
        read_until(&mut r, "event: welcome");
        read_until(&mut r, "event: confirm_subscription");
        endpoint.wait_for_sessions(1);
    }
}
