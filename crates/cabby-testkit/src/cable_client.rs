//! Synchronous Action Cable WebSocket client for scenario tests.
//!
//! Semantics follow AnyT's `Anyt::Client` (anyt-core 1.4.0, MIT, © palkan):
//! JSON messages are queued in arrival order, message types listed in `ignore`
//! are dropped, `receive` waits up to a deadline, and a close frame surfaces as
//! [`ClientError::Disconnected`]. Unlike AnyT, every wait is explicit and
//! "no further messages" checks take their window as an argument.

use std::collections::VecDeque;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Message, WebSocket};

/// AnyT: `WAIT_WHEN_EXPECTING_EVENT`.
pub const RECEIVE_DEADLINE: Duration = Duration::from_secs(5);
/// AnyT: `WAIT_WHEN_NOT_EXPECTING_EVENT`, used for negative checks.
pub const QUIET_WINDOW: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    Timeout,
    /// Close frame received (code, reason), or `None` for an abnormal close.
    Disconnected(Option<(u16, String)>),
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Timeout => write!(f, "Timed out to receive message"),
            ClientError::Disconnected(Some((c, r))) => write!(f, "WebSocket disconnected (code={c}, reason={r})"),
            ClientError::Disconnected(None) => write!(f, "WebSocket disconnected abnormally"),
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone)]
pub struct Options {
    /// Message `type`s to drop (AnyT `ignore:`), e.g. `["ping"]`.
    pub ignore: Vec<String>,
    /// Query string appended to the URL.
    pub qs: String,
    /// Sent as the `cookie` header (AnyT always sends it, possibly empty).
    pub cookies: String,
    pub headers: Vec<(String, String)>,
    pub protocol: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            ignore: Vec::new(),
            qs: String::new(),
            cookies: String::new(),
            headers: Vec::new(),
            protocol: "actioncable-v1-json".into(),
        }
    }
}

enum Event {
    Message(Value),
    Text(String),
    Closed(Option<(u16, String)>),
    Error(String),
}

enum Command {
    Send(Message),
    Close,
}

pub struct CableClient {
    url: String,
    events: Receiver<Event>,
    commands: Sender<Command>,
    queue: VecDeque<Event>,
    closed: Option<Option<(u16, String)>>,
    worker: Option<JoinHandle<()>>,
}

impl CableClient {
    /// Connects to `url` (e.g. `ws://127.0.0.1:PORT/cable`) and completes the
    /// handshake within [`RECEIVE_DEADLINE`].
    pub fn connect(url: &str, opts: Options) -> Result<Self, ClientError> {
        let mut full = url.to_owned();
        if !opts.qs.is_empty() {
            full.push(if full.contains('?') { '&' } else { '?' });
            full.push_str(&opts.qs);
        }
        let mut req = full.as_str().into_client_request().map_err(|e| ClientError::Protocol(e.to_string()))?;
        let h = req.headers_mut();
        let hv = |v: &str| HeaderValue::from_str(v).map_err(|e| ClientError::Protocol(e.to_string()));
        for (k, v) in &opts.headers {
            let name = tungstenite::http::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| ClientError::Protocol(e.to_string()))?;
            h.insert(name, hv(v)?);
        }
        h.insert("cookie", hv(&opts.cookies)?);
        h.insert("Sec-WebSocket-Protocol", hv(&opts.protocol)?);

        let host = req.uri().host().unwrap_or("127.0.0.1").to_owned();
        let port = req.uri().port_u16().unwrap_or(80);
        let addr = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), port))
            .map_err(|e| ClientError::Protocol(e.to_string()))?
            .next()
            .ok_or_else(|| ClientError::Protocol("unresolvable host".into()))?;
        let stream =
            TcpStream::connect_timeout(&addr, RECEIVE_DEADLINE).map_err(|e| ClientError::Protocol(e.to_string()))?;
        stream.set_read_timeout(Some(RECEIVE_DEADLINE)).ok();
        stream.set_write_timeout(Some(RECEIVE_DEADLINE)).ok();
        let (ws, _resp) = tungstenite::client(req, stream).map_err(|e| ClientError::Protocol(e.to_string()))?;
        ws.get_ref().set_read_timeout(Some(POLL)).ok();

        let (etx, erx) = mpsc::channel();
        let (ctx, crx) = mpsc::channel();
        let ignore = opts.ignore.clone();
        let worker = std::thread::spawn(move || pump(ws, etx, crx, ignore));
        Ok(Self { url: full, events: erx, commands: ctx, queue: VecDeque::new(), closed: None, worker: Some(worker) })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn fill(&mut self, wait: Duration) {
        match self.events.recv_timeout(wait) {
            Ok(ev) => self.queue.push_back(ev),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                if self.closed.is_none() {
                    self.closed = Some(None);
                }
            }
        }
        while let Ok(ev) = self.events.try_recv() {
            self.queue.push_back(ev);
        }
    }

    /// Next JSON message, waiting up to `timeout`.
    pub fn receive_within(&mut self, timeout: Duration) -> Result<Value, ClientError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(ev) = self.queue.pop_front() {
                return match ev {
                    Event::Message(v) => Ok(v),
                    Event::Text(t) => Err(ClientError::Protocol(format!("non-JSON frame {t:?}"))),
                    Event::Closed(c) => {
                        self.closed = Some(c.clone());
                        Err(ClientError::Disconnected(c))
                    }
                    Event::Error(e) => Err(ClientError::Protocol(e)),
                };
            }
            if let Some(c) = &self.closed {
                return Err(ClientError::Disconnected(c.clone()));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(ClientError::Timeout);
            }
            self.fill(deadline - now);
        }
    }

    /// AnyT `receive`: next message within [`RECEIVE_DEADLINE`].
    pub fn receive(&mut self) -> Result<Value, ClientError> {
        self.receive_within(RECEIVE_DEADLINE)
    }

    /// Sends `message` as a JSON text frame.
    pub fn send(&mut self, message: &Value) {
        let _ = self.commands.send(Command::Send(Message::text(message.to_string())));
    }

    /// Sends a raw text frame (for malformed-input scenarios).
    pub fn send_raw(&mut self, text: &str) {
        let _ = self.commands.send(Command::Send(Message::text(text.to_owned())));
    }

    /// Messages that arrive within `window` (negative checks: expect none).
    pub fn drain_within(&mut self, window: Duration) -> Vec<Value> {
        let mut out = Vec::new();
        let deadline = Instant::now() + window;
        while let Ok(v) = self.receive_within(deadline.saturating_duration_since(Instant::now())) {
            out.push(v);
        }
        out
    }

    /// AnyT `close`: waits [`QUIET_WINDOW`]; unless `allow_messages`, panics if
    /// unprocessed messages remain. Then closes and waits for the close.
    pub fn close(mut self, allow_messages: bool) {
        let left = self.drain_within(QUIET_WINDOW);
        assert!(allow_messages || left.is_empty(), "{} messages unprocessed: {left:?}", left.len());
        let _ = self.commands.send(Command::Close);
        self.wait_for_close(RECEIVE_DEADLINE);
    }

    /// Waits until the connection is closed by either side.
    pub fn wait_for_close(&mut self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.closed.is_some() || self.queue.iter().any(|e| matches!(e, Event::Closed(_))) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            self.fill(deadline - now);
        }
    }

    pub fn is_closed(&mut self) -> bool {
        self.fill(Duration::ZERO);
        self.closed.is_some() || self.queue.iter().any(|e| matches!(e, Event::Closed(_)))
    }
}

impl Drop for CableClient {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Close);
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
    }
}

fn pump(mut ws: WebSocket<TcpStream>, events: Sender<Event>, commands: Receiver<Command>, ignore: Vec<String>) {
    let mut closing_since: Option<Instant> = None;
    loop {
        while let Ok(cmd) = commands.try_recv() {
            let r = match cmd {
                Command::Send(m) => ws.send(m),
                Command::Close => {
                    closing_since.get_or_insert_with(Instant::now);
                    ws.close(None)
                }
            };
            if let Err(e) = r {
                if !matches!(e, tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) {
                    let _ = events.send(Event::Error(e.to_string()));
                }
            }
        }
        if closing_since.is_some_and(|t| t.elapsed() > RECEIVE_DEADLINE) {
            let _ = events.send(Event::Closed(None));
            return;
        }
        match ws.read() {
            Ok(Message::Text(t)) => match serde_json::from_str::<Value>(&t) {
                Ok(v) => {
                    let skip = v.get("type").and_then(Value::as_str).is_some_and(|ty| ignore.iter().any(|i| i == ty));
                    if !skip && events.send(Event::Message(v)).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    let _ = events.send(Event::Text(t.to_string()));
                }
            },
            Ok(Message::Close(frame)) => {
                let info = frame.map(|f| (u16::from(f.code), f.reason.to_string()));
                let info = info.filter(|(c, _)| *c != u16::from(CloseCode::Status)).or(Some((1005, String::new())));
                let _ = events.send(Event::Closed(info));
                let _ = ws.flush();
                return;
            }
            Ok(_) => {} // ping/pong/binary frames are not Action Cable messages
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                let _ = events.send(Event::Closed(None));
                return;
            }
            Err(e) => {
                let _ = events.send(Event::Error(e.to_string()));
                let _ = events.send(Event::Closed(None));
                return;
            }
        }
    }
}
