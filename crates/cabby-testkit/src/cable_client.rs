//! Synchronous Action Cable WebSocket client for scenario tests.
//!
//! Semantics follow AnyT's `Anyt::Client` (anyt-core 1.4.0, MIT, © palkan):
//! JSON messages are queued in arrival order, message types listed in `ignore`
//! are dropped, `receive` waits up to a deadline, and a close frame surfaces as
//! [`ClientError::Disconnected`]. Unlike AnyT, every wait is explicit and
//! "no further messages" checks take their window as an argument.
//!
//! The opening handshake is written by hand and, like Ruby's
//! `websocket-client-simple` (used by AnyT and wsdirector), accepts any `101`
//! response whether or not the server selected an offered subprotocol
//! (tungstenite's own client rejects that, e.g. for `anycable-token.<jwt>`).

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::client::generate_key;
use tungstenite::protocol::Role;
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
    /// `Sec-WebSocket-Protocol`; empty sends none (wsdirector's base protocol).
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
        let ws = handshake(&full, &opts)?;
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

/// The client's transport: plain TCP (`ws://`) or TLS (`wss://`).
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Stream {
    /// TLS without certificate verification: the conformance TLS target serves
    /// a self-signed certificate (`etc/ssl/server.crt`), which AnyT's Ruby
    /// client does not verify either.
    fn tls(tcp: TcpStream, host: &str) -> Result<Self, ClientError> {
        let err = |e: &dyn std::fmt::Display| ClientError::Protocol(format!("TLS: {e}"));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| err(&e))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification(provider)))
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| err(&e))?;
        let conn = rustls::ClientConnection::new(Arc::new(config), name).map_err(|e| err(&e))?;
        Ok(Stream::Tls(Box::new(rustls::StreamOwned::new(conn, tcp))))
    }

    fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => &s.sock,
        }
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.tcp().set_read_timeout(timeout)
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

#[derive(Debug)]
struct NoVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Lenient client handshake: sends the upgrade request and accepts any `101`.
fn handshake(url: &str, opts: &Options) -> Result<WebSocket<Stream>, ClientError> {
    let err = |e: &dyn std::fmt::Display| ClientError::Protocol(e.to_string());
    let req = url.into_client_request().map_err(|e| err(&e))?;
    let host = req.uri().host().unwrap_or("127.0.0.1").to_owned();
    let tls = req.uri().scheme_str() == Some("wss");
    let port = req.uri().port_u16().unwrap_or(if tls { 443 } else { 80 });
    let target = req.uri().path_and_query().map_or("/", |p| p.as_str()).to_owned();
    let addr = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), port))
        .map_err(|e| err(&e))?
        .next()
        .ok_or_else(|| ClientError::Protocol("unresolvable host".into()))?;
    let tcp = TcpStream::connect_timeout(&addr, RECEIVE_DEADLINE).map_err(|e| err(&e))?;
    tcp.set_read_timeout(Some(RECEIVE_DEADLINE)).ok();
    tcp.set_write_timeout(Some(RECEIVE_DEADLINE)).ok();
    let mut stream = if tls { Stream::tls(tcp, &host)? } else { Stream::Plain(tcp) };
    let mut head = format!(
        "GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n",
        generate_key()
    );
    for (k, v) in &opts.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("cookie: {}\r\n", opts.cookies));
    if !opts.protocol.is_empty() {
        head.push_str(&format!("Sec-WebSocket-Protocol: {}\r\n", opts.protocol));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).map_err(|e| err(&e))?;
    // Read the response head byte by byte so that no frame bytes are consumed.
    let mut resp = Vec::new();
    let mut byte = [0u8; 1];
    while !resp.ends_with(b"\r\n\r\n") {
        if resp.len() > 64 * 1024 {
            return Err(ClientError::Protocol("oversized handshake response".into()));
        }
        match stream.read(&mut byte) {
            Ok(0) => return Err(ClientError::Protocol("connection closed during the handshake".into())),
            Ok(_) => resp.push(byte[0]),
            Err(e) => return Err(err(&e)),
        }
    }
    let text = String::from_utf8_lossy(&resp);
    if text.split_whitespace().nth(1) != Some("101") {
        return Err(ClientError::Protocol(format!("handshake rejected: {}", text.lines().next().unwrap_or(""))));
    }
    Ok(WebSocket::from_raw_socket(stream, Role::Client, None))
}

fn pump(mut ws: WebSocket<Stream>, events: Sender<Event>, commands: Receiver<Command>, ignore: Vec<String>) {
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
            if let Err(e) = r
                && !matches!(e, tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed)
            {
                let _ = events.send(Event::Error(e.to_string()));
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
