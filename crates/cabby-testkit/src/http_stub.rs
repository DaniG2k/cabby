//! Minimal HTTP/1.1 test doubles: a stub server with a programmable handler, a
//! blocking POST client, and a stalled WebSocket endpoint.
//!
//! These replace the `httptest.Server`s upstream tests build inline. They run outside
//! the code under test (Go or Rust) and talk to it over loopback TCP, so one test body
//! works against both backends. Every socket operation has a deadline.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result, bail};

const IO_DEADLINE: Duration = Duration::from_secs(30);

/// A received request.
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

/// A handler's answer.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Response {
    pub fn status(status: u16) -> Self {
        Self { status, body: Vec::new() }
    }
}

type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

/// A loopback HTTP server running `handler` for every request, one thread per
/// connection. Stops (and joins its accept thread) on drop.
pub struct HttpStub {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl HttpStub {
    pub fn start(handler: impl Fn(Request) -> Response + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding the HTTP stub");
        let addr = listener.local_addr().expect("stub address");
        let stop = Arc::new(AtomicBool::new(false));
        let handler: Handler = Arc::new(handler);
        let stop2 = Arc::clone(&stop);
        let accept = std::thread::spawn(move || {
            for conn in listener.incoming() {
                if stop2.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(conn) = conn else { continue };
                let handler = Arc::clone(&handler);
                std::thread::spawn(move || {
                    let _ = serve(conn, &handler);
                });
            }
        });
        Self { addr, stop, accept: Some(accept) }
    }

    /// `http://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for HttpStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the blocking accept.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_secs(1));
        if let Some(t) = self.accept.take() {
            let _ = t.join();
        }
    }
}

/// Request line (method, path) and lower-cased headers.
type Head = (String, String, Vec<(String, String)>);

fn read_head(reader: &mut BufReader<TcpStream>) -> Result<Option<Head>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h)?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
        }
    }
    Ok(Some((method, path, headers)))
}

fn content_length(headers: &[(String, String)]) -> usize {
    headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0)
}

/// Serves keep-alive requests on one connection until the peer closes it.
fn serve(conn: TcpStream, handler: &Handler) -> Result<()> {
    conn.set_read_timeout(Some(IO_DEADLINE))?;
    conn.set_write_timeout(Some(IO_DEADLINE))?;
    let mut writer = conn.try_clone()?;
    let mut reader = BufReader::new(conn);
    while let Some((method, path, headers)) = read_head(&mut reader)? {
        let mut body = vec![0u8; content_length(&headers)];
        reader.read_exact(&mut body)?;
        let resp = handler(Request { method, path, body });
        write!(
            writer,
            "HTTP/1.1 {} Stub\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n",
            resp.status,
            resp.body.len()
        )?;
        writer.write_all(&resp.body)?;
        writer.flush()?;
    }
    Ok(())
}

/// Splits `http://host:port/path` into (`host:port`, `/path`).
fn split_url(url: &str) -> Result<(String, String)> {
    let rest = url.strip_prefix("http://").with_context(|| format!("not an http:// URL: {url}"))?;
    let (host, path) = rest.split_once('/').map_or((rest, "/".to_owned()), |(h, p)| (h, format!("/{p}")));
    Ok((host.to_owned(), path))
}

/// POSTs `body` to `url` and returns (status, response body).
pub fn post(url: &str, content_type: &str, body: &[u8]) -> Result<(u16, Vec<u8>)> {
    let (host, path) = split_url(url)?;
    let addr: SocketAddr = std::net::ToSocketAddrs::to_socket_addrs(&host)?.next().context("resolving host")?;
    let mut conn = TcpStream::connect_timeout(&addr, IO_DEADLINE)?;
    conn.set_read_timeout(Some(IO_DEADLINE))?;
    conn.set_write_timeout(Some(IO_DEADLINE))?;
    write!(
        conn,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    conn.write_all(body)?;
    conn.flush()?;
    let mut reader = BufReader::new(conn);
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    let Some(status) = status_line.split_whitespace().nth(1).and_then(|s| s.parse().ok()) else {
        bail!("malformed status line {status_line:?}");
    };
    let mut length = None;
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h)? == 0 {
            break;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':')
            && k.trim().eq_ignore_ascii_case("content-length")
        {
            length = v.trim().parse::<usize>().ok();
        }
    }
    let mut resp = Vec::new();
    match length {
        Some(n) => {
            resp.resize(n, 0);
            reader.read_exact(&mut resp)?;
        }
        None => {
            reader.read_to_end(&mut resp)?;
        }
    }
    Ok((status, resp))
}

/// A WebSocket endpoint that accepts the upgrade, sends `{"type":"welcome"}` and then
/// reads and ignores every frame (upstream `TestClient_SubscribeTimeout`'s mock).
pub struct StalledCable {
    addr: SocketAddr,
    accepted: Arc<Mutex<usize>>,
    stop: Arc<AtomicBool>,
}

impl StalledCable {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding the stalled cable");
        let addr = listener.local_addr().expect("address");
        let accepted = Arc::new(Mutex::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (accepted2, stop2) = (Arc::clone(&accepted), Arc::clone(&stop));
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                if stop2.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(conn) = conn else { continue };
                let accepted = Arc::clone(&accepted2);
                let stop = Arc::clone(&stop2);
                std::thread::spawn(move || {
                    let _ = conn.set_read_timeout(Some(IO_DEADLINE));
                    let Ok(mut ws) = tungstenite::accept(conn) else { return };
                    let _ = ws.get_ref().set_read_timeout(Some(Duration::from_millis(200)));
                    *accepted.lock().unwrap_or_else(|e| e.into_inner()) += 1;
                    let _ = ws.send(tungstenite::Message::text(r#"{"type":"welcome"}"#));
                    // Drain client frames without answering until the peer goes away
                    // or the stub stops; read timeouts only re-check the stop flag.
                    while !stop.load(Ordering::SeqCst) {
                        match ws.read() {
                            Ok(_) => {}
                            Err(tungstenite::Error::Io(e))
                                if matches!(
                                    e.kind(),
                                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                                ) => {}
                            Err(_) => return,
                        }
                    }
                });
            }
        });
        Self { addr, accepted, stop }
    }

    /// `ws://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Number of completed upgrades.
    pub fn accepted(&self) -> usize {
        *self.accepted.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for StalledCable {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_secs(1));
    }
}
