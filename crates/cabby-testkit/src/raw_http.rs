//! Minimal HTTP/1.1 client for external scenarios: one request per connection,
//! every read bounded by a deadline. Supports streaming (chunked) responses such as
//! server-sent events, read event by event or line by line.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

pub const IO_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    /// Header names lower-cased, in received order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == name).map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A connection whose reads are bounded. Each read waits until `wait_until` when
/// set (an [`EventStream`] wait), else until `deadline` when set (a request
/// deadline, like a Go request context's), else for [`IO_DEADLINE`]; `deadline`
/// always caps the wait. A read past `deadline` fails with `TimedOut`.
struct Conn {
    stream: TcpStream,
    deadline: Option<Instant>,
    wait_until: Option<Instant>,
}

impl Conn {
    fn timeout(&self) -> io::Result<Duration> {
        let now = Instant::now();
        let until = match (self.wait_until, self.deadline) {
            (Some(w), Some(d)) => w.min(d),
            (Some(t), None) | (None, Some(t)) => t,
            (None, None) => now + IO_DEADLINE,
        };
        let left = until.saturating_duration_since(now);
        if left.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
        }
        Ok(left)
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let timeout = self.timeout()?;
        self.stream.set_read_timeout(Some(timeout))?;
        self.stream.read(buf)
    }
}

fn send_request(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
    deadline: Option<Instant>,
) -> Result<BufReader<Conn>> {
    let mut connect = IO_DEADLINE;
    if let Some(d) = deadline {
        connect = connect.min(d.saturating_duration_since(Instant::now()));
        if connect.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded")).context("connecting");
        }
    }
    let mut stream = TcpStream::connect_timeout(&addr, connect).context("connecting")?;
    stream.set_write_timeout(Some(IO_DEADLINE))?;
    let mut req = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\n");
    // One request per connection unless the caller chose a Connection header.
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("connection")) {
        req.push_str("Connection: close\r\n");
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes())?;
    if let Some(b) = body {
        stream.write_all(b)?;
    }
    Ok(BufReader::new(Conn { stream, deadline, wait_until: None }))
}

fn read_head(reader: &mut BufReader<Conn>) -> Result<(u16, Vec<(String, String)>)> {
    let mut line = String::new();
    reader.read_line(&mut line).context("reading status line")?;
    let status = line.split_whitespace().nth(1).and_then(|c| c.parse().ok());
    let Some(status) = status else { bail!("malformed status line {line:?}") };
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).context("reading headers")?;
        let h = h.trim_end_matches(['\r', '\n']);
        if h.is_empty() {
            break;
        }
        let (k, v) = h.split_once(':').with_context(|| format!("malformed header {h:?}"))?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    Ok((status, headers))
}

fn is_chunked(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(k, v)| k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked"))
}

/// Performs a complete request (the server must end the response); every read
/// waits at most [`IO_DEADLINE`].
pub fn request(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Result<Response> {
    request_inner(addr, method, target, headers, body, None)
}

/// [`request`] with the whole exchange (connect, head and body) bounded by
/// `deadline` instead of per read; past it a read fails with `TimedOut`.
pub fn request_until(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
    deadline: Instant,
) -> Result<Response> {
    request_inner(addr, method, target, headers, body, Some(deadline))
}

fn request_inner(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
    deadline: Option<Instant>,
) -> Result<Response> {
    let mut reader = send_request(addr, method, target, headers, body, deadline)?;
    let (status, headers) = read_head(&mut reader)?;
    let mut body = Vec::new();
    if method == "HEAD" || status == 204 || status == 304 || (100..200).contains(&status) {
        return Ok(Response { status, headers, body });
    }
    if is_chunked(&headers) {
        while let Some(chunk) = read_chunk(&mut reader)? {
            body.extend_from_slice(&chunk);
        }
    } else if let Some(len) = headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()) {
        body.resize(len, 0);
        reader.read_exact(&mut body).context("reading body")?;
    } else {
        reader.read_to_end(&mut body).context("reading body")?;
    }
    Ok(Response { status, headers, body })
}

/// Reads one chunk; `None` on the terminating zero-length chunk.
fn read_chunk(reader: &mut BufReader<Conn>) -> Result<Option<Vec<u8>>> {
    let mut size_line = String::new();
    if reader.read_line(&mut size_line).context("reading chunk size")? == 0 {
        bail!("connection closed inside a chunked body");
    }
    let size_hex = size_line.trim().split(';').next().unwrap_or("");
    let size = usize::from_str_radix(size_hex, 16).with_context(|| format!("bad chunk size {size_line:?}"))?;
    let mut data = vec![0; size];
    reader.read_exact(&mut data).context("reading chunk")?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).context("reading chunk terminator")?;
    if size == 0 {
        return Ok(None);
    }
    Ok(Some(data))
}

/// One line of a streaming response body (Go's `bufio.Reader.ReadString('\n')`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Line {
    /// A line that ended with `\n` (the `\n` is removed, a `\r` before it is kept).
    Complete(String),
    /// The response ended; what followed the last `\n` (possibly empty).
    Eof(String),
}

/// A streaming response read as server-sent events (blocks separated by a blank
/// line) or line by line.
pub struct EventStream {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    reader: BufReader<Conn>,
    chunked: bool,
    buf: Vec<u8>,
    ended: bool,
}

impl EventStream {
    /// Sends the request and waits (bounded) for the response head.
    pub fn open(
        addr: SocketAddr,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Self> {
        Self::open_inner(addr, method, target, headers, body, None)
    }

    /// [`EventStream::open`] with every later read of this response capped by
    /// `deadline` too (like a Go request context's deadline); past it a read fails
    /// with `TimedOut`.
    pub fn open_until(
        addr: SocketAddr,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
        deadline: Instant,
    ) -> Result<Self> {
        Self::open_inner(addr, method, target, headers, body, Some(deadline))
    }

    fn open_inner(
        addr: SocketAddr,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
        deadline: Option<Instant>,
    ) -> Result<Self> {
        let mut reader = send_request(addr, method, target, headers, body, deadline)?;
        let (status, headers) = read_head(&mut reader)?;
        let chunked = is_chunked(&headers);
        Ok(Self { status, headers, reader, chunked, buf: Vec::new(), ended: false })
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == name).map(|(_, v)| v.as_str())
    }

    /// The next event block (without its terminating blank line, `\n\n` only), or
    /// `None` when the server ended the response. Fails if nothing arrives before
    /// `timeout`.
    pub fn next_event(&mut self, timeout: Duration) -> Result<Option<String>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\n\n") {
                let event: Vec<u8> = self.buf.drain(..pos + 2).take(pos).collect();
                if event.is_empty() {
                    continue;
                }
                return Ok(Some(String::from_utf8_lossy(&event).into_owned()));
            }
            if self.ended {
                return Ok(None);
            }
            self.fill(deadline, timeout)?;
        }
    }

    /// The next body line. Fails if no complete line (nor the end of the
    /// response) arrives before `timeout`.
    pub fn next_line(&mut self, timeout: Duration) -> Result<Line> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).take(pos).collect();
                return Ok(Line::Complete(String::from_utf8_lossy(&line).into_owned()));
            }
            if self.ended {
                let rest = std::mem::take(&mut self.buf);
                return Ok(Line::Eof(String::from_utf8_lossy(&rest).into_owned()));
            }
            self.fill(deadline, timeout)?;
        }
    }

    /// Reads more of the body into `buf` (or marks its end), waiting until `deadline`.
    fn fill(&mut self, deadline: Instant, timeout: Duration) -> Result<()> {
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            bail!("no event within {timeout:?} (buffered: {:?})", String::from_utf8_lossy(&self.buf));
        }
        self.reader.get_mut().wait_until = Some(deadline);
        let read = if self.chunked {
            read_chunk(&mut self.reader)
        } else {
            let mut tmp = [0u8; 4096];
            match self.reader.read(&mut tmp) {
                Ok(0) => Ok(None),
                Ok(n) => Ok(Some(tmp[..n].to_vec())),
                Err(e) => Err(e.into()),
            }
        };
        match read {
            Ok(Some(data)) => self.buf.extend_from_slice(&data),
            Ok(None) => self.ended = true,
            Err(e) => {
                let timed_out = e.chain().any(|c| {
                    c.downcast_ref::<io::Error>()
                        .is_some_and(|io| matches!(io.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
                });
                if timed_out {
                    bail!("no event within {timeout:?} (buffered: {:?})", String::from_utf8_lossy(&self.buf));
                }
                return Err(e);
            }
        }
        Ok(())
    }
}
