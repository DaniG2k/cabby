//! A Durable Streams read client: the request/response behavior of the
//! `durable-streams/packages/client-go` v0.1.0 API the upstream `ds` integration
//! tests use (`Stream.Head`, `Stream.Read` and `ChunkIterator.Next`), ported as a
//! test client so the same scenarios run against any server.
//!
//! Every network wait is bounded. A read iterator's requests, including one in
//! flight, end at its deadline (Go's context timeout) with
//! [`Error::DeadlineExceeded`]; `Head` (upstream `context.Background()`) waits at
//! most [`crate::raw_http::IO_DEADLINE`] per read.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::raw_http::{self, EventStream, Line};

/// `durablestreams.StartOffset`.
pub const START_OFFSET: &str = "-1";

/// `durablestreams.LiveMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveMode {
    None,
    LongPoll,
    Sse,
}

/// Client errors; `Display` follows client-go's messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// `durablestreams.Done`.
    Done,
    /// The iterator's context deadline passed (`context.DeadlineExceeded`).
    DeadlineExceeded,
    /// `durablestreams.ErrAlreadyClosed`: `Next` after `Close`.
    AlreadyClosed,
    /// `*StreamError`: an operation failed, with the HTTP status when there was one.
    Stream { op: String, url: String, status: u16, message: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Done => f.write_str("durablestreams: no more items in iterator"),
            Error::DeadlineExceeded => f.write_str("context deadline exceeded"),
            Error::AlreadyClosed => f.write_str("durablestreams: iterator already closed"),
            Error::Stream { op, url, status, message } if *status > 0 => {
                write!(f, "durablestreams: {op} {url} failed with status {status}: {message}")
            }
            Error::Stream { op, url, message, .. } => write!(f, "durablestreams: {op} {url} failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// client-go `errorFromStatus`.
fn error_from_status(status: u16) -> String {
    match status {
        404 => "durablestreams: stream not found".into(),
        409 => "durablestreams: stream already exists with different config".into(),
        410 => "durablestreams: offset before retention window".into(),
        429 => "durablestreams: rate limited".into(),
        _ => format!("unexpected status code: {status}"),
    }
}

const CONTENT_TYPE_MISMATCH: &str = "durablestreams: content type mismatch";

/// `durablestreams.NewClient(durablestreams.WithBaseURL(base))`.
#[derive(Debug, Clone)]
pub struct Client {
    base: String,
}

impl Client {
    /// `base` is `http://host:port`.
    pub fn new(base: &str) -> Self {
        Self { base: base.trim_end_matches('/').to_owned() }
    }

    /// `client.Stream(path)`.
    pub fn stream(&self, path: &str) -> Stream {
        let host = self.base.strip_prefix("http://").expect("an http:// base URL");
        let addr: SocketAddr = host.parse().expect("base URL host is ip:port");
        Stream { url: format!("{}{path}", self.base), addr, path: path.to_owned() }
    }
}

/// `*durablestreams.Stream`.
#[derive(Debug, Clone)]
pub struct Stream {
    url: String,
    addr: SocketAddr,
    path: String,
}

/// `*durablestreams.Metadata` (the fields the tests read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub content_type: String,
    pub next_offset: String,
    pub etag: String,
}

/// `*durablestreams.Chunk`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Chunk {
    pub next_offset: String,
    pub data: Vec<u8>,
    pub up_to_date: bool,
    pub cursor: String,
    pub etag: String,
}

fn io_error(op: &str, url: &str, e: impl std::fmt::Display) -> Error {
    Error::Stream { op: op.into(), url: url.into(), status: 0, message: e.to_string() }
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Stream {
    /// `stream.Head(ctx, WithHeadHeaders(headers))`.
    pub fn head(&self, headers: &[(&str, &str)]) -> Result<Metadata, Error> {
        let resp = raw_http::request(self.addr, "HEAD", &self.path, headers, None)
            .map_err(|e| io_error("head", &self.url, e))?;
        match resp.status {
            200 => Ok(Metadata {
                content_type: resp.header("content-type").unwrap_or_default().to_owned(),
                next_offset: resp.header("stream-next-offset").unwrap_or_default().to_owned(),
                etag: resp.header("etag").unwrap_or_default().to_owned(),
            }),
            404 => Err(Error::Stream {
                op: "head".into(),
                url: self.url.clone(),
                status: 404,
                message: error_from_status(404),
            }),
            status => Err(Error::Stream {
                op: "head".into(),
                url: self.url.clone(),
                status,
                message: error_from_status(status),
            }),
        }
    }

    /// `stream.Read(ctx, WithOffset(offset), WithLive(live))`; `timeout` is the
    /// context deadline (upstream `context.WithTimeout`): it bounds every call,
    /// including a request (or an SSE read) in flight when it passes.
    pub fn read(&self, offset: &str, live: LiveMode, timeout: Duration) -> ChunkIterator {
        ChunkIterator {
            stream: self.clone(),
            deadline: Instant::now() + timeout,
            offset: offset.to_owned(),
            live,
            cursor: String::new(),
            headers: Vec::new(),
            up_to_date: false,
            done_once: false,
            closed: false,
            sse: None,
            sse_pending: None,
        }
    }

    /// client-go `buildReadURL` (query keys sorted, as `url.Values.Encode`).
    fn read_target(&self, offset: &str, live: LiveMode, cursor: &str) -> String {
        let offset = if offset.is_empty() { START_OFFSET } else { offset };
        let mut q = Vec::new();
        if !cursor.is_empty() {
            q.push(format!("cursor={}", query_escape(cursor)));
        }
        match live {
            LiveMode::LongPoll => q.push("live=long-poll".into()),
            LiveMode::Sse => q.push("live=sse".into()),
            LiveMode::None => {}
        }
        q.push(format!("offset={}", query_escape(offset)));
        format!("{}?{}", self.path, q.join("&"))
    }
}

/// `*durablestreams.ChunkIterator`.
pub struct ChunkIterator {
    stream: Stream,
    deadline: Instant,
    offset: String,
    live: LiveMode,
    cursor: String,
    headers: Vec<(String, String)>,
    pub up_to_date: bool,
    done_once: bool,
    closed: bool,
    sse: Option<(EventStream, SseParser)>,
    sse_pending: Option<Chunk>,
}

#[derive(Deserialize)]
struct ControlEvent {
    #[serde(rename = "streamNextOffset", default)]
    stream_next_offset: String,
    #[serde(rename = "streamCursor", default)]
    stream_cursor: String,
    #[serde(rename = "upToDate", default)]
    up_to_date: bool,
}

enum SseEvent {
    Data(String),
    Control(ControlEvent),
}

/// client-go `internal/sse.Parser`, fed one body line at a time.
#[derive(Default)]
struct SseParser {
    event_type: String,
    data_lines: Vec<String>,
}

/// What [`SseParser::feed`] made of a line.
enum Parsed {
    Event(SseEvent),
    /// Needs more lines.
    More,
    /// The response ended (`io.EOF`) with no event left to flush.
    Eof,
}

impl SseParser {
    /// `Parser.Next`'s handling of one `ReadString('\n')` result: a line ends at
    /// `\n` (one `\r` before it is dropped, so `\r\n` blank lines separate events
    /// too); at the end of the body the partial last line is discarded and the
    /// current event, if valid, is still delivered.
    fn feed(&mut self, line: Line) -> Parsed {
        let line = match line {
            Line::Eof(_) => return self.flush().map_or(Parsed::Eof, Parsed::Event),
            Line::Complete(l) => l,
        };
        let line = line.strip_suffix('\r').unwrap_or(&line);
        if line.is_empty() {
            return self.flush().map_or(Parsed::More, Parsed::Event);
        }
        if let Some(t) = line.strip_prefix("event:") {
            self.event_type = t.trim().to_owned();
        } else if let Some(d) = line.strip_prefix("data:") {
            self.data_lines.push(d.strip_prefix(' ').unwrap_or(d).to_owned());
        }
        Parsed::More
    }

    /// `flushEvent`: the current event if valid; resets the state.
    fn flush(&mut self) -> Option<SseEvent> {
        let kind = std::mem::take(&mut self.event_type);
        let data = std::mem::take(&mut self.data_lines);
        if kind.is_empty() || data.is_empty() {
            return None;
        }
        let data = data.join("\n");
        match kind.as_str() {
            "data" => Some(SseEvent::Data(data)),
            "control" => serde_json::from_str(&data).ok().map(SseEvent::Control),
            _ => None,
        }
    }
}

impl ChunkIterator {
    /// `WithReadHeaders`-style custom request headers.
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn left(&self) -> Result<Duration, Error> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() { Err(Error::DeadlineExceeded) } else { Ok(left) }
    }

    /// `it.Next()`.
    #[allow(clippy::should_implement_trait)] // Go's iterator API: Done is an error, not the end of an Iterator
    pub fn next(&mut self) -> Result<Chunk, Error> {
        if self.closed {
            return Err(Error::AlreadyClosed);
        }
        if self.done_once {
            return Err(Error::Done);
        }
        self.left()?;
        if self.live == LiveMode::Sse { self.next_sse() } else { self.next_http() }
    }

    fn next_http(&mut self) -> Result<Chunk, Error> {
        let target = self.stream.read_target(&self.offset, self.live, &self.cursor);
        let headers: Vec<(&str, &str)> = self.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let url = self.stream.url.clone();
        let resp = raw_http::request_until(self.stream.addr, "GET", &target, &headers, None, self.deadline)
            .map_err(|e| if self.left().is_err() { Error::DeadlineExceeded } else { io_error("read", &url, e) })?;
        let header = |name: &str| resp.header(name).unwrap_or_default().to_owned();
        match resp.status {
            200 => {
                let next_offset = header("stream-next-offset");
                let cursor = header("stream-cursor");
                let up_to_date = header("stream-up-to-date") == "true";
                self.offset = next_offset.clone();
                self.cursor = cursor.clone();
                self.up_to_date = up_to_date;
                if up_to_date && self.live == LiveMode::None {
                    self.done_once = true;
                }
                Ok(Chunk { next_offset, data: resp.body.clone(), up_to_date, cursor, etag: header("etag") })
            }
            204 => {
                let next_offset = header("stream-next-offset");
                let cursor = header("stream-cursor");
                let up_to_date = header("stream-up-to-date") == "true";
                if !next_offset.is_empty() {
                    self.offset = next_offset.clone();
                }
                if !cursor.is_empty() {
                    self.cursor = cursor.clone();
                }
                self.up_to_date = up_to_date;
                if self.live == LiveMode::None {
                    self.done_once = true;
                    return Err(Error::Done);
                }
                Ok(Chunk { next_offset, data: Vec::new(), up_to_date, cursor, etag: String::new() })
            }
            304 => {
                let cursor = header("stream-cursor");
                if !cursor.is_empty() {
                    self.cursor = cursor;
                }
                Ok(Chunk {
                    next_offset: self.offset.clone(),
                    data: Vec::new(),
                    up_to_date: self.up_to_date,
                    cursor: self.cursor.clone(),
                    etag: String::new(),
                })
            }
            status => Err(Error::Stream { op: "read".into(), url, status, message: error_from_status(status) }),
        }
    }

    fn establish_sse(&mut self) -> Result<(), Error> {
        let target = self.stream.read_target(&self.offset, LiveMode::Sse, &self.cursor);
        let mut headers: Vec<(&str, &str)> = vec![("Accept", "text/event-stream")];
        headers.extend(self.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let url = self.stream.url.clone();
        let es = EventStream::open_until(self.stream.addr, "GET", &target, &headers, None, self.deadline)
            .map_err(|e| if self.left().is_err() { Error::DeadlineExceeded } else { io_error("read", &url, e) })?;
        let fail =
            |status: u16, message: String| Error::Stream { op: "read".into(), url: url.clone(), status, message };
        match es.status {
            200 => {
                if !es.header("content-type").unwrap_or_default().starts_with("text/event-stream") {
                    return Err(fail(200, CONTENT_TYPE_MISMATCH.into()));
                }
                self.sse = Some((es, SseParser::default()));
                Ok(())
            }
            400 => Err(fail(400, CONTENT_TYPE_MISMATCH.into())),
            status => Err(fail(status, error_from_status(status))),
        }
    }

    fn next_sse(&mut self) -> Result<Chunk, Error> {
        if let Some(c) = self.sse_pending.take() {
            return Ok(c);
        }
        if self.sse.is_none() {
            self.establish_sse()?;
        }
        loop {
            let left = self.left()?;
            let (es, parser) = self.sse.as_mut().expect("SSE connection");
            let parsed = match es.next_line(left) {
                Ok(line) => parser.feed(line),
                Err(e) => {
                    self.sse = None;
                    self.left()?;
                    return Err(io_error("read", &self.stream.url, e));
                }
            };
            let event = match parsed {
                Parsed::Event(event) => event,
                Parsed::More => continue,
                Parsed::Eof => {
                    // Connection closed gracefully: reconnect (client-go behavior).
                    self.sse = None;
                    self.left()?;
                    self.establish_sse()?;
                    continue;
                }
            };
            match event {
                SseEvent::Data(d) => match &mut self.sse_pending {
                    Some(p) => p.data.extend_from_slice(d.as_bytes()),
                    None => self.sse_pending = Some(Chunk { data: d.into_bytes(), ..Default::default() }),
                },
                SseEvent::Control(c) => {
                    self.offset = c.stream_next_offset.clone();
                    if !c.stream_cursor.is_empty() {
                        self.cursor = c.stream_cursor.clone();
                    }
                    self.up_to_date = c.up_to_date;
                    if let Some(mut chunk) = self.sse_pending.take() {
                        chunk.next_offset = c.stream_next_offset;
                        chunk.cursor = c.stream_cursor;
                        chunk.up_to_date = c.up_to_date;
                        return Ok(chunk);
                    }
                    if c.up_to_date {
                        return Ok(Chunk {
                            next_offset: c.stream_next_offset,
                            cursor: c.stream_cursor,
                            up_to_date: true,
                            ..Default::default()
                        });
                    }
                }
            }
        }
    }

    /// `it.Close()`: drops the SSE connection (the server sees the client leave).
    pub fn close(&mut self) {
        self.closed = true;
        self.sse = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;

    /// Feeds `body` to a parser as `bufio.Reader.ReadString('\n')` would split it.
    fn parse(body: &str) -> Vec<String> {
        let mut p = SseParser::default();
        let mut out = Vec::new();
        let mut rest = body;
        loop {
            let line = match rest.find('\n') {
                Some(i) => {
                    let l = Line::Complete(rest[..i].to_owned());
                    rest = &rest[i + 1..];
                    l
                }
                None => Line::Eof(rest.to_owned()),
            };
            let eof = matches!(line, Line::Eof(_));
            match p.feed(line) {
                Parsed::Event(SseEvent::Data(d)) => out.push(format!("data {d}")),
                Parsed::Event(SseEvent::Control(c)) => {
                    out.push(format!("control {} {}", c.stream_next_offset, c.up_to_date))
                }
                Parsed::More => {}
                Parsed::Eof => return out,
            }
            if eof {
                // client-go's next Parser.Next hits io.EOF again with nothing to flush.
                assert!(matches!(p.feed(Line::Eof(String::new())), Parsed::Eof));
                return out;
            }
        }
    }

    #[test]
    fn parses_sse_events_like_client_go() {
        let control = r#"{"streamNextOffset":"1::e","upToDate":true}"#;
        assert_eq!(
            parse(&format!("event: data\ndata:[1]\n\n: ping\n\nevent: control\ndata:{control}\n\n")),
            ["data [1]", "control 1::e true"]
        );
        // CRLF line endings and blank lines.
        assert_eq!(
            parse(&format!("event: data\r\ndata: [1]\r\n\r\nevent: control\r\ndata: {control}\r\n\r\n")),
            ["data [1]", "control 1::e true"]
        );
        // A last event without a trailing blank line is flushed at EOF ...
        assert_eq!(parse("event: data\ndata: [2]\n"), ["data [2]"]);
        // ... but a partial last line (no `\n`) is discarded, as ReadString's EOF result is.
        assert_eq!(parse("event: data\ndata: [2]"), Vec::<String>::new());
        assert_eq!(parse("event: data\ndata: [2]\nevent: con"), ["data [2]"]);
        // Multi-line data and unknown or incomplete events.
        assert_eq!(parse("event: data\ndata: a\ndata: b\n\nevent: other\ndata: x\n\ndata: y\n\n"), ["data a\nb"]);
    }

    /// Serves one connection: reads the request head, writes `response`, then
    /// closes the connection (`hold: false`) or keeps it open until the returned
    /// guard is dropped.
    fn serve_once(response: &'static [u8], hold: bool) -> (String, std::sync::mpsc::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        let (guard, release) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            let mut head = Vec::new();
            let mut b = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && conn.read(&mut b).is_ok_and(|n| n == 1) {
                head.push(b[0]);
            }
            let _ = conn.write_all(response);
            if hold {
                let _ = release.recv_timeout(Duration::from_secs(30));
            }
        });
        (base, guard)
    }

    #[test]
    fn sse_read_delivers_crlf_events_and_a_last_event_at_eof() {
        let (base, _guard) = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
              event: data\r\ndata: [1]\r\n\r\n\
              event: control\ndata: {\"streamNextOffset\":\"7::e\",\"upToDate\":true}\n",
            false,
        );
        let mut it = Client::new(&base).stream("/ds/x").read(START_OFFSET, LiveMode::Sse, Duration::from_secs(5));
        let chunk = it.next().expect("chunk");
        assert_eq!(chunk.data, b"[1]");
        assert_eq!(chunk.next_offset, "7::e");
        assert!(chunk.up_to_date);
    }

    #[test]
    fn deadline_ends_an_in_flight_request() {
        for live in [LiveMode::LongPoll, LiveMode::Sse] {
            // The server accepts the request and never answers.
            let (base, _guard) = serve_once(b"", true);
            let started = Instant::now();
            let mut it = Client::new(&base).stream("/ds/x").read(START_OFFSET, live, Duration::from_millis(300));
            assert_eq!(it.next(), Err(Error::DeadlineExceeded), "{live:?}");
            assert!(started.elapsed() < Duration::from_secs(2), "{live:?}: took {:?}", started.elapsed());
        }
    }

    #[test]
    fn next_after_close_is_already_closed() {
        let mut it =
            Client::new("http://127.0.0.1:1").stream("/ds/x").read(START_OFFSET, LiveMode::None, Duration::ZERO);
        it.close();
        let err = it.next().expect_err("closed");
        assert_eq!(err, Error::AlreadyClosed);
        assert_eq!(err.to_string(), "durablestreams: iterator already closed");
    }

    #[test]
    fn builds_read_targets_like_client_go() {
        let s = Client::new("http://127.0.0.1:1").stream("/ds/x");
        assert_eq!(s.read_target("", LiveMode::None, ""), "/ds/x?offset=-1");
        assert_eq!(s.read_target("2::e", LiveMode::Sse, "7"), "/ds/x?cursor=7&live=sse&offset=2%3A%3Ae");
    }
}
