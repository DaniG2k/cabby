//! End-to-end tests of the HTTP server over real sockets (shared with the
//! WebSocket and broadcast tests through [`support`]).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use super::*;

pub(crate) mod support {
    use super::*;

    pub(crate) const IO_DEADLINE: Duration = Duration::from_secs(5);

    /// A started server on an ephemeral port.
    pub(crate) fn start(server: &Arc<HttpServer>) -> SocketAddr {
        server.start().expect("server starts");
        server.local_addr().expect("bound address")
    }

    /// A parsed HTTP/1.1 response.
    #[derive(Debug)]
    pub(crate) struct RawResponse {
        pub status: u16,
        pub headers: Vec<(String, String)>,
        pub body: Vec<u8>,
    }

    impl RawResponse {
        pub(crate) fn header(&self, name: &str) -> Option<&str> {
            self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
        }
    }

    /// Reads one response (body by `Content-Length`; none for 101).
    pub(crate) fn read_response(stream: &mut impl Read) -> RawResponse {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).expect("status line");
        let status = line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).expect("status code");
        let mut headers = Vec::new();
        loop {
            line.clear();
            reader.read_line(&mut line).expect("header line");
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break;
            }
            let (k, v) = trimmed.split_once(':').expect("header");
            headers.push((k.to_owned(), v.trim().to_owned()));
        }
        let mut resp = RawResponse { status, headers, body: Vec::new() };
        if let Some(len) = resp.header("content-length").and_then(|l| l.parse::<usize>().ok()) {
            let mut body = vec![0; len];
            reader.read_exact(&mut body).expect("body");
            resp.body = body;
        }
        resp
    }

    pub(crate) fn connect(addr: SocketAddr) -> TcpStream {
        let stream = TcpStream::connect_timeout(&addr, IO_DEADLINE).expect("connect");
        stream.set_read_timeout(Some(IO_DEADLINE)).unwrap();
        stream.set_write_timeout(Some(IO_DEADLINE)).unwrap();
        stream
    }

    /// Sends a raw request (`head` without the final blank line) and reads the response.
    pub(crate) fn request(addr: SocketAddr, head: &str, body: &[u8]) -> RawResponse {
        let mut stream = connect(addr);
        write!(stream, "{head}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        stream.write_all(body).unwrap();
        read_response(&mut stream)
    }

    pub(crate) fn get(addr: SocketAddr, path: &str) -> RawResponse {
        request(addr, &format!("GET {path} HTTP/1.1\r\nHost: {addr}"), b"")
    }
}

use support::*;

#[test]
fn serves_routes_and_go_404() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    server.setup_handler("/apps/1/*", |req: Request| async move {
        let path = req.uri().path().to_owned();
        let mut resp = status_response(200);
        *resp.body_mut() = full(path);
        resp
    });
    let addr = start(&server);

    let health = get(addr, "/health");
    assert_eq!(200, health.status);
    assert_eq!(HEALTH_MESSAGE, &health.body[..]);

    let wildcard = get(addr, "/apps/1/events");
    assert_eq!(200, wildcard.status);
    assert_eq!(b"/apps/1/events", &wildcard.body[..]);

    let missing = get(addr, "/health/");
    assert_eq!(404, missing.status);
    assert_eq!(b"404 page not found\n", &missing.body[..]);
    assert_eq!(Some("text/plain; charset=utf-8"), missing.header("content-type"));
    assert_eq!(404, get(addr, "/apps/1").status);

    assert!(server.running());
    server.start().expect("a second start is a no-op");
    server.shutdown();
}

#[test]
fn passes_connection_info_to_handlers() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/info", |req: Request| async move {
        let (parts, _) = req.into_parts();
        let info = RequestInfo::new(&HttpRequest::from_parts(&parts, &conn_info_of(&parts)), None).unwrap();
        let mut resp = status_response(200);
        *resp.body_mut() = full(format!("{}|{}", info.url, info.param("a")));
        resp
    });
    let addr = start(&server);

    let resp = get(addr, "/info?a=1&a=2");
    assert_eq!(format!("http://{addr}/info?a=1&a=2|2").as_bytes(), &resp.body[..]);
    server.shutdown();
}

fn conn_info_of(parts: &::http::request::Parts) -> ConnInfo {
    parts.extensions.get::<ConnInfo>().copied().expect("connection info")
}

#[test]
fn limits_concurrent_connections() {
    let server = HttpServer::new("127.0.0.1", "0", None, 1).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);

    // The first connection holds the only slot (keep-alive).
    let mut first = connect(addr);
    write!(first, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(200, read_response(&mut first).status);

    let mut second = connect(addr);
    write!(second, "GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    second.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut byte = [0u8; 1];
    let err = second.read(&mut byte).expect_err("not served while the slot is taken");
    assert!(matches!(err.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut), "{err}");

    drop(first);
    second.set_read_timeout(Some(IO_DEADLINE)).unwrap();
    assert_eq!(200, read_response(&mut second).status);
    server.shutdown();
}

#[test]
fn shuts_down_gracefully() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);

    // An idle keep-alive connection is closed by the shutdown.
    let mut idle = connect(addr);
    write!(idle, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(200, read_response(&mut idle).status);

    let token = server.shutdown_token();
    runtime_handle().block_on(server.shutdown_graceful(IO_DEADLINE)).expect("connections drained");
    assert!(server.stopped());
    assert!(token.is_cancelled());
    let mut rest = Vec::new();
    assert_eq!(0, idle.read_to_end(&mut rest).expect("closed by the server"));
    assert!(TcpStream::connect_timeout(&addr, IO_DEADLINE).is_err(), "no longer listening");
}

#[test]
fn reports_bind_errors() {
    let first = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    let addr = start(&first);
    let second = HttpServer::new("127.0.0.1", &addr.port().to_string(), None, 0).unwrap();
    let err = second.start().expect_err("port in use");
    assert!(err.0.starts_with(&format!("listen tcp 127.0.0.1:{}: ", addr.port())), "{err}");
    first.shutdown();
}

#[test]
fn reports_ssl_errors() {
    let ssl = SslConfig { cert_path: "missing.crt".into(), key_path: "missing.key".into() };
    let err = HttpServer::new("127.0.0.1", "0", Some(&ssl), 0).expect_err("missing files");
    assert!(err.0.starts_with("failed to load SSL certificate: "), "{err}");
}

/// Also pins proposed deviation D-022: ALPN selects HTTP/1.1 even when the client
/// offers `h2` (Go's TLS server offers HTTP/2 for plain HTTP endpoints).
#[test]
fn serves_over_tls() {
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
    let ssl = SslConfig { cert_path: format!("{fixtures}/server.crt"), key_path: format!("{fixtures}/server.key") };
    let server = HttpServer::new("127.0.0.1", "0", Some(&ssl), 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    assert_eq!("https://127.0.0.1:0", server.address());
    let addr = start(&server);

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_file_iter(&ssl.cert_path).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let client = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let mut tls = rustls::StreamOwned::new(client, connect(addr));
    write!(tls, "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
    let resp = read_response(&mut tls);
    assert_eq!(200, resp.status);
    assert_eq!(HEALTH_MESSAGE, &resp.body[..]);
    assert_eq!(Some(&b"http/1.1"[..]), tls.conn.alpn_protocol());
    server.shutdown();
}

use rustls::pki_types::pem::PemObject as _;

#[test]
fn writes_cors_headers_like_go() {
    let mut any = ::http::HeaderMap::new();
    write_cors_headers(&mut any, "http://example.com", &[]);
    assert_eq!("*", any["access-control-allow-origin"]);
    assert!(any.get("vary").is_none());

    let mut listed = ::http::HeaderMap::new();
    write_cors_headers(&mut listed, "http://App.example.com", &["*.example.com".to_owned()]);
    assert_eq!("http://app.example.com", listed["access-control-allow-origin"]);
    assert_eq!("Origin", listed["vary"]);
    assert_eq!("POST, OPTIONS", listed["access-control-allow-methods"]);

    let mut denied = ::http::HeaderMap::new();
    write_cors_headers(&mut denied, "http://other.com", &["example.com".to_owned()]);
    assert!(denied.get("access-control-allow-origin").is_none());
}

#[test]
fn mounts_metrics_and_the_api() {
    use std::sync::Mutex;

    use cabby_core::broker::{Broadcaster, Legacy};
    use cabby_core::common::{RemoteCommandMessage, StreamMessage};

    struct Noop;
    impl Broadcaster for Noop {
        fn broadcast(&self, _msg: &StreamMessage) {}
        fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
        fn subscribe(&self, _stream: &str) {}
        fn unsubscribe(&self, _stream: &str) {}
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Vec<u8>>>);
    impl crate::api::BroadcastHandler for Recorder {
        fn handle_broadcast(&self, payload: &[u8]) -> Result<(), crate::api::ApiError> {
            self.0.lock().unwrap().push(payload.to_vec());
            Ok(())
        }
        fn handle_pubsub(&self, _payload: &[u8]) {}
    }

    let metrics = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
    let recorder = Arc::new(Recorder::default());
    let mut conf = crate::api::Config::new();
    conf.max_body_size = 16;
    let api = crate::api::Server::new(&mut conf, Arc::new(Legacy::new(Arc::new(Noop))), recorder.clone()).unwrap();
    api.start().unwrap();

    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/metrics", metrics_handler(Arc::clone(&metrics)));
    server.setup_handler(&format!("{}/*", conf.path), api_handler(Arc::new(api), conf.max_body_size));
    let addr = start(&server);

    let resp = get(addr, "/metrics");
    assert_eq!(200, resp.status);
    assert_eq!(Some("text/plain; charset=utf-8"), resp.header("content-type"));

    let publish = |body: &[u8]| request(addr, &format!("POST /api/publish HTTP/1.1\r\nHost: {addr}"), body);
    assert_eq!(201, publish(b"{\"stream\":\"a\"}").status);
    assert_eq!(413, publish(&[b'x'; 64]).status);
    assert_eq!(1, recorder.0.lock().unwrap().len());
    assert_eq!(404, get(addr, "/api/unknown").status);
    server.shutdown();
}

// --- Go `http.Server` timeouts (independent review H5) ------------------------

/// Reads until the server closes the connection: how long that took and what
/// was received, or `None` if it stays open for `within`.
fn closed_within(stream: &mut TcpStream, within: Duration) -> Option<(Duration, Vec<u8>)> {
    let started = std::time::Instant::now();
    stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    while started.elapsed() < within {
        match stream.read(&mut buf) {
            Ok(0) => return Some((started.elapsed(), data)),
            Ok(n) => data.extend_from_slice(&buf[..n]),
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
            Err(_) => return Some((started.elapsed(), data)),
        }
    }
    None
}

/// H5: Go's `ReadHeaderTimeout` does not run between requests and upstream
/// sets no idle timeout, so a keep-alive connection idle for 12 s still
/// serves the next request. Before, hyper's header read timeout closed it
/// after 5 s (load balancers and pooled broadcast clients saw failures).
#[test]
fn idle_keep_alive_connections_stay_open() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);

    let mut stream = connect(addr);
    write!(stream, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(200, read_response(&mut stream).status);
    assert_eq!(None, closed_within(&mut stream, Duration::from_secs(12)), "idle connection closed");
    stream.set_read_timeout(Some(IO_DEADLINE)).unwrap();
    write!(stream, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(200, read_response(&mut stream).status);
    server.shutdown();
}

/// H5: the header timeout covers the first request's head from the accept,
/// and a later request's head from its first bytes; it closes the connection
/// without a response, as Go does.
#[test]
fn header_timeout_runs_while_a_head_is_read() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);
    let t0 = std::time::Instant::now();

    // A connection that sends nothing.
    let mut silent = connect(addr);
    // A first head that never completes.
    let mut partial = connect(addr);
    write!(partial, "GET /health HTTP/1.1\r\nHost: x\r\n").unwrap();
    // A second head that never completes, after 2 s of idling.
    let mut later = connect(addr);
    write!(later, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(200, read_response(&mut later).status);
    std::thread::sleep(Duration::from_secs(2));
    write!(later, "GET /health HTTP/1.1\r\n").unwrap();

    // Closed 5 s after the accept, or 5 s after the second head started (t0 + 2 s).
    for (name, stream, min) in
        [("silent", &mut silent, 4.5), ("partial", &mut partial, 4.5), ("later", &mut later, 6.5)]
    {
        let (_, data) = closed_within(stream, Duration::from_secs(10)).unwrap_or_else(|| panic!("{name} stays open"));
        let after = t0.elapsed().as_secs_f64();
        assert!(data.is_empty(), "{name}: no response, got {data:?}");
        assert!((min..min + 2.0).contains(&after), "{name}: closed {after:.1} s after the start");
    }
    server.shutdown();
}

/// H5: an upgraded connection (WebSocket) is not subject to the header
/// timeout, however long it stays quiet.
#[test]
fn upgraded_connections_have_no_header_timeout() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/echo", |mut req: Request| async move {
        let on_upgrade = hyper::upgrade::on(&mut req);
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut io = hyper_util::rt::TokioIo::new(on_upgrade.await.expect("upgraded"));
            let mut buf = [0u8; 64];
            while let Ok(n) = io.read(&mut buf).await {
                if n == 0 || io.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        });
        let mut resp = status_response(101);
        resp.headers_mut().insert("upgrade", ::http::HeaderValue::from_static("echo"));
        resp.headers_mut().insert("connection", ::http::HeaderValue::from_static("upgrade"));
        resp
    });
    let addr = start(&server);

    let mut stream = connect(addr);
    write!(stream, "GET /echo HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\n").unwrap();
    assert_eq!(101, read_response(&mut stream).status);
    let mut buf = [0u8; 4];
    for _ in 0..2 {
        write!(stream, "ping").unwrap();
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(b"ping", &buf);
        std::thread::sleep(Duration::from_secs(6));
    }
    write!(stream, "pong").unwrap();
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(b"pong", &buf);
    server.shutdown();
}

/// Low (HTTP framing): net/http accepts up to 1 MiB (+4 KiB) of request line
/// and headers; hyper's defaults are about 400 KiB and 100 fields.
#[test]
fn accepts_net_http_sized_headers() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);

    let big = "a".repeat(1_000_000);
    assert_eq!(200, request(addr, &format!("GET /health HTTP/1.1\r\nHost: x\r\nX-Big: {big}"), b"").status);
    let many: String = (0..500).map(|i| format!("\r\nX-H{i}: v")).collect();
    assert_eq!(200, request(addr, &format!("GET /health HTTP/1.1\r\nHost: x{many}"), b"").status);
    let huge = "a".repeat(1_100_000);
    let mut stream = connect(addr);
    let _ = write!(stream, "GET /health HTTP/1.1\r\nHost: x\r\nX-Big: {huge}\r\n\r\n");
    assert_eq!(431, read_response(&mut stream).status);
    server.shutdown();
}

// --- API body handling (independent review M3) ---------------------------------

mod api_support {
    use std::sync::Mutex;

    use cabby_core::broker::{Broadcaster, Legacy};
    use cabby_core::common::{RemoteCommandMessage, StreamMessage};

    use super::*;

    struct Noop;
    impl Broadcaster for Noop {
        fn broadcast(&self, _msg: &StreamMessage) {}
        fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
        fn subscribe(&self, _stream: &str) {}
        fn unsubscribe(&self, _stream: &str) {}
    }

    #[derive(Default)]
    pub(super) struct Recorder(pub Mutex<Vec<Vec<u8>>>);
    impl crate::api::BroadcastHandler for Recorder {
        fn handle_broadcast(&self, payload: &[u8]) -> Result<(), crate::api::ApiError> {
            self.0.lock().unwrap().push(payload.to_vec());
            Ok(())
        }
        fn handle_pubsub(&self, _payload: &[u8]) {}
    }

    /// A server with the API mounted as the CLI mounts it.
    pub(super) fn api_server(secret: &str, max_body_size: i64) -> (Arc<HttpServer>, SocketAddr, Arc<Recorder>) {
        let recorder = Arc::new(Recorder::default());
        let mut conf = crate::api::Config::new();
        conf.secret = secret.to_owned();
        conf.max_body_size = max_body_size;
        let api = crate::api::Server::new(&mut conf, Arc::new(Legacy::new(Arc::new(Noop))), recorder.clone()).unwrap();
        api.start().unwrap();
        let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
        server.setup_handler(&format!("{}/*", conf.path), api_handler(Arc::new(api), conf.max_body_size));
        let addr = start(&server);
        (server, addr, recorder)
    }
}

/// M3: as upstream, the method and authorization are checked before the
/// body is read, so an unauthorized client announcing a huge body is answered
/// at once, with or without a body limit. Before, the body was read first (up
/// to the limit, or entirely without one).
#[test]
fn api_checks_authorization_before_reading_the_body() {
    for limit in [crate::api::DEFAULT_MAX_BODY_SIZE, 0] {
        let (server, addr, recorder) = api_support::api_server("sekret", limit);
        for (head, status) in [
            ("POST /api/publish HTTP/1.1\r\nAuthorization: Bearer wrong", 401),
            ("POST /api/publish HTTP/1.1", 401),
            ("GET /api/publish HTTP/1.1\r\nAuthorization: Bearer sekret", 422),
        ] {
            let mut stream = connect(addr);
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            // Only the head and 64 KiB: the announced 100 MiB body is never sent.
            write!(stream, "{head}\r\nHost: x\r\nContent-Length: 104857600\r\n\r\n").unwrap();
            stream.write_all(&[b'x'; 65536]).unwrap();
            assert_eq!(status, read_response(&mut stream).status, "{head} (limit {limit})");
        }
        assert!(recorder.0.lock().unwrap().is_empty());
        server.shutdown();
    }
}

/// M3: the body limit is enforced while reading: an authorized request over
/// the limit gets 413 once the limit is passed, before the whole body
/// arrives.
#[test]
fn api_enforces_the_body_limit_while_reading() {
    let (server, addr, recorder) = api_support::api_server("sekret", 16);
    let mut stream = connect(addr);
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    write!(
        stream,
        "POST /api/publish HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer sekret\r\nContent-Length: 104857600\r\n\r\n"
    )
    .unwrap();
    stream.write_all(&[b'x'; 64]).unwrap();
    assert_eq!(413, read_response(&mut stream).status);

    let ok =
        request(addr, "POST /api/publish HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer sekret", b"{\"stream\":\"a\"}");
    assert_eq!(201, ok.status);
    assert_eq!(1, recorder.0.lock().unwrap().len());
    server.shutdown();
}

/// Low (HTTP framing): paths under the API path without a handler get the
/// router's `404 page not found` (Go mounts only `/publish` and
/// `/presence/*`), not an empty 404.
#[test]
fn api_unknown_paths_get_go_404() {
    let (server, addr, _) = api_support::api_server("", 0);
    for path in ["/api/foo", "/api/", "/api/publish/"] {
        let resp = get(addr, path);
        assert_eq!(404, resp.status, "{path}");
        assert_eq!(b"404 page not found\n", &resp.body[..], "{path}");
        assert_eq!(Some("text/plain; charset=utf-8"), resp.header("content-type"), "{path}");
    }
    // A presence path the handler rejects is the handler's empty 404.
    let resp = get(addr, "/api/presence/a/b/users");
    assert_eq!(404, resp.status);
    assert!(resp.body.is_empty());
    server.shutdown();
}

// --- Go `net/http` framing (independent review, Low) ----------------------------

/// Sends raw bytes and returns everything received until the server closes
/// the connection or stays quiet for 500 ms.
fn exchange(addr: SocketAddr, raw: &[u8]) -> String {
    let mut stream = connect(addr);
    stream.write_all(raw).unwrap();
    stream.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = stream.read(&mut buf) {
        if n == 0 {
            out.extend_from_slice(b"<EOF>");
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Statuses, headers and lengths as Go's server writes them: Go's reason
/// phrases, `Connection: close` after a 413, no `Content-Type` on a 304, a
/// sniffed type on the health reply, `Content-Length` up to 2048 bytes and
/// chunked encoding above (unless the handler set a length).
#[test]
fn frames_responses_like_net_http() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    server.setup_handler("/status/*", |req: Request| async move {
        let code: u16 = req.uri().path().trim_start_matches("/status/").parse().unwrap();
        let mut resp = status_response(code);
        resp.headers_mut().insert("content-type", ::http::HeaderValue::from_static("text/plain"));
        resp
    });
    server.setup_handler("/size/*", |req: Request| async move {
        let n: usize = req.uri().path().trim_start_matches("/size/").parse().unwrap();
        let mut resp = ::http::Response::new(full(vec![b'x'; n]));
        if req.headers().contains_key("x-length") {
            resp.headers_mut().insert("content-length", ::http::HeaderValue::from(n));
        }
        resp
    });
    let addr = start(&server);

    let health = get(addr, "/health");
    assert_eq!(Some("text/plain; charset=utf-8"), health.header("content-type"));

    let too_large = exchange(addr, b"GET /status/413 HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(too_large.starts_with("HTTP/1.1 413 Request Entity Too Large\r\n"), "{too_large}");
    assert!(too_large.contains("\r\nConnection: close\r\n") && too_large.ends_with("<EOF>"), "{too_large}");

    let not_modified = exchange(addr, b"GET /status/304 HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(not_modified.starts_with("HTTP/1.1 304 Not Modified\r\n"), "{not_modified}");
    assert!(!not_modified.to_ascii_lowercase().contains("content-type"), "{not_modified}");

    let small = exchange(addr, b"GET /size/2048 HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(small.contains("\r\nContent-Length: 2048\r\n"), "{small}");
    let large = exchange(addr, b"GET /size/2049 HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(large.contains("\r\nTransfer-Encoding: chunked\r\n") && !large.contains("Content-Length"), "{large}");
    let declared = exchange(addr, b"GET /size/4000 HTTP/1.1\r\nHost: x\r\nX-Length: 1\r\n\r\n");
    assert!(declared.contains("\r\nContent-Length: 4000\r\n"), "{declared}");
    server.shutdown();
}

/// Requests Go's server answers itself: `OPTIONS *`, HTTP/1.1 without
/// `Host`, a path with an invalid escape.
#[test]
fn answers_malformed_requests_like_net_http() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    let addr = start(&server);

    let options = exchange(addr, b"OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(options.starts_with("HTTP/1.1 200 OK\r\n") && options.contains("\r\nContent-Length: 0\r\n"), "{options}");

    let no_host = exchange(addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(no_host.starts_with("HTTP/1.1 400 Bad Request: missing required Host header\r\n"), "{no_host}");
    assert!(no_host.ends_with("\r\n\r\n400 Bad Request: missing required Host header<EOF>"), "{no_host}");
    assert!(no_host.contains("\r\nConnection: close\r\n"), "{no_host}");
    // HTTP/1.0 needs no Host.
    assert!(exchange(addr, b"GET /health HTTP/1.0\r\n\r\n").starts_with("HTTP/1.0 200 OK\r\n"));

    let bad_escape = exchange(addr, b"GET /health%zz HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(bad_escape.starts_with("HTTP/1.1 400 Bad Request\r\n"), "{bad_escape}");
    assert!(bad_escape.ends_with("\r\n\r\n400 Bad Request<EOF>"), "{bad_escape}");
    server.shutdown();
}

/// A client that shuts down its side after sending a request still gets the
/// response of a long-running handler, which sees the client go away (Go
/// cancels the request context). Before, hyper closed the connection without
/// a response.
#[test]
fn responds_after_a_client_half_close() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/wait", |req: Request| async move {
        let (mut writer, gone, response) = channel_response_for(&req);
        std::thread::spawn(move || {
            let seen = runtime_handle()
                .block_on(async { tokio::time::timeout(Duration::from_secs(3), gone.cancelled()).await.is_ok() });
            writer.write_header(200);
            let _ = writer.write(if seen { b"client went away" } else { b"timed out" });
            writer.finish();
        });
        response.await
    });
    let addr = start(&server);

    let mut stream = connect(addr);
    write!(stream, "GET /wait HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let resp = read_response(&mut stream);
    assert_eq!(200, resp.status);
    assert_eq!(b"client went away", &resp.body[..]);
    server.shutdown();
}

/// Deviation D-045: requests hyper's parser rejects or limits where net/http
/// accepts them (obs-fold, a request target over 64 KiB, more than 1024
/// header fields), and hyper's own error responses (empty body).
#[test]
fn deviation_d045_hyper_parser_limits() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/health", health_endpoint());
    server.setup_handler("/echo", |req: Request| async move {
        let mut resp = status_response(200);
        *resp.body_mut() = full(req.uri().to_string());
        resp
    });
    let addr = start(&server);

    // hyper's URI parser drops a `#` and what follows (Go keeps it in the query).
    assert_eq!(b"/echo?a=1", &request(addr, "GET /echo?a=1#frag HTTP/1.1\r\nHost: x", b"").body[..]);

    let folded = exchange(addr, b"GET /health HTTP/1.1\r\nHost: x\r\nX-A: a\r\n b\r\n\r\n");
    assert!(folded.starts_with("HTTP/1.1 400 Bad Request\r\n") && folded.contains("Content-Length: 0"), "{folded}");
    let long_uri = format!("GET /health?{} HTTP/1.1\r\nHost: x\r\n\r\n", "a".repeat(70_000));
    assert!(exchange(addr, long_uri.as_bytes()).starts_with("HTTP/1.1 414 "));
    let many: String = (0..1100).map(|i| format!("X-H{i}: v\r\n")).collect();
    let many = format!("GET /health HTTP/1.1\r\nHost: x\r\n{many}\r\n");
    assert!(exchange(addr, many.as_bytes()).starts_with("HTTP/1.1 431 "));
    server.shutdown();
}

/// Deviation D-046: a response to a request whose body was not read is sent
/// at once and the connection is closed (Go first reads and discards up to
/// 256 KiB of the body and keeps the connection), and a client that
/// half-closes after sending a request with a body gets no response (Go
/// answers it; a request without a body is answered, see
/// `responds_after_a_client_half_close`).
#[test]
fn deviation_d046_unread_bodies_and_half_closed_posts() {
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/ignore", |_req: Request| async { status_response(401) });
    server.setup_handler("/slow", |_req: Request| async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        status_response(200)
    });
    let addr = start(&server);

    // Half of the body: answered at once, then closed.
    let started = std::time::Instant::now();
    let unread = exchange(addr, b"POST /ignore HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabcde");
    assert!(unread.starts_with("HTTP/1.1 401 Unauthorized\r\n") && unread.ends_with("<EOF>"), "{unread}");
    assert!(started.elapsed() < Duration::from_secs(1));

    let mut stream = connect(addr);
    stream.write_all(b"POST /slow HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nab").unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut rest = Vec::new();
    let _ = stream.read_to_end(&mut rest);
    assert!(rest.is_empty(), "{:?}", String::from_utf8_lossy(&rest));
    server.shutdown();
}
