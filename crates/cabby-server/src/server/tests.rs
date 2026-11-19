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

/// Also pins proposed deviation D-016: ALPN selects HTTP/1.1 even when the client
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
