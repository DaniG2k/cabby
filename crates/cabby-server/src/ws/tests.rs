//! End-to-end tests of the WebSocket endpoint and connection over real sockets.

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{ClientRequestBuilder, Message, WebSocket};

use super::*;
use crate::node::{Connection, ConnectionError};
use crate::server::tests::support::{IO_DEADLINE, connect, read_response, request, start};
use crate::server::{HttpServer, RequestInfo};

type Client = WebSocket<TcpStream>;

/// What a test session handler reports back.
enum Event {
    Session(WsConnection, RequestInfo),
}

/// A server with the WebSocket endpoint at `/cable`; each session is sent to the
/// returned channel.
fn ws_server(config: &Config) -> (Arc<HttpServer>, SocketAddr, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let handler: SessionHandler = Arc::new(move |conn, info, _callback| {
        tx.lock().unwrap().send(Event::Session(conn, info)).map_err(|e| e.to_string())
    });
    let protocols = vec!["actioncable-v1-json".to_owned(), "actioncable-v1-ext-json".to_owned()];
    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler("/cable", websocket_handler(protocols, None, config, handler));
    let addr = start(&server);
    (server, addr, rx)
}

fn session(rx: &mpsc::Receiver<Event>) -> (WsConnection, RequestInfo) {
    match rx.recv_timeout(IO_DEADLINE).expect("a session") {
        Event::Session(conn, info) => (conn, info),
    }
}

fn client(addr: SocketAddr, protocols: &str) -> Client {
    let uri: tungstenite::http::Uri = format!("ws://{addr}/cable?token=abc").parse().unwrap();
    let mut req = ClientRequestBuilder::new(uri);
    if !protocols.is_empty() {
        req = req.with_header("Sec-WebSocket-Protocol", protocols);
    }
    let (ws, _resp) = tungstenite::client(req, connect(addr)).expect("handshake");
    ws
}

fn deadline() -> Instant {
    Instant::now() + IO_DEADLINE
}

fn handshake_status(addr: SocketAddr, method: &str, extra: &str) -> u16 {
    request(
        addr,
        &format!(
            "{method} /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13{extra}"
        ),
        b"",
    )
    .status
}

#[test]
fn negotiates_subprotocols_and_exchanges_messages() {
    let (server, addr, rx) = ws_server(&Config::new());

    let mut stream = connect(addr);
    write!(
        stream,
        "GET /cable?token=abc HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: actioncable-v1-ext-json, actioncable-v1-json\r\nX-AnyCable-Sid: 42\r\n\r\n"
    )
    .unwrap();
    let resp = read_response(&mut stream);
    assert_eq!(101, resp.status);
    assert_eq!(Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), resp.header("sec-websocket-accept"));
    assert_eq!(Some("actioncable-v1-json"), resp.header("sec-websocket-protocol"));
    assert_eq!(Some(crate::telemetry::version().as_str()), resp.header("x-anycable-version"));

    let (conn, info) = session(&rx);
    assert_eq!("actioncable-v1-json", conn.subprotocol());
    assert_eq!(format!("http://{addr}/cable?token=abc"), info.url);
    assert_eq!("abc", info.param("token"));
    assert_eq!("42", info.anycable_header("X-AnyCable-SID"));
    assert_eq!(21, info.uid.len());

    let mut ws = WebSocket::from_raw_socket(stream, tungstenite::protocol::Role::Client, None);
    ws.send(Message::text("{\"command\":\"subscribe\"}")).unwrap();
    assert_eq!(b"{\"command\":\"subscribe\"}".to_vec(), conn.read().unwrap());
    ws.send(Message::binary(vec![1, 2, 3])).unwrap();
    assert_eq!(vec![1, 2, 3], conn.read().unwrap());

    conn.write(b"{\"type\":\"welcome\"}", deadline()).unwrap();
    assert_eq!(Message::text("{\"type\":\"welcome\"}"), ws.read().unwrap());
    conn.write_binary(&[9, 8], deadline()).unwrap();
    assert_eq!(Message::binary(vec![9, 8]), ws.read().unwrap());

    let shared = Arc::new(SentFrame::text(b"{\"message\":1}".to_vec()));
    conn.write_frame(&shared, deadline()).unwrap();
    assert_eq!(Message::text("{\"message\":1}"), ws.read().unwrap());
    server.shutdown();
}

#[test]
fn negotiates_no_subprotocol_when_none_matches() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut stream = connect(addr);
    write!(
        stream,
        "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: graphql-ws\r\n\r\n"
    )
    .unwrap();
    let resp = read_response(&mut stream);
    assert_eq!(101, resp.status);
    assert_eq!(None, resp.header("sec-websocket-protocol"));
    let mut ws = WebSocket::from_raw_socket(stream, tungstenite::protocol::Role::Client, None);
    let (conn, _) = session(&rx);
    assert_eq!("", conn.subprotocol());
    conn.write(b"hi", deadline()).unwrap();
    assert_eq!(Message::text("hi"), ws.read().unwrap());
    server.shutdown();
}

#[test]
fn rejects_bad_handshakes_like_gorilla() {
    let mut config = Config::new();
    config.allowed_origins = "secure.origin".into();
    let (server, addr, _rx) = ws_server(&config);

    assert_eq!(403, handshake_status(addr, "GET", ""));
    assert_eq!(403, handshake_status(addr, "GET", "\r\nOrigin: http://evil.origin"));
    assert_eq!(101, handshake_status(addr, "GET", "\r\nOrigin: http://SECURE.origin"));
    assert_eq!(405, handshake_status(addr, "POST", "\r\nOrigin: http://secure.origin"));

    let plain = request(addr, &format!("GET /cable HTTP/1.1\r\nHost: {addr}"), b"");
    assert_eq!(400, plain.status);
    assert_eq!(b"Bad Request\n", &plain.body[..]);
    assert_eq!(Some("13"), plain.header("sec-websocket-version"));

    let bad_version = request(
        addr,
        &format!(
            "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 8\r\nOrigin: http://secure.origin"
        ),
        b"",
    );
    assert_eq!(400, bad_version.status);

    let bad_key = request(
        addr,
        &format!(
            "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: c2hvcnQ=\r\nSec-WebSocket-Version: 13\r\nOrigin: http://secure.origin"
        ),
        b"",
    );
    assert_eq!(400, bad_key.status);
    server.shutdown();
}

#[test]
fn closes_with_code_and_reason() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    conn.write(b"{\"type\":\"disconnect\"}", deadline()).unwrap();
    conn.close(CLOSE_TRY_AGAIN_LATER, "Try again later");
    assert!(conn.is_closed(), "close waits until the frame is written");

    assert_eq!(Message::text("{\"type\":\"disconnect\"}"), ws.read().unwrap());
    match ws.read().unwrap() {
        Message::Close(Some(frame)) => {
            assert_eq!(CloseCode::from(1013), frame.code);
            assert_eq!("Try again later", frame.reason.as_str());
        }
        other => panic!("expected a close frame, got {other:?}"),
    }
    assert_eq!(Err(ConnectionError("websocket: close sent".into())), conn.write(b"late", deadline()));
    assert!(conn.read().is_err());
    server.shutdown();
}

#[test]
fn reports_client_closures_like_gorilla() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    ws.close(Some(tungstenite::protocol::CloseFrame { code: CloseCode::Away, reason: "bye".into() })).unwrap();
    let err = conn.read().expect_err("closed by the client");
    assert_eq!("websocket: close 1001 (going away): bye", err.0);
    assert!(is_close_error(&err));
    // The server answered the close handshake.
    assert!(matches!(ws.read(), Ok(Message::Close(_)) | Err(_)));
    server.shutdown();
}

#[test]
fn reports_dropped_connections_as_abnormal() {
    let (server, addr, rx) = ws_server(&Config::new());
    let ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    drop(ws);
    let err = conn.read().expect_err("connection dropped");
    assert_eq!("websocket: close 1006 (abnormal closure): unexpected EOF", err.0);
    assert!(!is_close_error(&err));
    server.shutdown();
}

#[test]
fn closes_oversized_messages_with_1009() {
    let mut config = Config::new();
    config.max_message_size = 16;
    let (server, addr, rx) = ws_server(&config);
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    ws.send(Message::text("x".repeat(100))).unwrap();
    assert_eq!(Err(ConnectionError("websocket: read limit exceeded".into())), conn.read());
    match ws.read().unwrap() {
        Message::Close(Some(frame)) => assert_eq!(CloseCode::Size, frame.code),
        other => panic!("expected a close frame, got {other:?}"),
    }
    server.shutdown();
}

#[test]
fn sends_pings_and_reports_pongs() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    let (pong_tx, pong_rx) = mpsc::channel();
    let pong_tx = Mutex::new(pong_tx);
    conn.set_pong_handler(move || {
        let _ = pong_tx.lock().unwrap().send(());
    });

    conn.write_ping(deadline()).unwrap();
    // The client answers while reading.
    assert!(matches!(ws.read().unwrap(), Message::Ping(_)));
    ws.flush().unwrap();
    pong_rx.recv_timeout(IO_DEADLINE).expect("pong handled");

    // Client pings are answered by the server.
    ws.send(Message::Ping("p".into())).unwrap();
    assert_eq!(Message::Pong("p".into()), ws.read().unwrap());
    server.shutdown();
}

#[test]
fn reading_applies_back_pressure() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    for i in 0..50 {
        ws.send(Message::text(format!("{i}"))).unwrap();
    }
    for i in 0..50 {
        assert_eq!(format!("{i}").into_bytes(), conn.read().unwrap());
    }
    server.shutdown();
}

#[test]
fn concurrent_writers_keep_per_writer_order() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    let total = Arc::new(AtomicUsize::new(0));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let conn = conn.clone();
            let total = Arc::clone(&total);
            std::thread::spawn(move || {
                for i in 0..200 {
                    conn.write(format!("{w}:{i}").as_bytes(), deadline()).unwrap();
                    total.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let mut next = [0usize; 4];
    for _ in 0..800 {
        let Message::Text(text) = ws.read().unwrap() else { panic!("text expected") };
        let (w, i) = text.as_str().split_once(':').unwrap();
        let (w, i): (usize, usize) = (w.parse().unwrap(), i.parse().unwrap());
        assert_eq!(next[w], i, "writer {w} out of order");
        next[w] += 1;
    }
    for writer in writers {
        writer.join().unwrap();
    }
    assert_eq!(800, total.load(Ordering::Relaxed));
    server.shutdown();
}

#[test]
fn writes_time_out_when_the_client_does_not_read() {
    let (server, addr, rx) = ws_server(&Config::new());
    let _ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    // Fill the socket buffers and the queue: some write must hit its deadline.
    let payload = vec![b'x'; 64 * 1024];
    let started = Instant::now();
    let err = loop {
        match conn.write(&payload, Instant::now() + Duration::from_millis(200)) {
            Ok(()) => assert!(started.elapsed() < Duration::from_secs(30), "writes never blocked"),
            Err(e) => break e,
        }
    };
    assert_eq!("i/o timeout", err.0);
    // The io loop failed the connection.
    assert!(conn.read().is_err());
    server.shutdown();
}

#[test]
fn dropping_every_handle_closes_the_socket() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);
    drop(conn);
    ws.get_mut().set_read_timeout(Some(IO_DEADLINE)).unwrap();
    assert!(ws.read().is_err(), "socket closed");
    server.shutdown();
}

#[test]
fn releases_the_pong_handler_when_the_connection_ends() {
    let (server, addr, rx) = ws_server(&Config::new());
    let ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    // Stands for the session the handler usually captures (which holds `conn`).
    let session_ref = Arc::new(());
    let held = Arc::clone(&session_ref);
    conn.set_pong_handler(move || {
        let _ = &held;
    });
    assert_eq!(2, Arc::strong_count(&session_ref));

    drop(ws);
    assert!(conn.read().is_err());
    assert_eq!(1, Arc::strong_count(&session_ref), "no reference cycle through the connection");
    server.shutdown();
}

/// Proposed deviation D-014: compression is accepted in the configuration but
/// never negotiated (Go negotiates permessage-deflate).
#[test]
fn deviation_d014_compression_is_not_negotiated() {
    let mut config = Config::new();
    config.enable_compression = true;
    let (server, addr, _rx) = ws_server(&config);
    let mut stream = connect(addr);
    write!(
        stream,
        "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n"
    )
    .unwrap();
    let resp = read_response(&mut stream);
    assert_eq!(101, resp.status);
    assert_eq!(None, resp.header("sec-websocket-extensions"));
    server.shutdown();
}

/// Proposed deviation D-015: a text frame with invalid UTF-8 fails the connection
/// with 1007 (Go hands the bytes to the session, whose decoding then fails).
#[test]
fn deviation_d015_invalid_utf8_text_closes_with_1007() {
    use tungstenite::protocol::frame::Frame;
    use tungstenite::protocol::frame::coding::{Data, OpCode};

    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _) = session(&rx);

    ws.send(Message::Frame(Frame::message(vec![0xff, 0xfe], OpCode::Data(Data::Text), true))).unwrap();
    let err = conn.read().expect_err("invalid UTF-8");
    assert!(err.0.starts_with("websocket: invalid UTF-8"), "{err}");
    match ws.read().unwrap() {
        Message::Close(Some(frame)) => assert_eq!(CloseCode::Invalid, frame.code),
        other => panic!("expected a close frame, got {other:?}"),
    }
    server.shutdown();
}
