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
    assert_eq!(Some(crate::version::ANYCABLE_COMPAT), resp.header("x-anycable-version"));

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

/// Proposed deviation D-020: compression is accepted in the configuration but
/// never negotiated (Go negotiates permessage-deflate).
#[test]
fn deviation_d020_compression_is_not_negotiated() {
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

/// Proposed deviation D-021: a text frame with invalid UTF-8 fails the connection
/// with 1007 (Go hands the bytes to the session, whose decoding then fails).
#[test]
fn deviation_d021_invalid_utf8_text_closes_with_1007() {
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

type Delivery = (Vec<u8>, ReadPermit);

/// Records push-mode deliveries.
struct Pushed {
    messages: Mutex<mpsc::Sender<Delivery>>,
    ends: Mutex<mpsc::Sender<String>>,
}

impl InboundHandler for Pushed {
    fn on_message(&self, msg: bytes::Bytes, permit: ReadPermit) {
        let _ = self.messages.lock().unwrap().send((msg.to_vec(), permit));
    }

    fn on_end(&self, err: ConnectionError) {
        let _ = self.ends.lock().unwrap().send(err.0);
    }
}

fn pushed() -> (Arc<Pushed>, mpsc::Receiver<Delivery>, mpsc::Receiver<String>) {
    let (mtx, messages) = mpsc::channel();
    let (etx, ends) = mpsc::channel();
    (Arc::new(Pushed { messages: Mutex::new(mtx), ends: Mutex::new(etx) }), messages, ends)
}

#[test]
fn push_mode_delivers_one_message_at_a_time_then_the_end() {
    let (server, addr, rx) = ws_server(&Config::new());
    let mut ws = client(addr, "actioncable-v1-json");
    let (conn, _info) = session(&rx);

    // Received before the handler is set: delivered when it is.
    ws.send(Message::text("one")).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let (handler, messages, ends) = pushed();
    conn.set_inbound_handler(handler);
    ws.send(Message::text("two")).unwrap();
    ws.send(Message::binary(vec![3])).unwrap();

    let (first, permit) = messages.recv_timeout(IO_DEADLINE).expect("the first message");
    assert_eq!(b"one".to_vec(), first);
    assert!(messages.recv_timeout(Duration::from_millis(200)).is_err(), "the next message waits for the permit");
    drop(permit);
    let (second, permit) = messages.recv_timeout(IO_DEADLINE).expect("the second message");
    assert_eq!(b"two".to_vec(), second);
    drop(permit);
    let (third, permit) = messages.recv_timeout(IO_DEADLINE).expect("the third message");
    assert_eq!(vec![3], third);
    drop(permit);

    ws.close(None).unwrap();
    let _ = ws.flush();
    let end = ends.recv_timeout(IO_DEADLINE).expect("the end");
    assert!(is_close_error(&ConnectionError(end.clone())), "{end}");
    assert!(ends.recv_timeout(Duration::from_millis(100)).is_err(), "the end is reported once");

    // A handler set after the end gets it right away.
    let (late, _messages, late_ends) = pushed();
    conn.set_inbound_handler(late);
    assert_eq!(end, late_ends.recv_timeout(IO_DEADLINE).unwrap());
    server.shutdown();
}

// --- gorilla close and protocol-error frames (independent review, Low) ---------

/// A raw socket past the handshake, with its session.
fn raw_session(addr: SocketAddr, rx: &mpsc::Receiver<Event>) -> (TcpStream, WsConnection) {
    let mut stream = connect(addr);
    write!(
        stream,
        "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    )
    .unwrap();
    assert_eq!(101, read_response(&mut stream).status);
    let (conn, _) = session(rx);
    (stream, conn)
}

/// A client frame: masked unless `unmasked`; `b0` is FIN/RSV/opcode.
fn raw_frame(b0: u8, payload: &[u8], unmasked: bool) -> Vec<u8> {
    let mut out = vec![b0];
    let mask = [0x11, 0x22, 0x33, 0x44];
    let len = u8::try_from(payload.len()).expect("short payload");
    if unmasked {
        out.push(len);
        out.extend_from_slice(payload);
    } else {
        out.push(0x80 | len);
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    }
    out
}

/// Reads the server's next frame: `(opcode, payload)`.
fn read_raw_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    use std::io::Read;
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).unwrap();
    assert_eq!(0, head[1] & 0x80, "server frames are not masked");
    let mut payload = vec![0; usize::from(head[1] & 0x7f)];
    stream.read_exact(&mut payload).unwrap();
    (head[0] & 0x0f, payload)
}

/// gorilla's default close handler answers a close frame with its code and
/// no reason (an empty payload for none). Before, the reason was echoed.
#[test]
fn echoes_close_codes_without_the_reason() {
    let (server, addr, rx) = ws_server(&Config::new());
    for (payload, echo, end) in [
        (&b"\x03\xe8bye"[..], &b"\x03\xe8"[..], "websocket: close 1000 (normal): bye"),
        (b"\x0f\xa0custom", b"\x0f\xa0", "websocket: close 4000: custom"),
        (b"", b"", "websocket: close 1005 (no status)"),
        (b"\x03", b"", "websocket: close 1005 (no status)"),
    ] {
        let (mut stream, conn) = raw_session(addr, &rx);
        stream.write_all(&raw_frame(0x88, payload, false)).unwrap();
        assert_eq!(Err(ConnectionError(end.into())), conn.read(), "{payload:?}");
        assert_eq!((8, echo.to_vec()), read_raw_frame(&mut stream), "{payload:?}");
    }
    server.shutdown();
}

/// Protocol errors close with 1002 and gorilla's texts. Before, the close
/// reasons were tungstenite's error messages.
#[test]
fn closes_protocol_errors_with_gorilla_texts() {
    let (server, addr, rx) = ws_server(&Config::new());
    let cases: [(Vec<u8>, &str); 9] = [
        (raw_frame(0x81, b"{}", true), "bad MASK"),
        (raw_frame(0xC1, b"{}", false), "RSV1 set"),
        (raw_frame(0xF1, b"{}", true), "RSV1 set, RSV2 set, RSV3 set, bad MASK"),
        (raw_frame(0x83, b"x", false), "bad opcode 3"),
        (raw_frame(0x80, b"x", false), "continuation after FIN"),
        (raw_frame(0x09, b"", false), "FIN not set on control"),
        ([raw_frame(0x01, b"a", false), raw_frame(0x81, b"b", false)].concat(), "data before FIN"),
        (raw_frame(0x88, b"\x03\xed", false), "bad close code 1005"),
        (raw_frame(0x88, b"\x03\xe8\xff", false), "invalid utf8 payload in close frame"),
    ];
    for (bytes, text) in cases {
        let (mut stream, conn) = raw_session(addr, &rx);
        stream.write_all(&bytes).unwrap();
        assert_eq!(Err(ConnectionError(format!("websocket: {text}"))), conn.read());
        let mut expected = 1002u16.to_be_bytes().to_vec();
        expected.extend_from_slice(text.as_bytes());
        assert_eq!((8, expected), read_raw_frame(&mut stream), "{text}");
    }

    // A control frame over 125 bytes (gorilla checks the 7-bit length).
    let (mut stream, conn) = raw_session(addr, &rx);
    stream.write_all(&[0x89, 0x80 | 126, 0, 200, 1, 2, 3, 4]).unwrap();
    assert_eq!(Err(ConnectionError("websocket: len > 125 for control".into())), conn.read());
    assert_eq!(8, read_raw_frame(&mut stream).0);

    // Valid frames before an invalid one in the same read are delivered.
    let (mut stream, conn) = raw_session(addr, &rx);
    stream.write_all(&[raw_frame(0x81, b"ok", false), raw_frame(0x83, b"x", false)].concat()).unwrap();
    assert_eq!(Ok(b"ok".to_vec()), conn.read());
    assert_eq!(Err(ConnectionError("websocket: bad opcode 3".into())), conn.read());
    server.shutdown();
}
