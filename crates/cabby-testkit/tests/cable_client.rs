//! Self-tests for the scenario WebSocket client against an in-process server.

use std::net::TcpListener;
use std::time::Duration;

use cabby_testkit::cable_client::{CableClient, ClientError, Options};
use serde_json::json;
use tungstenite::Message;
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;

/// Serves one connection: echoes the subprotocol, sends `frames`, then waits for
/// one client message and replies with it, then closes with 1000/"bye".
fn server(frames: Vec<Message>) -> (u16, std::thread::JoinHandle<Option<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let cb = |req: &tungstenite::handshake::server::Request, mut resp: tungstenite::handshake::server::Response| {
            if let Some(p) = req.headers().get("Sec-WebSocket-Protocol") {
                resp.headers_mut().insert("Sec-WebSocket-Protocol", p.clone());
            }
            Ok(resp)
        };
        let mut ws = tungstenite::accept_hdr(stream, cb).unwrap();
        for f in frames {
            ws.send(f).unwrap();
        }
        let got = match ws.read() {
            Ok(Message::Text(t)) => Some(t.to_string()),
            _ => None,
        };
        if let Some(t) = &got {
            ws.send(Message::text(t.clone())).unwrap();
        }
        let _ = ws.close(Some(CloseFrame { code: CloseCode::Normal, reason: "bye".into() }));
        let _ = ws.flush();
        while ws.read().is_ok() {}
        got
    });
    (port, h)
}

#[test]
fn receives_in_order_ignores_types_and_reports_close() {
    let (port, h) = server(vec![
        Message::text(json!({"type": "welcome"}).to_string()),
        Message::text(json!({"type": "ping", "message": 1}).to_string()),
        Message::text(json!({"identifier": "x", "message": {"a": 1}}).to_string()),
    ]);
    let mut c = CableClient::connect(
        &format!("ws://127.0.0.1:{port}/cable"),
        Options { ignore: vec!["ping".into()], ..Default::default() },
    )
    .unwrap();
    assert_eq!(json!({"type": "welcome"}), c.receive().unwrap());
    assert_eq!(json!({"identifier": "x", "message": {"a": 1}}), c.receive().unwrap());

    c.send(&json!({"command": "subscribe", "identifier": "x"}));
    assert_eq!(json!({"command": "subscribe", "identifier": "x"}), c.receive().unwrap());
    assert_eq!(Err(ClientError::Disconnected(Some((1000, "bye".into())))), c.receive());
    assert!(c.is_closed());
    assert_eq!(Some(json!({"command": "subscribe", "identifier": "x"}).to_string()), h.join().unwrap());
}

#[test]
fn receive_times_out_without_messages() {
    let (port, _h) = server(vec![]);
    let mut c = CableClient::connect(&format!("ws://127.0.0.1:{port}/cable"), Options::default()).unwrap();
    assert_eq!(Err(ClientError::Timeout), c.receive_within(Duration::from_millis(100)));
}
