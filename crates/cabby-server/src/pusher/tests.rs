//! Pusher unit tests beyond the translated suite: deviation pins and an
//! end-to-end run of the mount API (WebSocket endpoint and REST API on a real
//! server, node, router and memory broker).

use std::io::Cursor;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, OnceLock, Weak};

use cabby_core::broker::{self as core_broker, Broadcaster};
use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_core::router::RouterController;
use serde_json::json;
use tungstenite::{ClientRequestBuilder, WebSocket};

use super::*;
use crate::node::NullController;
use crate::server::tests::support::{IO_DEADLINE, connect, request, start};

// --- D-028 / D-029: upstream nil dereferences ---

/// D-028: `pusher:subscribe` / `pusher:unsubscribe` without a data object crash
/// the Go server (nil pointer dereference in `Decode`); here they fail to decode.
#[test]
fn deviation_d028_subscription_without_data_is_a_decode_error() {
    let coder = Encoder::new();
    for raw in [
        r#"{"event":"pusher:subscribe"}"#,
        r#"{"event":"pusher:subscribe","data":null}"#,
        r#"{"event":"pusher:unsubscribe"}"#,
    ] {
        let err = coder.decode_message(raw.as_bytes()).expect_err(raw);
        assert_eq!("missing subscription data", err.0, "{raw}");
    }
}

/// D-029: a signed presence `channel_data` of `null` crashes the Go server (the
/// controller dereferences the nil `*UserPresence`); here the subscription
/// fails with an error status.
#[test]
fn deviation_d029_null_presence_data_fails_the_subscription() {
    let ctrl = Controller::new(None, Config::new());
    let identifier = channel_to_identifier("presence-room");
    let mut env = SessionEnv::new("ws://example.com/app/key", None);
    env.merge_channel_state(&identifier, &StringMap::from([("channel_data".to_owned(), "null".to_owned())]));

    let out = ctrl.subscribe("1.1", Some(&env), "", &identifier);

    assert_eq!(Status::Error, out.result.expect("result").status);
    let err = out.error.expect("error");
    assert!(err.message.starts_with("failed to unmarshal presence data"), "{}", err.message);
}

/// D-029: a signed `POST .../events` with a `null` body makes the Go handler
/// panic (net/http drops the connection); here it is answered 422.
#[test]
fn deviation_d029_null_event_body_is_unprocessable() {
    struct Forbidden;
    impl BroadcastHandler for Forbidden {
        fn handle_broadcast(&self, _: &[u8]) -> Result<(), BroadcastError> {
            panic!("unexpected broadcast")
        }
        fn handle_pubsub(&self, _: &[u8]) {
            panic!("unexpected broadcast")
        }
    }

    let mut conf = Config::new();
    conf.app_id = "app".into();
    conf.app_key = "key".into();
    conf.secret = "secret".into();
    let broker =
        Arc::new(core_broker::Memory::new(None, None, &core_broker::Config::new(), Arc::new(core_broker::SystemClock)));
    let api = RestApi::new(Arc::new(Forbidden), broker, conf.clone());

    let body = b"null";
    let target = signed_target(&conf, "POST", "/apps/app/events", Some(body));
    let req = http::Request::builder()
        .method("POST")
        .uri(target)
        .body(Box::new(Cursor::new(body.to_vec())) as Box<dyn Read + Send>)
        .unwrap();

    assert_eq!(422, api.handle(req).status().as_u16());
}

// --- Socket IDs ---

#[test]
fn keeps_valid_socket_ids_and_generates_others() {
    assert_eq!("1234.5678", generate_socket_id("1234.5678"));
    for uid in ["", "abc", "1234", "1234.", ".1234", "12.34.56", "1٣.2", "1234.5678\n"] {
        let id = generate_socket_id(uid);
        assert!(valid_socket_id(&id), "{uid:?} -> {id:?}");
        let (secs, frac) = id.split_once('.').unwrap();
        assert!(secs.parse::<u64>().unwrap() > 1_600_000_000, "{id}");
        assert!(frac.parse::<u64>().unwrap() <= 1_000_000, "{id}");
    }
}

// --- End to end ---

/// Signs a REST request as Pusher clients do (`auth_version=1.0`).
fn signed_target(conf: &Config, method: &str, path: &str, body: Option<&[u8]>) -> String {
    use md5::Digest as _;

    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let mut params = format!("auth_key={}&auth_timestamp={ts}&auth_version=1.0", conf.app_key);
    if let Some(body) = body {
        params.push_str(&format!("&body_md5={}", hex(&md5::Md5::digest(body))));
    }
    let signature = Verifier::new(&conf.app_key, &conf.secret).sign(&format!("{method}\n{path}\n{params}"));
    format!("{path}?{params}&auth_signature={signature}")
}

/// Hands broadcasts straight back to the node (a single-node pub/sub).
#[derive(Default)]
struct Loopback(OnceLock<Weak<Node>>);

impl Loopback {
    fn node(&self) -> Option<Arc<Node>> {
        self.0.get().and_then(Weak::upgrade)
    }
}

impl Broadcaster for Loopback {
    fn broadcast(&self, msg: &StreamMessage) {
        if let Some(node) = self.node() {
            node.broadcast(msg);
        }
    }

    fn broadcast_command(&self, msg: &RemoteCommandMessage) {
        if let Some(node) = self.node() {
            node.execute_remote_command(msg);
        }
    }

    fn subscribe(&self, _: &str) {}

    fn unsubscribe(&self, _: &str) {}
}

struct App {
    conf: Config,
    addr: SocketAddr,
    _server: Arc<HttpServer>,
    _node: Arc<Node>,
}

/// What `cli.Runner` mounts when Pusher is enabled, on a fresh server.
fn app() -> App {
    let mut conf = Config::new();
    conf.app_id = "my-test-id".into();
    conf.app_key = "278d425bdf160c739803".into();
    conf.secret = "7ad3773142a6692b25b8".into();

    let node = Node::new(crate::node::Config::new(), Vec::new());
    let loopback = Arc::new(Loopback::default());
    loopback.0.set(Arc::downgrade(&node)).ok();
    let broker: Arc<dyn Broker> = Arc::new(core_broker::Memory::new(
        Some(loopback),
        Some(node.clone()),
        &core_broker::Config::new(),
        Arc::new(core_broker::SystemClock),
    ));
    broker.start().unwrap();
    node.set_broker(broker.clone());

    let mut router = RouterController::new(Arc::new(NullController));
    router.route(CHANNEL_NAME, Arc::new(Controller::new(Some(broker.clone()), conf.clone()))).unwrap();
    node.set_controller(Arc::new(router));
    node.start().unwrap();

    let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
    server.setup_handler(&conf.websocket_path(), websocket_handler(node.clone(), &conf, &ws::Config::new(), None, 15));
    let api = Arc::new(RestApi::new(Arc::new(NodeBroadcastHandler(node.clone())), broker, conf.clone()));
    server.setup_handler(&conf.api_path(), api.into_handler());
    let addr = start(&server);

    App { conf, addr, _server: server, _node: node }
}

type Client = WebSocket<TcpStream>;

impl App {
    fn connect(&self, socket_id: &str) -> Client {
        let uri: tungstenite::http::Uri = format!("ws://{}{}", self.addr, self.conf.websocket_path()).parse().unwrap();
        let req = ClientRequestBuilder::new(uri).with_header("X-Request-ID", socket_id);
        let (mut ws, _resp) = tungstenite::client(req, connect(self.addr)).expect("handshake");
        let welcome = format!(
            r#"{{"event":"pusher:connection_established","data":"{{\"socket_id\":\"{socket_id}\",\"activity_timeout\":30}}"}}"#
        );
        assert_eq!(welcome, recv(&mut ws));
        ws
    }

    fn subscribe(&self, ws: &mut Client, socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
        let verifier = Verifier::new(&self.conf.app_key, &self.conf.secret);
        let auth = match channel_data {
            Some(data) => format!("{}:{}", self.conf.app_key, verifier.sign(&format!("{socket_id}:{channel}:{data}"))),
            None => format!("{}:{}", self.conf.app_key, verifier.sign(&format!("{socket_id}:{channel}"))),
        };
        let mut data = json!({"channel": channel, "auth": auth});
        if let Some(d) = channel_data {
            data["channel_data"] = json!(d);
        }
        send(ws, &json!({"event": "pusher:subscribe", "data": data}).to_string());
        recv(ws)
    }

    fn api(&self, method: &str, target: &str, body: &[u8]) -> (u16, String) {
        let res = request(self.addr, &format!("{method} {target} HTTP/1.1\r\nHost: {}", self.addr), body);
        (res.status, String::from_utf8(res.body).unwrap())
    }
}

fn recv(ws: &mut Client) -> String {
    let deadline = std::time::Instant::now() + IO_DEADLINE;
    loop {
        assert!(std::time::Instant::now() < deadline, "no frame in time");
        match ws.read().expect("a frame") {
            tungstenite::Message::Text(t) => return t.as_str().to_owned(),
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
}

fn send(ws: &mut Client, text: &str) {
    ws.send(tungstenite::Message::text(text)).expect("send");
}

#[test]
fn serves_the_pusher_protocol_and_rest_api() {
    let app = app();

    // Public channel + REST event.
    let mut a = app.connect("1234.1234");
    assert_eq!(
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"all-chat"}"#,
        app.subscribe(&mut a, "1234.1234", "all-chat", None)
    );

    let body = br#"{"name":"test-event","channel":"all-chat","data":"{\"message\":\"hello\"}"}"#;
    let target = signed_target(&app.conf, "POST", "/apps/my-test-id/events", Some(body));
    assert_eq!((200, "{}".to_owned()), app.api("POST", &target, body));
    assert_eq!(r#"{"event":"test-event","data":"{\"message\":\"hello\"}","channel":"all-chat"}"#, recv(&mut a));

    // Ping/pong.
    send(&mut a, r#"{"event":"pusher:ping","data":{}}"#);
    assert_eq!(r#"{"event":"pusher:pong"}"#, recv(&mut a));

    // Private channel: a bad signature is rejected; client events reach the others.
    let mut b = app.connect("1234.4321");
    send(&mut b, r#"{"event":"pusher:subscribe","data":{"channel":"private-foobar","auth":"bla-bla"}}"#);
    assert_eq!(r#"{"event":"pusher:error","data":{"message":"rejected","code":4009}}"#, recv(&mut b));

    let ok = r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-foobar"}"#;
    assert_eq!(ok, app.subscribe(&mut a, "1234.1234", "private-foobar", None));
    assert_eq!(ok, app.subscribe(&mut b, "1234.4321", "private-foobar", None));

    send(&mut a, r#"{"event":"client-message","channel":"private-foobar","data":{"text":"hi","n":1.50}}"#);
    assert_eq!(r#"{"event":"client-message","data":{"n":1.5,"text":"hi"},"channel":"private-foobar"}"#, recv(&mut b));

    // Presence: confirmation with members, member_added for the others, users API.
    let data_a = r#"{"user_id":"10","user_info":{"name":"Mr. Channels"}}"#;
    assert_eq!(
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":1,\"ids\":[\"10\"],\"hash\":{\"10\":{\"name\":\"Mr. Channels\"}}}}","channel":"presence-room"}"#,
        app.subscribe(&mut a, "1234.1234", "presence-room", Some(data_a))
    );
    // The join is broadcast to every member, the joining one included (as upstream).
    assert_eq!(
        r#"{"event":"pusher_internal:member_added","data":"{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}","channel":"presence-room"}"#,
        recv(&mut a)
    );
    let data_b = r#"{"user_id":"42","user_info":{"name":"Lolo"}}"#;
    let confirmed = app.subscribe(&mut b, "1234.4321", "presence-room", Some(data_b));
    assert!(confirmed.contains(r#"\"count\":2"#), "{confirmed}");
    assert_eq!(
        r#"{"event":"pusher_internal:member_added","data":"{\"user_id\":\"42\",\"user_info\":{\"name\":\"Lolo\"}}","channel":"presence-room"}"#,
        recv(&mut a)
    );

    let target = signed_target(&app.conf, "GET", "/apps/my-test-id/channels/presence-room/users", None);
    let (status, users) = app.api("GET", &target, b"");
    assert_eq!(200, status);
    let users: Value = serde_json::from_str(&users).unwrap();
    let mut ids: Vec<&str> = users["users"].as_array().unwrap().iter().map(|u| u["id"].as_str().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(vec!["10", "42"], ids);

    let target = signed_target(&app.conf, "GET", "/apps/my-test-id/channels/private-foobar/users", None);
    assert_eq!(400, app.api("GET", &target, b"").0);

    // Unauthenticated requests and unsupported methods.
    assert_eq!(401, app.api("POST", "/apps/my-test-id/events?auth_timestamp=1", body).0);
    assert_eq!(422, app.api("DELETE", "/apps/my-test-id/events", b"").0);
}
