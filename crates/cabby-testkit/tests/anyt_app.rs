//! Self-tests of the AnyT RPC application (`cabby_testkit::anyt_app`), driven
//! through its gRPC and HTTP RPC endpoints. Expected values follow the Ruby
//! stack the application reimplements (anyt-core 1.4.0, anycable 1.6.4,
//! anycable-rails 1.6.2, Action Cable 7.0).

use std::collections::HashMap;

use cabby_core::protos;
use cabby_server::rpc::{Context, RpcClient};
use cabby_testkit::anyt_app::{AnytApp, AppOptions, active_support_json, channel, message_matches};
use cabby_testkit::fake_http::HttpRecorder;
use cabby_testkit::publish::Target;
use cabby_testkit::raw_http;
use cabby_testkit::rpc_doubles::TonicRpcClient;
use cabby_testkit::runtime::{DEADLINE, block_on_within};
use serde_json::{Value, json};

const SUCCESS: i32 = protos::Status::Success as i32;
const FAILURE: i32 = protos::Status::Failure as i32;
const ERROR: i32 = protos::Status::Error as i32;

struct Fixture {
    app: AnytApp,
    client: TonicRpcClient,
    broadcasts: HttpRecorder,
}

fn fixture() -> Fixture {
    let app = AnytApp::start(AppOptions {
        http_rpc_secret: None,
        target_host: "localhost".into(),
        stream_prefix: String::new(),
    });
    let broadcasts = HttpRecorder::start();
    broadcasts.respond_with(201);
    app.set_broadcaster(Target::Http { addr: broadcasts.addr(), path: "/_broadcast".into(), key: None });
    let client = TonicRpcClient::new(&app.grpc_target());
    Fixture { app, client, broadcasts }
}

fn ctx(sid: &str) -> Context {
    Context { metadata: [("sid".to_owned(), sid.to_owned())].into_iter().collect(), ..Default::default() }
}

fn env(url: &str, headers: &[(&str, &str)], istate: HashMap<String, String>) -> Option<protos::Env> {
    Some(protos::Env {
        url: url.into(),
        headers: headers.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect(),
        istate,
        ..Default::default()
    })
}

impl Fixture {
    fn connect(&self, url: &str, headers: &[(&str, &str)]) -> protos::ConnectionResponse {
        let req = protos::ConnectionRequest { env: env(url, headers, HashMap::new()) };
        block_on_within(DEADLINE, self.client.connect(&ctx("sid-1"), req)).expect("connect reply")
    }

    fn command(
        &self,
        command: &str,
        identifier: &str,
        data: &str,
        ids: &str,
        url: &str,
        istate: HashMap<String, String>,
    ) -> protos::CommandResponse {
        let req = protos::CommandMessage {
            command: command.into(),
            identifier: identifier.into(),
            connection_identifiers: ids.into(),
            data: data.into(),
            env: env(url, &[], istate),
        };
        block_on_within(DEADLINE, self.client.command(&ctx("sid-1"), req)).expect("command reply")
    }

    fn broadcast_bodies(&self) -> Vec<Value> {
        assert!(self.app.errors().is_empty(), "app errors: {:?}", self.app.errors());
        self.broadcasts.requests().iter().map(|r| r.body_json()).collect()
    }
}

fn id(class: &str) -> String {
    format!("{{\"channel\":\"{}\"}}", channel(class))
}

fn parse(s: &str) -> Value {
    serde_json::from_str(s).expect("JSON transmission")
}

#[test]
fn connect_welcomes_with_sid_and_identifiers() {
    let f = fixture();

    let res = f.connect("ws://localhost:8080/cable", &[]);
    assert_eq!(SUCCESS, res.status);
    assert_eq!("{}", res.identifiers);
    assert_eq!(vec![r#"{"type":"welcome","sid":"sid-1"}"#.to_owned()], res.transmissions);

    let res = f.connect("ws://localhost:8080/cable?test=uid&uid=26", &[]);
    assert_eq!(SUCCESS, res.status);
    assert_eq!(r#"{"uid":"26"}"#, res.identifiers);
}

#[test]
fn connect_handlers_reject_with_disconnect_message() {
    let f = fixture();
    let rejected = |res: protos::ConnectionResponse| {
        assert_eq!(FAILURE, res.status, "{res:?}");
        assert_eq!(
            vec![r#"{"type":"disconnect","reason":"unauthorized","reconnect":false}"#.to_owned()],
            res.transmissions
        );
    };

    rejected(f.connect("ws://localhost/cable?test=uid", &[]));
    rejected(f.connect("ws://localhost/cable?test=reasons&reason=unauthorized", &[]));
    rejected(f.connect("ws://localhost/cable?test=cookies", &[("cookie", "username=john")]));
    rejected(f.connect("ws://localhost/cable?test=headers", &[("x-api-token", "abd")]));
    rejected(f.connect("ws://otherhost/cable?test=request_url", &[]));

    assert_eq!(SUCCESS, f.connect("ws://localhost/cable?test=reasons&reason=server_restart", &[]).status);
    assert_eq!(
        SUCCESS,
        f.connect("ws://localhost/cable?test=cookies", &[("cookie", "a=b; username=john green")]).status
    );
    assert_eq!(SUCCESS, f.connect("ws://localhost/cable?test=cookies", &[("cookie", "username=john+green")]).status);
    assert_eq!(SUCCESS, f.connect("ws://localhost/cable?test=headers", &[("x-api-token", "abc")]).status);
    assert_eq!(SUCCESS, f.connect("ws://localhost:8080/cable?test=request_url", &[]).status);
}

#[test]
fn subscribe_confirms_rejects_and_transmits_in_order() {
    let f = fixture();
    let url = "ws://localhost/cable";

    let res = f.command("subscribe", &id("SingleStreamChannel"), "", "{}", url, HashMap::new());
    assert_eq!(SUCCESS, res.status);
    assert_eq!(vec!["a"], res.streams);
    assert!(!res.stop_streams);
    assert_eq!(
        vec![format!(r#"{{"identifier":{},"type":"confirm_subscription"}}"#, json!(id("SingleStreamChannel")))],
        res.transmissions
    );

    let res = f.command("subscribe", &id("SubscriptionAknowledgementRejectorChannel"), "", "{}", url, HashMap::new());
    assert_eq!(FAILURE, res.status);
    assert!(res.stop_streams, "reject_subscription runs stop_all_streams");
    assert_eq!("reject_subscription", parse(&res.transmissions[0])["type"]);

    let res = f.command("subscribe", &id("SubscriptionTransmissionsChannel"), "", "{}", url, HashMap::new());
    let types: Vec<Value> = res.transmissions.iter().map(|t| parse(t)).collect();
    assert_eq!(json!("hello"), types[0]["message"]);
    assert_eq!(json!("world"), types[1]["message"]);
    assert_eq!(json!("confirm_subscription"), types[2]["type"]);

    // `delegate :params, to: :connection`: the request's query string.
    let res =
        f.command("subscribe", &id("RequestChannel"), "", "{}", "ws://localhost/cable?token=secret", HashMap::new());
    assert_eq!(SUCCESS, res.status);
    let res = f.command("subscribe", &id("RequestChannel"), "", "{}", url, HashMap::new());
    assert_eq!(FAILURE, res.status);

    let res = f.command("subscribe", r#"{"channel":"Missing"}"#, "", "{}", url, HashMap::new());
    assert_eq!(ERROR, res.status);
    assert_eq!("Channel not found: Missing", res.error_msg);
}

#[test]
fn channel_state_round_trips_through_istate() {
    let f = fixture();
    let url = "ws://localhost/cable";
    let ident = format!(r#"{{"channel":"{}","name":"chipolino"}}"#, channel("ChannelStateChannel"));

    let res = f.command("subscribe", &ident, "", "{}", url, HashMap::new());
    let istate = res.env.expect("env").istate;
    assert_eq!(Some(&r#"{"name":"chipolino"}"#.to_owned()), istate.get("user"));
    assert_eq!(Some(&"1".to_owned()), istate.get("count"));
    assert_eq!(vec!["state_counts"], res.streams);

    let res = f.command("message", &ident, r#"{"action":"tick"}"#, "{}", url, istate);
    assert_eq!(SUCCESS, res.status);
    assert!(message_matches(
        &json!({"identifier": ident, "message": {"count": 3, "name": "chipolino"}}),
        &parse(&res.transmissions[0])
    ));
    // Only changed fields are returned.
    assert_eq!(HashMap::from([("count".to_owned(), "3".to_owned())]), res.env.expect("env").istate);
}

#[test]
fn unsubscribe_runs_callbacks_and_stops_streams() {
    let f = fixture();
    let ident =
        format!(r#"{{"channel":"{}","name":"chipollone","notify_disconnect":true}}"#, channel("ChannelStateChannel"));
    let istate = HashMap::from([("user".to_owned(), r#"{"name":"chipollone"}"#.to_owned())]);

    let res = f.command("unsubscribe", &ident, "", "{}", "ws://localhost/cable", istate);

    assert_eq!(SUCCESS, res.status);
    assert!(res.stop_streams);
    assert_eq!(
        vec![json!({"stream": "state_counts", "data": "{\"data\":\"user left: chipollone\"}"})],
        f.broadcast_bodies()
    );
}

#[test]
fn disconnect_unsubscribes_every_subscription() {
    let f = fixture();
    let c = channel("RequestCChannel");
    let req = protos::DisconnectRequest {
        identifiers: "{}".into(),
        subscriptions: vec![format!(r#"{{"channel":"{c}","id":1}}"#), format!(r#"{{"channel":"{c}","id":2}}"#)],
        env: env("ws://localhost/cable", &[], HashMap::new()),
    };

    let res = block_on_within(DEADLINE, f.client.disconnect(&ctx("sid-1"), req)).expect("disconnect reply");

    assert_eq!(SUCCESS, res.status);
    let data: Vec<Value> = f.broadcast_bodies().iter().map(|b| b["data"].clone()).collect();
    assert_eq!(vec![json!("{\"data\":\"user left1\"}"), json!("{\"data\":\"user left2\"}")], data);
}

#[test]
fn broadcast_to_others_excludes_the_sender_socket() {
    let f = fixture();
    let ident = id("BroadcastWithOptionsChannel");

    let res = f.command(
        "message",
        &ident,
        r#"{"action":"speak","content":"The Other Side"}"#,
        "{}",
        "ws://h/cable",
        HashMap::new(),
    );

    assert_eq!(SUCCESS, res.status);
    let raw = String::from_utf8(f.broadcasts.requests()[0].body.clone()).unwrap();
    assert_eq!(r#"{"stream":"a","data":"{\"content\":\"The Other Side\"}","meta":{"exclude_socket":"sid-1"}}"#, raw);
}

#[test]
fn whisper_and_presence_use_istate_keys() {
    let f = fixture();

    let res = f.command("subscribe", &id("WhisperChannel"), "", "{}", "ws://h/cable", HashMap::new());
    assert_eq!(Some(&"a".to_owned()), res.env.expect("env").istate.get("$w"));

    let url = "ws://h/cable?test=presence_subscribe&user_id=2024&name=Bart";
    let res = f.command("subscribe", &id("PresenceImplicitChannel"), "", r#"{"uid":"2024"}"#, url, HashMap::new());
    let p = res.presence.expect("presence");
    assert_eq!(("join", "2024", r#"{"name":"Bart"}"#), (p.r#type.as_str(), p.id.as_str(), p.info.as_str()));
    assert_eq!(Some(&"a".to_owned()), res.env.expect("env").istate.get("$p"));

    let res = f.command("subscribe", &id("PresenceExplicitChannel"), "", r#"{"uid":"2024"}"#, url, HashMap::new());
    let p = res.presence.expect("presence");
    assert_eq!(("join", "4202", r#"{"whois":"Bart"}"#), (p.r#type.as_str(), p.id.as_str(), p.info.as_str()));
    assert_eq!(vec!["b", "b_p"], res.streams);

    // Without uid, no presence.
    let res = f.command("subscribe", &id("PresenceImplicitChannel"), "", "{}", "ws://h/cable", HashMap::new());
    assert!(res.presence.is_none());

    let res = f.command(
        "message",
        &id("PresenceChannel"),
        r#"{"action":"unfollow"}"#,
        r#"{"uid":"2024"}"#,
        url,
        HashMap::new(),
    );
    let p = res.presence.expect("presence");
    assert_eq!(("leave", "2024"), (p.r#type.as_str(), p.id.as_str()));
}

#[test]
fn benchmark_channel_echoes_and_broadcasts() {
    let f = fixture();
    let ident = r#"{"channel":"BenchmarkChannel"}"#;

    let res = f.command("subscribe", ident, "", "{}", "ws://h/cable", HashMap::new());
    assert_eq!(vec!["all"], res.streams);

    let res = f.command(
        "message",
        ident,
        r#"{"message":"hello","action":"broadcast"}"#,
        "{}",
        "ws://h/cable",
        HashMap::new(),
    );
    assert_eq!(json!({"message": "hello", "action": "broadcastResult"}), parse(&res.transmissions[0])["message"]);
    let body = &f.broadcast_bodies()[0];
    assert_eq!(json!("all"), body["stream"]);
    assert_eq!(json!({"message": "hello", "action": "broadcast"}), parse(body["data"].as_str().unwrap()));
}

#[test]
fn http_rpc_requires_the_secret() {
    let app = AnytApp::start(AppOptions {
        http_rpc_secret: Some("rpc_secret".into()),
        target_host: "h".into(),
        stream_prefix: String::new(),
    });
    let body = br#"{"env":{"url":"ws://h/cable"}}"#;
    let addr = app.rpc().http_addr();
    let post = |headers: &[(&str, &str)]| {
        raw_http::request(addr, "POST", "/_anycable/connect", headers, Some(body)).expect("HTTP RPC").status
    };

    assert_eq!(401, post(&[("Content-Type", "application/json")]));
    assert_eq!(200, post(&[("Content-Type", "application/json"), ("Authorization", "Bearer rpc_secret")]));
}

#[test]
fn active_support_json_escapes_html_entities() {
    let expected = "\"\\u003cscript\\u003ea \\u0026 b\\u003c/script\\u003e\"";
    assert_eq!(expected, active_support_json(&json!("<script>a & b</script>")));
}

#[test]
fn stream_prefix_applies_to_streams_and_broadcasts() {
    let app =
        AnytApp::start(AppOptions { http_rpc_secret: None, target_host: "h".into(), stream_prefix: "t1:".into() });
    let broadcasts = HttpRecorder::start();
    broadcasts.respond_with(201);
    app.set_broadcaster(Target::Http { addr: broadcasts.addr(), path: "/_broadcast".into(), key: None });
    let client = TonicRpcClient::new(&app.grpc_target());
    let req = protos::CommandMessage {
        command: "subscribe".into(),
        identifier: id("MultipleStreamsChannel"),
        connection_identifiers: "{}".into(),
        data: String::new(),
        env: env("ws://h/cable", &[], HashMap::new()),
    };

    let res = block_on_within(DEADLINE, client.command(&ctx("sid-1"), req)).expect("command reply");
    app.broadcast("a", &json!({"data": "X"}));

    assert_eq!(vec!["t1:a", "t1:b"], res.streams);
    assert_eq!(json!("t1:a"), broadcasts.requests()[0].body_json()["stream"]);
}
