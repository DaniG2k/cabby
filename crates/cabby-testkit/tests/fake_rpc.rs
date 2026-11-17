//! Self-tests of the fake RPC application and the other test endpoints.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use cabby_core::protos;
use cabby_server::rpc::{Context, RpcClient};
use cabby_testkit::fake_http::HttpRecorder;
use cabby_testkit::fake_rpc::{CallState, FakeRpc, FakeRpcOptions, Method, Reply, Transport};
use cabby_testkit::rpc_doubles::TonicRpcClient;
use cabby_testkit::runtime::{DEADLINE, block_on_within, spawn};
use cabby_testkit::udp::UdpSink;
use tonic::Code;

fn connect_request(url: &str) -> protos::ConnectionRequest {
    protos::ConnectionRequest { env: Some(protos::Env { url: url.into(), ..Default::default() }) }
}

fn ctx_with(md: &[(&str, &str)]) -> Context {
    Context { metadata: md.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect(), ..Default::default() }
}

#[test]
fn grpc_replies_from_script_and_records_request_and_metadata() {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).with_request(connect_request("/cable")).reply(protos::ConnectionResponse {
        identifiers: "user=john".into(),
        status: protos::Status::Success as i32,
        ..Default::default()
    });
    let client = TonicRpcClient::new(&fake.grpc_target());
    let ctx = ctx_with(&[("sid", "42"), ("protov", "v1")]);

    let res = block_on_within(DEADLINE, client.connect(&ctx, connect_request("/cable"))).expect("reply");

    assert_eq!("user=john", res.identifiers);
    let calls = fake.calls();
    assert_eq!(1, calls.len());
    assert_eq!(Transport::Grpc, calls[0].transport);
    assert_eq!(&connect_request("/cable"), calls[0].request.connect());
    assert_eq!(vec!["42"], calls[0].meta_all("sid"));
    assert_eq!(Some("v1"), calls[0].meta("protov"));
    assert_eq!(CallState::Replied, calls[0].state);
    assert!(!calls[0].unexpected);
    assert_eq!(1, fake.grpc_connections());
}

#[test]
fn unmatched_calls_fail_and_are_recorded_as_unexpected() {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).with_request(connect_request("/a")).reply(protos::ConnectionResponse::default());
    let client = TonicRpcClient::new(&fake.grpc_target());

    let err = block_on_within(DEADLINE, client.connect(&Context::default(), connect_request("/b"))).unwrap_err();

    assert_eq!(Code::Unimplemented, err.code);
    assert_eq!(1, fake.unexpected_calls().len());
}

#[test]
fn once_expectations_are_consumed_in_order() {
    let fake = FakeRpc::start();
    fake.on(Method::Command).once().reply(Reply::status(Code::ResourceExhausted, "slow down"));
    fake.on(Method::Command).reply(protos::CommandResponse { transmissions: vec!["ok".into()], ..Default::default() });
    let client = TonicRpcClient::new(&fake.grpc_target());
    let ctx = Context::default();

    let first = block_on_within(DEADLINE, client.command(&ctx, protos::CommandMessage::default())).unwrap_err();
    let second = block_on_within(DEADLINE, client.command(&ctx, protos::CommandMessage::default())).unwrap();
    let third = block_on_within(DEADLINE, client.command(&ctx, protos::CommandMessage::default())).unwrap();

    assert_eq!(Code::ResourceExhausted, first.code);
    assert_eq!("slow down", first.message);
    assert_eq!(vec!["ok".to_owned()], second.transmissions);
    assert_eq!(second, third);
    assert_eq!(3, fake.call_count(Method::Command));
}

#[test]
fn computed_replies_see_the_call() {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(Reply::with(|call| {
        protos::ConnectionResponse {
            identifiers: call.meta("sid").unwrap_or_default().to_owned(),
            ..Default::default()
        }
        .into()
    }));
    let client = TonicRpcClient::new(&fake.grpc_target());

    let res = block_on_within(DEADLINE, client.connect(&ctx_with(&[("sid", "s-1")]), connect_request("/"))).unwrap();

    assert_eq!("s-1", res.identifiers);
}

#[test]
fn gates_hold_calls_until_released() {
    let fake = FakeRpc::start();
    let gate = fake.gate();
    fake.on(Method::Disconnect).held_by(&gate).reply(protos::DisconnectResponse::default());
    let client = TonicRpcClient::new(&fake.grpc_target());

    let calls: Vec<_> = (0..3)
        .map(|_| {
            let c = client.clone();
            spawn(async move { c.disconnect(&Context::default(), protos::DisconnectRequest::default()).await })
        })
        .collect();
    gate.await_held(3, DEADLINE);
    assert_eq!(0, gate.passed());

    gate.release_one();
    fake.await_in_state(CallState::Replied, 1, DEADLINE);
    assert_eq!(2, gate.held());
    assert_eq!(1, gate.passed());
    gate.open();
    for c in calls {
        block_on_within(DEADLINE, c).expect("task").expect("reply");
    }
    assert_eq!(3, fake.calls().iter().filter(|c| c.state == CallState::Replied).count());
    assert_eq!(3, gate.passed());
}

#[test]
fn calls_abandoned_by_the_client_are_marked() {
    let fake = FakeRpc::start();
    let gate = fake.gate();
    fake.on(Method::Connect).held_by(&gate).reply(protos::ConnectionResponse::default());
    let client = TonicRpcClient::new(&fake.grpc_target());
    let ctx = Context::default();
    let cancel = ctx.cancel.clone();

    let call = spawn(async move { client.connect(&ctx, connect_request("/")).await });
    gate.await_held(1, DEADLINE);
    cancel.cancel();
    let err = block_on_within(DEADLINE, call).expect("task").unwrap_err();

    assert_eq!(Code::Cancelled, err.code);
    assert_eq!(CallState::Abandoned, fake.await_call_settled(0, DEADLINE));
    assert_eq!(0, gate.held());
}

#[test]
fn dropping_the_fake_unblocks_held_calls() {
    let fake = FakeRpc::start();
    let gate = fake.gate();
    fake.on(Method::Connect).held_by(&gate).reply(protos::ConnectionResponse::default());
    let client = TonicRpcClient::new(&fake.grpc_target());

    let call = spawn(async move { client.connect(&Context::default(), connect_request("/")).await });
    gate.await_held(1, DEADLINE);
    drop(fake);

    assert!(block_on_within(DEADLINE, call).expect("task").is_err());
}

#[test]
fn max_concurrent_streams_option_is_accepted() {
    let fake = FakeRpc::start_with(FakeRpcOptions { grpc_max_concurrent_streams: Some(1) });
    fake.on(Method::Connect).reply(protos::ConnectionResponse::default());
    let client = TonicRpcClient::new(&fake.grpc_target());
    for _ in 0..3 {
        block_on_within(DEADLINE, client.connect(&Context::default(), connect_request("/"))).unwrap();
    }
    assert_eq!(3, fake.call_count(Method::Connect));
}

/// Sends one HTTP/1.1 request on `stream` and returns (status, body).
fn http_exchange(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n", body.len());
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    stream.write_all(req.as_bytes()).unwrap();
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).expect("reading HTTP response");
        assert!(n > 0, "connection closed before a full response");
        buf.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&buf).into_owned();
        if let Some(head_end) = text.find("\r\n\r\n") {
            let head = &text[..head_end];
            let len = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase().strip_prefix("content-length: ").map(|v| v.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if buf.len() >= head_end + 4 + len {
                let status = head.split(' ').nth(1).unwrap().parse().unwrap();
                return (status, text[head_end + 4..head_end + 4 + len].to_owned());
            }
        }
    }
}

fn open(addr: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&addr, DEADLINE).expect("connecting")
}

#[test]
fn http_rpc_decodes_requests_and_records_headers() {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(protos::ConnectionResponse {
        identifiers: "id".into(),
        status: protos::Status::Success as i32,
        transmissions: vec!["welcome".into()],
        ..Default::default()
    });
    let mut s = open(fake.http_addr());

    let (status, body) = http_exchange(
        &mut s,
        "POST",
        "/_anycable/connect",
        &[("Content-Type", "application/json"), ("X-Anycable-Meta-Sid", "42")],
        r#"{"env":{"url":"ws://x/cable","headers":{"cookie":"a=b"}}}"#,
    );

    assert_eq!(200, status);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!("id", json["identifiers"]);
    assert_eq!(1, json["status"]);
    let call = &fake.calls()[0];
    assert_eq!(Transport::Http, call.transport);
    let env = call.request.connect().env.as_ref().unwrap();
    assert_eq!("ws://x/cable", env.url);
    assert_eq!(Some(&"a=b".to_owned()), env.headers.get("cookie"));
    assert_eq!(Some("42"), call.meta("x-anycable-meta-sid"));
    assert_eq!("/_anycable/connect", call.http.as_ref().unwrap().path);
}

#[test]
fn http_rpc_status_mapping_stray_requests_and_keep_alive() {
    let fake = FakeRpc::start();
    fake.on(Method::Command).once().reply(Reply::status(Code::Unauthenticated, "no"));
    fake.on(Method::Command).once().reply(Reply::http(422, "bad"));
    let mut s = open(fake.http_addr());

    assert_eq!(401, http_exchange(&mut s, "POST", "/command", &[], "{}").0);
    assert_eq!((422, "bad".to_owned()), http_exchange(&mut s, "POST", "/command", &[], "{}"));
    assert_eq!(404, http_exchange(&mut s, "POST", "/command", &[], "{}").0); // unexpected
    assert_eq!(404, http_exchange(&mut s, "POST", "/other", &[], "{}").0);
    assert_eq!(405, http_exchange(&mut s, "GET", "/connect", &[], "").0);
    assert_eq!(400, http_exchange(&mut s, "POST", "/connect", &[], "not json").0);

    assert_eq!(3, fake.call_count(Method::Command));
    assert_eq!(1, fake.unexpected_calls().len());
    assert_eq!(3, fake.stray_http().len());
    assert_eq!(1, fake.http_connections(), "keep-alive reuses the connection");
}

#[test]
fn http_recorder_records_requests() {
    let rec = HttpRecorder::start();
    let mut s = open(rec.addr());

    assert_eq!(200, http_exchange(&mut s, "POST", "/", &[("Authorization", "secret")], r#"{"event":"boot"}"#).0);
    rec.respond_with(401);
    assert_eq!(401, http_exchange(&mut s, "POST", "/x", &[], "{}").0);

    let reqs = rec.await_requests(2, DEADLINE);
    assert_eq!("boot", reqs[0].body_json()["event"]);
    assert_eq!(Some("secret"), reqs[0].header("authorization"));
    assert_eq!("/x", reqs[1].path);
}

#[test]
fn udp_sink_receives_datagrams() {
    let sink = UdpSink::bind();
    let tx = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    tx.send_to(b"metric:1|c", sink.addr()).unwrap();
    assert_eq!(Some(b"metric:1|c".to_vec()), sink.recv(DEADLINE));
    assert_eq!(None, sink.recv(Duration::from_millis(1)));
}
