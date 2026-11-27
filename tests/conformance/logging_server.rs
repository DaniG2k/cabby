//! Log lines of the server periphery (review finding H7, logging parity): the
//! RPC controller, the WebSocket, SSE, Durable Streams and API handlers, the
//! streams controller, the JWT identifier and the metrics writers. Every
//! scenario runs against both servers and asserts the same lines (time, node
//! id and session ids normalized), so Go pins the expected text.

use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use cabby_testkit::jwt::sign_raw;
use cabby_testkit::ports;
use cabby_testkit::raw_http::{self, EventStream};
use cabby_testkit::sut::{Sut, handshake_status};

/// How long a scenario waits for its log lines.
const LOG_DEADLINE: Duration = Duration::from_secs(20);

/// The server's output with the time, `nodeid` and `sid` values normalized:
/// `DBG msg nodeid=N context=... sid=S ...`.
fn normalized(sut: &Sut) -> Vec<String> {
    sut.process
        .output()
        .lines()
        .map(|l| {
            // `2006-01-02 15:04:05.000 LVL ...`
            let rest = if l.len() > 24 && l.as_bytes()[4] == b'-' && l.as_bytes()[10] == b' ' { &l[24..] } else { l };
            let mut out = String::with_capacity(rest.len());
            let mut words = rest.split(' ').peekable();
            while let Some(w) = words.next() {
                if let Some(_v) = w.strip_prefix("nodeid=") {
                    out.push_str("nodeid=N");
                } else if let Some(_v) = w.strip_prefix("sid=") {
                    out.push_str("sid=S");
                } else {
                    out.push_str(w);
                }
                if words.peek().is_some() {
                    out.push(' ');
                }
            }
            out
        })
        .collect()
}

/// Waits until every expected line is in the server's (normalized) output.
fn assert_logged(sut: &Sut, expected: &[String]) {
    let deadline = Instant::now() + LOG_DEADLINE;
    loop {
        let lines = normalized(sut);
        let missing: Vec<&String> = expected.iter().filter(|e| !lines.contains(e)).collect();
        if missing.is_empty() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("missing log lines:\n{}\n\nSUT output:\n{}", missing_list(&missing), sut.process.output());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn missing_list(lines: &[&String]) -> String {
    lines.iter().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n")
}

fn start(args: &[&str]) -> Sut {
    let mut all = vec!["--debug", "--broadcast_adapter=http"];
    all.extend_from_slice(args);
    Sut::start(&all)
}

/// Sends a raw request and reads what comes back until the server closes or
/// a short timeout.
fn raw(addr: SocketAddr, request: &str) {
    use std::io::{Read, Write};
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_millis(500))).expect("timeout");
    s.write_all(request.as_bytes()).expect("write");
    let mut buf = [0u8; 4096];
    while matches!(s.read(&mut buf), Ok(n) if n > 0) {}
}

/// regression-for: rpc/rpc_test.go#TestAuthenticate/Error
/// note: Review finding H7: an unreachable HTTP RPC server logs each retried call (`RPC failed`, `context=rpc` with the session id, the gRPC code and Go's dial error) and the failed session (`WebSocket session failed`, the runner's logger with the session id only).
#[test]
fn unreachable_http_rpc_logs_failed_calls() {
    // Reserved, so nothing listens there during the test.
    let port = ports::reserve();
    let host = format!("http://127.0.0.1:{port}/_anycable");
    let sut = start(&[&format!("--rpc_host={host}")]);
    let _ = handshake_status(sut.addr(), "/cable", &[]);
    let cause = format!("Post \\\"{host}/connect\\\": dial tcp 127.0.0.1:{port}: connect: connection refused");
    assert_logged(
        &sut,
        &[
            format!("DBG RPC failed nodeid=N context=rpc sid=S code=Unavailable error=\"{cause}\""),
            format!(
                "ERR WebSocket session failed nodeid=N sid=S error=\"failed to authenticate, cause: synthetic.wrap: \
                 retry interrupted, cause: context deadline exceeded (hidden: rpc error: code = Unavailable desc = \
                 {cause})\""
            ),
        ],
    );
}

/// regression-for: rpc/rpc_test.go#TestAuthenticate/Error
/// note: Review finding H7: an unreachable gRPC server: `RPC failed` with grpc-go's dial error, then the client helper's `connection is lost, trying to reconnect immediately` (`context=rpc impl=grpc`).
#[test]
fn unreachable_grpc_rpc_logs_failed_calls() {
    // Reserved, so nothing listens there during the test.
    let port = ports::reserve();
    let sut = start(&[&format!("--rpc_host=127.0.0.1:{port}")]);
    let _ = handshake_status(sut.addr(), "/cable", &[]);
    assert_logged(
        &sut,
        &[
            format!(
                "DBG RPC failed nodeid=N context=rpc sid=S code=Unavailable error=\"connection error: desc = \
                 \\\"transport: Error while dialing: dial tcp 127.0.0.1:{port}: connect: connection refused\\\"\""
            ),
            "WRN connection is lost, trying to reconnect immediately nodeid=N context=rpc impl=grpc".to_owned(),
        ],
    );
}

/// regression-for: identity/jwt_test.go#TestJWTIdentifierIdentify/with_invalid_token
/// note: Review finding H7: the JWT identifier logs why a token is rejected (`context=jwt`), with golang-jwt's error texts.
#[test]
fn jwt_identifier_logs_rejected_tokens() {
    let sut = start(&["--norpc", "--jwt_secret=abc"]);
    let addr = sut.addr();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock").as_secs();
    let hs = r#"{"alg":"HS256"}"#;
    let tokens = [
        "abc".to_owned(),
        "!!.e30.e30".to_owned(),
        sign_raw(hs, r#"{"ext":"x"}"#, "wrong"),
        sign_raw(hs, &format!(r#"{{"ext":"x","exp":{}}}"#, now - 10), "abc"),
        sign_raw(hs, &format!(r#"{{"ext":"x","nbf":{}}}"#, now + 1000), "abc"),
        sign_raw(hs, r#"{"ext":"x","exp":"soon"}"#, "abc"),
        sign_raw(r#"{"alg":"RS256"}"#, r#"{"ext":"x"}"#, "abc"),
        sign_raw(r#"{"alg":"XX1"}"#, r#"{"ext":"x"}"#, "abc"),
        sign_raw(r#"{"typ":"JWT"}"#, r#"{"ext":"x"}"#, "abc"),
    ];
    let _ = handshake_status(addr, "/cable", &[]);
    for t in &tokens {
        let _ = handshake_status(addr, &format!("/cable?jid={t}"), &[]);
    }
    let invalid = |e: &str| format!("DBG invalid token nodeid=N context=jwt error=\"{e}\"");
    assert_logged(
        &sut,
        &[
            format!("DBG no token is found nodeid=N context=jwt url=http://{addr}/cable headers=[REMOTE_ADDR]"),
            invalid("token is malformed: token contains an invalid number of segments"),
            invalid("token is malformed: could not base64 decode header: illegal base64 data at input byte 0"),
            invalid("token signature is invalid: signature is invalid"),
            "DBG token has expired nodeid=N context=jwt".to_owned(),
            invalid("token has invalid claims: token is not valid yet"),
            invalid("token has invalid claims: invalid type for claim: exp is invalid"),
            invalid("token is unverifiable: error while executing keyfunc: unexpected signing method: RS256"),
            invalid("token is unverifiable: signing method (alg) is unavailable"),
            invalid("token is unverifiable: signing method (alg) is unspecified"),
        ],
    );
}

/// regression-for: ws/handler_test.go#TestCheckOrigin
/// note: Review finding H7: failed WebSocket upgrades are logged (`context=ws`) with gorilla's `HandshakeError` texts, for every check.
#[test]
fn websocket_upgrade_failures_are_logged() {
    let sut = start(&["--norpc"]);
    let addr = sut.addr();
    let head = |lines: &str| format!("GET /cable HTTP/1.1\r\nHost: x\r\n{lines}\r\n");
    raw(addr, &head("Connection: close\r\n"));
    raw(addr, &head("Connection: Upgrade\r\n"));
    raw(
        addr,
        "POST /cable HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nContent-Length: 0\r\n\r\n",
    );
    raw(addr, &head("Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 12\r\n"));
    raw(
        addr,
        &head("Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: x\r\n"),
    );
    let failed =
        |e: &str| format!("DBG WebSocket connection upgrade failed nodeid=N context=ws error=\"websocket: {e}\"");
    let bad = "the client is not using the websocket protocol: ";
    assert_logged(
        &sut,
        &[
            failed(&format!("{bad}'upgrade' token not found in 'Connection' header")),
            failed(&format!("{bad}'websocket' token not found in 'Upgrade' header")),
            failed(&format!("{bad}request method is not GET")),
            failed("unsupported version: 13 not found in 'Sec-Websocket-Version' header"),
            failed(
                "not a websocket handshake: 'Sec-WebSocket-Key' header must be Base64 encoded value of 16-byte in length",
            ),
        ],
    );
}

/// regression-for: sse/handler_test.go#TestSSEHandler
/// note: Review finding H7: debug lines of the API (`context=api`), SSE (`sid`, `transport=sse`), Durable Streams (`transport=ds` before a session, then the session logger `context=node sid ds=<path>`) handlers and the streams controller (`context=streams`, including its line with an empty message).
#[test]
fn http_endpoints_log_requests() {
    let sut = start(&["--public", "--sse", "--ds", "--broker=memory", "--presets=broker"]);
    let addr = sut.addr();
    let close = [("Connection", "close")];
    let _ = raw_http::request(addr, "GET", "/api/publish", &close, None);
    let _ = raw_http::request(addr, "GET", "/api/presence/x/userz", &close, None);
    if let Ok(stream) = EventStream::open(addr, "GET", "/events?stream=public1", &[], None) {
        // Established once the response head arrives; dropping it ends the request.
        drop(stream);
    }
    let _ = raw_http::request(addr, "POST", "/ds/public1", &close, Some(b""));
    let _ = raw_http::request(addr, "GET", "/ds/public1?offset=-1", &close, None);

    let identifier = r#"identifier="{\"channel\":\"$pubsub\",\"stream_name\":\"public1\"}""#;
    assert_logged(
        &sut,
        &[
            "DBG invalid request method nodeid=N context=api method=GET".to_owned(),
            "DBG invalid path for get users nodeid=N context=api path=/api/presence/x/userz".to_owned(),
            format!("DBG unsigned nodeid=N context=streams {identifier} stream=public1"),
            format!("DBG  nodeid=N context=streams {identifier} stream=public1"),
            "DBG session established nodeid=N sid=S transport=sse".to_owned(),
            "DBG request terminated nodeid=N sid=S transport=sse".to_owned(),
            "DBG non-read operations are not supported nodeid=N transport=ds".to_owned(),
            "DBG session established nodeid=N context=node sid=S ds=public1 mode=\"\" offset=-1".to_owned(),
            "DBG write history nodeid=N context=node sid=S ds=public1 len=0".to_owned(),
            "DBG session completed nodeid=N context=node sid=S ds=public1".to_owned(),
        ],
    );
}

/// regression-for: metrics/metrics_test.go#TestRunWithDedicatedServerStartsRotation
/// note: Review finding H7: the metrics writers' lines: the printer logs through the runner's logger (no `context`), its snapshot has an empty message (tint prints two spaces), rotation is logged at debug level (`context=metrics`, a Go duration), and the StatsD writer announces itself.
#[test]
fn metrics_writers_log_their_lines() {
    let sut = start(&[
        "--norpc",
        "--metrics_log",
        "--metrics_rotate_interval=1",
        "--metrics_log_filter=clients_num",
        "--statsd_host=127.0.0.1:9",
        "--metrics_tags=a:b,c:d",
    ]);
    assert_logged(
        &sut,
        &[
            "INF log metrics nodeid=N interval=1 fields=clients_num".to_owned(),
            "INF Send statsd metrics to 127.0.0.1:9 with every 1s (prefix=anycable_go., tags=map[a:b c:d], style=datadog) nodeid=N"
                .to_owned(),
            "DBG rotate metrics nodeid=N context=metrics interval=1s".to_owned(),
            "INF  nodeid=N clients_num=0".to_owned(),
        ],
    );
}
