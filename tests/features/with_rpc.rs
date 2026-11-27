//! Feature scenarios that launch the AnyT RPC application
//! (`bundle exec anyt --only-rpc`): metrics logging, mruby metrics printers,
//! pings, ping/pong timeouts and graceful shutdown.

use std::time::Duration;

use cabby_testkit::fake_rpc::Method;
use cabby_testkit::wsdirector::{Client, Connection, Expect, Protocol, StepError};
use serde_json::json;

use crate::support::*;

fn rpc_host(app: &cabby_testkit::anyt_app::AnytApp) -> String {
    format!("--rpc_host={}", app.grpc_target())
}

/// welcome, ping, ping (`multiplier: 2`), the two clients running concurrently.
/// Returns the step results and the (still connected) clients.
type GroupResults = Vec<(&'static str, Result<(), StepError>)>;

fn welcome_and_two_pings(url: &str) -> (GroupResults, Vec<Client>) {
    let one = || -> Result<Client, StepError> {
        let mut c = wsd(url, Protocol::Base, Connection::default())?;
        c.receive(&Expect::Partial(json!({"type": "welcome"})))?;
        c.receive(&Expect::Partial(json!({"type": "ping"})))?;
        c.receive(&Expect::Partial(json!({"type": "ping"})))?;
        Ok(c)
    };
    let outcomes = std::thread::scope(|s| {
        let a = s.spawn(one);
        let b = s.spawn(one);
        [a.join().expect("client thread"), b.join().expect("client thread")]
    });
    let mut results = Vec::new();
    let mut clients = Vec::new();
    for o in outcomes {
        match o {
            Ok(c) => {
                results.push(("default", Ok(())));
                clients.push(c);
            }
            Err(e) => results.push(("default", Err(e))),
        }
    }
    (results, clients)
}

/// upstream: features/metrics.testfile#scenario
/// note: `sleep 2` replaced by waiting for the metrics line (rotation 3 s); inherent window: pings at --ping_interval=1
#[test]
fn metrics() {
    let app = rpc_app();
    let rpc = rpc_host(&app);
    let mut sut = launch(
        &[
            &rpc,
            "--disconnect_mode=never",
            "--metrics_rotate_interval=3",
            "--ping_interval=1",
            "--metrics_log",
            "--metrics_log_filter=rpc_call_total,failed_auths_total",
        ],
        &[],
    );

    let (results, clients) = welcome_and_two_pings(&ws_url(&sut, "/cable"));
    assert_groups(&results);
    drop(clients);

    // "sleep 2 # Wait for metrics to be logged": wait for the line (rotation 3 s).
    wait_output(&sut, "metrics with both counters", READY_DEADLINE, |l| {
        l.contains("failed_auths_total=0") && l.contains("rpc_call_total=2")
    });
    let logs = stop(&mut sut, Duration::from_secs(10));

    // We disabled disconnector, so there should be just 2 RPC calls (Connect)
    assert!(
        logs.contains("failed_auths_total=0") && logs.contains("rpc_call_total=2"),
        "Metrics logs not found:\n{logs}"
    );
}

/// upstream: features/mruby_printers.testfile#scenario
/// note: clients stay connected until the printer reports 2 connections (the clients_num gauge is refreshed periodically); inherent window: pings at --ping_interval=3
#[test]
fn mruby_printers() {
    let app = rpc_app();
    let rpc = rpc_host(&app);
    let formatter = cabby_testkit::workspace_root().join("compat/reference/features/simple_logger.rb");
    let formatter = format!("--metrics_log_formatter={}", formatter.display());
    let mut sut = launch(
        &[&rpc, "--broadcast_adapter=http", &formatter, "--metrics_rotate_interval=1", "--ping_interval=3"],
        &[("PRINTER_NAME", "MRUBY PRINTER")],
    );

    // Inherent window: two pings at the 3 s interval (about 6 s), within the
    // 5 s receive timeout each.
    let (results, clients) = welcome_and_two_pings(&ws_url(&sut, "/cable"));
    assert_groups(&results);

    // The printed `clients_num` is a gauge the server refreshes periodically, so
    // the first snapshot with both connections may come after wsdirector's
    // clients have finished their steps (upstream stops right away and relies
    // on timing). The clients stay connected until the line is printed (bounded).
    wait_output(&sut, "the mruby printer line", READY_DEADLINE, |l| l.contains("[MRUBY PRINTER] Connections: 2"));
    drop(clients);
    let logs = stop(&mut sut, Duration::from_secs(10));
    assert!(logs.contains("[MRUBY PRINTER] Connections: 2"), "Missing metrics in logs:\n{logs}");
}

/// upstream: features/ping.testfile#scenario
/// note: inherent window: with pi=1 a ping must arrive within wsdirector's 5 s although --ping_interval=10
/// Inherent window: the per-connection `pi=1` interval must override the
/// server's `--ping_interval=10`, so a ping arrives within wsdirector's 5 s.
#[test]
fn ping() {
    let app = rpc_app();
    let rpc = rpc_host(&app);
    let sut = launch(&[&rpc, "--ping_interval=10"], &[]);
    let url = ws_url(&sut, "/cable");

    let result = (|| -> Result<(), StepError> {
        let mut c =
            wsd(&url, Protocol::Base, Connection { query: vec![("pi".into(), "1".into())], ..Default::default() })?;
        c.receive(&Expect::Partial(json!({"type": "welcome"})))?;
        c.receive(&Expect::Partial(json!({"type": "ping"})))?;
        Ok(())
    })();

    assert_groups(&[("default", result)]);
}

/// upstream: features/ping_pong.testfile#scenario
/// note: inherent windows: a pong 0.5 s after each ping keeps the connection, 1 s of silence ends it with no_pong (--pong_timeout=1)
/// Inherent windows (wsdirector `sleep` steps against `--pong_timeout=1`): a
/// pong 0.5 s after each ping keeps the connection, 1 s of silence after the
/// fourth ping ends it with `no_pong`.
#[test]
fn ping_pong() {
    let app = rpc_app();
    let rpc = rpc_host(&app);
    let sut = launch(&[&rpc, "--pong_timeout=1"], &[]);
    let url = ws_url(&sut, "/cable");
    let ping = Expect::Partial(json!({"type": "ping"}));

    let result = (|| -> Result<(), StepError> {
        let mut c: Client = wsd(
            &url,
            Protocol::Base,
            Connection {
                subprotocol: Some("actioncable-v1-ext-json".into()),
                query: vec![("pi".into(), "1".into()), ("ptp".into(), "ns".into())],
                ..Default::default()
            },
        )?;
        c.receive(&Expect::Partial(json!({"type": "welcome"})))?;
        c.receive(&ping)?;
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(500));
            c.send(&json!({"command": "pong"}));
            c.receive(&ping)?;
        }
        std::thread::sleep(Duration::from_secs(1));
        c.receive(&Expect::Exact(json!({"type": "disconnect", "reason": "no_pong", "reconnect": true})))?;
        Ok(())
    })();

    assert_groups(&[("default", result)]);
}

/// upstream: features/shutdown.testfile#scenario
/// note: `sleep 1` replaced by awaiting the three welcomes; Disconnect calls are counted on the AnyT app instead of its debug log
/// The upstream `sleep 1` before stopping waits for the three clients to
/// connect; here their welcomes are awaited. They stay connected (wsdirector
/// `sleep 4`) while the server shuts down.
#[test]
fn shutdown() {
    let app = rpc_app();
    let rpc = rpc_host(&app);
    let mut sut = launch(&[&rpc, "--disconnect_mode=always", "--disconnect_rate=1"], &[]);
    let url = ws_url(&sut, "/cable");

    let mut clients = Vec::new();
    for _ in 0..3 {
        let mut c = wsd(&url, Protocol::Base, Connection::default()).unwrap_or_else(|e| panic!("{e}"));
        c.receive(&Expect::Partial(json!({"type": "welcome"}))).unwrap_or_else(|e| panic!("{e}"));
        clients.push(c);
    }

    // `stop :anycable`: ChildProcess sends SIGTERM and SIGKILL after 3 s.
    stop(&mut sut, Duration::from_secs(3));
    let disconnect_calls = app.rpc().call_count(Method::Disconnect);
    drop(app); // stop :rpc

    assert_eq!(3, disconnect_calls, "Expected to receive 3 Disconnect RPC calls. Got {disconnect_calls}");
    drop(clients);
}

/// `kill -<name> <server pid>`.
fn send_signal(sut: &cabby_testkit::sut::Sut, name: &str) {
    let sent = std::process::Command::new("kill")
        .args([format!("-{name}"), sut.process.pid().to_string()])
        .status()
        .expect("running kill");
    assert!(sent.success(), "kill -{name}");
}

/// A second signal stops the server at once while its HTTP server waits for a
/// request whose head has not fully arrived (the wait would otherwise last
/// until the 5 s header timeout). The first signal's shutdown is still running
/// when the second one is sent.
/// regression-for: features/shutdown.testfile#scenario
/// note: a connection holding half a request head keeps the HTTP server's graceful shutdown waiting; on both servers the second signal ends that wait (Go: `http.Server.Shutdown` returns when its context is cancelled)
#[test]
fn a_second_signal_stops_a_server_waiting_for_a_request() {
    use std::io::{BufRead, BufReader, Write};

    let mut sut = launch(&["--shutdown_timeout=30"], &[]);
    let mut pending = std::net::TcpStream::connect(local(sut.port)).expect("connecting");
    pending.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n").expect("half a request head");
    // Connections are accepted in order: once a later one is answered, the
    // server has the pending one.
    let mut later = std::net::TcpStream::connect(local(sut.port)).expect("connecting");
    later.set_read_timeout(Some(READY_DEADLINE)).expect("read timeout");
    later.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").expect("a request");
    let status = BufReader::new(later).lines().next().expect("a status line").expect("a status line");
    assert!(status.contains(" 200 "), "{status}");

    send_signal(&sut, "INT");
    wait_output(&sut, "the shutdown", READY_DEADLINE, |out| out.contains("Shutting down..."));
    assert!(sut.process.wait_exit(Duration::from_millis(500)).is_err(), "the shutdown waits for the request");

    send_signal(&sut, "INT");
    let sent = std::time::Instant::now();
    let status = sut.process.wait_exit(Duration::from_secs(3)).unwrap_or_else(|e| panic!("{e:#}"));
    assert!(status.success(), "{status}");
    assert!(sent.elapsed() < Duration::from_secs(2), "stopped {:?} after the second signal", sent.elapsed());
    drop(pending);
}

/// A second signal while sessions are being disconnected (their Disconnect
/// calls still waiting for the application) ends the process with status 0
/// and nothing about panics in its output.
/// regression-for: features/shutdown.testfile#scenario
/// note: an HTTP RPC double holds the Disconnect calls; the second signal is sent once the first one reached it
#[test]
fn a_second_signal_during_disconnect_calls_exits_cleanly() {
    use cabby_testkit::http_stub::{HttpStub, Response};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};

    let disconnects = Arc::new(AtomicUsize::new(0));
    let (release, held) = mpsc::channel::<()>();
    let held = Mutex::new(held);
    let seen = disconnects.clone();
    let rpc = HttpStub::start(move |req| {
        if req.path.ends_with("/disconnect") {
            seen.fetch_add(1, Ordering::SeqCst);
            // Until the test ends (the sender is dropped), or for at most 20 s.
            let _ =
                held.lock().unwrap_or_else(std::sync::PoisonError::into_inner).recv_timeout(Duration::from_secs(20));
            return Response { status: 200, body: br#"{"status":1}"#.to_vec() };
        }
        let welcome = json!({"status": 1, "identifiers": "{}", "transmissions": [r#"{"type":"welcome"}"#]});
        Response { status: 200, body: welcome.to_string().into_bytes() }
    });
    let rpc_host = format!("--rpc_host={}", rpc.url());
    let mut sut = launch(&[&rpc_host, "--disconnect_mode=always", "--shutdown_timeout=30"], &[]);
    let url = ws_url(&sut, "/cable");
    let mut client = wsd(&url, Protocol::Base, Connection::default()).unwrap_or_else(|e| panic!("{e}"));
    client.receive(&Expect::Partial(json!({"type": "welcome"}))).unwrap_or_else(|e| panic!("{e}"));

    send_signal(&sut, "INT");
    await_ready("a Disconnect call", READY_DEADLINE, || {
        if disconnects.load(Ordering::SeqCst) > 0 { Ok(()) } else { Err("none yet".to_owned()) }
    });
    send_signal(&sut, "INT");
    let status = sut.process.wait_exit(Duration::from_secs(5)).unwrap_or_else(|e| panic!("{e:#}"));

    let out = sut.process.output();
    assert!(status.success(), "{status}. Output:\n{out}");
    assert!(!out.contains("panicked"), "Output:\n{out}");
    drop(release);
    drop(client);
}
