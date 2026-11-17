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

    stop(&mut sut, Duration::from_secs(30));
    let disconnect_calls = app.rpc().call_count(Method::Disconnect);
    drop(app); // stop :rpc

    assert_eq!(3, disconnect_calls, "Expected to receive 3 Disconnect RPC calls. Got {disconnect_calls}");
    drop(clients);
}
