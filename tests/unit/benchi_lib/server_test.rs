//! upstream file: etc/benchi/lib/server_test.go

use std::time::{Duration, Instant};

use cabby_testkit::http_stub;
use cabby_testkit::reference::benchi::*;

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_Defaults
#[test]
fn test_build_server_defaults() {
    let srv = build_server(ServerConfig::default()).expect("BuildServer");

    let b_url = srv.broadcast_url();
    let ws_url = srv.websocket_url();

    assert!(!b_url.is_empty());
    assert!(!ws_url.is_empty());
    assert!(b_url.contains("127.0.0.1"), "{b_url}");
    assert!(ws_url.contains("127.0.0.1"), "{ws_url}");
    assert!(ws_url.starts_with("ws://"), "websocket URL must use ws:// scheme, got {ws_url:?}");
    assert!(b_url.ends_with("/broadcast"), "broadcast URL must end in /broadcast, got {b_url:?}");
    assert!(ws_url.ends_with("/cable"), "websocket URL must end in /cable, got {ws_url:?}");

    assert_eq!(Ok(()), srv.shutdown());
}

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_BroadcastURL_AcceptsPOST
#[test]
fn test_build_server_broadcast_url_accepts_post() {
    let srv = build_server(ServerConfig::default()).expect("BuildServer");

    let (status, _) = http_stub::post(&srv.broadcast_url(), "application/json", br#"{"stream":"foo","data":"x"}"#)
        .expect("POST broadcast");

    assert_eq!(201, status);

    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_LogSuppression
/// note: adaptation: upstream redirects os.Stderr around the embedded server. Go: the bridge's stderr is read after a sentinel came through the pipe. Rust: the server is an external process with captured stdio, so this checks a harness default (ServerLog::Discard forwards nothing to the harness's stderr).
///
/// Runtime-specific adaptation: upstream swaps `os.Stderr` for a pipe around
/// `BuildServer` + `Shutdown` of the embedded server and expects nothing written. The
/// Rust harness runs the server as a child process with captured stdio, so the
/// equivalent invariant is that the harness forwards none of the server's output to
/// its own stderr by default (Go backend: the bridge process's stderr stays empty).
#[test]
fn test_build_server_log_suppression() {
    let out = build_and_shutdown_stderr(ServerConfig::default()).expect("BuildServer + Shutdown");

    assert!(out.is_empty(), "no server output expected on stderr, got: {out}");
}

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_BrokerSwap
#[test]
fn test_build_server_broker_swap() {
    let srv = build_server(ServerConfig { broker_adapter: "memory".into(), ..Default::default() });
    let srv = srv.expect("BuildServer with the memory broker");

    let _ = srv.shutdown();
}

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_DoubleShutdownReturnsNil
#[test]
fn test_build_server_double_shutdown_returns_nil() {
    let srv = build_server(ServerConfig::default()).expect("BuildServer");

    assert_eq!(Ok(()), srv.shutdown());
    // Upstream `assert.NotPanics` around the second call: a panic fails this test.
    assert_eq!(Ok(()), srv.shutdown());
}

/// upstream: etc/benchi/lib/server_test.go#TestBuildServer_StartsAndStops_InOneSecond
/// note: adaptation: the 1s bound covers spawning the server process, waiting until it accepts connections, and stopping it with SIGTERM (upstream: an in-process runner).
///
/// Runtime-specific adaptation: upstream starts and stops an in-process runner; here
/// the bound covers spawning the server process, waiting for it to accept
/// connections, and stopping it (SIGTERM).
#[test]
fn test_build_server_starts_and_stops_in_one_second() {
    let start = Instant::now();
    let srv = build_server(ServerConfig::default()).expect("BuildServer");
    assert_eq!(Ok(()), srv.shutdown());
    let elapsed = start.elapsed();

    assert!(elapsed < Duration::from_secs(1), "BuildServer + Shutdown took {elapsed:?}, expected <1s");
}
