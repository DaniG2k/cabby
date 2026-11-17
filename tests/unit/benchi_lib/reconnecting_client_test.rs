//! upstream file: etc/benchi/lib/reconnecting_client_test.go

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use cabby_testkit::reference::benchi::*;

const POLL: Duration = Duration::from_millis(10);
const WAIT: Duration = Duration::from_secs(5);

/// Upstream `buildRPCServer`: a fake RPC application and a server in RPC mode.
fn build_rpc_server(rpc_cfg: RpcServerConfig, tuning: ServerTuning) -> (Server, RpcServer) {
    let rpc = RpcServer::new(rpc_cfg);
    let srv =
        build_server(ServerConfig { rpc_host: Some(rpc.url()), tuning, ..Default::default() }).expect("BuildServer");
    (srv, rpc)
}

/// Upstream `startClients`: `n` clients running until the guard is dropped (the
/// upstream `t.Cleanup` cancels the context and waits for every `Run`).
struct Running {
    ctx: Arc<RunContext>,
    clients: Vec<Arc<ReconnectingClient>>,
    threads: Vec<JoinHandle<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.ctx.cancel();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn start_clients(n: usize, cfg: ReconnectingClientConfig<'_>) -> Running {
    let ctx = Arc::new(RunContext::new());
    let mut clients = Vec::new();
    let mut threads = Vec::new();
    for i in 0..n {
        let client = Arc::new(ReconnectingClient::new(ReconnectingClientConfig { seed: i as u64, ..cfg.clone() }));
        let (c, ctx) = (Arc::clone(&client), Arc::clone(&ctx));
        threads.push(std::thread::spawn(move || c.run(&ctx, Duration::ZERO)));
        clients.push(client);
    }
    Running { ctx, clients, threads }
}

fn bench_channels() -> Vec<String> {
    vec![r#"{"channel":"BenchChannel","id":"1"}"#.into(), r#"{"channel":"BenchChannel","id":"2"}"#.into()]
}

fn config<'a>(url: String, stats: &'a ClientStats) -> ReconnectingClientConfig<'a> {
    ReconnectingClientConfig {
        url,
        channels: bench_channels(),
        sequential_subscribe: false,
        subscribe_retry_interval: Duration::from_secs(5),
        subscribe_retries_max: 0,
        ping_timeout: Duration::from_secs(6),
        probe_interval: Duration::ZERO,
        backoff: Backoff { min: Duration::from_secs(3600), max: Duration::ZERO },
        stats,
        seed: 0,
    }
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_BecomesHealthyAndRecoversFromDrop
#[test]
fn test_reconnecting_client_becomes_healthy_and_recovers_from_drop() {
    let (srv, rpc) = build_rpc_server(
        RpcServerConfig { latency: Duration::from_millis(5), jitter: 0.5, ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let clients = start_clients(
        5,
        ReconnectingClientConfig {
            backoff: Backoff { min: Duration::from_millis(10), max: Duration::from_millis(100) },
            ..config(srv.websocket_url(), &stats)
        },
    );

    assert!(eventually(WAIT, POLL, || stats.state(ClientState::Healthy) == 5));
    assert_eq!(5, rpc.stats(RpcMethod::Connect).completed);
    assert_eq!(10, rpc.stats(RpcMethod::Command).completed);
    assert_eq!(5, stats.welcome().count());
    assert_eq!(10, stats.confirm().count());

    assert!(clients.clients[0].drop_connection());

    // The client records the close reason before it leaves the healthy state, so
    // wait for the reconnect attempt too.
    assert!(eventually(WAIT, POLL, || {
        stats.reason(CloseReason::Dropped) == 1 && stats.attempts() == 6 && stats.state(ClientState::Healthy) == 5
    }));
    assert!(eventually(WAIT, POLL, || rpc.stats(RpcMethod::Disconnect).completed == 1));
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SubscribeTimeout
#[test]
fn test_reconnecting_client_subscribe_timeout() {
    let (srv, _rpc) = build_rpc_server(
        RpcServerConfig { command_latency: Duration::from_secs(1), ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig {
            subscribe_retry_interval: Duration::from_millis(100),
            ..config(srv.websocket_url(), &stats)
        },
    );

    assert!(eventually(WAIT, POLL, || stats.reason(CloseReason::SubscribeTimeout) == 1));
    assert_eq!(1, stats.state(ClientState::Backoff));
    assert_eq!(0, stats.confirm().count());
    assert_eq!(0, stats.subscribe_retries());
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SubscribeRetriesThenReconnects
#[test]
fn test_reconnecting_client_subscribe_retries_then_reconnects() {
    let (srv, _rpc) = build_rpc_server(
        RpcServerConfig { command_latency: Duration::from_secs(1), ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig {
            subscribe_retry_interval: Duration::from_millis(100),
            subscribe_retries_max: 2,
            ..config(srv.websocket_url(), &stats)
        },
    );

    assert!(eventually(WAIT, POLL, || stats.reason(CloseReason::SubscribeTimeout) == 1));
    // Both channels are re-sent twice before the client gives up.
    assert_eq!(4, stats.subscribe_retries());
    assert_eq!(1, stats.attempts());
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SubscribeRetryConfirmed
#[test]
fn test_reconnecting_client_subscribe_retry_confirmed() {
    let (srv, rpc) = build_rpc_server(
        RpcServerConfig { command_latency: Duration::from_millis(300), ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig {
            channels: bench_channels()[..1].to_vec(),
            subscribe_retry_interval: Duration::from_millis(200),
            subscribe_retries_max: 2,
            ..config(srv.websocket_url(), &stats)
        },
    );

    assert!(eventually(WAIT, POLL, || stats.state(ClientState::Healthy) == 1));
    assert_eq!(1, stats.subscribe_retries());

    // The re-sent command does not break the subscription. Upstream sleeps 1s: this
    // is an observation window (nothing should change), not synchronization.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(1, stats.state(ClientState::Healthy));
    assert_eq!(1, stats.attempts());
    assert_eq!(1, stats.confirm().count());
    assert_eq!(1, rpc.stats(RpcMethod::Command).completed);
}

fn sequential_subscribe(sequential: bool) {
    let (srv, _rpc) = build_rpc_server(
        RpcServerConfig { command_latency: Duration::from_millis(200), ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig { sequential_subscribe: sequential, ..config(srv.websocket_url(), &stats) },
    );

    assert!(eventually(WAIT, POLL, || stats.state(ClientState::Healthy) == 1));

    // The server handles a session's commands one at a time. When all commands go
    // out at once, the second one waits for the first.
    let confirm = stats.confirm();
    assert_eq!(2, confirm.count());
    if sequential {
        assert!(confirm.max() < Duration::from_millis(300), "max confirm {:?}", confirm.max());
    } else {
        assert!(confirm.max() > Duration::from_millis(350), "max confirm {:?}", confirm.max());
    }
}

mod test_reconnecting_client_sequential_subscribe {
    use super::*;

    /// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SequentialSubscribe/sequential=false
    #[test]
    fn sequential_false() {
        sequential_subscribe(false);
    }

    /// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SequentialSubscribe/sequential=true
    #[test]
    fn sequential_true() {
        sequential_subscribe(true);
    }
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_PingTimeoutBeforeWelcome
#[test]
fn test_reconnecting_client_ping_timeout_before_welcome() {
    let (srv, _rpc) = build_rpc_server(
        RpcServerConfig { connect_latency: Duration::from_secs(2), ..Default::default() },
        ServerTuning { ping_interval: Some(10), ..Default::default() },
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig { ping_timeout: Duration::from_millis(200), ..config(srv.websocket_url(), &stats) },
    );

    assert!(eventually(WAIT, POLL, || stats.reason(CloseReason::PingTimeout) == 1));
    assert_eq!(0, stats.welcome().count());
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_Probe
#[test]
fn test_reconnecting_client_probe() {
    let (srv, rpc) = build_rpc_server(
        RpcServerConfig { latency: Duration::from_millis(5), ..Default::default() },
        ServerTuning::default(),
    );

    let stats = ClientStats::new();
    let _clients = start_clients(
        1,
        ReconnectingClientConfig {
            channels: Vec::new(),
            probe_interval: Duration::from_millis(20),
            ..config(srv.websocket_url(), &stats)
        },
    );

    assert!(eventually(WAIT, POLL, || stats.probe().count() >= 5));
    assert!(stats.probe().max() < Duration::from_secs(1));
    // The server answers pings without RPC calls.
    assert_eq!(0, rpc.stats(RpcMethod::Command).started);
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestReconnectingClient_SessionIDs
#[test]
fn test_reconnecting_client_session_ids() {
    let stats = Arc::new(ClientStats::new());
    let seen: Arc<Mutex<HashMap<String, i64>>> = Arc::default();
    let (seen2, stats2) = (Arc::clone(&seen), Arc::clone(&stats));
    let (srv, _rpc) = build_rpc_server(
        RpcServerConfig {
            latency: Duration::from_millis(5),
            session_closed: Some(Arc::new(move |sid: &str| {
                *seen2.lock().unwrap().entry(sid.to_owned()).or_default() += 1;
                stats2.session_closed(sid)
            })),
            ..Default::default()
        },
        ServerTuning::default(),
    );

    let clients = start_clients(
        1,
        ReconnectingClientConfig {
            backoff: Backoff { min: Duration::from_millis(10), max: Duration::ZERO },
            ..config(srv.websocket_url(), &stats)
        },
    );
    assert!(eventually(WAIT, POLL, || stats.state(ClientState::Healthy) == 1));

    // One Connect and two Command calls carry the first session's ID.
    let expected: HashMap<String, i64> = [("1".to_owned(), 3)].into();
    assert_eq!(expected, *seen.lock().unwrap());
    assert!(!stats.session_closed("1"));

    assert!(clients.clients[0].drop_connection());
    assert!(stats.session_closed("1"));

    assert!(eventually(WAIT, POLL, || seen.lock().unwrap().get("2") == Some(&3)));
    assert!(!stats.session_closed("2"));
}

/// upstream: etc/benchi/lib/reconnecting_client_test.go#TestHistogramQuantile
#[test]
fn test_histogram_quantile() {
    let h = Histogram::new();
    for i in 1..=100 {
        h.record(Duration::from_millis(i));
    }
    let s = h.snapshot();
    assert_eq!(100, s.count());
    let within = |got: Duration, want: Duration, delta: Duration| {
        let diff = got.abs_diff(want);
        assert!(diff <= delta, "{got:?} not within {delta:?} of {want:?}");
    };
    within(s.quantile(0.5), Duration::from_millis(50), Duration::from_millis(3));
    within(s.quantile(0.99), Duration::from_millis(99), Duration::from_millis(5));
    assert_eq!(Duration::from_millis(100), s.quantile(1.0));

    h.swap();
    assert_eq!(0, h.snapshot().count());
}
