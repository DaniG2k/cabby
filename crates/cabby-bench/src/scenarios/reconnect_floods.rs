//! `reconnect_floods` (upstream `scenarios/reconnect_floods/main.go`): reconnect
//! storms against an RPC-backed server. Setup (`-c` clients at `--ramp-rate`), settle,
//! waves that drop N random connections without a close frame, recovery, then the
//! disconnect-queue drain. Probe clients measure ping → pong latency.
//!
//! Exit codes: 0 all clients healthy after the last wave, 1 setup or recovery failed,
//! 2 flag error, 130 interrupted (summary printed with `interrupted=true`).
//!
//! Server gauges and counters (`rpc_pending_num`, `clients_num`,
//! `rpc_canceled_total`, `rpc_wait_timeout_total`, `disconnect_queue_discarded_total`,
//! `disconnect_queue_size`) come from the server's metrics endpoint. Upstream's
//! process-wide `peak_goroutines` / `peak_heap_mb` (clients and server in one Go
//! process) become `peak_server_threads` / `peak_server_rss_mb` of the server process
//! (see [`crate::resources`]). The summary ends with the generator's health
//! (`generator_*` keys, see [`super::GeneratorReport`]).
//!
//! # Servers without RPC or metrics
//!
//! A server that runs the application in-process (Action Cable, `bench/actioncable`)
//! makes no RPC calls; it gets the fake RPC app's latency in its environment
//! ([`app_latency_env`]) to apply in its own callbacks. A server without a metrics
//! endpoint ([`Server::has_metrics`]) prints `server_metrics=false`; the gauge-based
//! keys (`disconnect_discarded`, `rpc_canceled_calls`, `rpc_wait_timeout_calls`,
//! `peak_disconnect_queue`, `peak_rpc_pending`, `peak_server_sessions`) are then left
//! out, the disconnect-queue drain is not observable (`wave_<n>_drained_seconds=-1`,
//! and the last wave ends at recovery), and without RPC calls the command stall is
//! unknown (`max_command_stall_seconds=-1`).
//!
//! # Pusher mode
//!
//! `--protocol=pusher` runs the same waves over the Pusher protocol
//! ([`crate::reconnecting_client`], [`crate::pusher`]). Pusher has no RPC: channels are
//! public (`bench-1..bench-<s>`), the server authorizes nothing and calls no
//! application, so there is no application latency (`--rpc-*` flags only tune Cabby's
//! and AnyCable's unused RPC settings) and no disconnect queue to drain. The scenario
//! then measures what Pusher servers do on a reconnect storm: accept the connections,
//! confirm the subscriptions and keep answering pings. The gauge-based keys are left out
//! for every server, as for a server without metrics (`server_metrics=false`), so the
//! four Pusher servers print the same keys, and the last wave ends at recovery. The
//! watchdog is client-driven (`pusher:ping` after `--ping-interval` seconds without a
//! server message), since Pusher servers do not all ping on a schedule.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{FlagDef, FlagError, GeneratorMonitor, GeneratorReport, Kind, SUT_FLAGS, SignalScope, Ui, mib, ms};
use crate::cancel::CancelToken;
use crate::histogram::HistogramSnapshot;
use crate::pusher::Protocol;
use crate::reconnecting_client::{
    Backoff, ClientState, ClientStats, CloseReason, ReconnectingClient, ReconnectingClientConfig,
};
use crate::rng::Rng;
use crate::rpc_server::{RpcMethod, RpcServer, RpcServerConfig};
use crate::runtime::{block_on, rt};
use crate::server::{Server, ServerConfig, ServerLog, ServerTuning};

/// The server's stats refresh interval in this scenario (upstream sets
/// `App.StatsRefreshInterval = 1`).
const STATS_REFRESH: Duration = Duration::from_secs(1);
/// Slack added to [`STATS_REFRESH`] before a gauge read counts as fresh.
const STATS_SLACK: Duration = Duration::from_millis(250);

/// Parsed flags; defaults as upstream. `-c` and `--waves` are required.
#[derive(Debug, Clone, PartialEq)]
pub struct RunConfig {
    pub c: usize,
    /// `-s`, default 3.
    pub s: usize,
    pub waves: Vec<usize>,
    /// default 60s
    pub wave_interval: Duration,
    /// default 200
    pub ramp_rate: u32,
    /// default 2m
    pub setup_timeout: Duration,
    /// default 5s
    pub settle: Duration,
    /// default 5m
    pub recovery_timeout: Duration,
    /// default 100ms
    pub rpc_latency: Duration,
    /// default 0.3
    pub rpc_jitter: f64,
    pub connect_latency: Duration,
    pub command_latency: Duration,
    pub disconnect_latency: Duration,
    /// default 28
    pub rpc_concurrency: u32,
    /// `--rpc-wait-timeout`; negative (default) = command timeout, 0 = no limit.
    pub rpc_wait_timeout_ms: i64,
    /// default 100
    pub disconnect_rate: u32,
    /// default 8192
    pub disconnect_backlog: u32,
    /// default 3 (seconds)
    pub ping_interval: u32,
    /// default 6s
    pub ping_timeout: Duration,
    pub sequential_subscribe: bool,
    /// default 5s
    pub sub_retry_interval: Duration,
    /// default 2
    pub sub_retries_max: u32,
    /// default 10
    pub probes: usize,
    /// default 1s
    pub probe_interval: Duration,
    /// default 500ms
    pub backoff_min: Duration,
    /// default 10s
    pub backoff_max: Duration,
    pub seed: u64,
    pub non_interactive: bool,
    pub timeline: Option<PathBuf>,
    pub server_log: Option<PathBuf>,
    pub sut: Option<PathBuf>,
    pub server_args: Vec<String>,
    /// `--protocol`, default Action Cable.
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WaveResult {
    pub size: usize,
    pub dropped: usize,
    pub fired_at: Duration,
    /// Wave → all clients healthy; `None` = not recovered in time (printed as -1).
    pub recovery: Option<Duration>,
    /// Wave → empty disconnect queue, after recovery; `None` printed as -1.
    pub drained: Option<Duration>,
}

/// One per-second sample of client, RPC and server state (upstream `sample`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    pub at: Duration,
    /// Indexed by [`crate::reconnecting_client::ClientState::ALL`] order.
    pub states: [i64; 4],
    /// Indexed by [`crate::rpc_server::RpcMethod::ALL`] order.
    pub rpc_started: [i64; 3],
    pub rpc_done: [i64; 3],
    pub rpc_in_flight: i64,
    pub rpc_pending: u64,
    pub rpc_canceled: u64,
    pub rpc_wait_timeout: u64,
    /// Connect, Command.
    pub after_close: [i64; 2],
    pub discarded: u64,
    pub sessions: u64,
    pub disconnect_queue: u64,
    /// Server process threads (upstream: process goroutines).
    pub server_threads: u64,
    /// Server process RSS (upstream: process `HeapInuse`).
    pub server_rss_bytes: u64,
    pub reasons: Vec<(CloseReason, i64)>,
    pub attempts: i64,
    pub reconnects: i64,
    pub sub_retries: i64,
    pub welcome: Option<HistogramSnapshot>,
    pub confirm: Option<HistogramSnapshot>,
    pub probe: Option<HistogramSnapshot>,
}

impl Sample {
    fn reason(&self, r: CloseReason) -> i64 {
        self.reasons.iter().find(|(x, _)| *x == r).map_or(0, |(_, n)| *n)
    }
}

const NAME: &str = "reconnect_floods";

const DEFS: &[FlagDef] = &[
    FlagDef { name: "c", kind: Kind::Int, default: "", usage: "number of clients (required, > 0)" },
    FlagDef { name: "s", kind: Kind::Int, default: "3", usage: "RPC-backed subscriptions per client" },
    FlagDef {
        name: "waves",
        kind: Kind::Str,
        default: "",
        usage: "comma-separated wave sizes: clients dropped at once per wave, e.g. 1000,1000,3000 (required)",
    },
    FlagDef { name: "wave-interval", kind: Kind::Duration, default: "1m0s", usage: "time between waves" },
    FlagDef { name: "ramp-rate", kind: Kind::Int, default: "200", usage: "initial connects per second during setup" },
    FlagDef {
        name: "setup-timeout",
        kind: Kind::Duration,
        default: "2m0s",
        usage: "max time for all clients to become healthy before the first wave",
    },
    FlagDef {
        name: "settle",
        kind: Kind::Duration,
        default: "5s",
        usage: "steady-state time between setup and the first wave",
    },
    FlagDef {
        name: "recovery-timeout",
        kind: Kind::Duration,
        default: "5m0s",
        usage: "max time after the last wave for all clients to become healthy (the run then waits for the disconnect queue to drain)",
    },
    FlagDef {
        name: "rpc-latency",
        kind: Kind::Duration,
        default: "100ms",
        usage: "default latency of every fake RPC call",
    },
    FlagDef {
        name: "rpc-jitter",
        kind: Kind::Float,
        default: "0.3",
        usage: "RPC latency jitter as a fraction: each call takes latency × (1 ± jitter)",
    },
    FlagDef {
        name: "rpc-connect-latency",
        kind: Kind::Duration,
        default: "",
        usage: "Connect RPC latency (0 = --rpc-latency)",
    },
    FlagDef {
        name: "rpc-command-latency",
        kind: Kind::Duration,
        default: "",
        usage: "Command RPC latency (0 = --rpc-latency)",
    },
    FlagDef {
        name: "rpc-disconnect-latency",
        kind: Kind::Duration,
        default: "",
        usage: "Disconnect RPC latency (0 = --rpc-latency)",
    },
    FlagDef {
        name: "rpc-concurrency",
        kind: Kind::Int,
        default: "28",
        usage: "server: rpc_concurrency (shared by all RPC methods)",
    },
    FlagDef {
        name: "rpc-wait-timeout",
        kind: Kind::Duration,
        default: "-1ns",
        usage: "server: rpc_wait_timeout (max wait for a free RPC slot; negative = command timeout, 0 = no limit)",
    },
    FlagDef {
        name: "disconnect-rate",
        kind: Kind::Int,
        default: "100",
        usage: "server: disconnect_rate (Disconnect calls per second)",
    },
    FlagDef {
        name: "disconnect-backlog",
        kind: Kind::Int,
        default: "8192",
        usage: "server: max_disconnect_backlog_size (Disconnect calls over the limit are discarded)",
    },
    FlagDef {
        name: "ping-interval",
        kind: Kind::Int,
        default: "3",
        usage: "server: ping_interval in seconds (Pusher mode: the clients send pusher:ping after this many seconds without a server message)",
    },
    FlagDef {
        name: "ping-timeout",
        kind: Kind::Duration,
        default: "6s",
        usage: "client: reconnect when no ping arrives for this long (watch starts at socket open)",
    },
    FlagDef {
        name: "sequential-subscribe",
        kind: Kind::Bool,
        default: "",
        usage: "client: send the next subscribe command only after the previous one is confirmed",
    },
    FlagDef {
        name: "sub-retry-interval",
        kind: Kind::Duration,
        default: "5s",
        usage: "client: re-send a subscribe command when it is not confirmed for this long",
    },
    FlagDef {
        name: "sub-retries-max",
        kind: Kind::Int,
        default: "2",
        usage: "client: re-sends per channel; reconnect when the last one is not confirmed in --sub-retry-interval",
    },
    FlagDef {
        name: "probes",
        kind: Kind::Int,
        default: "10",
        usage: "extra clients that send a ping command every --probe-interval and measure the time to the pong; they have no subscriptions and waves do not drop them (0 = no probes)",
    },
    FlagDef { name: "probe-interval", kind: Kind::Duration, default: "1s", usage: "time between probe pings" },
    FlagDef {
        name: "backoff-min",
        kind: Kind::Duration,
        default: "500ms",
        usage: "client: first reconnect delay; doubles per failed attempt, ±50% jitter",
    },
    FlagDef { name: "backoff-max", kind: Kind::Duration, default: "10s", usage: "client: max reconnect delay" },
    FlagDef { name: "seed", kind: Kind::Int, default: "1", usage: "RNG seed for wave selection and backoff jitter" },
    FlagDef {
        name: "server-log",
        kind: Kind::Str,
        default: "",
        usage: "if set, write the server debug log to this path (large: ~1KB per connection attempt)",
    },
    FlagDef {
        name: "timeline",
        kind: Kind::Str,
        default: "",
        usage: "if set, write a per-second CSV timeline (client states, RPC calls/s, server gauges, timings) to this path",
    },
    FlagDef {
        name: "non-interactive",
        kind: Kind::Bool,
        default: "",
        usage: "suppress the live status on stderr; emit only the key=value summary on stdout (auto-on when stderr is not a TTY)",
    },
    super::PROTOCOL_FLAG,
    SUT_FLAGS[0],
    SUT_FLAGS[1],
];

/// Entry point: parse flags, install SIGINT/SIGTERM handling (a second signal kills
/// the process), run.
pub fn run(args: &[String], stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    let cfg = match parse_flags(args, stderr) {
        Ok(cfg) => cfg,
        Err(e) => {
            if e != "help" {
                let _ = writeln!(stderr, "error: {e}");
            }
            return super::EXIT_USAGE;
        }
    };
    let interrupt = CancelToken::new();
    let _signals = SignalScope::install(&interrupt);
    run_with_config(&interrupt, cfg, stdout, stderr)
}

fn optional_path(s: String) -> Option<PathBuf> {
    (!s.is_empty()).then(|| PathBuf::from(s))
}

pub fn parse_flags(args: &[String], stderr: &mut dyn Write) -> Result<RunConfig, String> {
    let p = match super::parse(NAME, DEFS, args, stderr) {
        Ok(p) => p,
        Err(FlagError::Help) => return Err("help".into()),
        Err(FlagError::Invalid(m)) => return Err(m),
    };
    let c = p.int("c", 0);
    let waves_flag = p.string("waves");
    if c <= 0 || waves_flag.is_empty() {
        super::write_usage(stderr, NAME, DEFS);
        return Err("required flags missing: -c must be > 0 and --waves must be set".into());
    }
    let mut waves = Vec::new();
    for part in waves_flag.split(',') {
        let n = part.trim().parse::<i64>().ok().filter(|n| *n > 0);
        let Some(n) = n else { return Err(format!("--waves: invalid wave size {part:?}")) };
        if n > c {
            return Err(format!("--waves: wave size {n} exceeds -c {c}"));
        }
        waves.push(n as usize);
    }
    let (s, ramp, conc, rate, backlog) = (
        p.int("s", 3),
        p.int("ramp-rate", 200),
        p.int("rpc-concurrency", 28),
        p.int("disconnect-rate", 100),
        p.int("disconnect-backlog", 8192),
    );
    if s < 0 || ramp <= 0 || conc <= 0 || rate <= 0 || backlog <= 0 {
        return Err(
            "-s, --ramp-rate, --rpc-concurrency, --disconnect-rate, --disconnect-backlog must be positive".into()
        );
    }
    let (probes, probe_interval) = (p.int("probes", 10), p.duration_ns("probe-interval", 1_000_000_000));
    if probes < 0 || probe_interval <= 0 {
        return Err("--probes must not be negative and --probe-interval must be positive".into());
    }
    let (retry_interval, retries_max) =
        (p.duration_ns("sub-retry-interval", 5_000_000_000), p.int("sub-retries-max", 2));
    if retry_interval <= 0 || retries_max < 0 {
        return Err("--sub-retry-interval must be positive and --sub-retries-max must not be negative".into());
    }
    let jitter = p.float("rpc-jitter", 0.3);
    if !(0.0..=1.0).contains(&jitter) {
        return Err("--rpc-jitter must be in [0, 1]".into());
    }
    let to_u32 = |v: i64| u32::try_from(v).unwrap_or(u32::MAX);
    let nonneg = |ns: i64| Duration::from_nanos(u64::try_from(ns).unwrap_or(0));
    let wait_timeout = p.duration_ns("rpc-wait-timeout", -1);
    let protocol = Protocol::parse(&p.string("protocol"))?;
    Ok(RunConfig {
        c: c as usize,
        s: s as usize,
        waves,
        wave_interval: p.duration("wave-interval", Duration::from_secs(60)),
        ramp_rate: to_u32(ramp),
        setup_timeout: p.duration("setup-timeout", Duration::from_secs(120)),
        settle: p.duration("settle", Duration::from_secs(5)),
        recovery_timeout: p.duration("recovery-timeout", Duration::from_secs(300)),
        rpc_latency: p.duration("rpc-latency", Duration::from_millis(100)),
        rpc_jitter: jitter,
        connect_latency: p.duration("rpc-connect-latency", Duration::ZERO),
        command_latency: p.duration("rpc-command-latency", Duration::ZERO),
        disconnect_latency: p.duration("rpc-disconnect-latency", Duration::ZERO),
        rpc_concurrency: to_u32(conc),
        // Upstream: whole milliseconds, any negative value means -1.
        rpc_wait_timeout_ms: if wait_timeout < 0 { -1 } else { wait_timeout / 1_000_000 },
        disconnect_rate: to_u32(rate),
        disconnect_backlog: to_u32(backlog),
        ping_interval: to_u32(p.int("ping-interval", 3).max(0)),
        ping_timeout: p.duration("ping-timeout", Duration::from_secs(6)),
        sequential_subscribe: p.boolean("sequential-subscribe"),
        sub_retry_interval: nonneg(retry_interval),
        sub_retries_max: to_u32(retries_max),
        probes: probes as usize,
        probe_interval: nonneg(probe_interval),
        backoff_min: p.duration("backoff-min", Duration::from_millis(500)),
        backoff_max: p.duration("backoff-max", Duration::from_secs(10)),
        seed: p.int("seed", 1) as u64,
        non_interactive: p.boolean("non-interactive"),
        timeline: optional_path(p.string("timeline")),
        server_log: optional_path(p.string("server-log")),
        sut: optional_path(p.string("sut")),
        server_args: p.list("server-arg"),
        protocol,
    })
}

/// Records one [`Sample`] per second on a background thread.
struct Sampler {
    samples: Arc<Mutex<Vec<Sample>>>,
    stop: CancelToken,
    thread: Mutex<Option<JoinHandle<()>>>,
    record: Arc<dyn Fn() -> Sample + Send + Sync>,
}

impl Sampler {
    fn start(record: Arc<dyn Fn() -> Sample + Send + Sync>) -> Self {
        let samples = Arc::new(Mutex::new(vec![record()]));
        let stop = CancelToken::new();
        let (s, st, r) = (Arc::clone(&samples), stop.clone(), Arc::clone(&record));
        let thread = std::thread::Builder::new()
            .name("floods-sampler".into())
            .spawn(move || {
                let mut next = Instant::now() + Duration::from_secs(1);
                loop {
                    let wait = next.saturating_duration_since(Instant::now());
                    if !st.sleep(wait) {
                        return;
                    }
                    let smp = r();
                    s.lock().unwrap_or_else(|e| e.into_inner()).push(smp);
                    next += Duration::from_secs(1);
                }
            })
            .expect("spawning the sampler");
        Self { samples, stop, thread: Mutex::new(Some(thread)), record }
    }

    fn stop(&self) {
        self.stop.cancel();
        if let Some(t) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = t.join();
            let smp = (self.record)();
            self.samples.lock().unwrap_or_else(|e| e.into_inner()).push(smp);
        }
    }

    fn all(&self) -> Vec<Sample> {
        self.samples.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Runs until done or until `interrupt` is cancelled (then prints the summary with
/// `interrupted=true` and returns 130).
pub fn run_with_config(interrupt: &CancelToken, cfg: RunConfig, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    let start = Instant::now();
    let ui = Ui::new(cfg.non_interactive, start);
    let Some(sut) = super::resolve_sut(cfg.sut.clone(), None) else {
        let _ = writeln!(stderr, "error: no server binary: pass --sut or set ${}", super::SUT_ENV);
        return super::EXIT_USAGE;
    };

    let stats = Arc::new(ClientStats::new());
    let probe_stats = Arc::new(ClientStats::new());
    let (s1, s2) = (Arc::clone(&stats), Arc::clone(&probe_stats));
    let rpc = Arc::new(RpcServer::new(RpcServerConfig {
        latency: cfg.rpc_latency,
        connect_latency: cfg.connect_latency,
        command_latency: cfg.command_latency,
        disconnect_latency: cfg.disconnect_latency,
        jitter: cfg.rpc_jitter,
        session_closed: Some(Arc::new(move |sid: &str| s1.session_closed(sid) || s2.session_closed(sid))),
    }));

    let pusher = cfg.protocol.pusher().cloned();
    let server = match Server::build(ServerConfig {
        log: cfg.server_log.clone().map_or(ServerLog::Discard, ServerLog::File),
        // Pusher channels are public: no RPC application.
        rpc_host: pusher.is_none().then(|| rpc.url()),
        pusher: pusher.clone(),
        tuning: ServerTuning {
            ping_interval: Some(cfg.ping_interval),
            rpc_concurrency: Some(cfg.rpc_concurrency),
            rpc_wait_timeout_ms: Some(cfg.rpc_wait_timeout_ms),
            disconnect_rate: Some(cfg.disconnect_rate),
            disconnect_backlog: Some(cfg.disconnect_backlog),
            stats_refresh_interval: Some(STATS_REFRESH.as_secs() as u32),
        },
        extra_args: cfg.server_args.clone(),
        env: if pusher.is_none() { app_latency_env(&cfg) } else { Vec::new() },
        ..ServerConfig::new(&sut)
    }) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            let _ = writeln!(stdout, "server build failed: {e}");
            rpc.close();
            return 1;
        }
    };

    // In Pusher mode no server has a disconnect queue to drain, and the keys stay the
    // same for every server (see the module docs).
    let metrics = pusher.is_none() && server.has_metrics();
    let channels: Vec<String> = match pusher {
        Some(_) => (1..=cfg.s).map(|i| format!("bench-{i}")).collect(),
        None => (1..=cfg.s).map(|i| format!(r#"{{"channel":"BenchChannel","id":"{i}"}}"#)).collect(),
    };
    let wire = cfg.protocol.wire();
    let keepalive = Duration::from_secs(u64::from(cfg.ping_interval.max(1)));
    let ctx = CancelToken::new();
    let mut tasks = Vec::new();
    let mut clients = Vec::with_capacity(cfg.c);
    for i in 0..cfg.c {
        let client = Arc::new(ReconnectingClient::new(ReconnectingClientConfig {
            url: server.websocket_url(),
            channels: channels.clone(),
            sequential_subscribe: cfg.sequential_subscribe,
            subscribe_retry_interval: cfg.sub_retry_interval,
            subscribe_retries_max: cfg.sub_retries_max,
            ping_timeout: cfg.ping_timeout,
            probe_interval: Duration::ZERO,
            backoff: Backoff { min: cfg.backoff_min, max: cfg.backoff_max },
            stats: Arc::clone(&stats),
            seed: cfg.seed.wrapping_add(i as u64),
            wire,
            keepalive,
        }));
        let delay = Duration::from_secs(1).mul_f64(i as f64 / f64::from(cfg.ramp_rate.max(1)));
        let (c, t) = (Arc::clone(&client), ctx.clone());
        tasks.push(rt().spawn(async move { c.run_async(&t, delay).await }));
        clients.push(client);
    }
    // Probes use their own stats, so they do not count as clients. They connect
    // first and are never dropped by waves.
    for i in 0..cfg.probes {
        let probe = ReconnectingClient::new(ReconnectingClientConfig {
            url: server.websocket_url(),
            channels: Vec::new(),
            sequential_subscribe: false,
            subscribe_retry_interval: cfg.sub_retry_interval,
            subscribe_retries_max: 0,
            ping_timeout: cfg.ping_timeout,
            probe_interval: cfg.probe_interval,
            backoff: Backoff { min: cfg.backoff_min, max: cfg.backoff_max },
            stats: Arc::clone(&probe_stats),
            seed: cfg.seed.wrapping_add((cfg.c + i) as u64),
            wire,
            keepalive,
        });
        let t = ctx.clone();
        tasks.push(rt().spawn(async move { probe.run_async(&t, Duration::ZERO).await }));
    }

    let record = {
        let (rpc, server, stats, probe_stats) =
            (Arc::clone(&rpc), Arc::clone(&server), Arc::clone(&stats), Arc::clone(&probe_stats));
        Arc::new(move || record_sample(start, &rpc, &server, &stats, &probe_stats))
    };
    let sampler = Sampler::start(record);
    let generator = GeneratorMonitor::start();

    let mut waves: Vec<WaveResult> = Vec::with_capacity(cfg.waves.len());
    let mut recovered = false;
    ui.log(stderr, &format!("setup: connecting {} clients at {}/s", cfg.c, cfg.ramp_rate));
    let setup_ok = wait_healthy(interrupt, &stats, cfg.c, cfg.setup_timeout);
    if !setup_ok && interrupt.is_cancelled() {
        ui.log(
            stderr,
            &format!("interrupted during setup: {}/{} clients healthy", stats.state(ClientState::Healthy), cfg.c),
        );
    } else if !setup_ok {
        ui.log(
            stderr,
            &format!(
                "setup failed: {}/{} clients healthy after {}",
                stats.state(ClientState::Healthy),
                cfg.c,
                super::go_duration_string(cfg.setup_timeout)
            ),
        );
    } else {
        ui.log(stderr, &format!("setup done: {} clients healthy in {:.1}s", cfg.c, start.elapsed().as_secs_f64()));
        interrupt.sleep(cfg.settle);
        let mut rng = Rng::new(cfg.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
        for (i, &size) in cfg.waves.iter().enumerate() {
            if interrupt.is_cancelled() {
                break;
            }
            let last = i == cfg.waves.len() - 1;
            let dropped_before = stats.reason(CloseReason::Dropped);
            let discarded_before = server.counter("disconnect_queue_discarded_total");
            let fired = Instant::now();
            let mut drain = DrainTracker::new(fired, rpc.stats(RpcMethod::Disconnect).started, discarded_before);
            let dropped = drop_wave(&clients, size, &mut rng);
            drain.dropped = dropped;
            let mut w = WaveResult { size, dropped, fired_at: fired - start, recovery: None, drained: None };
            ui.log(
                stderr,
                &format!("wave {}/{}: dropped {dropped} clients (requested {size})", i + 1, cfg.waves.len()),
            );

            // After the last wave recovers, the run keeps going until the disconnect
            // queue is empty (or it is interrupted).
            let wait = if last { cfg.recovery_timeout } else { cfg.wave_interval };
            let deadline = fired + wait;
            while !interrupt.is_cancelled() && (Instant::now() < deadline || (last && w.recovery.is_some())) {
                if w.recovery.is_none()
                    && stats.reason(CloseReason::Dropped) - dropped_before >= dropped as i64
                    && stats.state(ClientState::Healthy) == cfg.c as i64
                {
                    w.recovery = Some(fired.elapsed());
                    ui.log(
                        stderr,
                        &format!(
                            "wave {}/{}: all clients healthy after {:.1}s",
                            i + 1,
                            cfg.waves.len(),
                            fired.elapsed().as_secs_f64()
                        ),
                    );
                }
                if last && !metrics && w.recovery.is_some() {
                    // Without the queue gauge the drain is not observable.
                    break;
                }
                drain.track_rpc(rpc.stats(RpcMethod::Disconnect).started);
                if let Some(recovered_at) = w.recovery.map(|r| fired + r)
                    && w.drained.is_none()
                    && let Ok(m) = server.metrics()
                    && let Some(at) = drain.observe(start, recovered_at, &m)
                {
                    w.drained = Some(at - fired);
                    let secs = (at - fired).as_secs_f64();
                    ui.log(
                        stderr,
                        &format!("wave {}/{}: disconnect queue empty after {secs:.1}s", i + 1, cfg.waves.len()),
                    );
                    if last {
                        break;
                    }
                }
                interrupt.sleep(Duration::from_millis(100));
            }
            if interrupt.is_cancelled() {
                ui.log(stderr, &format!("wave {}/{}: interrupted", i + 1, cfg.waves.len()));
            } else if w.recovery.is_none() {
                ui.log(
                    stderr,
                    &format!(
                        "wave {}/{}: not recovered after {}",
                        i + 1,
                        cfg.waves.len(),
                        super::go_duration_string(wait)
                    ),
                );
            }
            waves.push(w);
        }
        // Waves left out by an interrupt do not count as recovered.
        recovered = waves.len() == cfg.waves.len() && waves.last().is_some_and(|w| w.recovery.is_some());
    }
    let interrupted = interrupt.is_cancelled();

    sampler.stop();
    let samples = sampler.all();
    let mut sum = build_summary(&cfg, setup_ok, recovered, interrupted, start.elapsed(), waves, &stats, &rpc, &samples);
    sum.server_metrics = metrics;
    sum.generator = generator.finish();
    print_summary(stdout, &sum);
    if let Some(path) = &cfg.timeline
        && let Err(e) = write_timeline(path, &samples)
    {
        let _ = writeln!(stdout, "timeline write failed: {e}");
    }

    ctx.cancel();
    block_on(async move {
        for t in tasks {
            let _ = t.await;
        }
    });
    // After a storm, thousands of closed sessions can wait in the disconnect queue;
    // the run is over, so the shutdown is bounded (SIGKILL after the grace period).
    let _ = server.shutdown();
    rpc.close();

    if interrupted {
        return super::EXIT_INTERRUPTED;
    }
    if !setup_ok || !recovered {
        return 1;
    }
    0
}

/// The fake RPC app's per-method latency (milliseconds) and jitter, for servers that
/// run the application in-process instead of calling it:
/// `CABBY_BENCH_APP_LATENCY_{CONNECT,COMMAND,DISCONNECT}_MS` and
/// `CABBY_BENCH_APP_LATENCY_JITTER`. Servers that use the RPC app ignore them.
pub fn app_latency_env(cfg: &RunConfig) -> Vec<(String, String)> {
    let effective = |d: Duration| if d.is_zero() { cfg.rpc_latency } else { d };
    let mut env: Vec<(String, String)> =
        [("CONNECT", cfg.connect_latency), ("COMMAND", cfg.command_latency), ("DISCONNECT", cfg.disconnect_latency)]
            .into_iter()
            .map(|(m, d)| (format!("CABBY_BENCH_APP_LATENCY_{m}_MS"), format!("{}", ms(effective(d)))))
            .collect();
    env.push(("CABBY_BENCH_APP_LATENCY_JITTER".to_owned(), cfg.rpc_jitter.to_string()));
    env
}

/// How long the disconnect queue must read empty, with no Disconnect progress,
/// before a drain counts even though the dropped sessions are not all accounted
/// for (a session closed before its Connect call finished gets no Disconnect).
const DRAIN_QUIET: Duration = Duration::from_secs(5);

/// Follows one wave's disconnect-queue drain across 100 ms polls.
///
/// The reported drain time is the latest of: recovery (as upstream, the drain is
/// measured after it), the poll at which the dropped sessions were all accounted
/// for (Disconnect calls started plus discards; a call starts when its session
/// leaves the queue, and the fake RPC sees it within one 100 ms poll), and the
/// latest moment a non-zero gauge proves the queue was still non-empty (its read
/// time minus the refresh interval). The drain is only declared on a fresh zero
/// gauge ([`wave_drained`]), so a stale gauge can neither end it early nor delay
/// its time by a refresh interval.
struct DrainTracker {
    fired: Instant,
    dropped: usize,
    disconnects_before: i64,
    discarded_before: u64,
    disconnects: i64,
    last_progress: Instant,
    accounted_at: Option<Instant>,
    last_nonzero: Instant,
    zero_since: Option<Instant>,
}

impl DrainTracker {
    fn new(fired: Instant, disconnects_before: i64, discarded_before: u64) -> Self {
        Self {
            fired,
            dropped: 0,
            disconnects_before,
            discarded_before,
            disconnects: disconnects_before,
            last_progress: fired,
            accounted_at: None,
            last_nonzero: fired,
            zero_since: None,
        }
    }

    /// Records the Disconnect calls started so far (every poll).
    fn track_rpc(&mut self, started: i64) {
        if started > self.disconnects {
            self.disconnects = started;
            self.last_progress = Instant::now();
        }
    }

    /// Evaluates a metrics read taken after recovery; the drain time once drained.
    fn observe(&mut self, start: Instant, recovered_at: Instant, m: &crate::server::Metrics) -> Option<Instant> {
        let now = Instant::now();
        let obs = DrainObservation {
            fired_at: self.fired - start,
            gauge_refreshed_at: now.saturating_duration_since(start).saturating_sub(STATS_REFRESH + STATS_SLACK),
            queue_size: m.get("disconnect_queue_size"),
            disconnects_since_wave: self.disconnects - self.disconnects_before,
            discarded_since_wave: m.get("disconnect_queue_discarded_total").saturating_sub(self.discarded_before),
            dropped: self.dropped,
        };
        let accounted = u64::try_from(obs.disconnects_since_wave.max(0)).unwrap_or(0) + obs.discarded_since_wave;
        if self.accounted_at.is_none() && accounted >= self.dropped as u64 {
            self.accounted_at = Some(now);
        }
        if obs.queue_size > 0 {
            // The read may be stale: the queue was certainly non-empty only at the
            // refresh that produced it, no earlier than one refresh interval ago.
            let known = now.checked_sub(STATS_REFRESH + STATS_SLACK).unwrap_or(self.fired);
            self.last_nonzero = self.last_nonzero.max(known);
            self.zero_since = None;
        } else if obs.gauge_refreshed_at > obs.fired_at {
            self.zero_since.get_or_insert(now);
        }
        let at = |accounted: Instant| recovered_at.max(accounted).max(self.last_nonzero);
        if wave_drained(&obs) {
            return Some(at(self.accounted_at.unwrap_or(now)));
        }
        let quiet = self.zero_since.is_some_and(|z| now - z >= DRAIN_QUIET) && now - self.last_progress >= DRAIN_QUIET;
        quiet.then(|| at(self.last_progress))
    }
}

fn record_sample(
    start: Instant,
    rpc: &RpcServer,
    server: &Server,
    stats: &ClientStats,
    probe_stats: &ClientStats,
) -> Sample {
    let m = server.metrics().unwrap_or_default();
    let res = server.resources().unwrap_or_default();
    let mut smp = Sample {
        at: start.elapsed(),
        rpc_pending: m.get("rpc_pending_num"),
        // Calls dropped while waiting for an RPC slot because the session closed.
        rpc_canceled: m.get("rpc_canceled_total"),
        // Calls that waited for an RPC slot longer than rpc_wait_timeout.
        rpc_wait_timeout: m.get("rpc_wait_timeout_total"),
        // Sessions dropped from the full disconnect queue.
        discarded: m.get("disconnect_queue_discarded_total"),
        sessions: m.get("clients_num"),
        disconnect_queue: m.get("disconnect_queue_size"),
        server_threads: res.threads,
        server_rss_bytes: res.rss_bytes,
        reasons: CloseReason::ALL.iter().map(|r| (*r, stats.reason(*r))).collect(),
        attempts: stats.attempts(),
        reconnects: stats.reconnects(),
        sub_retries: stats.subscribe_retries(),
        welcome: Some(stats.welcome.snapshot()),
        confirm: Some(stats.confirm.snapshot()),
        probe: Some(probe_stats.probe.snapshot()),
        ..Default::default()
    };
    for (i, st) in ClientState::ALL.iter().enumerate() {
        smp.states[i] = stats.state(*st);
    }
    for (i, m) in RpcMethod::ALL.iter().enumerate() {
        let s = rpc.stats(*m);
        smp.rpc_started[i] = s.started;
        smp.rpc_done[i] = s.completed;
        smp.rpc_in_flight += s.in_flight;
        if i < 2 {
            smp.after_close[i] = s.after_close;
        }
    }
    smp
}

/// Abruptly closes up to `size` open connections, chosen at random. Clients
/// between connections are skipped, so the result can be lower than `size`.
fn drop_wave(clients: &[Arc<ReconnectingClient>], size: usize, rng: &mut Rng) -> usize {
    let mut dropped = 0;
    for i in rng.perm(clients.len()) {
        if dropped == size {
            break;
        }
        if clients[i].drop_connection() {
            dropped += 1;
        }
    }
    dropped
}

fn wait_healthy(interrupt: &CancelToken, stats: &ClientStats, c: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline && !interrupt.is_cancelled() {
        if stats.state(ClientState::Healthy) == c as i64 {
            return true;
        }
        interrupt.sleep(Duration::from_millis(100));
    }
    false
}

/// What [`wave_drained`] looks at, captured at one poll after a wave.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainObservation {
    /// Time of the wave, on the harness clock.
    pub fired_at: Duration,
    /// When the server last refreshed its gauges (from the metrics endpoint), on the
    /// harness clock.
    pub gauge_refreshed_at: Duration,
    /// The `disconnect_queue_size` gauge.
    pub queue_size: u64,
    /// Disconnect calls that reached the fake RPC since the wave (a call starts
    /// when its session leaves the queue).
    pub disconnects_since_wave: i64,
    /// `disconnect_queue_discarded_total` growth since the wave.
    pub discarded_since_wave: u64,
    /// Connections the wave closed.
    pub dropped: usize,
}

/// Whether the disconnect queue drained after a wave. Upstream reads the embedded
/// queue's size directly; with an external server the gauge can be stale, so a 0
/// only counts when it was refreshed after the wave *and* every dropped session has
/// been accounted for by a completed Disconnect call or a discard.
///
/// The scenario takes `gauge_refreshed_at` as a lower bound: a gauge read at time
/// `t` reflects a refresh no earlier than `t − refresh interval − slack`.
pub fn wave_drained(o: &DrainObservation) -> bool {
    let accounted = u64::try_from(o.disconnects_since_wave.max(0)).unwrap_or(0).saturating_add(o.discarded_since_wave);
    o.gauge_refreshed_at > o.fired_at && o.queue_size == 0 && accounted >= o.dropped as u64
}

/// Current and longest run of consecutive samples in which clients waited for
/// confirmations but no Command call completed (upstream `commandStall`).
pub fn command_stall(samples: &[Sample]) -> (Duration, Duration) {
    let subscribing = ClientState::Subscribing as usize;
    let command = 1; // RpcMethod::Command in RpcMethod::ALL order
    let mut run_start: Option<Duration> = None;
    let (mut current, mut longest) = (Duration::ZERO, Duration::ZERO);
    for pair in samples.windows(2) {
        let (prev, cur) = (&pair[0], &pair[1]);
        let stalled = cur.states[subscribing] > 0 && cur.rpc_done[command] == prev.rpc_done[command];
        if stalled {
            run_start.get_or_insert(prev.at);
        } else {
            run_start = None;
        }
        match run_start {
            Some(rs) => {
                current = cur.at.saturating_sub(rs);
                longest = longest.max(current);
            }
            None => current = Duration::ZERO,
        }
    }
    (current, longest)
}

struct Summary {
    setup_ok: bool,
    recovered: bool,
    interrupted: bool,
    clients: usize,
    run_seconds: f64,
    waves: Vec<WaveResult>,
    attempts: i64,
    reconnects: i64,
    sub_retries: i64,
    reasons: Vec<(CloseReason, i64)>,
    rpc_calls: [i64; 3],
    rpc_after_close: [i64; 2],
    discarded: u64,
    /// `None` when the server made no RPC calls (the stall is measured on them).
    max_command_stall: Option<Duration>,
    welcome: HistogramSnapshot,
    confirm: HistogramSnapshot,
    healthy: HistogramSnapshot,
    probe: HistogramSnapshot,
    peak_disconnect_queue: u64,
    peak_rpc_pending: u64,
    rpc_canceled: u64,
    rpc_wait_timeout: u64,
    peak_sessions: u64,
    peak_server_threads: u64,
    peak_server_rss: u64,
    /// The server has a metrics endpoint ([`Server::has_metrics`]).
    server_metrics: bool,
    generator: GeneratorReport,
}

#[allow(clippy::too_many_arguments)]
fn build_summary(
    cfg: &RunConfig,
    setup_ok: bool,
    recovered: bool,
    interrupted: bool,
    elapsed: Duration,
    waves: Vec<WaveResult>,
    stats: &ClientStats,
    rpc: &RpcServer,
    samples: &[Sample],
) -> Summary {
    let mut s = Summary {
        setup_ok,
        recovered,
        interrupted,
        clients: cfg.c,
        run_seconds: elapsed.as_secs_f64(),
        waves,
        attempts: stats.attempts(),
        reconnects: stats.reconnects(),
        sub_retries: stats.subscribe_retries(),
        reasons: CloseReason::ALL.iter().map(|r| (*r, stats.reason(*r))).collect(),
        rpc_calls: [0; 3],
        rpc_after_close: [0; 2],
        discarded: 0,
        max_command_stall: (rpc.stats(RpcMethod::Connect).started > 0).then(|| command_stall(samples).1),
        welcome: stats.welcome.snapshot(),
        confirm: stats.confirm.snapshot(),
        healthy: stats.healthy.snapshot(),
        probe: HistogramSnapshot::default(),
        peak_disconnect_queue: 0,
        peak_rpc_pending: 0,
        rpc_canceled: 0,
        rpc_wait_timeout: 0,
        peak_sessions: 0,
        peak_server_threads: 0,
        peak_server_rss: 0,
        server_metrics: true,
        generator: GeneratorReport::default(),
    };
    for (i, m) in RpcMethod::ALL.iter().enumerate() {
        s.rpc_calls[i] = rpc.stats(*m).completed;
        if i < 2 {
            s.rpc_after_close[i] = rpc.stats(*m).after_close;
        }
    }
    if let Some(last) = samples.last() {
        s.rpc_canceled = last.rpc_canceled;
        s.rpc_wait_timeout = last.rpc_wait_timeout;
        s.discarded = last.discarded;
        s.probe = last.probe.clone().unwrap_or_default();
    }
    for x in samples {
        s.peak_disconnect_queue = s.peak_disconnect_queue.max(x.disconnect_queue);
        s.peak_rpc_pending = s.peak_rpc_pending.max(x.rpc_pending);
        s.peak_sessions = s.peak_sessions.max(x.sessions);
        s.peak_server_threads = s.peak_server_threads.max(x.server_threads);
        s.peak_server_rss = s.peak_server_rss.max(x.server_rss_bytes);
    }
    s
}

fn dur_or_neg(d: Option<Duration>) -> f64 {
    d.map_or(-1.0, |d| d.as_secs_f64())
}

fn print_summary(w: &mut dyn Write, s: &Summary) {
    let mut o = String::new();
    o.push_str(&format!("setup_ok={}\n", s.setup_ok));
    o.push_str(&format!("recovered={}\n", s.recovered));
    o.push_str(&format!("interrupted={}\n", s.interrupted));
    o.push_str(&format!("clients={}\n", s.clients));
    o.push_str(&format!("run_seconds={:.2}\n", s.run_seconds));
    o.push_str(&format!("waves_fired={}\n", s.waves.len()));
    for (i, wv) in s.waves.iter().enumerate() {
        let n = i + 1;
        o.push_str(&format!("wave_{n}_size={}\n", wv.size));
        o.push_str(&format!("wave_{n}_dropped={}\n", wv.dropped));
        o.push_str(&format!("wave_{n}_fired_at_seconds={:.2}\n", wv.fired_at.as_secs_f64()));
        o.push_str(&format!("wave_{n}_recovery_seconds={:.2}\n", dur_or_neg(wv.recovery)));
        o.push_str(&format!("wave_{n}_drained_seconds={:.2}\n", dur_or_neg(wv.drained)));
    }
    o.push_str(&format!("connection_attempts={}\n", s.attempts));
    o.push_str(&format!("reconnects={}\n", s.reconnects));
    o.push_str(&format!("subscribe_retries={}\n", s.sub_retries));
    for (r, n) in &s.reasons {
        o.push_str(&format!("close_{}={n}\n", r.as_str()));
    }
    o.push_str(&format!("rpc_connect_calls={}\n", s.rpc_calls[0]));
    o.push_str(&format!("rpc_command_calls={}\n", s.rpc_calls[1]));
    o.push_str(&format!("rpc_disconnect_calls={}\n", s.rpc_calls[2]));
    if s.server_metrics {
        o.push_str(&format!("rpc_canceled_calls={}\n", s.rpc_canceled));
        o.push_str(&format!("rpc_wait_timeout_calls={}\n", s.rpc_wait_timeout));
    }
    o.push_str(&format!("rpc_connect_after_close_calls={}\n", s.rpc_after_close[0]));
    o.push_str(&format!("rpc_command_after_close_calls={}\n", s.rpc_after_close[1]));
    if s.server_metrics {
        o.push_str(&format!("disconnect_discarded={}\n", s.discarded));
    }
    o.push_str(&format!("max_command_stall_seconds={:.0}\n", dur_or_neg(s.max_command_stall)));
    for (name, h) in [("welcome", &s.welcome), ("confirm", &s.confirm), ("healthy", &s.healthy), ("probe", &s.probe)] {
        o.push_str(&format!("{name}_count={}\n", h.count));
        o.push_str(&format!("{name}_p50_ms={:.1}\n", ms(h.quantile(0.5))));
        o.push_str(&format!("{name}_p95_ms={:.1}\n", ms(h.quantile(0.95))));
        o.push_str(&format!("{name}_p99_ms={:.1}\n", ms(h.quantile(0.99))));
        o.push_str(&format!("{name}_max_ms={:.1}\n", ms(h.max)));
    }
    if s.server_metrics {
        o.push_str(&format!("peak_disconnect_queue={}\n", s.peak_disconnect_queue));
        o.push_str(&format!("peak_rpc_pending={}\n", s.peak_rpc_pending));
        o.push_str(&format!("peak_server_sessions={}\n", s.peak_sessions));
    }
    o.push_str(&format!("peak_server_threads={}\n", s.peak_server_threads));
    o.push_str(&format!("peak_server_rss_mb={:.1}\n", mib(s.peak_server_rss)));
    o.push_str(&format!("server_metrics={}\n", s.server_metrics));
    // Cabby addition: whether the generator kept up (a starved generator misses
    // pings and confirmations by itself).
    s.generator.write(&mut o, s.generator.starved());
    let _ = w.write_all(o.as_bytes());
}

/// One CSV row per 1s sample. Rates are per second against the previous sample;
/// timings are p50/p99 of the samples recorded within that second.
fn write_timeline(path: &std::path::Path, samples: &[Sample]) -> std::io::Result<()> {
    let mut header: Vec<String> = [
        "t",
        "healthy",
        "subscribing",
        "connecting",
        "backoff",
        "rpc_connect_ps",
        "rpc_command_ps",
        "rpc_disconnect_ps",
        "rpc_canceled_ps",
        "rpc_wait_timeout_ps",
        "rpc_connect_after_close_ps",
        "rpc_command_after_close_ps",
        "rpc_in_flight",
        "rpc_pending",
        "sessions",
        "disconnect_queue",
        "disconnect_discarded_ps",
        "server_threads",
        "server_rss_mb",
        "attempts_ps",
        "sub_retries_ps",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    header.extend(CloseReason::ALL.iter().map(|r| format!("close_{}_ps", r.as_str())));
    header.extend(
        ["welcome_p50_ms", "welcome_p99_ms", "confirm_p50_ms", "confirm_p99_ms", "probe_p50_ms", "probe_max_ms"]
            .iter()
            .map(|s| (*s).to_owned()),
    );
    let mut out = header.join(",") + "\n";
    for pair in samples.windows(2) {
        let (prev, cur) = (&pair[0], &pair[1]);
        let secs = cur.at.saturating_sub(prev.at).as_secs_f64().max(1e-9);
        let rate = |a: i64, b: i64| format!("{:.1}", (a - b) as f64 / secs);
        let (h, sub, conn, back) = (cur.states[2], cur.states[1], cur.states[0], cur.states[3]);
        let mut row = vec![
            format!("{:.0}", cur.at.as_secs_f64()),
            h.to_string(),
            sub.to_string(),
            conn.to_string(),
            back.to_string(),
            rate(cur.rpc_done[0], prev.rpc_done[0]),
            rate(cur.rpc_done[1], prev.rpc_done[1]),
            rate(cur.rpc_done[2], prev.rpc_done[2]),
            rate(cur.rpc_canceled as i64, prev.rpc_canceled as i64),
            rate(cur.rpc_wait_timeout as i64, prev.rpc_wait_timeout as i64),
            rate(cur.after_close[0], prev.after_close[0]),
            rate(cur.after_close[1], prev.after_close[1]),
            cur.rpc_in_flight.to_string(),
            cur.rpc_pending.to_string(),
            cur.sessions.to_string(),
            cur.disconnect_queue.to_string(),
            rate(cur.discarded as i64, prev.discarded as i64),
            cur.server_threads.to_string(),
            format!("{:.1}", mib(cur.server_rss_bytes)),
            rate(cur.attempts, prev.attempts),
            rate(cur.sub_retries, prev.sub_retries),
        ];
        row.extend(CloseReason::ALL.iter().map(|r| rate(cur.reason(*r), prev.reason(*r))));
        let diff = |c: &Option<HistogramSnapshot>, p: &Option<HistogramSnapshot>| match (c, p) {
            (Some(c), Some(p)) => c.sub(p),
            (Some(c), None) => c.clone(),
            _ => HistogramSnapshot::default(),
        };
        let (wl, cf, pr) =
            (diff(&cur.welcome, &prev.welcome), diff(&cur.confirm, &prev.confirm), diff(&cur.probe, &prev.probe));
        for v in [
            ms(wl.quantile(0.5)),
            ms(wl.quantile(0.99)),
            ms(cf.quantile(0.5)),
            ms(cf.quantile(0.99)),
            ms(pr.quantile(0.5)),
            ms(pr.max),
        ] {
            row.push(format!("{v:.0}"));
        }
        out.push_str(&row.join(","));
        out.push('\n');
    }
    std::fs::write(path, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stale_zero_does_not_count_as_drained() {
        let fresh = DrainObservation {
            fired_at: Duration::from_secs(10),
            gauge_refreshed_at: Duration::from_secs(11),
            queue_size: 0,
            disconnects_since_wave: 4,
            discarded_since_wave: 1,
            dropped: 5,
        };
        assert!(wave_drained(&fresh));
        assert!(!wave_drained(&DrainObservation { gauge_refreshed_at: Duration::from_secs(9), ..fresh }));
        assert!(!wave_drained(&DrainObservation { disconnects_since_wave: 3, ..fresh }));
        assert!(!wave_drained(&DrainObservation { queue_size: 1, ..fresh }));
    }
}
