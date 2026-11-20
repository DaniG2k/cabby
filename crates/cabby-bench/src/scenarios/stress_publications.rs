//! `stress_publications` (upstream `scenarios/stress_publications/main.go`): dial `-c`
//! clients, subscribe each to `-s` random streams of `1..=-S`, publish at `-r`/s for
//! `-d` on an absolute tick grid, drain, reconcile, print a key=value summary.
//!
//! Exit codes: 0 clean, 1 setup failure or `clients_short > 0` or
//! `publications_dropped > 0` (Cabby: or duplicates, ordering violations or
//! misrouted messages), 2 flag error (usage on stderr).
//!
//! # Accuracy
//!
//! * Every broadcast carries `<stream>:<seq>` plus the publisher worker and its
//!   dispatch time ([`crate::ledger`]); deliveries are reconciled by id per client
//!   and stream, not by counting accepted publications, and the delivery latency
//!   is measured end to end on the harness's monotonic clock.
//! * Server resources (peak RSS and threads, CPU time over the publish + drain
//!   window, residual sockets/fds after the clients leave) are sampled from
//!   `/proc/<pid>` of the external server.
//! * The summary says whether the load generator kept up (`generator_saturated`):
//!   the publish schedule fell behind (more than 1% late ticks), the harness
//!   runtime was starved (a 5 ms timer fired 5 ms or more late at p99), or the
//!   harness kept 90% or more of its worker threads busy. A saturated run measures
//!   the generator (or an overloaded host), not the server; rerun with fewer
//!   clients or a lower rate, or give the harness its own cores (`taskset`,
//!   `CABBY_BENCH_THREADS`). Late ticks also flag a host whose CPUs are all busy
//!   (server plus generator), which skews any comparison.
//!
//! # Pusher mode
//!
//! `--protocol=pusher` runs the same workload over the Pusher protocol ([`crate::pusher`]):
//! the server is started in Pusher mode, each stream is a public Pusher channel of the
//! same name, the clients speak pusher-js's protocol 7, and the publisher posts signed
//! events to the Pusher HTTP API (`--publish-batch` above 1 uses `batch_events`, capped
//! at 10 events). Payloads, reconciliation, latency stamping and the summary are those
//! of the Action Cable mode.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::{Duration, Instant};

use super::{FlagDef, FlagError, GeneratorMonitor, GeneratorReport, Kind, SUT_FLAGS, Ui, mib, ms};
use crate::counter::Accumulator;
use crate::histogram::HistogramSnapshot;
use crate::ledger::{self, DeliveryLedger};
use crate::pool::{ClientPool, PoolConfig, PoolHook};
use crate::publisher::{Publisher, PublisherOptions};
use crate::pusher::Protocol;
use crate::resources::ResourceMonitor;
use crate::rng::Rng;
use crate::server::{Server, ServerConfig};
use crate::throughput::{self, ReceiveSampler};

/// Sampling interval of the receive and publish timelines (upstream
/// `progressTickInterval`).
const TICK: Duration = Duration::from_millis(100);

/// Parsed flags. `-c`, `-r`, `-d`, `-S`, `-s` are required (> 0, `s <= S`).
#[derive(Debug, Clone, PartialEq)]
pub struct RunConfig {
    pub c: usize,
    pub r: usize,
    pub d: Duration,
    pub big_s: usize,
    pub s: usize,
    /// `--drain-timeout`, default 30s.
    pub drain_timeout: Duration,
    /// `--max-inflight`, default 4096.
    pub max_inflight: usize,
    /// `--publish-workers`, default 64.
    pub publish_workers: usize,
    /// `--publish-batch`, default 64.
    pub publish_batch: usize,
    /// `--setup-failure-tolerance`, default 0.
    pub tolerance: usize,
    /// `--seed`, default 1 (an int64 reinterpreted as u64).
    pub seed: u64,
    pub non_interactive: bool,
    /// `--sut`.
    pub sut: Option<PathBuf>,
    /// `--server-arg`, repeatable.
    pub server_args: Vec<String>,
    /// `--protocol`, default Action Cable.
    pub protocol: Protocol,
}

/// Runs after the server, pool and publisher are built, before the publish window.
pub type SetupHook = Box<dyn FnOnce(&Server, &ClientPool, &Publisher)>;

/// Test seams (upstream `runOpts`); production uses the default.
#[derive(Default)]
pub struct RunOpts {
    /// Overrides the pool's random subsets.
    pub stream_subsets: Option<Vec<Vec<String>>>,
    /// Replaces the publisher target.
    pub broadcast_url: Option<String>,
    pub setup_hook: Option<SetupHook>,
    /// Server binary when `--sut` is not given.
    pub sut: Option<PathBuf>,
}

/// The reconciled numbers printed at the end of a run, in print order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Summary {
    pub throughput_msgs_per_sec: f64,
    pub throughput_max_msgs_per_sec: f64,
    pub throughput_min_msgs_per_sec: f64,
    pub throughput_p50_msgs_per_sec: f64,
    pub throughput_p95_msgs_per_sec: f64,
    pub expected_msgs_per_sec: f64,
    pub clients_complete: usize,
    pub clients_short: usize,
    pub messages_missing: i64,
    pub publications_issued: i64,
    pub publications_completed: i64,
    pub publications_dropped: i64,
    pub scheduler_late_publishes: u64,
    pub observed_publish_rate: f64,
    pub observed_publish_rate_max: f64,
    pub observed_publish_rate_min: f64,
    pub observed_publish_rate_p50: f64,
    pub observed_publish_rate_p95: f64,
    pub publish_window_wall_seconds: f64,
    // Cabby additions, printed after the upstream keys (SPEC.md §10): sequence
    // reconciliation, latency, server-process resources and generator health.
    /// Issued sequence numbers never received (by id, per client and stream).
    pub messages_missing_ids: u64,
    pub duplicates: u64,
    pub ordering_violations: u64,
    pub misrouted: u64,
    /// Delivery latency, POST sent → message received.
    pub latency: HistogramSnapshot,
    /// POST round trips of the publisher.
    pub post_round_trip: HistogramSnapshot,
    pub publications_failed_posts: i64,
    pub publisher_queue_peak: usize,
    pub peak_server_rss_mb: f64,
    pub peak_server_threads: u64,
    pub peak_server_sockets: u64,
    /// Server CPU time over the publish + drain window.
    pub server_cpu_seconds: f64,
    /// `server_cpu_seconds` divided by the window: average busy cores.
    pub server_cpu_cores: f64,
    /// Server sockets/fds/threads left above the pre-pool baseline after the
    /// clients closed (sockets and fds wait up to 2s to return).
    pub server_residual_sockets: i64,
    pub server_residual_fds: i64,
    pub server_residual_threads: i64,
    /// Harness health over the publish + drain window.
    pub generator: GeneratorReport,
    /// [`GeneratorReport::starved`], or more than 1% of the publish ticks late.
    pub generator_saturated: bool,
}

const NAME: &str = "stress_publications";

const DEFS: &[FlagDef] = &[
    FlagDef { name: "c", kind: Kind::Int, default: "", usage: "number of clients (required, must be > 0)" },
    FlagDef {
        name: "r",
        kind: Kind::Int,
        default: "",
        usage: "publish rate in messages per second (required, must be > 0)",
    },
    FlagDef { name: "d", kind: Kind::Duration, default: "", usage: "publish window duration (required, must be > 0)" },
    FlagDef {
        name: "S",
        kind: Kind::Int,
        default: "",
        usage: "size of the stream universe; streams are named 1..S (required, must be > 0)",
    },
    FlagDef { name: "s", kind: Kind::Int, default: "", usage: "per-client subscription count (required, 0 < s <= S)" },
    FlagDef {
        name: "drain-timeout",
        kind: Kind::Duration,
        default: "30s",
        usage: "max wall time to wait for in-flight messages after the publish window closes",
    },
    FlagDef {
        name: "max-inflight",
        kind: Kind::Int,
        default: "4096",
        usage: "publisher queue depth — full queue increments publications_dropped",
    },
    FlagDef {
        name: "publish-workers",
        kind: Kind::Int,
        default: "64",
        usage: "HTTP workers draining the publisher queue, one keep-alive connection each",
    },
    FlagDef {
        name: "publish-batch",
        kind: Kind::Int,
        default: "64",
        usage: "max broadcasts coalesced into a single POST under backpressure (1 disables batching); Pusher mode: default 1, at most 10 (batch_events, which AnyCable's Pusher API lacks)",
    },
    FlagDef {
        name: "setup-failure-tolerance",
        kind: Kind::Int,
        default: "",
        usage: "max client setup failures before aborting",
    },
    FlagDef {
        name: "seed",
        kind: Kind::Int,
        default: "1",
        usage: "RNG seed for stream-subset selection and per-tick stream choice",
    },
    FlagDef {
        name: "non-interactive",
        kind: Kind::Bool,
        default: "",
        usage: "suppress lifecycle logs on stderr; emit only the key=value summary on stdout (auto-on when stderr is not a TTY)",
    },
    super::PROTOCOL_FLAG,
    SUT_FLAGS[0],
    SUT_FLAGS[1],
];

/// Entry point: parse flags, run, return the exit code.
pub fn run(args: &[String], stdout: &mut dyn Write, stderr: &mut dyn Write, opts: RunOpts) -> i32 {
    match parse_flags(args, stderr) {
        Ok(cfg) => run_with_config(cfg, opts, stdout, stderr),
        Err(e) => {
            if e != "help" {
                let _ = writeln!(stderr, "error: {e}");
            }
            super::EXIT_USAGE
        }
    }
}

fn positive(v: i64) -> usize {
    usize::try_from(v).unwrap_or(0)
}

/// Upstream `parseFlags`; writes usage to `stderr` on missing required flags.
pub fn parse_flags(args: &[String], stderr: &mut dyn Write) -> Result<RunConfig, String> {
    let p = match super::parse(NAME, DEFS, args, stderr) {
        Ok(p) => p,
        Err(FlagError::Help) => return Err("help".into()),
        Err(FlagError::Invalid(m)) => return Err(m),
    };
    let (c, r, d, big_s, s) = (p.int("c", 0), p.int("r", 0), p.duration_ns("d", 0), p.int("S", 0), p.int("s", 0));
    if c <= 0 || r <= 0 || d <= 0 || big_s <= 0 || s <= 0 {
        super::write_usage(stderr, NAME, DEFS);
        return Err("required flags missing: -c, -r, -d, -S, -s must all be > 0".into());
    }
    if s > big_s {
        return Err(format!("-s ({s}) must be <= -S ({big_s})"));
    }
    let sut = p.string("sut");
    let protocol = Protocol::parse(&p.string("protocol"))?;
    Ok(RunConfig {
        c: positive(c),
        r: positive(r),
        d: Duration::from_nanos(u64::try_from(d).unwrap_or(0)),
        big_s: positive(big_s),
        s: positive(s),
        drain_timeout: p.duration("drain-timeout", Duration::from_secs(30)),
        max_inflight: positive(p.int("max-inflight", 4096)),
        publish_workers: positive(p.int("publish-workers", 64)),
        // AnyCable's Pusher API has no batch_events endpoint (a batch is accepted and
        // dropped), so Pusher mode batches only when asked to.
        publish_batch: positive(p.int("publish-batch", if protocol == Protocol::ActionCable { 64 } else { 1 })),
        tolerance: positive(p.int("setup-failure-tolerance", 0)),
        seed: p.int("seed", 1) as u64,
        non_interactive: p.boolean("non-interactive"),
        sut: (!sut.is_empty()).then(|| PathBuf::from(sut)),
        server_args: p.list("server-arg"),
        protocol,
    })
}

pub fn run_with_config(cfg: RunConfig, opts: RunOpts, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    let ui = Ui::new(cfg.non_interactive, Instant::now());
    let Some(sut) = super::resolve_sut(cfg.sut.clone(), opts.sut.clone()) else {
        let _ = writeln!(stderr, "error: no server binary: pass --sut or set ${}", super::SUT_ENV);
        return super::EXIT_USAGE;
    };

    ui.log(stderr, &format!("starting server {}...", sut.display()));
    let server = match Server::build(ServerConfig {
        extra_args: cfg.server_args.clone(),
        pusher: cfg.protocol.pusher().cloned(),
        ..ServerConfig::new(&sut)
    }) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(stdout, "server build failed: {e}");
            return 1;
        }
    };
    ui.log(stderr, &format!("server ready at {} (pid {})", server.websocket_url(), server.pid()));
    let baseline = server.resources().unwrap_or_default();
    let monitor = ResourceMonitor::start(server.pid(), TICK);

    let connected = Arc::new(AtomicI64::new(0));
    let subscribed = Arc::new(AtomicI64::new(0));
    let acc = Arc::new(Accumulator::new());
    let deliveries = Arc::new(DeliveryLedger::new());
    let pool_start = Instant::now();
    ui.log(stderr, &format!("building pool: {} clients × {} subscriptions = {} total...", cfg.c, cfg.s, cfg.c * cfg.s));
    let counter = |n: &Arc<AtomicI64>| -> PoolHook {
        let n = Arc::clone(n);
        Arc::new(move || {
            n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
    };
    let pool = match ClientPool::build(PoolConfig {
        server_url: server.websocket_url(),
        c: cfg.c,
        s: cfg.s,
        big_s: cfg.big_s,
        tolerance: cfg.tolerance,
        seed: cfg.seed,
        accumulator: Some(Arc::clone(&acc)),
        ledger: Some(Arc::clone(&deliveries)),
        streams: opts.stream_subsets,
        on_connect: Some(counter(&connected)),
        on_subscribe: Some(counter(&subscribed)),
        wire: cfg.protocol.wire(),
    }) {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(stdout, "pool setup failed: {e}");
            let _ = server.shutdown();
            return 1;
        }
    };
    ui.log(
        stderr,
        &format!(
            "pool ready: {} connected, {} subscribed ({:.2}s)",
            connected.load(std::sync::atomic::Ordering::Relaxed),
            subscribed.load(std::sync::atomic::Ordering::Relaxed),
            pool_start.elapsed().as_secs_f64()
        ),
    );

    let broadcast_url = opts.broadcast_url.clone().unwrap_or_else(|| server.broadcast_url());
    let publisher = Publisher::with_protocol(
        &broadcast_url,
        cfg.max_inflight as i64,
        PublisherOptions::default()
            .with_workers(cfg.publish_workers as i64)
            .with_batch_size(cfg.publish_batch as i64)
            .with_stamping(true),
        &cfg.protocol,
    );
    if let Some(hook) = opts.setup_hook {
        hook(&server, &pool, &publisher);
    }

    let interval = publish_interval(&cfg);
    let total_schedule = (cfg.d.as_nanos() / interval.as_nanos()) as i64;
    let expected_rate = cfg.r as f64 * cfg.c as f64 * cfg.s as f64 / cfg.big_s as f64;
    ui.log(
        stderr,
        &format!(
            "expected throughput: {expected_rate:.0} msg/s (r × c × s / S = {} × {} × {} / {})",
            cfg.r, cfg.c, cfg.s, cfg.big_s
        ),
    );
    ui.log(
        stderr,
        &format!(
            "publish window: {}Hz for {} ({total_schedule} scheduled publishes)...",
            cfg.r,
            super::go_duration_string(cfg.d)
        ),
    );

    let acc_src = Arc::clone(&acc);
    let recv_sampler = ReceiveSampler::new(TICK, Arc::new(move || acc_src.total_received()));
    let pub_counters = Arc::new(publisher);
    let pub_src = Arc::clone(&pub_counters);
    let pub_sampler = ReceiveSampler::new(TICK, Arc::new(move || pub_src.completed_count()));
    let generator = GeneratorMonitor::start();
    let server_cpu_start = crate::resources::cpu_time(&server.pid().to_string()).unwrap_or_default();
    recv_sampler.start();
    pub_sampler.start();

    let window_start = Instant::now();
    let late = publish_window(&cfg, &pub_counters);
    pub_counters.close();
    let publish_wall = window_start.elapsed().as_secs_f64();
    pub_sampler.stop();

    let targets = pub_counters.target_counts();
    let subsets = pool.streams();
    let expected = ledger::expected_per_client(&subsets, &targets);
    let drain_start = Instant::now();
    poll_drain(&acc, &expected, cfg.drain_timeout);
    let drain_elapsed = drain_start.elapsed();
    recv_sampler.stop();
    let window = window_start.elapsed();
    let server_cpu =
        crate::resources::cpu_time(&server.pid().to_string()).unwrap_or_default().saturating_sub(server_cpu_start);
    let generator = generator.finish();
    ui.log(
        stderr,
        &format!(
            "publish window done: {} issued, {} completed, {late} late, {} dropped in {publish_wall:.2}s",
            pub_counters.issued_count(),
            pub_counters.completed_count(),
            pub_counters.dropped_count()
        ),
    );
    ui.log(stderr, &format!("drain complete ({:.2}s)", drain_elapsed.as_secs_f64()));

    let snap = acc.snapshot();
    let recv_samples = recv_sampler.samples();
    let pub_samples = pub_sampler.samples();
    let recv_ws = throughput::compute_window_stats(&recv_samples, cfg.d);
    let pub_ws = throughput::compute_window_stats(&pub_samples, cfg.d);
    let counts = ledger::reconcile_counts(&snap, &expected);
    let issued: HashMap<String, u64> =
        targets.iter().map(|(k, v)| (k.clone(), u64::try_from(*v).unwrap_or(0))).collect();
    let seq = deliveries.report(&subsets, &issued);

    // Clients leave; the server's sockets and fds should return to the baseline.
    pool.close();
    let mut after = server.resources().unwrap_or_default();
    let residual_deadline = Instant::now() + Duration::from_secs(2);
    while (after.sockets > baseline.sockets || after.open_fds > baseline.open_fds) && Instant::now() < residual_deadline
    {
        std::thread::sleep(Duration::from_millis(20));
        after = server.resources().unwrap_or(after);
    }
    let peaks = monitor.stop();
    let secs = window.as_secs_f64().max(1e-9);
    let issued_total = pub_counters.issued_count();
    let generator_saturated = (issued_total > 0 && late as f64 > issued_total as f64 * 0.01) || generator.starved();

    let s = Summary {
        throughput_msgs_per_sec: throughput::overall(&recv_samples),
        throughput_max_msgs_per_sec: recv_ws.max,
        throughput_min_msgs_per_sec: recv_ws.min,
        throughput_p50_msgs_per_sec: recv_ws.p50,
        throughput_p95_msgs_per_sec: recv_ws.p95,
        expected_msgs_per_sec: expected_rate,
        clients_complete: counts.clients_complete,
        clients_short: counts.clients_short,
        messages_missing: counts.messages_missing,
        publications_issued: issued_total,
        publications_completed: pub_counters.completed_count(),
        publications_dropped: pub_counters.dropped_count(),
        scheduler_late_publishes: late,
        observed_publish_rate: if publish_wall > 0.0 {
            pub_counters.completed_count() as f64 / publish_wall
        } else {
            0.0
        },
        observed_publish_rate_max: pub_ws.max,
        observed_publish_rate_min: pub_ws.min,
        observed_publish_rate_p50: pub_ws.p50,
        observed_publish_rate_p95: pub_ws.p95,
        publish_window_wall_seconds: publish_wall,
        messages_missing_ids: seq.missing,
        duplicates: seq.duplicates,
        ordering_violations: seq.ordering_violations,
        misrouted: seq.misrouted,
        latency: deliveries.latency(),
        post_round_trip: pub_counters.round_trip(),
        publications_failed_posts: pub_counters.failed_posts(),
        publisher_queue_peak: pub_counters.queue_peak(),
        peak_server_rss_mb: mib(peaks.rss_bytes),
        peak_server_threads: peaks.threads,
        peak_server_sockets: peaks.sockets,
        server_cpu_seconds: server_cpu.as_secs_f64(),
        server_cpu_cores: server_cpu.as_secs_f64() / secs,
        server_residual_sockets: after.sockets as i64 - baseline.sockets as i64,
        server_residual_fds: after.open_fds as i64 - baseline.open_fds as i64,
        server_residual_threads: after.threads as i64 - baseline.threads as i64,
        generator,
        generator_saturated,
    };
    ui.log(stderr, "summary:");
    print_summary(stdout, &s);
    let _ = server.shutdown();

    if s.clients_short > 0
        || s.publications_dropped > 0
        || s.duplicates > 0
        || s.ordering_violations > 0
        || s.misrouted > 0
    {
        return 1;
    }
    0
}

/// `1s / r` (at least 1ns).
pub fn publish_interval(cfg: &RunConfig) -> Duration {
    let r = u32::try_from(cfg.r.max(1)).unwrap_or(u32::MAX);
    (Duration::from_secs(1) / r).max(Duration::from_nanos(1))
}

/// Walks the tick grid `start + i·interval` for every tick `<= start + d`, sleeping to
/// each tick or firing late (catch-up). Returns the number of late publishes. The
/// issued count is deterministic for a given `(r, d)`.
///
/// Each tick publishes a ledger payload (`<stream>:<seq>`); sequence numbers are
/// per stream and advance only for enqueued publications, so the issued
/// publications of a stream are exactly `1..=target_counts()[stream]`.
pub fn publish_window(cfg: &RunConfig, publisher: &Publisher) -> u64 {
    let mut rng = Rng::new(cfg.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let interval_ns = u64::try_from(publish_interval(cfg).as_nanos()).unwrap_or(u64::MAX);
    let mut seqs: HashMap<String, u64> = HashMap::new();
    let mut late = 0;
    let start = Instant::now();
    let end = start + cfg.d;
    for i in 1u64.. {
        let tick = start + Duration::from_nanos(interval_ns.saturating_mul(i));
        if tick > end {
            break;
        }
        let now = Instant::now();
        if now < tick {
            std::thread::sleep(tick - now);
        } else {
            late += 1;
        }
        let stream = (rng.below(cfg.big_s.max(1) as u64) + 1).to_string();
        let seq = seqs.get(&stream).copied().unwrap_or(0) + 1;
        if publisher.try_publish(&stream, &DeliveryLedger::payload(&stream, seq)) {
            seqs.insert(stream, seq);
        }
    }
    late
}

fn poll_drain(acc: &Accumulator, expected: &HashMap<String, i64>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let expected_total: i64 = expected.values().sum();
    loop {
        // Cheap check first: every client complete implies the total is reached.
        if acc.total_received() >= expected_total && ledger::drained(&acc.snapshot(), expected) {
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        std::thread::sleep((deadline - now).min(Duration::from_millis(20)));
    }
}

/// `key=value` lines (floats with 2 decimals).
pub fn print_summary(w: &mut dyn Write, s: &Summary) {
    let mut out = String::new();
    let mut f = |k: &str, v: f64| out.push_str(&format!("{k}={v:.2}\n"));
    f("throughput_msgs_per_sec", s.throughput_msgs_per_sec);
    f("throughput_max_msgs_per_sec", s.throughput_max_msgs_per_sec);
    f("throughput_min_msgs_per_sec", s.throughput_min_msgs_per_sec);
    f("throughput_p50_msgs_per_sec", s.throughput_p50_msgs_per_sec);
    f("throughput_p95_msgs_per_sec", s.throughput_p95_msgs_per_sec);
    f("expected_msgs_per_sec", s.expected_msgs_per_sec);
    let mut ints = vec![
        ("clients_complete", s.clients_complete as i64),
        ("clients_short", s.clients_short as i64),
        ("messages_missing", s.messages_missing),
        ("publications_issued", s.publications_issued),
        ("publications_completed", s.publications_completed),
        ("publications_dropped", s.publications_dropped),
        ("scheduler_late_publishes", s.scheduler_late_publishes as i64),
    ];
    for (k, v) in ints.drain(..) {
        out.push_str(&format!("{k}={v}\n"));
    }
    let mut f = |k: &str, v: f64| out.push_str(&format!("{k}={v:.2}\n"));
    f("observed_publish_rate", s.observed_publish_rate);
    f("observed_publish_rate_max", s.observed_publish_rate_max);
    f("observed_publish_rate_min", s.observed_publish_rate_min);
    f("observed_publish_rate_p50", s.observed_publish_rate_p50);
    f("observed_publish_rate_p95", s.observed_publish_rate_p95);
    f("publish_window_wall_seconds", s.publish_window_wall_seconds);
    // Cabby additions.
    for (k, v) in [
        ("messages_missing_ids", s.messages_missing_ids),
        ("duplicates", s.duplicates),
        ("ordering_violations", s.ordering_violations),
        ("misrouted", s.misrouted),
    ] {
        out.push_str(&format!("{k}={v}\n"));
    }
    for (name, h) in [("latency", &s.latency), ("post_round_trip", &s.post_round_trip)] {
        out.push_str(&format!("{name}_count={}\n", h.count));
        for (q, label) in [(0.5, "p50"), (0.95, "p95"), (0.99, "p99")] {
            out.push_str(&format!("{name}_{label}_ms={:.2}\n", ms(h.quantile(q))));
        }
        out.push_str(&format!("{name}_max_ms={:.2}\n", ms(h.max)));
    }
    out.push_str(&format!("publications_failed_posts={}\n", s.publications_failed_posts));
    out.push_str(&format!("publisher_queue_peak={}\n", s.publisher_queue_peak));
    out.push_str(&format!("peak_server_rss_mb={:.1}\n", s.peak_server_rss_mb));
    out.push_str(&format!("peak_server_threads={}\n", s.peak_server_threads));
    out.push_str(&format!("peak_server_sockets={}\n", s.peak_server_sockets));
    out.push_str(&format!("server_cpu_seconds={:.2}\n", s.server_cpu_seconds));
    out.push_str(&format!("server_cpu_cores={:.2}\n", s.server_cpu_cores));
    out.push_str(&format!("server_residual_sockets={}\n", s.server_residual_sockets));
    out.push_str(&format!("server_residual_fds={}\n", s.server_residual_fds));
    out.push_str(&format!("server_residual_threads={}\n", s.server_residual_threads));
    s.generator.write(&mut out, s.generator_saturated);
    let _ = w.write_all(out.as_bytes());
}
