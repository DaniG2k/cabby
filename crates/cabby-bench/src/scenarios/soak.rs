//! `soak` (Cabby addition, no upstream counterpart): a memory-stability run in the
//! shape of AnyCable issue 265 ("Continuous memory leak"), deviation D-005.
//!
//! One server runs in RPC mode against the harness's fake RPC application
//! ([`crate::rpc_server`], `--rpc-latency` per call, default 5 ms) for `--duration`.
//! The traffic imitates a Rails app with Turbo pages: few broadcasts, a lot of
//! connection and subscription churn.
//!
//! * **Clients.** `-c` client tasks (default 200) each loop: connect; authenticate
//!   over RPC Connect (a `--reject-auth` fraction, default 2%, is rejected like a
//!   failed Rails authentication); subscribe to 1..=`-s` channels (default 3) whose
//!   `stream_from` streams are picked from `-S` streams (`soak-1..soak-<S>`); stay for
//!   a random lifetime in `--lifetime-min..--lifetime-max` (2–30 s), navigating
//!   0..=`--navigations` times (default 2) like Turbo visits (channels that are not on
//!   the next page are unsubscribed, new ones subscribed, shared ones kept); then
//!   disconnect, with a close handshake (`--clean-close`, default 50%) or by
//!   dropping the TCP connection; pause `--pause-min..--pause-max`; repeat. Client
//!   start times are spread over `--ramp`.
//! * **Publisher.** One task broadcasts at `-r` messages per second (default 20) to
//!   random streams over the HTTP broadcast endpoint, one POST at a time on a
//!   keep-alive connection. Each payload is `<stream>|<seq>|<dispatch time>`.
//! * **Reconciliation.** Each subscription records what it receives. When it ends
//!   (unsubscribe or disconnect), it expects exactly the broadcasts on its stream
//!   whose POST started at least `--grace` (default 1 s) after the confirmation and
//!   completed at least `--grace` before the unsubscribe or disconnect; broadcasts
//!   closer to either edge may or may not arrive. Expected broadcasts it did not get
//!   are `messages_missing`; a sequence number received twice on one subscription is a
//!   duplicate; a payload naming another stream is misrouted. Delivery latency is
//!   POST dispatch → receipt.
//! * **Sampling.** Every `--sample-interval` (default 30 s) the harness samples the
//!   server process from procfs (RSS, threads, fds, sockets) and, when its metrics
//!   endpoint has them, the `clients_num`, `broadcast_streams_num`,
//!   `disconnect_queue_size`, `goroutines_num` and `mem_sys_bytes` gauges, plus its own
//!   session and subscription counts. `--samples-out` writes them as CSV (one row per
//!   sample, flushed as it is taken, so an aborted run keeps its data); short runs also
//!   print them as compact series.
//! * **End.** The clients leave (with the same clean/abrupt mix), the harness waits for
//!   the disconnect queue to drain (RPC Disconnect calls settled and the server's
//!   `clients_num` and `disconnect_queue_size` gauges at 0, at most `--drain-timeout`),
//!   waits `--settle` (default 30 s) and samples the residual resources.
//!
//! Reported: `rss_start_mb` (first sample after `--warmup`, default 5 min),
//! `rss_end_mb` (when the clients stop), `rss_max_mb`, `rss_slope_mb_per_hour` (least
//! squares over the post-warm-up samples), the `residual_*` resources against the
//! `baseline_*` ones sampled before the first client, connection, authentication and
//! subscription counts, delivery reconciliation, and generator saturation
//! (`generator_*`, plus publisher ticks more than 5 ms late).
//!
//! Exit codes: 0 delivered as expected and drained, 1 server failure, a missing,
//! duplicate or misrouted delivery, or no drain, 2 flag error, 130 interrupted (the
//! summary is still printed, with `interrupted=true`).

use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;

use super::{FlagDef, FlagError, GeneratorMonitor, GeneratorReport, Kind, SUT_FLAGS, SignalScope, Ui, mib, ms};
use crate::cable::{Command, Conn, Route, Sink, SubFlags};
use crate::cancel::CancelToken;
use crate::gojson;
use crate::histogram::{Histogram, HistogramSnapshot};
use crate::net::{self, Target};
use crate::resources::ProcessResources;
use crate::rng::Rng;
use crate::rpc_server::{RpcMethod, RpcServer, RpcServerConfig};
use crate::runtime::{block_on, now_ns, rt};
use crate::server::{Server, ServerConfig, ServerLog, ServerTuning};

const NAME: &str = "soak";
/// The server's stats refresh interval: gauges are at most this stale.
const STATS_REFRESH: Duration = Duration::from_secs(1);
/// Deadlines of one client operation.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const POST_TIMEOUT: Duration = Duration::from_secs(10);
/// A publisher tick this late counts against the generator.
const LATE_TICK: Duration = Duration::from_millis(5);
/// Series printed in the summary when there are at most this many samples.
const MAX_INLINE_SAMPLES: usize = 120;
/// How long the Disconnect counters must stand still before the drain is declared
/// although fewer Disconnect calls than authenticated sessions were seen.
const DRAIN_QUIET: Duration = Duration::from_secs(5);

/// Parsed flags.
#[derive(Debug, Clone, PartialEq)]
pub struct RunConfig {
    /// `-c`, default 200.
    pub c: usize,
    /// `-S`, default 100.
    pub big_s: usize,
    /// `-s`: most channels per page, default 3.
    pub s: usize,
    /// `-r` broadcasts per second, default 20 (0: none).
    pub r: f64,
    pub duration: Duration,
    pub warmup: Duration,
    pub sample_interval: Duration,
    pub samples_out: Option<PathBuf>,
    pub rpc_latency: Duration,
    pub rpc_jitter: f64,
    pub reject_auth: f64,
    pub lifetime_min: Duration,
    pub lifetime_max: Duration,
    pub pause_min: Duration,
    pub pause_max: Duration,
    pub navigations: u64,
    pub clean_close: f64,
    pub grace: Duration,
    pub ramp: Duration,
    pub drain_timeout: Duration,
    pub settle: Duration,
    pub seed: u64,
    pub non_interactive: bool,
    pub server_log: Option<PathBuf>,
    pub sut: Option<PathBuf>,
    pub server_args: Vec<String>,
}

const DEFS: &[FlagDef] = &[
    FlagDef { name: "c", kind: Kind::Int, default: "200", usage: "concurrent clients" },
    FlagDef { name: "S", kind: Kind::Int, default: "100", usage: "streams in the universe (soak-1..soak-<S>)" },
    FlagDef { name: "s", kind: Kind::Int, default: "3", usage: "most channels per page (each page has 1..s)" },
    FlagDef { name: "r", kind: Kind::Float, default: "20", usage: "broadcasts per second to random streams (0: none)" },
    FlagDef { name: "duration", kind: Kind::Duration, default: "2h0m0s", usage: "load duration" },
    FlagDef {
        name: "warmup",
        kind: Kind::Duration,
        default: "5m0s",
        usage: "samples before this are left out of rss_start_mb and the slope",
    },
    FlagDef { name: "sample-interval", kind: Kind::Duration, default: "30s", usage: "server sampling interval" },
    FlagDef { name: "samples-out", kind: Kind::Str, default: "", usage: "CSV file for the samples" },
    FlagDef { name: "rpc-latency", kind: Kind::Duration, default: "5ms", usage: "fake RPC latency per call" },
    FlagDef { name: "rpc-jitter", kind: Kind::Float, default: "0.3", usage: "RPC latency jitter fraction (0..1)" },
    FlagDef {
        name: "reject-auth",
        kind: Kind::Float,
        default: "0.02",
        usage: "fraction of connections the RPC app rejects (unauthorized)",
    },
    FlagDef { name: "lifetime-min", kind: Kind::Duration, default: "2s", usage: "shortest connection lifetime" },
    FlagDef { name: "lifetime-max", kind: Kind::Duration, default: "30s", usage: "longest connection lifetime" },
    FlagDef { name: "pause-min", kind: Kind::Duration, default: "500ms", usage: "shortest pause between connections" },
    FlagDef { name: "pause-max", kind: Kind::Duration, default: "5s", usage: "longest pause between connections" },
    FlagDef {
        name: "navigations",
        kind: Kind::Int,
        default: "2",
        usage: "most page visits per connection (unsubscribe/resubscribe), 0..n",
    },
    FlagDef {
        name: "clean-close",
        kind: Kind::Float,
        default: "0.5",
        usage: "fraction of disconnects with a close handshake (the rest drop the TCP connection)",
    },
    FlagDef {
        name: "grace",
        kind: Kind::Duration,
        default: "1s",
        usage: "broadcasts this close to a subscription's confirmation or end are not expected",
    },
    FlagDef { name: "ramp", kind: Kind::Duration, default: "10s", usage: "client start times are spread over this" },
    FlagDef {
        name: "drain-timeout",
        kind: Kind::Duration,
        default: "2m0s",
        usage: "max wait for the disconnect queue after the clients leave",
    },
    FlagDef {
        name: "settle",
        kind: Kind::Duration,
        default: "30s",
        usage: "wait after the drain before sampling residual resources",
    },
    FlagDef { name: "seed", kind: Kind::Int, default: "1", usage: "random seed" },
    FlagDef { name: "non-interactive", kind: Kind::Bool, default: "", usage: "no progress output on stderr" },
    FlagDef { name: "server-log", kind: Kind::Str, default: "", usage: "write the server's debug log to this file" },
    SUT_FLAGS[0],
    SUT_FLAGS[1],
];

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
    let d = |name: &str, default: Duration| p.duration(name, default);
    let cfg = RunConfig {
        c: usize::try_from(p.int("c", 200)).unwrap_or(0),
        big_s: usize::try_from(p.int("S", 100)).unwrap_or(0),
        s: usize::try_from(p.int("s", 3)).unwrap_or(0),
        r: p.float("r", 20.0),
        duration: d("duration", Duration::from_secs(7200)),
        warmup: d("warmup", Duration::from_secs(300)),
        sample_interval: d("sample-interval", Duration::from_secs(30)),
        samples_out: optional_path(p.string("samples-out")),
        rpc_latency: d("rpc-latency", Duration::from_millis(5)),
        rpc_jitter: p.float("rpc-jitter", 0.3),
        reject_auth: p.float("reject-auth", 0.02),
        lifetime_min: d("lifetime-min", Duration::from_secs(2)),
        lifetime_max: d("lifetime-max", Duration::from_secs(30)),
        pause_min: d("pause-min", Duration::from_millis(500)),
        pause_max: d("pause-max", Duration::from_secs(5)),
        navigations: u64::try_from(p.int("navigations", 2)).unwrap_or(0),
        clean_close: p.float("clean-close", 0.5),
        grace: d("grace", Duration::from_secs(1)),
        ramp: d("ramp", Duration::from_secs(10)),
        drain_timeout: d("drain-timeout", Duration::from_secs(120)),
        settle: d("settle", Duration::from_secs(30)),
        seed: p.int("seed", 1) as u64,
        non_interactive: p.boolean("non-interactive"),
        server_log: optional_path(p.string("server-log")),
        sut: optional_path(p.string("sut")),
        server_args: p.list("server-arg"),
    };
    let unit = |v: f64| (0.0..=1.0).contains(&v);
    let problem = if cfg.c == 0 || cfg.big_s == 0 || cfg.s == 0 {
        Some("-c, -S and -s must be > 0")
    } else if cfg.s > cfg.big_s {
        Some("-s must not exceed -S")
    } else if !(cfg.r >= 0.0 && cfg.r.is_finite()) {
        Some("-r must be >= 0")
    } else if cfg.duration.is_zero() || cfg.sample_interval.is_zero() {
        Some("--duration and --sample-interval must be > 0")
    } else if cfg.warmup >= cfg.duration {
        Some("--warmup must be shorter than --duration")
    } else if cfg.lifetime_min > cfg.lifetime_max || cfg.pause_min > cfg.pause_max {
        Some("--lifetime-min/--pause-min must not exceed --lifetime-max/--pause-max")
    } else if !unit(cfg.rpc_jitter) || !unit(cfg.reject_auth) || !unit(cfg.clean_close) {
        Some("--rpc-jitter, --reject-auth and --clean-close must be in 0..1")
    } else {
        None
    };
    if let Some(m) = problem {
        super::write_usage(stderr, NAME, DEFS);
        return Err(m.into());
    }
    Ok(cfg)
}

// ------------------------------------------------------------------ shared state

/// Run-wide counters, updated by the client and publisher tasks.
#[derive(Default)]
struct Stats {
    sessions_attempted: AtomicU64,
    sessions_authenticated: AtomicU64,
    auth_rejected: AtomicU64,
    /// Marked for rejection but welcomed.
    auth_unexpected_accepts: AtomicU64,
    /// Marked for rejection but failed in another way.
    auth_unexpected_failures: AtomicU64,
    connect_errors: AtomicU64,
    /// The server closed a session the client did not close.
    unexpected_disconnects: AtomicU64,
    clean_disconnects: AtomicU64,
    /// Clean disconnects whose close handshake did not finish in time.
    clean_close_timeouts: AtomicU64,
    abrupt_disconnects: AtomicU64,
    subscriptions_confirmed: AtomicU64,
    subscription_failures: AtomicU64,
    unsubscribes: AtomicU64,
    navigations: AtomicU64,
    deliveries_expected: AtomicU64,
    deliveries_matched: AtomicU64,
    messages_received: AtomicU64,
    messages_missing: AtomicU64,
    messages_duplicate: AtomicU64,
    messages_misrouted: AtomicU64,
    publications_ok: AtomicU64,
    publications_failed: AtomicU64,
    publish_ticks: AtomicU64,
    publish_late_ticks: AtomicU64,
    live_sessions: AtomicI64,
    live_subscriptions: AtomicI64,
    first_error: Mutex<Option<String>>,
}

fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

impl Stats {
    fn error(&self, e: String) {
        let mut first = self.first_error.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            *first = Some(e);
        }
    }
}

/// A completed broadcast: its sequence number and when its POST started and ended.
#[derive(Debug, Clone, Copy)]
struct Publication {
    seq: u64,
    start: u64,
    end: u64,
}

/// Completed broadcasts per stream, in dispatch order. The single sequential
/// publisher makes both `start` and `end` increase along each queue.
struct PublicationLog {
    streams: Vec<Mutex<VecDeque<Publication>>>,
    /// Entries older than this (ns) can no longer fall into a subscription window.
    horizon_ns: u64,
}

impl PublicationLog {
    fn push(&self, stream: usize, p: Publication) {
        let mut q = self.streams[stream].lock().unwrap_or_else(|e| e.into_inner());
        q.push_back(p);
        let oldest = p.start.saturating_sub(self.horizon_ns);
        while q.front().is_some_and(|f| f.start < oldest) {
            q.pop_front();
        }
    }

    /// Sequence numbers of the broadcasts on `stream` that started at `lo` or later
    /// and completed at `hi` or earlier.
    fn window(&self, stream: usize, lo: u64, hi: u64) -> Vec<u64> {
        let q = self.streams[stream].lock().unwrap_or_else(|e| e.into_inner());
        let a = q.partition_point(|p| p.start < lo);
        let b = q.partition_point(|p| p.end <= hi);
        q.range(a..b.max(a)).map(|p| p.seq).collect()
    }
}

fn stream_name(i: usize) -> String {
    format!("soak-{}", i + 1)
}

/// The channel identifier whose RPC subscription streams from `stream`.
fn channel_identifier(stream: &str) -> String {
    format!(r#"{{"channel":"SoakChannel","stream":{}}}"#, gojson::string(stream))
}

/// What one subscription received.
struct SubState {
    stream: String,
    seqs: Mutex<HashSet<u64>>,
}

/// A confirmed subscription of a live session.
struct ActiveSub {
    stream: usize,
    identifier: String,
    state: Arc<SubState>,
    confirmed_ns: u64,
}

struct Shared {
    cfg: RunConfig,
    url: String,
    stats: Stats,
    log: PublicationLog,
    latency: Histogram,
}

fn uniform(rng: &mut Rng, lo: Duration, hi: Duration) -> Duration {
    lo + (hi.saturating_sub(lo)).mul_f64(rng.unit())
}

impl Shared {
    /// Called by the connection reader for every message of `sub`.
    fn record(&self, sub: &SubState, payload: &str) {
        inc(&self.stats.messages_received);
        let mut parts = payload.split('|');
        let (stream, seq, sent) = (parts.next(), parts.next(), parts.next());
        if stream != Some(sub.stream.as_str()) {
            inc(&self.stats.messages_misrouted);
            return;
        }
        let Some(seq) = seq.and_then(|s| s.parse::<u64>().ok()) else {
            inc(&self.stats.messages_misrouted);
            return;
        };
        if let Some(sent) = sent.and_then(|s| s.parse::<u64>().ok()) {
            self.latency.record(Duration::from_nanos(now_ns().saturating_sub(sent)));
        }
        if !sub.seqs.lock().unwrap_or_else(|e| e.into_inner()).insert(seq) {
            inc(&self.stats.messages_duplicate);
        }
    }

    /// Reconciles a subscription that ended at `end_ns`.
    fn reconcile(&self, sub: &ActiveSub, end_ns: u64) {
        let grace = u64::try_from(self.cfg.grace.as_nanos()).unwrap_or(u64::MAX);
        let lo = sub.confirmed_ns.saturating_add(grace);
        let hi = end_ns.saturating_sub(grace);
        if hi <= lo {
            return;
        }
        let expected = self.log.window(sub.stream, lo, hi);
        let got = sub.state.seqs.lock().unwrap_or_else(|e| e.into_inner());
        let matched = expected.iter().filter(|s| got.contains(s)).count() as u64;
        let n = expected.len() as u64;
        self.stats.deliveries_expected.fetch_add(n, Ordering::Relaxed);
        self.stats.deliveries_matched.fetch_add(matched, Ordering::Relaxed);
        self.stats.messages_missing.fetch_add(n - matched, Ordering::Relaxed);
    }

    /// 1..=s distinct random streams.
    fn page(&self, rng: &mut Rng) -> Vec<usize> {
        let k = 1 + rng.below(self.cfg.s as u64) as usize;
        let mut picked = Vec::with_capacity(k);
        while picked.len() < k {
            let i = rng.below(self.cfg.big_s as u64) as usize;
            if !picked.contains(&i) {
                picked.push(i);
            }
        }
        picked
    }

    /// Subscribes to `streams` (all commands first, then the confirmations).
    async fn subscribe(self: &Arc<Self>, conn: &Arc<Conn>, streams: &[usize], subs: &mut Vec<ActiveSub>) {
        let mut waiting = Vec::with_capacity(streams.len());
        for &i in streams {
            let name = stream_name(i);
            let identifier = channel_identifier(&name);
            let state = Arc::new(SubState { stream: name, seqs: Mutex::new(HashSet::new()) });
            let flags = SubFlags::new();
            let (me, st) = (Arc::clone(self), Arc::clone(&state));
            let sink = Sink::Callback(Arc::new(move |payload: &str| me.record(&st, payload)));
            if conn.register(&identifier, Route { flags: Arc::clone(&flags), sink }).is_err() {
                return; // connection lost
            }
            let cmd =
                Command { command: "subscribe".into(), identifier: Some(identifier.clone()), ..Default::default() };
            if conn.send_command(&cmd).await.is_err() {
                conn.unregister(&identifier);
                return;
            }
            waiting.push((i, identifier, flags, state, tokio::time::Instant::now() + SUBSCRIBE_TIMEOUT));
        }
        for (stream, identifier, flags, state, deadline) in waiting {
            let resolved = tokio::time::timeout_at(deadline, flags.wait_done()).await.is_ok();
            if resolved && flags.subscribed() && !flags.rejected() {
                inc(&self.stats.subscriptions_confirmed);
                self.stats.live_subscriptions.fetch_add(1, Ordering::Relaxed);
                subs.push(ActiveSub { stream, identifier, state, confirmed_ns: now_ns() });
            } else {
                conn.unregister(&identifier);
                if conn.ended_ns().is_none() {
                    inc(&self.stats.subscription_failures);
                    self.stats.error(format!(
                        "subscription to {} {}",
                        stream_name(stream),
                        if flags.rejected() { "rejected" } else { "not confirmed in 5s" }
                    ));
                }
            }
        }
    }

    /// Ends `sub` at `end_ns`: stops recording, optionally sends `unsubscribe`, and
    /// reconciles it.
    async fn end_sub(&self, conn: &Conn, sub: ActiveSub, end_ns: u64, unsubscribe: bool) {
        conn.unregister(&sub.identifier);
        if unsubscribe {
            let cmd = Command {
                command: "unsubscribe".into(),
                identifier: Some(sub.identifier.clone()),
                ..Default::default()
            };
            if conn.send_command(&cmd).await.is_ok() {
                inc(&self.stats.unsubscribes);
            }
        }
        self.reconcile(&sub, end_ns);
        self.stats.live_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    /// One connection, from dial to disconnect.
    async fn session(self: &Arc<Self>, client: usize, n: u64, rng: &mut Rng, stop: &CancelToken) {
        let cfg = &self.cfg;
        let reject = rng.unit() < cfg.reject_auth;
        let url = format!("{}?bench_sid=soak-{client}-{n}{}", self.url, if reject { "&bench_reject=1" } else { "" });
        inc(&self.stats.sessions_attempted);
        let conn = match tokio::time::timeout(DIAL_TIMEOUT, Conn::dial(&url)).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) if reject => {
                // The server forwards the app's `disconnect` (or just closes).
                if e.0.contains("unexpected message type: disconnect") || e.0.starts_with("error reading message") {
                    inc(&self.stats.auth_rejected);
                } else {
                    inc(&self.stats.auth_unexpected_failures);
                    self.stats.error(format!("rejected connect: {e}"));
                }
                return;
            }
            Ok(Err(e)) => {
                inc(&self.stats.connect_errors);
                self.stats.error(format!("connect: {e}"));
                return;
            }
            Err(_) => {
                inc(&self.stats.connect_errors);
                self.stats.error("connect: no welcome in 10s".into());
                return;
            }
        };
        if reject {
            inc(&self.stats.auth_unexpected_accepts);
            conn.close().await;
            return;
        }
        inc(&self.stats.sessions_authenticated);
        self.stats.live_sessions.fetch_add(1, Ordering::Relaxed);

        let started = tokio::time::Instant::now();
        let lifetime = uniform(rng, cfg.lifetime_min, cfg.lifetime_max);
        let mut visits: Vec<Duration> =
            (0..rng.below(cfg.navigations + 1)).map(|_| lifetime.mul_f64(rng.unit())).collect();
        visits.sort();
        let mut subs = Vec::new();
        let first = self.page(rng);
        self.subscribe(&conn, &first, &mut subs).await;
        let wait = |at: tokio::time::Instant| {
            let conn = Arc::clone(&conn);
            async move {
                tokio::select! {
                    () = tokio::time::sleep_until(at) => true,
                    () = conn.ended() => false,
                    () = stop.cancelled() => false,
                }
            }
        };
        let mut alive = true;
        for at in visits {
            alive = wait(started + at).await;
            if !alive {
                break;
            }
            inc(&self.stats.navigations);
            let next = self.page(rng);
            let now = now_ns();
            let (keep, leave): (Vec<ActiveSub>, Vec<ActiveSub>) =
                std::mem::take(&mut subs).into_iter().partition(|s| next.contains(&s.stream));
            for sub in leave {
                self.end_sub(&conn, sub, now, true).await;
            }
            let new: Vec<usize> = next.iter().copied().filter(|i| !keep.iter().any(|s| s.stream == *i)).collect();
            subs = keep;
            self.subscribe(&conn, &new, &mut subs).await;
        }
        if alive {
            wait(started + lifetime).await;
        }
        let lost = conn.ended_ns();
        let end_ns = lost.unwrap_or_else(now_ns);
        if lost.is_some() {
            inc(&self.stats.unexpected_disconnects);
            self.stats.error("the server closed a session".into());
        }
        for sub in std::mem::take(&mut subs) {
            self.end_sub(&conn, sub, end_ns, false).await;
        }
        if lost.is_some() {
            conn.close().await;
        } else if rng.unit() < cfg.clean_close {
            if !conn.close_clean(CLOSE_TIMEOUT).await {
                inc(&self.stats.clean_close_timeouts);
            }
            inc(&self.stats.clean_disconnects);
        } else {
            conn.close().await;
            inc(&self.stats.abrupt_disconnects);
        }
        self.stats.live_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    async fn client_loop(self: Arc<Self>, client: usize, stop: CancelToken) {
        let mut rng = Rng::new(self.cfg.seed ^ (client as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        if !stop.sleep_async(self.cfg.ramp.mul_f64(rng.unit())).await {
            return;
        }
        let mut n = 0;
        while !stop.is_cancelled() {
            n += 1;
            self.session(client, n, &mut rng, &stop).await;
            if !stop.sleep_async(uniform(&mut rng, self.cfg.pause_min, self.cfg.pause_max)).await {
                break;
            }
        }
    }

    async fn publisher(self: Arc<Self>, broadcast_url: String, stop: CancelToken) {
        let Ok(target) = net::parse_url(&broadcast_url) else { return };
        let mut poster = Poster { target, conn: None };
        let mut rng = Rng::new(self.cfg.seed ^ 0x5EED_B10A_DCA5_0001);
        let interval = Duration::from_secs_f64(1.0 / self.cfg.r);
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut seq = 0u64;
        loop {
            let scheduled = tokio::select! {
                () = stop.cancelled() => return,
                t = ticks.tick() => t,
            };
            inc(&self.stats.publish_ticks);
            if scheduled.elapsed() > LATE_TICK {
                inc(&self.stats.publish_late_ticks);
            }
            seq += 1;
            let stream = rng.below(self.cfg.big_s as u64) as usize;
            let name = stream_name(stream);
            let start = now_ns();
            let mut body = String::from("{\"stream\":");
            gojson::push_str(&mut body, &name);
            body.push_str(",\"data\":");
            gojson::push_str(&mut body, &format!("{name}|{seq}|{start}"));
            body.push('}');
            if poster.post(body).await {
                self.log.push(stream, Publication { seq, start, end: now_ns() });
                inc(&self.stats.publications_ok);
            } else {
                inc(&self.stats.publications_failed);
            }
        }
    }
}

/// One keep-alive HTTP/1.1 connection to the broadcast endpoint.
struct Poster {
    target: Target,
    conn: Option<SendRequest<Full<Bytes>>>,
}

impl Poster {
    /// POSTs `body`; whether the server answered 2xx within [`POST_TIMEOUT`].
    async fn post(&mut self, body: String) -> bool {
        let ok = matches!(tokio::time::timeout(POST_TIMEOUT, self.try_post(body)).await, Ok(Ok(true)));
        if !ok {
            self.conn = None;
        }
        ok
    }

    async fn try_post(&mut self, body: String) -> Result<bool, String> {
        let reusable = match self.conn.as_mut() {
            Some(c) => !c.is_closed() && c.ready().await.is_ok(),
            None => false,
        };
        if !reusable {
            let stream = net::connect_tcp(&self.target).await.map_err(|e| e.to_string())?;
            let (sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.map_err(|e| e.to_string())?;
            rt().spawn(async move {
                let _ = conn.await;
            });
            self.conn = Some(sender);
        }
        let sender = self.conn.as_mut().ok_or("no connection")?;
        let req = http::Request::post(self.target.path.as_str())
            .header(http::header::HOST, format!("{}:{}", self.target.host, self.target.port))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| e.to_string())?;
        let resp = sender.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status();
        resp.into_body().collect().await.map_err(|e| e.to_string())?;
        Ok(status.is_success())
    }
}

// ------------------------------------------------------------------ sampling

/// One sample of the server and the harness.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    /// Since the load started.
    pub elapsed: Duration,
    /// `baseline` (before the first client), `warmup`, `load`, `end` (the clients
    /// stop) or `residual` (after the drain and settle).
    pub phase: &'static str,
    pub server: ProcessResources,
    /// Server gauges, when its metrics endpoint reports them.
    pub sessions: Option<f64>,
    pub streams: Option<f64>,
    pub disconnect_queue: Option<f64>,
    pub goroutines: Option<f64>,
    pub mem_sys_bytes: Option<f64>,
    pub harness_sessions: i64,
    pub harness_subscriptions: i64,
    pub publications: u64,
}

impl Sample {
    fn rss_mb(&self) -> f64 {
        mib(self.server.rss_bytes)
    }

    fn csv_header() -> &'static str {
        "elapsed_s,phase,rss_mb,threads,fds,sockets,server_sessions,server_streams,disconnect_queue,\
         goroutines,mem_sys_mb,harness_sessions,harness_subscriptions,publications"
    }

    fn csv_row(&self) -> String {
        let opt = |v: Option<f64>| v.map_or(String::new(), |v| format!("{v:.0}"));
        format!(
            "{:.1},{},{:.1},{},{},{},{},{},{},{},{},{},{},{}",
            self.elapsed.as_secs_f64(),
            self.phase,
            self.rss_mb(),
            self.server.threads,
            self.server.open_fds,
            self.server.sockets,
            opt(self.sessions),
            opt(self.streams),
            opt(self.disconnect_queue),
            opt(self.goroutines),
            self.mem_sys_bytes.map_or(String::new(), |v| format!("{:.1}", v / (1u64 << 20) as f64)),
            self.harness_sessions,
            self.harness_subscriptions,
            self.publications
        )
    }
}

struct Sampler {
    server: Arc<Server>,
    shared: Arc<Shared>,
    start: Instant,
    samples: Mutex<Vec<Sample>>,
    csv: Mutex<Option<BufWriter<File>>>,
}

impl Sampler {
    fn take(&self, phase: &'static str) -> Sample {
        let m = self.server.metrics().ok();
        let gauge = |name: &str| m.as_ref().and_then(|m| m.value(name));
        let st = &self.shared.stats;
        let s = Sample {
            elapsed: self.start.elapsed(),
            phase,
            server: self.server.resources().unwrap_or_default(),
            sessions: gauge("clients_num"),
            streams: gauge("broadcast_streams_num"),
            disconnect_queue: gauge("disconnect_queue_size"),
            goroutines: gauge("goroutines_num"),
            mem_sys_bytes: gauge("mem_sys_bytes"),
            harness_sessions: st.live_sessions.load(Ordering::Relaxed),
            harness_subscriptions: st.live_subscriptions.load(Ordering::Relaxed),
            publications: get(&st.publications_ok),
        };
        if let Some(w) = self.csv.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            let _ = writeln!(w, "{}", s.csv_row());
            let _ = w.flush();
        }
        self.samples.lock().unwrap_or_else(|e| e.into_inner()).push(s.clone());
        s
    }

    /// Samples every `interval` after the start (the baseline is the sample at 0),
    /// before `duration` and until `stop`; the caller takes the `end` sample.
    fn spawn(self: &Arc<Self>, cfg: &RunConfig, stop: CancelToken) -> JoinHandle<()> {
        let me = Arc::clone(self);
        let (interval, warmup, duration) = (cfg.sample_interval, cfg.warmup, cfg.duration);
        std::thread::Builder::new()
            .name("soak-sampler".into())
            .spawn(move || {
                let mut k: u32 = 1;
                while interval * k < duration {
                    let at = me.start + interval * k;
                    if !stop.sleep(at.saturating_duration_since(Instant::now())) {
                        return;
                    }
                    me.take(if interval * k < warmup { "warmup" } else { "load" });
                    k += 1;
                }
            })
            .expect("spawning the sampler")
    }

    fn all(&self) -> Vec<Sample> {
        self.samples.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Least-squares slope of `(hours, MB)` points, in MB per hour; `None` for fewer
/// than two distinct times.
pub fn slope_per_hour(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
    let my = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|p| (p.0 - mx).powi(2)).sum();
    let sxy: f64 = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    (sxx > 0.0).then(|| sxy / sxx)
}

// ------------------------------------------------------------------ run

/// Waits until the sessions the clients closed are all gone from the server: RPC
/// Disconnect calls done (one per authenticated session, or no new ones for
/// [`DRAIN_QUIET`]) and, with metrics, the `clients_num` and `disconnect_queue_size`
/// gauges at 0 in a read taken at least one stats refresh after the clients stopped.
fn wait_drained(server: &Server, rpc: &RpcServer, authenticated: u64, stopped: Instant, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let (mut last_started, mut last_progress) = (-1, Instant::now());
    loop {
        let d = rpc.stats(RpcMethod::Disconnect);
        if d.started != last_started {
            (last_started, last_progress) = (d.started, Instant::now());
        }
        let calls_done = d.in_flight == 0
            && (u64::try_from(d.completed).unwrap_or(0) >= authenticated || last_progress.elapsed() >= DRAIN_QUIET);
        let fresh = stopped.elapsed() >= STATS_REFRESH + Duration::from_millis(250);
        let gauges_zero = !server.has_metrics()
            || server.metrics().is_ok_and(|m| {
                m.value("clients_num").unwrap_or(0.0) == 0.0 && m.value("disconnect_queue_size").unwrap_or(0.0) == 0.0
            });
        if calls_done && fresh && gauges_zero {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

pub fn run_with_config(interrupt: &CancelToken, cfg: RunConfig, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    let start = Instant::now();
    let ui = Ui::new(cfg.non_interactive, start);
    let Some(sut) = super::resolve_sut(cfg.sut.clone(), None) else {
        let _ = writeln!(stderr, "error: no server binary: pass --sut or set ${}", super::SUT_ENV);
        return super::EXIT_USAGE;
    };
    let csv = match &cfg.samples_out {
        Some(path) => match File::create(path) {
            Ok(f) => {
                let mut w = BufWriter::new(f);
                let _ = writeln!(w, "{}", Sample::csv_header());
                Some(w)
            }
            Err(e) => {
                let _ = writeln!(stderr, "error: --samples-out {}: {e}", path.display());
                return super::EXIT_USAGE;
            }
        },
        None => None,
    };

    let rpc = Arc::new(RpcServer::new(RpcServerConfig {
        latency: cfg.rpc_latency,
        jitter: cfg.rpc_jitter,
        ..Default::default()
    }));
    ui.log(stderr, &format!("starting server {}...", sut.display()));
    let server = match Server::build(ServerConfig {
        log: cfg.server_log.clone().map_or(ServerLog::Discard, ServerLog::File),
        rpc_host: Some(rpc.url()),
        tuning: ServerTuning { stats_refresh_interval: Some(STATS_REFRESH.as_secs() as u32), ..Default::default() },
        extra_args: cfg.server_args.clone(),
        ..ServerConfig::new(&sut)
    }) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            let _ = writeln!(stdout, "server build failed: {e}");
            rpc.close();
            return 1;
        }
    };
    ui.log(stderr, &format!("server ready at {} (pid {})", server.websocket_url(), server.pid()));
    interrupt.sleep(Duration::from_secs(1));

    let horizon_ns = u64::try_from((cfg.lifetime_max * 2 + Duration::from_secs(600)).as_nanos()).unwrap_or(u64::MAX);
    let shared = Arc::new(Shared {
        url: server.websocket_url(),
        stats: Stats::default(),
        log: PublicationLog { streams: (0..cfg.big_s).map(|_| Mutex::new(VecDeque::new())).collect(), horizon_ns },
        latency: Histogram::new(),
        cfg: cfg.clone(),
    });
    let sampler = Arc::new(Sampler {
        server: Arc::clone(&server),
        shared: Arc::clone(&shared),
        start: Instant::now(),
        samples: Mutex::new(Vec::new()),
        csv: Mutex::new(csv),
    });
    let baseline = sampler.take("baseline");

    let generator = GeneratorMonitor::start();
    let load_stop = CancelToken::new();
    let sampler_stop = CancelToken::new();
    let sampler_thread = sampler.spawn(&cfg, sampler_stop.clone());
    let mut tasks = Vec::with_capacity(cfg.c + 1);
    if cfg.r > 0.0 {
        tasks.push(rt().spawn(Arc::clone(&shared).publisher(server.broadcast_url(), load_stop.clone())));
    }
    for i in 0..cfg.c {
        tasks.push(rt().spawn(Arc::clone(&shared).client_loop(i, load_stop.clone())));
    }
    ui.log(
        stderr,
        &format!(
            "load: {} clients, {} streams, {} msg/s for {}",
            cfg.c,
            cfg.big_s,
            cfg.r,
            super::go_duration_string(cfg.duration)
        ),
    );

    // The load window, with a progress line every sample interval.
    let load_end = sampler.start + cfg.duration;
    let mut server_died = false;
    while !interrupt.is_cancelled() && Instant::now() < load_end {
        let step = load_end.saturating_duration_since(Instant::now()).min(Duration::from_secs(1));
        interrupt.sleep(step);
        if server.resources().is_err() {
            server_died = true;
            break;
        }
    }
    let interrupted = interrupt.is_cancelled();
    sampler_stop.cancel();
    let _ = sampler_thread.join();
    let end_sample = sampler.take("end");
    ui.log(stderr, "load done: stopping clients...");
    load_stop.cancel();
    block_on(async move {
        for t in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(30), t).await;
        }
    });
    let stopped = Instant::now();
    let generator = generator.finish();

    let authenticated = get(&shared.stats.sessions_authenticated);
    let drained = !server_died && wait_drained(&server, &rpc, authenticated, stopped, cfg.drain_timeout);
    let drain_seconds = stopped.elapsed().as_secs_f64();
    ui.log(stderr, &format!("drain {} after {drain_seconds:.1}s", if drained { "complete" } else { "incomplete" }));
    if !interrupted && !server_died {
        ui.log(stderr, &format!("settling {}...", super::go_duration_string(cfg.settle)));
        interrupt.sleep(cfg.settle);
    }
    let residual = sampler.take("residual");

    let samples = sampler.all();
    let summary = Summary::build(
        &cfg,
        &shared,
        &rpc,
        &samples,
        Measured { baseline, end: end_sample, residual, drained, drain_seconds, server_died, interrupted },
        generator,
    );
    let _ = stdout.write_all(summary.text.as_bytes());

    let _ = server.shutdown();
    rpc.close();
    if interrupted {
        return super::EXIT_INTERRUPTED;
    }
    if summary.failed {
        return 1;
    }
    0
}

struct Measured {
    baseline: Sample,
    end: Sample,
    residual: Sample,
    drained: bool,
    drain_seconds: f64,
    server_died: bool,
    interrupted: bool,
}

/// The key=value summary and whether the run failed.
struct Summary {
    text: String,
    failed: bool,
}

impl Summary {
    fn build(
        cfg: &RunConfig,
        shared: &Shared,
        rpc: &RpcServer,
        samples: &[Sample],
        m: Measured,
        generator: GeneratorReport,
    ) -> Summary {
        let st = &shared.stats;
        let mut out = String::new();
        let mut kv = |k: &str, v: String| out.push_str(&format!("{k}={v}\n"));
        let in_load: Vec<&Sample> = samples.iter().filter(|s| !matches!(s.phase, "baseline" | "residual")).collect();
        let post_warmup: Vec<&Sample> = in_load.iter().copied().filter(|s| s.elapsed >= cfg.warmup).collect();
        let points: Vec<(f64, f64)> =
            post_warmup.iter().map(|s| (s.elapsed.as_secs_f64() / 3600.0, s.rss_mb())).collect();
        let slope = slope_per_hour(&points);
        let max_of = |f: &dyn Fn(&Sample) -> u64| in_load.iter().map(|s| f(s)).max().unwrap_or(0);

        kv("duration_seconds", format!("{:.0}", cfg.duration.as_secs_f64()));
        kv("warmup_seconds", format!("{:.0}", cfg.warmup.as_secs_f64()));
        kv("sample_interval_seconds", format!("{:.0}", cfg.sample_interval.as_secs_f64()));
        kv("clients", cfg.c.to_string());
        kv("streams", cfg.big_s.to_string());
        kv("publish_rate", format!("{}", cfg.r));
        kv("samples", in_load.len().to_string());
        kv("samples_post_warmup", post_warmup.len().to_string());
        kv("baseline_rss_mb", format!("{:.1}", m.baseline.rss_mb()));
        kv("baseline_threads", m.baseline.server.threads.to_string());
        kv("baseline_fds", m.baseline.server.open_fds.to_string());
        kv("baseline_sockets", m.baseline.server.sockets.to_string());
        if let Some(v) = m.baseline.goroutines {
            kv("baseline_goroutines", format!("{v:.0}"));
        }
        kv("rss_start_mb", post_warmup.first().map_or("nan".into(), |s| format!("{:.1}", s.rss_mb())));
        kv("rss_end_mb", format!("{:.1}", m.end.rss_mb()));
        kv("rss_max_mb", format!("{:.1}", mib(max_of(&|s| s.server.rss_bytes))));
        kv("rss_slope_mb_per_hour", slope.map_or("nan".into(), |v| format!("{v:.3}")));
        kv("threads_max", max_of(&|s| s.server.threads).to_string());
        kv("fds_max", max_of(&|s| s.server.open_fds).to_string());
        kv("sockets_max", max_of(&|s| s.server.sockets).to_string());
        if let Some(v) = in_load.iter().filter_map(|s| s.sessions).reduce(f64::max) {
            kv("server_sessions_max", format!("{v:.0}"));
        }
        kv("residual_rss_mb", format!("{:.1}", m.residual.rss_mb()));
        kv("residual_threads", m.residual.server.threads.to_string());
        kv("residual_fds", m.residual.server.open_fds.to_string());
        kv("residual_sockets", m.residual.server.sockets.to_string());
        if let Some(v) = m.residual.sessions {
            kv("residual_server_sessions", format!("{v:.0}"));
        }
        if let Some(v) = m.residual.goroutines {
            kv("residual_goroutines", format!("{v:.0}"));
        }
        let back = crate::resources::Residual { baseline: m.baseline.server, after: m.residual.server }
            .returned_to_baseline(2);
        kv("residual_returned_to_baseline", back.to_string());
        kv("drained", m.drained.to_string());
        kv("drain_seconds", format!("{:.1}", m.drain_seconds));

        for (k, c) in [
            ("sessions_attempted", &st.sessions_attempted),
            ("sessions_authenticated", &st.sessions_authenticated),
            ("auth_rejected", &st.auth_rejected),
            ("auth_unexpected_accepts", &st.auth_unexpected_accepts),
            ("auth_unexpected_failures", &st.auth_unexpected_failures),
            ("connect_errors", &st.connect_errors),
            ("unexpected_disconnects", &st.unexpected_disconnects),
            ("clean_disconnects", &st.clean_disconnects),
            ("clean_close_timeouts", &st.clean_close_timeouts),
            ("abrupt_disconnects", &st.abrupt_disconnects),
            ("subscriptions_confirmed", &st.subscriptions_confirmed),
            ("subscription_failures", &st.subscription_failures),
            ("navigations", &st.navigations),
            ("unsubscribes", &st.unsubscribes),
        ] {
            kv(k, get(c).to_string());
        }
        for (k, method) in [
            ("rpc_connect_calls", RpcMethod::Connect),
            ("rpc_command_calls", RpcMethod::Command),
            ("rpc_disconnect_calls", RpcMethod::Disconnect),
        ] {
            kv(k, rpc.stats(method).completed.to_string());
        }
        for (k, c) in [
            ("publications_ok", &st.publications_ok),
            ("publications_failed", &st.publications_failed),
            ("publish_ticks", &st.publish_ticks),
            ("publish_late_ticks", &st.publish_late_ticks),
            ("messages_received", &st.messages_received),
            ("deliveries_expected", &st.deliveries_expected),
            ("deliveries_matched", &st.deliveries_matched),
            ("messages_missing", &st.messages_missing),
            ("messages_duplicate", &st.messages_duplicate),
            ("messages_misrouted", &st.messages_misrouted),
        ] {
            kv(k, get(c).to_string());
        }
        let lat: HistogramSnapshot = shared.latency.snapshot();
        kv("latency_count", lat.count.to_string());
        for (q, label) in [(0.5, "p50"), (0.99, "p99")] {
            kv(&format!("latency_{label}_ms"), format!("{:.2}", ms(lat.quantile(q))));
        }
        kv("latency_max_ms", format!("{:.2}", ms(lat.max)));
        if let Some(e) = st.first_error.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            kv("first_error", e.replace('\n', " "));
        }
        if in_load.len() <= MAX_INLINE_SAMPLES {
            let series = |f: &dyn Fn(&Sample) -> String| in_load.iter().map(|s| f(s)).collect::<Vec<_>>().join(",");
            kv("series_elapsed_s", series(&|s| format!("{:.0}", s.elapsed.as_secs_f64())));
            kv("series_rss_mb", series(&|s| format!("{:.1}", s.rss_mb())));
            kv("series_threads", series(&|s| s.server.threads.to_string()));
            kv("series_sockets", series(&|s| s.server.sockets.to_string()));
            kv("series_sessions", series(&|s| s.harness_sessions.to_string()));
        }
        kv("server_died", m.server_died.to_string());
        kv("interrupted", m.interrupted.to_string());
        let ticks = get(&st.publish_ticks);
        let saturated = generator.starved() || get(&st.publish_late_ticks) * 100 > ticks;
        generator.write(&mut out, saturated);
        let failed = m.server_died
            || !m.drained
            || get(&st.messages_missing) > 0
            || get(&st.messages_duplicate) > 0
            || get(&st.messages_misrouted) > 0;
        Summary { text: out, failed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slope_is_least_squares() {
        let pts = [(0.0, 100.0), (1.0, 102.0), (2.0, 104.0)];
        assert!((slope_per_hour(&pts).expect("slope") - 2.0).abs() < 1e-9);
        assert_eq!(None, slope_per_hour(&[(1.0, 1.0)]));
        assert_eq!(None, slope_per_hour(&[(1.0, 1.0), (1.0, 2.0)]));
    }

    #[test]
    fn window_takes_broadcasts_inside_both_edges() {
        let log = PublicationLog { streams: vec![Mutex::new(VecDeque::new())], horizon_ns: u64::MAX };
        for (seq, start) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            log.push(0, Publication { seq, start, end: start + 5 });
        }
        assert_eq!(vec![2, 3], log.window(0, 20, 35));
        assert_eq!(Vec::<u64>::new(), log.window(0, 41, 100));
        assert_eq!(Vec::<u64>::new(), log.window(0, 30, 30));
    }

    #[test]
    fn flags_validate() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let mut err = Vec::new();
        let cfg = parse_flags(&args(&["--duration=3m", "--warmup=30s", "-c", "50"]), &mut err).expect("parsed");
        assert_eq!((50, Duration::from_secs(180), Duration::from_secs(30)), (cfg.c, cfg.duration, cfg.warmup));
        assert_eq!((100, 3, 20.0), (cfg.big_s, cfg.s, cfg.r));
        assert!(parse_flags(&args(&["--duration=1m", "--warmup=1m"]), &mut err).is_err());
        assert!(parse_flags(&args(&["-s", "5", "-S", "4"]), &mut err).is_err());
    }
}
