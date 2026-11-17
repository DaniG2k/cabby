//! `stress_publications` (upstream `scenarios/stress_publications/main.go`): dial `-c`
//! clients, subscribe each to `-s` random streams of `1..=-S`, publish at `-r`/s for
//! `-d` on an absolute tick grid, drain, reconcile, print a key=value summary.
//!
//! Exit codes: 0 clean, 1 setup failure or `clients_short > 0` or
//! `publications_dropped > 0`, 2 flag error (usage on stderr).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use cabby_core::pending;

use crate::pool::ClientPool;
use crate::publisher::Publisher;
use crate::server::Server;

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
    // reconciliation and server-process resources.
    pub duplicates: u64,
    pub ordering_violations: u64,
    pub peak_server_rss_mb: f64,
    pub peak_server_threads: u64,
}

/// Entry point: parse flags, run, return the exit code.
pub fn run(_args: &[String], _stdout: &mut dyn Write, _stderr: &mut dyn Write, _opts: RunOpts) -> i32 {
    pending!("benchi::stress_publications::run")
}

/// Upstream `parseFlags`; writes usage to `stderr` on missing required flags.
pub fn parse_flags(_args: &[String], _stderr: &mut dyn Write) -> Result<RunConfig, String> {
    pending!("benchi::stress_publications::parse_flags")
}

pub fn run_with_config(_cfg: RunConfig, _opts: RunOpts, _stdout: &mut dyn Write, _stderr: &mut dyn Write) -> i32 {
    pending!("benchi::stress_publications::run_with_config")
}

/// `1s / r` (at least 1ns).
pub fn publish_interval(_cfg: &RunConfig) -> Duration {
    pending!("benchi::stress_publications::publish_interval")
}

/// Walks the tick grid `start + i·interval` for every tick `<= start + d`, sleeping to
/// each tick or firing late (catch-up). Returns the number of late publishes. The
/// issued count is deterministic for a given `(r, d)`.
pub fn publish_window(_cfg: &RunConfig, _publisher: &Publisher) -> u64 {
    pending!("benchi::stress_publications::publish_window")
}

/// `key=value` lines (floats with 2 decimals).
pub fn print_summary(_w: &mut dyn Write, _s: &Summary) {
    pending!("benchi::stress_publications::print_summary")
}
