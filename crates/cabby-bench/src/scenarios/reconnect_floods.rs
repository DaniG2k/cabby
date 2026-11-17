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
//! (see [`crate::resources`]).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use cabby_core::pending;

use crate::cancel::CancelToken;
use crate::histogram::HistogramSnapshot;
use crate::reconnecting_client::CloseReason;

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

/// Entry point: parse flags, install SIGINT/SIGTERM handling (a second signal kills
/// the process), run.
pub fn run(_args: &[String], _stdout: &mut dyn Write, _stderr: &mut dyn Write) -> i32 {
    pending!("benchi::reconnect_floods::run")
}

pub fn parse_flags(_args: &[String], _stderr: &mut dyn Write) -> Result<RunConfig, String> {
    pending!("benchi::reconnect_floods::parse_flags")
}

/// Runs until done or until `interrupt` is cancelled (then prints the summary with
/// `interrupted=true` and returns 130).
pub fn run_with_config(
    _interrupt: &CancelToken,
    _cfg: RunConfig,
    _stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> i32 {
    pending!("benchi::reconnect_floods::run_with_config")
}

/// Current and longest run of consecutive samples in which clients waited for
/// confirmations but no Command call completed (upstream `commandStall`).
pub fn command_stall(_samples: &[Sample]) -> (Duration, Duration) {
    pending!("benchi::reconnect_floods::command_stall")
}
