//! The server under test as an external process (upstream `lib/server.go`, which
//! embeds the Go server instead).
//!
//! [`Server::build`] launches `binary` with the AnyCable CLI flags both Cabby and
//! `anycable-go` accept. In the default (v1) mode that is public streams + skip-auth +
//! no RPC, the HTTP broadcaster only, no pub/sub and no presence:
//!
//! ```text
//! --host=127.0.0.1 --port=<p> --path=/cable --http_broadcast_port=<p> (same port)
//! --public_streams --noauth --rpc_impl=none --presence=false
//! --broadcast_adapter=http --pubsub=none --broker=<broker_adapter>
//! --metrics_http=/metrics --metrics_port=<m> --disable_telemetry
//! ```
//!
//! With `rpc_host` set it switches to HTTP RPC mode (`--rpc_host=<url>`, auth and
//! public streams off). [`ServerTuning`] maps to the matching node flags. Every wait
//! (readiness, shutdown) has a deadline; shutdown sends SIGTERM, then SIGKILL.

use std::path::PathBuf;
use std::time::Duration;

use cabby_core::pending;

use crate::BenchError;
use crate::resources::ProcessResources;

/// Where the server's own stdout/stderr go. The harness's stdout is the summary
/// contract, so server output is never forwarded there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ServerLog {
    /// Captured and dropped (upstream: discard logger).
    #[default]
    Discard,
    /// Written to a file (upstream reconnect_floods `--server-log`), debug level.
    File(PathBuf),
    /// Forwarded to the harness's stderr (development only).
    Stderr,
}

/// Declarative replacement for upstream's `ServerConfig.Configure func(*config.Config)`:
/// each field becomes a CLI flag. `None` keeps the server default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerTuning {
    /// `--ping_interval` (seconds).
    pub ping_interval: Option<u32>,
    /// `--rpc_concurrency`.
    pub rpc_concurrency: Option<u32>,
    /// `--rpc_wait_timeout` in milliseconds; negative means the command timeout,
    /// zero no limit.
    pub rpc_wait_timeout_ms: Option<i64>,
    /// `--disconnect_rate` (calls per second).
    pub disconnect_rate: Option<u32>,
    /// `--max_disconnect_backlog_size`.
    pub disconnect_backlog: Option<u32>,
    /// `--stats_refresh_interval` (seconds); gauges lag by up to this much.
    pub stats_refresh_interval: Option<u32>,
}

/// Configuration of the server under test (upstream `ServerConfig`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Server executable: `cabby` or `anycable-go`.
    pub binary: PathBuf,
    pub log: ServerLog,
    /// `""`/`"none"` (legacy broker, no history), `"memory"`, `"nats"`.
    pub broker_adapter: String,
    /// HTTP RPC application URL; switches off public streams and skip-auth.
    pub rpc_host: Option<String>,
    pub tuning: ServerTuning,
    /// Extra CLI flags appended after the defaults (upstream `ExtraOptions`).
    pub extra_args: Vec<String>,
    /// Deadline for the server to accept connections.
    pub start_timeout: Duration,
}

impl ServerConfig {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            log: ServerLog::Discard,
            broker_adapter: String::new(),
            rpc_host: None,
            tuning: ServerTuning::default(),
            extra_args: Vec::new(),
            start_timeout: Duration::from_secs(20),
        }
    }

    /// The CLI flags for this configuration, given the allocated ports.
    pub fn args(&self, _port: u16, _metrics_port: u16) -> Vec<String> {
        pending!("benchi::server_args")
    }
}

/// A running server process.
pub struct Server {
    _child: (),
}

impl Server {
    /// Launches the server and waits until both the WebSocket and broadcast endpoints
    /// accept connections.
    pub fn build(_cfg: ServerConfig) -> Result<Server, BenchError> {
        pending!("benchi::build_server")
    }

    /// Checks that `binary` can be executed (`--version`) and returns its version line:
    /// the external counterpart of upstream's module linkage.
    pub fn probe_binary(_binary: &std::path::Path) -> Result<String, BenchError> {
        pending!("benchi::probe_binary")
    }

    pub fn pid(&self) -> u32 {
        pending!("benchi::server_pid")
    }

    /// `ws://127.0.0.1:<port>/cable`.
    pub fn websocket_url(&self) -> String {
        pending!("benchi::server_websocket_url")
    }

    /// `http://127.0.0.1:<port>/broadcast`.
    pub fn broadcast_url(&self) -> String {
        pending!("benchi::server_broadcast_url")
    }

    /// Stops the server (SIGTERM, then SIGKILL after a grace period). The first call
    /// reports the shutdown result; later calls are no-ops returning `Ok`.
    pub fn shutdown(&self) -> Result<(), BenchError> {
        pending!("benchi::server_shutdown")
    }

    /// Server gauge `name` from the metrics endpoint (0 when unknown). Gauges lag by
    /// up to the stats refresh interval.
    pub fn gauge(&self, _name: &str) -> u64 {
        pending!("benchi::server_gauge")
    }

    /// Server counter `name` from the metrics endpoint (0 when unknown).
    pub fn counter(&self, _name: &str) -> u64 {
        pending!("benchi::server_counter")
    }

    /// Sessions waiting in the disconnect queue: the `disconnect_queue_size` gauge.
    /// Upstream reads the queue directly (never stale); an external server only
    /// exposes the gauge, refreshed every stats interval, so a 0 read can be stale.
    /// Callers that wait for the queue to drain must use
    /// [`crate::scenarios::reconnect_floods::wave_drained`], not this value alone.
    pub fn disconnect_queue_size(&self) -> u64 {
        pending!("benchi::server_disconnect_queue_size")
    }

    /// RSS, threads and file descriptors of the server process (`/proc/<pid>`).
    pub fn resources(&self) -> Result<ProcessResources, BenchError> {
        pending!("benchi::server_resources")
    }

    /// Everything the harness forwarded from the server's stdout/stderr to its own
    /// stderr. Empty unless the log is [`ServerLog::Stderr`].
    pub fn forwarded_output(&self) -> String {
        pending!("benchi::server_forwarded_output")
    }
}
