//! The server under test as an external process (upstream `lib/server.go`, which
//! embeds the Go server instead).
//!
//! [`Server::build`] launches `binary` with the AnyCable CLI flags both Cabby and
//! `anycable-go` accept. In the default (v1) mode that is public streams + skip-auth +
//! no RPC, the HTTP broadcaster only, no pub/sub and no presence:
//!
//! ```text
//! --host=127.0.0.1 --port=<p> --path=/cable --http_broadcast_port=<p> (same port)
//! --http_broadcast_path=/broadcast --broadcast_adapter=http --pubsub=none
//! --broker=<broker_adapter or none> --streams_presence=false
//! --metrics_http=/metrics --log_level=error
//! --public_streams --noauth --norpc
//! ```
//!
//! With `rpc_host` set it switches to HTTP RPC mode (`--rpc_host=<url>
//! --rpc_impl=http`, auth and public streams off). [`ServerTuning`] maps to the
//! matching node flags; `extra_args` come last. The metrics endpoint shares the
//! main port unless another one is given. The environment is scrubbed as for
//! every server under test ([`crate::server_env`], D-064), telemetry is disabled for the Go server
//! (`ANYCABLE_DISABLE_TELEMETRY`; Cabby has none, D-058) and
//! [`ServerConfig::env`] is added.
//!
//! With [`ServerConfig::pusher`] set, the server also gets the Pusher app flags
//! (`--pusher_app_id`, `--pusher_app_key`, `--pusher_secret`; Cabby and `anycable-go`
//! take them as they are, the `bench/soketi` and `bench/reverb` wrappers translate them),
//! [`Server::websocket_url`] is the Pusher endpoint `/app/<key>?protocol=7..` and
//! [`Server::broadcast_url`] the Pusher HTTP API's `/apps/<id>/events`.
//!
//! The metrics endpoint is optional: a server without one (Action Cable, see
//! `bench/actioncable`) answers it with an HTTP error, [`Server::has_metrics`] then
//! reports `false`, every later read fails fast, and scenarios report what depends on
//! server gauges as unknown instead of waiting for it.
//! Every wait (readiness, shutdown) has a deadline; shutdown sends SIGTERM, then
//! SIGKILL.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::BenchError;
use crate::net::loopback;
use crate::pusher::PusherApp;
use crate::resources::ProcessResources;

/// Interval between readiness and exit probes (observed state, every wait bounded).
const PROBE_INTERVAL: Duration = Duration::from_millis(5);
/// Server output kept for error reports when it is otherwise discarded.
const OUTPUT_TAIL: usize = 64 * 1024;
const METRICS_TIMEOUT: Duration = Duration::from_secs(5);

/// Where the server's own stdout/stderr go. The harness's stdout is the summary
/// contract, so server output is never forwarded there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ServerLog {
    /// Captured and dropped (upstream: discard logger). The server runs at
    /// `--log_level=error`, so log formatting costs (nearly) nothing, as with
    /// upstream's discard logger.
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
    /// Extra environment variables for the server process.
    pub env: Vec<(String, String)>,
    /// Deadline for the server to accept connections.
    pub start_timeout: Duration,
    /// Time between SIGTERM and SIGKILL on shutdown.
    pub shutdown_grace: Duration,
    /// Pusher mode: the application the server serves ([`crate::pusher`]).
    pub pusher: Option<PusherApp>,
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
            env: Vec::new(),
            start_timeout: Duration::from_secs(20),
            shutdown_grace: Duration::from_secs(5),
            pusher: None,
        }
    }

    /// The CLI flags for this configuration, given the allocated ports.
    pub fn args(&self, port: u16, metrics_port: u16) -> Vec<String> {
        let broker = match self.broker_adapter.as_str() {
            "" => "none",
            b => b,
        };
        let mut a = vec![
            "--host=127.0.0.1".to_owned(),
            format!("--port={port}"),
            "--path=/cable".to_owned(),
            format!("--http_broadcast_port={port}"),
            "--http_broadcast_path=/broadcast".to_owned(),
            "--broadcast_adapter=http".to_owned(),
            "--pubsub=none".to_owned(),
            format!("--broker={broker}"),
            // The legacy broker rejects presence operations.
            "--streams_presence=false".to_owned(),
            "--metrics_http=/metrics".to_owned(),
        ];
        if metrics_port != port {
            a.push(format!("--metrics_port={metrics_port}"));
        }
        match &self.log {
            ServerLog::Discard => a.push("--log_level=error".to_owned()),
            ServerLog::File(_) => a.push("--log_level=debug".to_owned()),
            ServerLog::Stderr => {}
        }
        match &self.rpc_host {
            Some(host) => {
                a.push(format!("--rpc_host={host}"));
                a.push("--rpc_impl=http".to_owned());
            }
            None => {
                a.push("--public_streams".to_owned());
                a.push("--noauth".to_owned());
                a.push("--norpc".to_owned());
            }
        }
        let t = &self.tuning;
        let flags: [(&str, Option<String>); 6] = [
            ("ping_interval", t.ping_interval.map(|v| v.to_string())),
            ("rpc_concurrency", t.rpc_concurrency.map(|v| v.to_string())),
            ("rpc_wait_timeout", t.rpc_wait_timeout_ms.map(|v| if v < 0 { "-1".to_owned() } else { v.to_string() })),
            ("disconnect_rate", t.disconnect_rate.map(|v| v.to_string())),
            ("max_disconnect_backlog_size", t.disconnect_backlog.map(|v| v.to_string())),
            ("stats_refresh_interval", t.stats_refresh_interval.map(|v| v.to_string())),
        ];
        for (name, value) in flags {
            if let Some(v) = value {
                a.push(format!("--{name}={v}"));
            }
        }
        if let Some(app) = &self.pusher {
            a.extend(app.server_args());
        }
        a.extend(self.extra_args.iter().cloned());
        a
    }
}

/// A loopback port that is free now and was never handed out before by this
/// process (parallel builds cannot collide in-process; other processes can, and
/// [`Server::build`] retries then).
fn free_port() -> u16 {
    static TAKEN: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let taken = TAKEN.get_or_init(Default::default);
    loop {
        let port = TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map_or(0, |a| a.port());
        if port != 0 && taken.lock().unwrap_or_else(|e| e.into_inner()).insert(port) {
            return port;
        }
    }
}

/// Whether process `pid` holds a TCP socket in LISTEN state on `port` (procfs), so
/// a foreign process that grabbed the port in the meantime is not mistaken for
/// the server.
fn listener_owned_by(pid: u32, port: u16) -> bool {
    const LISTEN: &str = "0A";
    let mut inodes = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else { continue };
        for line in text.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            let (Some(local), Some(state), Some(inode)) = (f.get(1), f.get(3), f.get(9)) else { continue };
            let local_port = local.rsplit(':').next().and_then(|p| u16::from_str_radix(p, 16).ok());
            if *state == LISTEN && local_port == Some(port) {
                inodes.push(format!("socket:[{inode}]"));
            }
        }
    }
    if inodes.is_empty() {
        return false;
    }
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else { return false };
    fds.flatten().filter_map(|e| std::fs::read_link(e.path()).ok()).any(|t| {
        let t = t.to_string_lossy();
        inodes.iter().any(|i| *i == t)
    })
}

fn listening(pid: u32, port: u16) -> bool {
    if std::path::Path::new("/proc/net/tcp").exists() {
        listener_owned_by(pid, port)
    } else {
        TcpStream::connect_timeout(&loopback(port), PROBE_INTERVAL).is_ok()
    }
}

/// The tail of the server's output, for error reports.
#[derive(Default)]
struct Captured {
    tail: VecDeque<u8>,
    /// Everything forwarded to the harness's stderr ([`ServerLog::Stderr`]).
    forwarded: Vec<u8>,
}

/// Server metrics from the Prometheus endpoint.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metrics {
    values: HashMap<String, f64>,
}

impl Metrics {
    /// Parses the Prometheus text format (unlabeled samples are keyed by name).
    pub fn parse(text: &str) -> Self {
        let mut values = HashMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let (Some(name), Some(value)) = (parts.next(), parts.next()) else { continue };
            if let Ok(v) = value.parse::<f64>() {
                values.insert(name.to_owned(), v);
            }
        }
        Self { values }
    }

    /// Metric `name` (with or without the `anycable_go_` namespace); `None` when the
    /// server does not report it.
    pub fn value(&self, name: &str) -> Option<f64> {
        self.values.get(&format!("anycable_go_{name}")).or_else(|| self.values.get(name)).copied()
    }

    /// Metric `name` (with or without the `anycable_go_` namespace); 0 when unknown.
    pub fn get(&self, name: &str) -> u64 {
        let v = self.values.get(&format!("anycable_go_{name}")).or_else(|| self.values.get(name)).copied();
        v.map_or(0, |v| if v.is_finite() && v > 0.0 { v as u64 } else { 0 })
    }
}

/// A running server process.
pub struct Server {
    child: Mutex<Child>,
    pid: u32,
    port: u16,
    metrics_port: u16,
    captured: Arc<Mutex<Captured>>,
    readers: Mutex<Vec<JoinHandle<()>>>,
    grace: Duration,
    stopped: Mutex<bool>,
    /// Whether the metrics endpoint exists ([`Server::has_metrics`]), probed once.
    metrics_served: OnceLock<bool>,
    pusher: Option<PusherApp>,
}

fn capture(mut stream: impl Read + Send + 'static, sink: Arc<Mutex<Captured>>, forward: bool) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        while let Ok(n @ 1..) = stream.read(&mut buf) {
            let chunk = &buf[..n];
            if forward {
                let _ = std::io::stderr().write_all(chunk);
            }
            let mut c = sink.lock().unwrap_or_else(|e| e.into_inner());
            c.tail.extend(chunk);
            let excess = c.tail.len().saturating_sub(OUTPUT_TAIL);
            c.tail.drain(..excess);
            if forward {
                c.forwarded.extend_from_slice(chunk);
            }
        }
    })
}

impl Server {
    /// Launches the server and waits until both the WebSocket and broadcast endpoints
    /// accept connections.
    pub fn build(cfg: ServerConfig) -> Result<Server, BenchError> {
        const ATTEMPTS: usize = 3;
        let mut last = None;
        for _ in 0..ATTEMPTS {
            match Self::start_once(&cfg) {
                Ok(s) => return Ok(s),
                Err(e) if e.0.contains("address already in use") => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| BenchError("server did not start".into())))
    }

    /// The server's command line and environment: `inherited` (the harness's
    /// variable names) scrubbed as for every server under test (D-064), then
    /// [`ServerConfig::env`].
    fn command<'a>(cfg: &ServerConfig, port: u16, inherited: impl IntoIterator<Item = &'a str>) -> Command {
        let mut cmd = Command::new(&cfg.binary);
        crate::server_env::scrub_env_of(&mut cmd, inherited);
        cmd.env("ANYCABLE_DISABLE_TELEMETRY", "true").args(cfg.args(port, port)).stdin(Stdio::null());
        cmd.envs(cfg.env.iter().map(|(k, v)| (k, v)));
        cmd
    }

    fn start_once(cfg: &ServerConfig) -> Result<Server, BenchError> {
        let port = free_port();
        let names: Vec<String> = std::env::vars_os().map(|(k, _)| k.to_string_lossy().into_owned()).collect();
        let mut cmd = Self::command(cfg, port, names.iter().map(String::as_str));
        let forward = cfg.log == ServerLog::Stderr;
        match &cfg.log {
            ServerLog::File(path) => {
                let f = std::fs::File::create(path)
                    .map_err(|e| BenchError(format!("server log {}: {e}", path.display())))?;
                let f2 = f.try_clone().map_err(|e| BenchError(e.to_string()))?;
                cmd.stdout(f).stderr(f2);
            }
            ServerLog::Discard | ServerLog::Stderr => {
                cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
        }
        let mut child =
            cmd.spawn().map_err(|e| BenchError(format!("starting server {}: {e}", cfg.binary.display())))?;
        let captured = Arc::new(Mutex::new(Captured::default()));
        let mut readers = Vec::new();
        if let Some(out) = child.stdout.take() {
            readers.push(capture(out, Arc::clone(&captured), forward));
        }
        if let Some(err) = child.stderr.take() {
            readers.push(capture(err, Arc::clone(&captured), forward));
        }
        let server = Server {
            pid: child.id(),
            child: Mutex::new(child),
            port,
            metrics_port: port,
            captured,
            readers: Mutex::new(readers),
            grace: cfg.shutdown_grace,
            stopped: Mutex::new(false),
            metrics_served: OnceLock::new(),
            pusher: cfg.pusher.clone(),
        };
        let deadline = Instant::now() + cfg.start_timeout;
        loop {
            if listening(server.pid, port) {
                return Ok(server);
            }
            if let Some(status) = server.try_wait() {
                server.join_readers();
                *server.stopped.lock().unwrap_or_else(|e| e.into_inner()) = true;
                return Err(BenchError(format!(
                    "server {} exited ({status}) before listening on port {port}. Output:\n{}",
                    cfg.binary.display(),
                    server.output_tail()
                )));
            }
            if Instant::now() >= deadline {
                let _ = server.kill();
                return Err(BenchError(format!(
                    "server {} did not listen on port {port} within {:?}. Output:\n{}",
                    cfg.binary.display(),
                    cfg.start_timeout,
                    server.output_tail()
                )));
            }
            std::thread::sleep(PROBE_INTERVAL);
        }
    }

    /// Checks that `binary` can be executed (`--version`) and returns its version line:
    /// the external counterpart of upstream's module linkage.
    pub fn probe_binary(binary: &std::path::Path) -> Result<String, BenchError> {
        let out = Command::new(binary)
            .arg("--version")
            .env("ANYCABLE_DISABLE_TELEMETRY", "true")
            .stdin(Stdio::null())
            .output()
            .map_err(|e| BenchError(format!("running {} --version: {e}", binary.display())))?;
        if !out.status.success() {
            return Err(BenchError(format!("{} --version exited with {}", binary.display(), out.status)));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let line = text.lines().find(|l| !l.trim().is_empty()).unwrap_or_default().trim().to_owned();
        Ok(line)
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// `ws://127.0.0.1:<port>/cable`, or the Pusher endpoint in Pusher mode.
    pub fn websocket_url(&self) -> String {
        match &self.pusher {
            Some(app) => format!("ws://127.0.0.1:{}{}", self.port, app.websocket_path()),
            None => format!("ws://127.0.0.1:{}/cable", self.port),
        }
    }

    /// `http://127.0.0.1:<port>/broadcast`, or the Pusher HTTP API's events endpoint in
    /// Pusher mode.
    pub fn broadcast_url(&self) -> String {
        match &self.pusher {
            Some(app) => format!("http://127.0.0.1:{}{}", self.port, app.events_path()),
            None => format!("http://127.0.0.1:{}/broadcast", self.port),
        }
    }

    /// `http://127.0.0.1:<port>/metrics`.
    pub fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.metrics_port)
    }

    fn try_wait(&self) -> Option<ExitStatus> {
        self.child.lock().unwrap_or_else(|e| e.into_inner()).try_wait().ok().flatten()
    }

    fn kill(&self) -> std::io::Result<ExitStatus> {
        let mut child = self.child.lock().unwrap_or_else(|e| e.into_inner());
        child.kill()?;
        child.wait()
    }

    fn join_readers(&self) {
        for r in self.readers.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            let _ = r.join();
        }
    }

    fn output_tail(&self) -> String {
        let c = self.captured.lock().unwrap_or_else(|e| e.into_inner());
        String::from_utf8_lossy(&c.tail.iter().copied().collect::<Vec<u8>>()).into_owned()
    }

    /// Stops the server (SIGTERM, then SIGKILL after a grace period). The first call
    /// reports the shutdown result; later calls are no-ops returning `Ok`.
    pub fn shutdown(&self) -> Result<(), BenchError> {
        let mut stopped = self.stopped.lock().unwrap_or_else(|e| e.into_inner());
        if *stopped {
            return Ok(());
        }
        *stopped = true;
        if let Some(status) = self.try_wait() {
            self.join_readers();
            return Err(BenchError(format!(
                "server exited before shutdown ({status}). Output:\n{}",
                self.output_tail()
            )));
        }
        let _ = Command::new("kill")
            .args(["-TERM", &self.pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + self.grace;
        let result = loop {
            if self.try_wait().is_some() {
                break Ok(());
            }
            if Instant::now() >= deadline {
                let _ = self.kill();
                break Err(BenchError(format!("server did not stop within {:?} of SIGTERM; killed", self.grace)));
            }
            std::thread::sleep(PROBE_INTERVAL);
        };
        self.join_readers();
        result
    }

    /// All server metrics from the metrics endpoint; an error without a request when
    /// the server has none ([`Server::has_metrics`]).
    pub fn metrics(&self) -> Result<Metrics, BenchError> {
        if !self.has_metrics() {
            return Err(BenchError("the server has no metrics endpoint".into()));
        }
        fetch_metrics(self.metrics_port).map(|t| Metrics::parse(&t)).map_err(MetricsError::into_bench)
    }

    /// Whether the server serves the metrics endpoint, probed on first use: an HTTP
    /// error answer means it has none; transport errors are retried a few times.
    pub fn has_metrics(&self) -> bool {
        *self.metrics_served.get_or_init(|| {
            const ATTEMPTS: usize = 3;
            for _ in 0..ATTEMPTS {
                match fetch_metrics(self.metrics_port) {
                    Ok(_) => return true,
                    Err(MetricsError::Status(_)) => return false,
                    Err(MetricsError::Other(_)) => std::thread::sleep(Duration::from_millis(100)),
                }
            }
            false
        })
    }

    /// Server gauge `name` from the metrics endpoint (0 when unknown). Gauges lag by
    /// up to the stats refresh interval.
    pub fn gauge(&self, name: &str) -> u64 {
        self.metrics().map_or(0, |m| m.get(name))
    }

    /// Server counter `name` from the metrics endpoint (0 when unknown).
    pub fn counter(&self, name: &str) -> u64 {
        self.metrics().map_or(0, |m| m.get(name))
    }

    /// Sessions waiting in the disconnect queue: the `disconnect_queue_size` gauge.
    /// Upstream reads the queue directly (never stale); an external server only
    /// exposes the gauge, refreshed every stats interval, so a 0 read can be stale.
    /// Callers that wait for the queue to drain must use
    /// [`crate::scenarios::reconnect_floods::wave_drained`], not this value alone.
    pub fn disconnect_queue_size(&self) -> u64 {
        self.gauge("disconnect_queue_size")
    }

    /// RSS, threads and file descriptors of the server process (`/proc/<pid>`).
    pub fn resources(&self) -> Result<ProcessResources, BenchError> {
        crate::resources::sample(self.pid)
    }

    /// Everything the harness forwarded from the server's stdout/stderr to its own
    /// stderr. Empty unless the log is [`ServerLog::Stderr`].
    pub fn forwarded_output(&self) -> String {
        String::from_utf8_lossy(&self.captured.lock().unwrap_or_else(|e| e.into_inner()).forwarded).into_owned()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if !*self.stopped.lock().unwrap_or_else(|e| e.into_inner()) {
            let _ = self.kill();
        }
    }
}

/// Why a metrics read failed.
enum MetricsError {
    /// The server answered with a non-2xx status line.
    Status(String),
    /// Transport error or malformed response.
    Other(String),
}

impl MetricsError {
    fn into_bench(self) -> BenchError {
        match self {
            MetricsError::Status(line) => BenchError(format!("metrics endpoint answered {line}")),
            MetricsError::Other(e) => BenchError(e),
        }
    }
}

/// GETs the metrics page over plain HTTP/1.0 (no chunked encoding).
fn fetch_metrics(port: u16) -> Result<String, MetricsError> {
    let err = |e: std::io::Error| MetricsError::Other(format!("metrics endpoint: {e}"));
    let mut conn = TcpStream::connect_timeout(&loopback(port), METRICS_TIMEOUT).map_err(err)?;
    conn.set_read_timeout(Some(METRICS_TIMEOUT)).map_err(err)?;
    conn.set_write_timeout(Some(METRICS_TIMEOUT)).map_err(err)?;
    write!(conn, "GET /metrics HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n").map_err(err)?;
    let mut raw = Vec::new();
    conn.read_to_end(&mut raw).map_err(err)?;
    let text = String::from_utf8_lossy(&raw);
    let (head, body) =
        text.split_once("\r\n\r\n").ok_or_else(|| MetricsError::Other("malformed metrics response".into()))?;
    if !head.split_whitespace().nth(1).is_some_and(|s| s.starts_with('2')) {
        return Err(MetricsError::Status(head.lines().next().unwrap_or_default().to_owned()));
    }
    Ok(body.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins D-064: a benchmarked server inherits no server configuration from
    /// the harness's environment, as servers under test don't.
    #[test]
    fn server_config_variables_are_not_inherited() {
        let cfg = ServerConfig { env: vec![("PORT".into(), "1".into())], ..ServerConfig::new("cabby") };
        let inherited =
            ["ANYCABLE_SECRET", "CABBY_PORT", "CABBY_NATS_SERVER_BIN", "CABBY_TEST_REDIS_URL", "PORT", "PATH"];
        let cmd = Server::command(&cfg, 8080, inherited);
        let envs: Vec<(String, Option<String>)> = cmd
            .get_envs()
            .map(|(k, v)| (k.to_string_lossy().into_owned(), v.map(|v| v.to_string_lossy().into_owned())))
            .collect();
        for removed in ["ANYCABLE_SECRET", "CABBY_PORT"] {
            assert!(envs.contains(&(removed.into(), None)), "{removed} is inherited: {envs:?}");
        }
        for kept in ["CABBY_NATS_SERVER_BIN", "CABBY_TEST_REDIS_URL", "PATH"] {
            assert!(!envs.iter().any(|(k, _)| k == kept), "{kept} is touched: {envs:?}");
        }
        // The harness's own settings still apply.
        assert!(envs.contains(&("PORT".into(), Some("1".into()))), "{envs:?}");
    }

    #[test]
    fn parses_prometheus_text() {
        let m = Metrics::parse(
            "# HELP anycable_go_clients_num x\n# TYPE anycable_go_clients_num gauge\nanycable_go_clients_num 12\n\
             anycable_go_rpc_canceled_total 3\nother{a=\"b\"} 4\n",
        );
        assert_eq!(12, m.get("clients_num"));
        assert_eq!(3, m.get("rpc_canceled_total"));
        assert_eq!(0, m.get("missing"));
    }

    #[test]
    fn v1_and_rpc_args() {
        let mut cfg = ServerConfig::new("cabby");
        let a = cfg.args(8080, 8080);
        assert!(a.contains(&"--public_streams".to_owned()) && a.contains(&"--broker=none".to_owned()));
        cfg.rpc_host = Some("http://127.0.0.1:9".into());
        cfg.tuning.rpc_wait_timeout_ms = Some(-5);
        let a = cfg.args(8080, 9090);
        assert!(a.contains(&"--rpc_host=http://127.0.0.1:9".to_owned()));
        assert!(a.contains(&"--rpc_wait_timeout=-1".to_owned()));
        assert!(a.contains(&"--metrics_port=9090".to_owned()));
        assert!(!a.contains(&"--noauth".to_owned()));
    }

    #[test]
    fn pusher_args() {
        let cfg = ServerConfig { pusher: Some(PusherApp::default()), ..ServerConfig::new("cabby") };
        let a = cfg.args(8080, 8080);
        assert!(a.contains(&"--pusher_app_key=cabby-bench-key".to_owned()));
        assert!(a.contains(&"--public_streams".to_owned()));
    }
}
