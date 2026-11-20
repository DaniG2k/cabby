//! Embedded NATS server (upstream package `enats`).
//!
//! Upstream links the NATS server (a Go library) into the binary. There is no
//! maintained NATS server in Rust, so Cabby runs the official `nats-server`
//! binary as a supervised child process configured from [`Config`] (see
//! `docs/enats.md` for the strategy and its limits). The binary is found in
//! `CABBY_NATS_SERVER`, next to the Cabby executable, or on `PATH`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use cabby_core::logger::{Attr, Level, LogValue};
use cabby_core::toml_config::string_list;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnatsError(pub String);

impl std::fmt::Display for EnatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EnatsError {}

/// Upstream `enats.Config`. `Default` is Go's zero value (`var c Config`), which
/// upstream tests use directly; defaults come from [`Config::new`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub enabled: bool,
    pub debug: bool,
    pub trace: bool,
    pub name: String,
    pub service_addr: String,
    pub cluster_addr: String,
    pub cluster_name: String,
    pub gateway_addr: String,
    pub gateway_advertise: String,
    pub gateways: Vec<String>,
    pub routes: Vec<String>,
    pub jetstream: bool,
    pub store_dir: String,
    /// Seconds.
    pub jetstream_ready_timeout: i64,
    /// Bytes; 0 means the server default.
    pub max_payload: i64,
}

cabby_core::toml_section!(Config, "enats.Config" {
    "enabled" => enabled,
    "debug" => debug,
    "trace" => trace,
    "name" => name,
    "service_addr" => service_addr,
    "cluster_addr" => cluster_addr,
    "cluster_name" => cluster_name,
    "gateway_addr" => gateway_addr,
    "gateway_advertise" => gateway_advertise,
    "gateways" => gateways,
    "routes" => routes,
    "jetstream" => jetstream,
    "jetstream_store_dir" => store_dir,
    "jetstream_ready_timeout" => jetstream_ready_timeout,
    "max_payload" => max_payload,
});

impl Config {
    /// Upstream `enats.NewConfig()`.
    pub fn new() -> Self {
        Self {
            service_addr: crate::nats::DEFAULT_URL.to_owned(),
            cluster_name: "anycable-cluster".to_owned(),
            // seconds
            jetstream_ready_timeout: 30,
            ..Default::default()
        }
    }

    pub fn to_toml(&self) -> String {
        fn flag(out: &mut String, key: &str, on: bool) {
            out.push_str(if on { "" } else { "# " });
            out.push_str(key);
            out.push_str(" = true\n");
        }
        fn optional(out: &mut String, key: &str, value: &str) {
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }
        fn list(out: &mut String, key: &str, values: &[String]) {
            if values.is_empty() {
                out.push_str(&format!("# {key} = []\n"));
            } else {
                out.push_str(&format!("{key} = {}\n", string_list(values)));
            }
        }

        let mut out = String::new();
        flag(&mut out, "enabled", self.enabled);
        out.push_str("#\n# Verbose  logging settings\n");
        flag(&mut out, "debug", self.debug);
        flag(&mut out, "trace", self.trace);
        out.push_str("#\n# Service name\n");
        out.push_str(&format!("name = \"{}\"\n", self.name));
        out.push_str("#\n# Service address\n");
        out.push_str(&format!("service_addr = \"{}\"\n", self.service_addr));
        out.push_str("#\n# Cluster configuration\n#\n");
        optional(&mut out, "cluster_addr", &self.cluster_addr);
        if self.cluster_name.is_empty() {
            out.push_str("# cluster_name = \"\"\n");
        } else if self.cluster_addr.is_empty() {
            out.push_str(&format!("# cluster_name = \"{}\"\n", self.cluster_name));
        } else {
            out.push_str(&format!("cluster_name = \"{}\"\n", self.cluster_name));
        }
        optional(&mut out, "gateway_addr", &self.gateway_addr);
        optional(&mut out, "gateway_advertise", &self.gateway_advertise);
        list(&mut out, "gateways", &self.gateways);
        list(&mut out, "routes", &self.routes);
        out.push_str("#\n# JetStream configuration\n#\n");
        flag(&mut out, "jetstream", self.jetstream);
        optional(&mut out, "jetstream_store_dir", &self.store_dir);
        if self.jetstream {
            out.push_str(&format!("jetstream_ready_timeout = {}\n", self.jetstream_ready_timeout));
        } else {
            out.push_str(&format!("# jetstream_ready_timeout = {}\n", self.jetstream_ready_timeout));
        }
        out.push_str("#\n# Maximum message payload size\n#\n");
        if self.max_payload > 0 {
            out.push_str(&format!("max_payload = {}\n", self.max_payload));
        } else {
            out.push_str("# max_payload = 1048576\n");
        }
        out
    }

    /// Decodes TOML into `self`, keeping fields the document omits.
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), EnatsError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| EnatsError(e.0))?;
        Ok(())
    }
}

/// Upstream `serverStartTimeout`: how long `start` waits for the server to
/// accept connections.
const SERVER_START_TIMEOUT: Duration = Duration::from_secs(5);
/// How long `shutdown` waits for a graceful exit before killing the server.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_INTERVAL: Duration = Duration::from_millis(20);

/// `host` and `port` of a `scheme://[user@]host:port` address (upstream
/// `url.Parse` + `Hostname()`/`Port()`). A missing port is reported as `None`.
fn parse_host_port(addr: &str) -> Result<(String, Option<u16>), EnatsError> {
    let rest = addr.split_once("://").map_or(addr, |(_, r)| r);
    let rest = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let hostport = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    let (host, port) = if let Some(v6) = hostport.strip_prefix('[') {
        let (host, tail) = v6.split_once(']').ok_or_else(|| EnatsError(format!("invalid address: {addr}")))?;
        (host, tail.strip_prefix(':'))
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (hostport, None),
        }
    };
    let port = match port.filter(|p| !p.is_empty()) {
        Some(p) => Some(p.parse::<u16>().map_err(|e| EnatsError(format!("port is not valid: {p}: {e}")))?),
        None => None,
    };
    Ok((host.to_owned(), port))
}

fn address_with_port(addr: &str, what: &str) -> Result<(String, u16), EnatsError> {
    match parse_host_port(addr) {
        Ok((host, Some(port))) => Ok((host, port)),
        Ok((_, None)) => Err(EnatsError(format!("failed to parse {what}, cause: port cannot be empty"))),
        Err(e) => Err(EnatsError(format!("failed to parse {what}, cause: {e}"))),
    }
}

/// A quoted string in the nats-server configuration format (JSON-compatible).
fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

fn quote_list<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    format!("[{}]", items.into_iter().map(quote).collect::<Vec<_>>().join(", "))
}

fn random_suffix(size: usize) -> String {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut bytes = vec![0u8; size];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

/// The `nats-server` executable: `CABBY_NATS_SERVER`, a `nats-server` next to
/// the running executable, or `nats-server` on `PATH`.
pub fn server_binary() -> PathBuf {
    if let Some(p) = std::env::var_os("CABBY_NATS_SERVER").filter(|p| !p.is_empty()) {
        return PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        let bundled = exe.with_file_name(format!("nats-server{}", std::env::consts::EXE_SUFFIX));
        if bundled.is_file() {
            return bundled;
        }
    }
    PathBuf::from("nats-server")
}

struct Running {
    child: Child,
    output: Arc<Mutex<OutputTail>>,
    conf_path: PathBuf,
    host: String,
    port: u16,
}

/// The most server output kept (for start-up and exit errors).
const OUTPUT_LIMIT: usize = 64 * 1024;
/// How much of the kept output an unexpected exit logs.
const EXIT_OUTPUT_LIMIT: usize = 4 * 1024;
/// How often the running server is checked.
const WATCH_INTERVAL: Duration = Duration::from_millis(200);

/// The last [`OUTPUT_LIMIT`] bytes of the server's output.
#[derive(Default)]
struct OutputTail {
    buf: std::collections::VecDeque<u8>,
}

impl OutputTail {
    fn push_line(&mut self, line: &[u8]) {
        self.buf.extend(line);
        self.buf.push_back(b'\n');
        let excess = self.buf.len().saturating_sub(OUTPUT_LIMIT);
        self.buf.drain(..excess);
    }

    /// The last `limit` bytes, from a line start when possible.
    fn text(&self, limit: usize) -> String {
        let skip = self.buf.len().saturating_sub(limit);
        let mut bytes: Vec<u8> = self.buf.iter().skip(skip).copied().collect();
        if skip > 0
            && let Some(nl) = bytes.iter().position(|b| *b == b'\n')
        {
            bytes.drain(..=nl);
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

type ExitHandler = Box<dyn Fn(EnatsError) + Send + Sync>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Embedded NATS service (upstream `enats.Service`), backed by a supervised
/// `nats-server` child process.
pub struct Service {
    config: Config,
    name: Mutex<String>,
    running: Arc<Mutex<Option<Running>>>,
    on_exit: Arc<Mutex<Option<ExitHandler>>>,
}

impl Service {
    /// Upstream `NewService(config)`.
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
            name: Mutex::new(String::new()),
            running: Arc::default(),
            on_exit: Arc::default(),
        }
    }

    /// Called (once) if the server process exits on its own after it started;
    /// upstream's in-process server cannot outlive the AnyCable process, so the
    /// runner stops with this error.
    pub fn on_exit(&self, handler: impl Fn(EnatsError) + Send + Sync + 'static) {
        *lock(&self.on_exit) = Some(Box::new(handler));
    }

    /// The server name: the configured one, or the service address with `:`
    /// and `/` replaced plus a random suffix (upstream `serverName`).
    pub fn server_name(&self) -> String {
        let mut name = lock(&self.name);
        if name.is_empty() {
            *name = if self.config.name.is_empty() {
                format!("{}-{}", self.config.service_addr.replace(':', "-").replace('/', ""), random_suffix(3))
            } else {
                self.config.name.clone()
            };
        }
        name.clone()
    }

    /// The nats-server configuration equivalent to upstream's `server.Options`.
    pub fn server_config(&self) -> Result<String, EnatsError> {
        let c = &self.config;
        let (host, port) = match parse_host_port(&c.service_addr) {
            Ok((host, Some(port))) => (host, port),
            Ok((_, None)) => {
                return Err(EnatsError("failed to parse NATS server URL, can not fetch port".into()));
            }
            Err(e) => return Err(EnatsError(format!("error parsing NATS service addr, cause: {e}"))),
        };
        let name = self.server_name();
        let mut out = String::new();
        out.push_str(&format!("server_name: {}\n", quote(&name)));
        out.push_str(&format!("host: {}\nport: {port}\n", quote(&host)));
        out.push_str(&format!("debug: {}\ntrace: {}\n", c.debug, c.trace));
        if c.max_payload > 0 {
            out.push_str(&format!("max_payload: {}\n", c.max_payload));
        }
        if c.jetstream {
            let store = if c.store_dir.is_empty() {
                std::env::temp_dir().join("nats-data").join(&name).display().to_string()
            } else {
                c.store_dir.clone()
            };
            out.push_str(&format!("jetstream: {{\n  store_dir: {}\n}}\n", quote(&store)));
        }
        let cluster = if !c.cluster_addr.is_empty() && !c.cluster_name.is_empty() {
            Some(
                address_with_port(&c.cluster_addr, "cluster URL")
                    .map_err(|e| EnatsError(format!("failed to configure NATS cluster, cause: {}", e.0)))?,
            )
        } else {
            None
        };
        if cluster.is_some() || !c.routes.is_empty() {
            out.push_str("cluster: {\n");
            if let Some((h, p)) = &cluster {
                out.push_str(&format!("  name: {}\n  host: {}\n  port: {p}\n", quote(&c.cluster_name), quote(h)));
            }
            if !c.routes.is_empty() {
                out.push_str(&format!("  routes: {}\n", quote_list(c.routes.iter().map(String::as_str))));
            }
            out.push_str("}\n");
        }
        if !c.gateway_addr.is_empty() && !c.cluster_name.is_empty() {
            let (h, p) = address_with_port(&c.gateway_addr, "gateway URL")
                .map_err(|e| EnatsError(format!("failed to configure NATS gateway, cause: {}", e.0)))?;
            out.push_str(&format!(
                "gateway: {{\n  name: {}\n  host: {}\n  port: {p}\n",
                quote(&c.cluster_name),
                quote(&h)
            ));
            if !c.gateway_advertise.is_empty() {
                out.push_str(&format!("  advertise: {}\n", quote(&c.gateway_advertise)));
            }
            if !c.gateways.is_empty() {
                out.push_str("  gateways: [\n");
                for g in &c.gateways {
                    let (gname, urls) = g.split_once(':').ok_or_else(|| {
                        EnatsError(format!("failed to configure NATS gateway, cause: gateway has unknown format: {g}"))
                    })?;
                    out.push_str(&format!(
                        "    {{ name: {}, urls: {} }}\n",
                        quote(gname),
                        quote_list(urls.split(',').map(str::trim))
                    ));
                }
                out.push_str("  ]\n");
            }
            out.push_str("}\n");
        }
        Ok(out)
    }

    /// Starts the server and waits until it accepts connections (upstream
    /// `Start` + `WaitReady`). JetStream readiness is not awaited, as upstream
    /// (see [`Service::wait_jetstream_ready`]).
    pub fn start(&self) -> Result<(), EnatsError> {
        self.start_with(&server_binary())
    }

    /// [`Service::start`] with an explicit `nats-server` executable.
    pub fn start_with(&self, bin: &Path) -> Result<(), EnatsError> {
        let conf = self.server_config()?;
        let (host, port) = match parse_host_port(&self.config.service_addr)? {
            (h, Some(p)) => (h, p),
            (_, None) => return Err(EnatsError("failed to parse NATS server URL, can not fetch port".into())),
        };
        let conf_path = std::env::temp_dir().join(format!("cabby-enats-{}-{}.conf", std::process::id(), port));
        std::fs::write(&conf_path, conf)
            .map_err(|e| EnatsError(format!("failed to start NATS server, cause: writing {conf_path:?}: {e}")))?;

        let mut child = Command::new(bin)
            .arg("-c")
            .arg(&conf_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| EnatsError(format!("failed to start NATS server, cause: running {bin:?}: {e}")))?;

        // Drain the output continuously; it is echoed to stderr in debug mode
        // (upstream routes server logs to its logger only when debugging).
        let output = Arc::new(Mutex::new(OutputTail::default()));
        let echo = self.config.debug || self.config.trace;
        let streams: [Box<dyn Read + Send>; 2] = [
            Box::new(child.stdout.take().expect("piped stdout")),
            Box::new(child.stderr.take().expect("piped stderr")),
        ];
        for stream in streams {
            let sink = Arc::clone(&output);
            std::thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    if echo {
                        let _ = writeln!(std::io::stderr(), "{line}");
                    }
                    lock(&sink).push_line(line.as_bytes());
                }
            });
        }

        *lock(&self.running) = Some(Running { child, output, conf_path, host, port });
        self.wait_ready()?;
        self.watch();
        Ok(())
    }

    /// Supervises the started server: if it exits before [`Service::shutdown`],
    /// the exit is logged and reported to the [`Service::on_exit`] handler.
    fn watch(&self) {
        let (running, on_exit) = (Arc::clone(&self.running), Arc::clone(&self.on_exit));
        let spawned = std::thread::Builder::new().name("cabby-enats-watch".into()).spawn(move || {
            loop {
                std::thread::sleep(WATCH_INTERVAL);
                let mut guard = lock(&running);
                let Some(r) = guard.as_mut() else { return };
                let Ok(Some(status)) = r.child.try_wait() else { continue };
                let Some(r) = guard.take() else { return };
                drop(guard);
                let _ = std::fs::remove_file(&r.conf_path);
                let output = lock(&r.output).text(EXIT_OUTPUT_LIMIT);
                let err = EnatsError(format!("embedded NATS server exited unexpectedly: {status}"));
                crate::metrics::log(
                    Level::Error,
                    "embedded NATS server exited unexpectedly",
                    &[
                        Attr::new("context", LogValue::String("enats".into())),
                        Attr::new("status", LogValue::String(status.to_string())),
                        Attr::new("output", LogValue::String(output)),
                    ],
                );
                if let Some(handler) = lock(&on_exit).as_ref() {
                    handler(err);
                }
                return;
            }
        });
        drop(spawned);
    }

    /// Waits until the server accepts connections (upstream `WaitReady`).
    pub fn wait_ready(&self) -> Result<(), EnatsError> {
        let deadline = Instant::now() + SERVER_START_TIMEOUT;
        loop {
            {
                let mut running = lock(&self.running);
                let Some(r) = running.as_mut() else {
                    return Err(EnatsError("NATS server is not started".into()));
                };
                if let Ok(Some(status)) = r.child.try_wait() {
                    let out = lock(&r.output).text(OUTPUT_LIMIT);
                    return Err(EnatsError(format!("failed to start NATS server: exited with {status}\n{out}")));
                }
                let addr = (r.host.as_str(), r.port).to_socket_addrs().ok().and_then(|mut a| a.next());
                if let Some(addr) = addr
                    && TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
                {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(EnatsError(format!(
                    "failed to start NATS server within {} seconds",
                    SERVER_START_TIMEOUT.as_secs()
                )));
            }
            std::thread::sleep(PROBE_INTERVAL);
        }
    }

    /// Waits up to `max_seconds` for JetStream to answer API requests
    /// (upstream `WaitJetStreamReady`); a no-op without JetStream.
    pub fn wait_jetstream_ready(&self, max_seconds: i64) -> Result<(), EnatsError> {
        if !self.config.jetstream {
            return Ok(());
        }
        let url = {
            let running = lock(&self.running);
            let r = running.as_ref().ok_or_else(|| EnatsError("NATS server is not started".into()))?;
            format!("nats://{}:{}", r.host, r.port)
        };
        let max = Duration::from_secs(u64::try_from(max_seconds).unwrap_or(0));
        cabby_core::nats::client::block_on(async move {
            let start = Instant::now();
            loop {
                if start.elapsed() > max {
                    return Err(EnatsError(format!("JetStream is not ready after {} seconds", max.as_secs())));
                }
                if let Ok(client) = async_connect(&url).await {
                    let js = async_nats::jetstream::new(client);
                    if js.query_account().await.is_ok() {
                        return Ok(());
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    /// A one-line summary for logs (upstream `Description`).
    pub fn description(&self) -> String {
        let c = &self.config;
        let mut out = format!("server_name: {}", self.server_name());
        if !c.cluster_addr.is_empty() {
            out.push_str(&format!(", cluster: {}, cluster_name: {}", c.cluster_addr, c.cluster_name));
        }
        if !c.routes.is_empty() {
            out.push_str(&format!(", routes: {}", c.routes.join(",")));
        }
        if !c.gateway_addr.is_empty() {
            out.push_str(&format!(", gateway: {}, gateways: [{}]", c.gateway_addr, c.gateways.join(" ")));
            if !c.gateway_advertise.is_empty() {
                out.push_str(&format!(", gateway_advertise: {}", c.gateway_advertise));
            }
        }
        out
    }

    /// Stops the server gracefully (SIGINT: JetStream is flushed, lame-duck
    /// clients are told), killing it if it does not exit in time.
    pub fn shutdown(&self) -> Result<(), EnatsError> {
        let Some(mut r) = lock(&self.running).take() else {
            return Ok(());
        };
        stop_child(&mut r.child);
        let _ = std::fs::remove_file(&r.conf_path);
        Ok(())
    }
}

async fn async_connect(url: &str) -> Result<async_nats::Client, async_nats::ConnectError> {
    async_nats::ConnectOptions::new().connection_timeout(Duration::from_secs(1)).connect(url).await
}

fn stop_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    #[cfg(unix)]
    {
        let _ = Command::new("kill")
            .arg("-INT")
            .arg(child.id().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        while Instant::now() < deadline {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(PROBE_INTERVAL);
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_addresses() {
        assert_eq!(("127.0.0.1".to_owned(), Some(4222)), parse_host_port("nats://127.0.0.1:4222").unwrap());
        assert_eq!(("localhost".to_owned(), Some(6222)), parse_host_port("localhost:6222").unwrap());
        assert_eq!(("::1".to_owned(), Some(4222)), parse_host_port("nats://[::1]:4222").unwrap());
        assert_eq!(("h".to_owned(), None), parse_host_port("nats://user:pw@h").unwrap());
        assert!(parse_host_port("nats://h:port").is_err());
    }

    #[test]
    fn server_config_mirrors_upstream_options() {
        let config = Config {
            name: "n1".into(),
            service_addr: "nats://127.0.0.1:4242".into(),
            cluster_addr: "nats://127.0.0.1:4342".into(),
            cluster_name: "c1".into(),
            gateway_addr: "nats://127.0.0.1:4442".into(),
            gateways: vec!["c2:nats://127.0.0.1:4443,nats://127.0.0.1:4444".into()],
            routes: vec!["nats://127.0.0.1:4343".into()],
            jetstream: true,
            store_dir: "/tmp/store".into(),
            max_payload: 2048,
            ..Config::new()
        };
        let conf = Service::new(&config).server_config().unwrap();
        for line in [
            "server_name: \"n1\"",
            "host: \"127.0.0.1\"\nport: 4242",
            "max_payload: 2048",
            "store_dir: \"/tmp/store\"",
            "  name: \"c1\"\n  host: \"127.0.0.1\"\n  port: 4342",
            "  routes: [\"nats://127.0.0.1:4343\"]",
            "gateway: {\n  name: \"c1\"\n  host: \"127.0.0.1\"\n  port: 4442",
            "{ name: \"c2\", urls: [\"nats://127.0.0.1:4443\", \"nats://127.0.0.1:4444\"] }",
        ] {
            assert!(conf.contains(line), "{line:?} not in:\n{conf}");
        }

        let mut no_port = Config::new();
        no_port.service_addr = "nats://127.0.0.1".into();
        assert_eq!(
            "failed to parse NATS server URL, can not fetch port",
            Service::new(&no_port).server_config().unwrap_err().0
        );
    }

    #[test]
    fn generated_server_name() {
        let service = Service::new(&Config::new());
        let name = service.server_name();
        assert!(name.starts_with("nats-127.0.0.1-4222-"), "{name}");
        assert_eq!(name, service.server_name());
    }

    /// Runs the real server when the test services binary is available
    /// (`target/test-services/nats-server`, built by the test kit) or
    /// `CABBY_NATS_SERVER` is set.
    #[test]
    fn starts_and_stops_nats_server() {
        let built = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-services/nats-server");
        if std::env::var_os("CABBY_NATS_SERVER").is_none() && !built.is_file() {
            return;
        }
        let bin = std::env::var_os("CABBY_NATS_SERVER").map_or(built, PathBuf::from);
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let store = std::env::temp_dir().join(format!("cabby-enats-test-{port}"));
        let config = Config {
            service_addr: format!("nats://127.0.0.1:{port}"),
            jetstream: true,
            store_dir: store.display().to_string(),
            ..Config::new()
        };
        let service = Service::new(&config);
        service.start_with(&bin).expect("start");
        service.wait_jetstream_ready(10).expect("JetStream ready");
        service.shutdown().expect("shutdown");
        assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
        let _ = std::fs::remove_dir_all(store);
    }

    #[test]
    fn keeps_only_the_tail_of_the_server_output() {
        let mut tail = OutputTail::default();
        let line = vec![b'x'; 1000];
        for _ in 0..1000 {
            tail.push_line(&line);
        }
        tail.push_line(b"last line");
        assert!(tail.buf.len() <= OUTPUT_LIMIT);
        assert_eq!("last line\n", tail.text(100), "the tail starts at a line boundary");
    }

    /// A server that dies after starting is reported (upstream's in-process
    /// server cannot die and leave AnyCable running; the runner exits with the
    /// error). Pins D-044.
    #[test]
    fn deviation_d044_unexpected_server_exit_is_reported() {
        let built = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-services/nats-server");
        if std::env::var_os("CABBY_NATS_SERVER").is_none() && !built.is_file() {
            return;
        }
        let bin = std::env::var_os("CABBY_NATS_SERVER").map_or(built, PathBuf::from);
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let config = Config { service_addr: format!("nats://127.0.0.1:{port}"), ..Config::new() };
        let service = Service::new(&config);
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = Mutex::new(tx);
        service.on_exit(move |e| {
            let _ = lock(&tx).send(e);
        });
        service.start_with(&bin).expect("start");
        let pid = lock(&service.running).as_ref().expect("running").child.id();
        let killed = Command::new("kill").arg("-9").arg(pid.to_string()).status().expect("kill");
        assert!(killed.success());
        let err = rx.recv_timeout(Duration::from_secs(5)).expect("the exit is reported");
        assert!(err.0.starts_with("embedded NATS server exited unexpectedly: "), "{err}");
        assert!(lock(&service.running).is_none());
        service.shutdown().expect("shutdown after the exit is a no-op");
    }
}
