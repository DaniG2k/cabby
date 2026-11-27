//! Embedded NATS server (upstream package `enats`).
//!
//! Upstream links the NATS server (a Go library) into the binary. There is no
//! maintained NATS server in Rust, so Cabby runs the official `nats-server`
//! binary as a supervised child process configured from [`Config`] (see
//! `docs/enats.md` for the strategy and its limits). The binary is found in
//! `CABBY_NATS_SERVER_BIN` (or `CABBY_NATS_SERVER`), next to the Cabby
//! executable, or on `PATH`.

use std::io::{BufRead, BufReader, Read};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use cabby_core::logger::{Level, Logger};
use cabby_core::toml_config::{string_list, toml_quote};
use cabby_core::{log_at, log_debug, log_error};

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
                out.push_str(&format!("{key} = {}\n", toml_quote(value)));
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
        out.push_str(&format!("name = {}\n", toml_quote(&self.name)));
        out.push_str("#\n# Service address\n");
        out.push_str(&format!("service_addr = {}\n", toml_quote(&self.service_addr)));
        out.push_str("#\n# Cluster configuration\n#\n");
        optional(&mut out, "cluster_addr", &self.cluster_addr);
        if self.cluster_name.is_empty() {
            out.push_str("# cluster_name = \"\"\n");
        } else if self.cluster_addr.is_empty() {
            out.push_str(&format!("# cluster_name = {}\n", toml_quote(&self.cluster_name)));
        } else {
            out.push_str(&format!("cluster_name = {}\n", toml_quote(&self.cluster_name)));
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

/// The host this process connects to for a server listening on `host`: an
/// empty host listens on every interface (nats-server's default `0.0.0.0`), so
/// the server is reached on the IPv4 loopback address.
fn connect_host(host: &str) -> &str {
    if host.is_empty() { "127.0.0.1" } else { host }
}

/// `host:port`, with an IPv6 host in brackets (Go `net.JoinHostPort`).
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// The client URL of a server listening on `host` and `port`.
fn client_url(host: &str, port: u16) -> String {
    format!("nats://{}", join_host_port(connect_host(host), port))
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

/// Creates `dir` (and any missing parents) and makes it private: on Unix it
/// must be a directory (not a symlink) owned by `owner`, and gets mode 0700,
/// set through an open handle that is checked to be that directory. Errors
/// name `dir`.
fn create_private_dir(dir: &Path, owner: u32) -> std::io::Result<()> {
    let named = |e: std::io::Error| std::io::Error::new(e.kind(), format!("{}: {e}", dir.display()));
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir).map_err(named)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let refuse = |why: String| std::io::Error::other(format!("{}: {why}", dir.display()));
        let link = std::fs::symlink_metadata(dir).map_err(named)?;
        if !link.file_type().is_dir() {
            return Err(refuse("not a directory (a symlink or another file)".into()));
        }
        if link.uid() != owner {
            return Err(refuse(format!(
                "owned by uid {}, not by this user (uid {owner}); remove it or set --enats_store_dir",
                link.uid()
            )));
        }
        let handle = std::fs::File::open(dir).map_err(named)?;
        let opened = handle.metadata().map_err(named)?;
        if (opened.dev(), opened.ino()) != (link.dev(), link.ino()) {
            return Err(refuse("replaced while it was being opened".into()));
        }
        handle.set_permissions(std::fs::Permissions::from_mode(0o700)).map_err(named)?;
    }
    #[cfg(not(unix))]
    let _ = owner;
    Ok(())
}

/// Checks the shared parent of the default store dir (`<tmp>/nats-data`),
/// which another user may have created first: on Unix it must be a directory
/// (not a symlink) owned by `owner` or root, and either not writable by group
/// and others, or sticky (as `/tmp` is), so no other user can replace the
/// store dir inside it.
fn check_shared_parent(parent: &Path, owner: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let refuse = |why: String| {
            std::io::Error::other(format!("{}: {why}; remove it or set --enats_store_dir", parent.display()))
        };
        let meta = std::fs::symlink_metadata(parent)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{}: {e}", parent.display())))?;
        if !meta.file_type().is_dir() {
            return Err(refuse("not a directory (a symlink or another file)".into()));
        }
        if meta.uid() != owner && meta.uid() != 0 {
            return Err(refuse(format!("owned by uid {}, not by this user (uid {owner}) or root", meta.uid())));
        }
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            return Err(refuse(format!("writable by other users and not sticky (mode {:o})", mode & 0o777)));
        }
    }
    #[cfg(not(unix))]
    let _ = (parent, owner);
    Ok(())
}

/// Creates `path` as a new file (mode 0600 on Unix, never through an existing
/// path or symlink) and fills it with `fill`; removes it again if that fails.
/// Errors name the path.
fn create_private_file(
    path: &Path,
    fill: impl FnOnce(&mut std::fs::File) -> std::io::Result<()>,
) -> std::io::Result<std::fs::File> {
    let named = |e: std::io::Error| std::io::Error::new(e.kind(), format!("{}: {e}", path.display()));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path).map_err(named)?;
    if let Err(e) = fill(&mut file) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(named(e));
    }
    Ok(file)
}

/// Writes the `nats-server` config to a new private file with a random
/// suffix in the temp dir; returns its path and its owner (this process's
/// user, on Unix).
fn write_private_config(port: u16, conf: &str) -> std::io::Result<(PathBuf, u32)> {
    use std::io::Write as _;

    let path =
        std::env::temp_dir().join(format!("cabby-enats-{}-{}-{}.conf", std::process::id(), port, random_suffix(12)));
    let file = create_private_file(&path, |f| f.write_all(conf.as_bytes()))?;
    #[cfg(unix)]
    let owner = {
        use std::os::unix::fs::MetadataExt;
        file.metadata().map(|m| m.uid())
    };
    #[cfg(not(unix))]
    let owner: std::io::Result<u32> = {
        drop(file);
        Ok(0)
    };
    match owner {
        Ok(owner) => Ok((path, owner)),
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            Err(std::io::Error::new(e.kind(), format!("{}: {e}", path.display())))
        }
    }
}

/// The variables naming the `nats-server` executable, in precedence order:
/// `CABBY_NATS_SERVER_BIN` and its older spelling `CABBY_NATS_SERVER`. They are
/// the only server variables that aren't flags (D-061).
pub const SERVER_BINARY_ENV: [&str; 2] = ["CABBY_NATS_SERVER_BIN", "CABBY_NATS_SERVER"];

/// The `nats-server` executable: [`SERVER_BINARY_ENV`], a `nats-server` next to
/// the running executable, or `nats-server` on `PATH`.
pub fn server_binary() -> PathBuf {
    if let Some(p) = configured_server_binary(|k| std::env::var_os(k)) {
        return p;
    }
    if let Ok(exe) = std::env::current_exe() {
        let bundled = exe.with_file_name(format!("nats-server{}", std::env::consts::EXE_SUFFIX));
        if bundled.is_file() {
            return bundled;
        }
    }
    PathBuf::from("nats-server")
}

/// The first non-empty [`SERVER_BINARY_ENV`] variable, as a path.
fn configured_server_binary(get: impl Fn(&str) -> Option<std::ffi::OsString>) -> Option<PathBuf> {
    SERVER_BINARY_ENV.iter().find_map(|k| get(k).filter(|p| !p.is_empty())).map(PathBuf::from)
}

struct Running {
    child: Child,
    /// Stops the server if this process ends without stopping it (see
    /// [`spawn_watchdog`]); `None` when it could not be started.
    watchdog: Option<Child>,
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
    log: Logger,
    name: Mutex<String>,
    running: Arc<Mutex<Option<Running>>>,
    on_exit: Arc<Mutex<Option<ExitHandler>>>,
}

impl Service {
    /// Upstream `NewService(config)`.
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
            log: Logger::default().with("context", "enats"),
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

    /// The default JetStream store directory (`nats-data/<server name>` in the
    /// temp dir), when JetStream is on without `store_dir`.
    fn default_store_dir(&self) -> Option<PathBuf> {
        (self.config.jetstream && self.config.store_dir.is_empty())
            .then(|| std::env::temp_dir().join("nats-data").join(self.server_name()))
    }

    /// The nats-server configuration equivalent to upstream's `server.Options`.
    pub fn server_config(&self) -> Result<String, EnatsError> {
        self.config_text(self.default_store_dir().as_deref())
    }

    /// [`Service::server_config`] with the default store directory, if any.
    fn config_text(&self, default_store: Option<&Path>) -> Result<String, EnatsError> {
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
            let store = default_store.map_or_else(|| c.store_dir.clone(), |p| p.display().to_string());
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
        let default_store = self.default_store_dir();
        let conf = self.config_text(default_store.as_deref())?;
        let (host, port) = match parse_host_port(&self.config.service_addr)? {
            (h, Some(p)) => (h, p),
            (_, None) => return Err(EnatsError("failed to parse NATS server URL, can not fetch port".into())),
        };

        let (conf_path, owner) = write_private_config(port, &conf)
            .map_err(|e| EnatsError(format!("failed to start NATS server, cause: writing config: {e}")))?;
        if let Some(store) = &default_store
            && let Err(e) = create_private_dir(store, owner)
                .and_then(|()| store.parent().map_or(Ok(()), |parent| check_shared_parent(parent, owner)))
        {
            let _ = std::fs::remove_file(&conf_path);
            return Err(EnatsError(format!("failed to start NATS server, cause: creating store dir: {e}")));
        }

        let mut command = Command::new(bin);
        command.arg("-c").arg(&conf_path).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        own_process_group(&mut command);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let _ = std::fs::remove_file(&conf_path);
                return Err(EnatsError(format!("failed to start NATS server, cause: running {bin:?}: {e}")));
            }
        };

        // Drain the output continuously. In debug mode its records go to the
        // log (upstream sets the server's logger to `log.With("service",
        // "nats")` only when debugging).
        let output = Arc::new(Mutex::new(OutputTail::default()));
        let echo = self.config.debug.then(|| self.log.with("service", "nats"));
        let streams: [Box<dyn Read + Send>; 2] = [
            Box::new(child.stdout.take().expect("piped stdout")),
            Box::new(child.stderr.take().expect("piped stderr")),
        ];
        for stream in streams {
            let sink = Arc::clone(&output);
            let echo = echo.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    if let Some(log) = &echo {
                        let (level, msg) = server_log_record(&line);
                        log_at!(log, level, msg);
                    }
                    lock(&sink).push_line(line.as_bytes());
                }
            });
        }

        let url = client_url(&host, port);
        let watchdog = spawn_watchdog(child.id(), &conf_path);
        if watchdog.is_none() {
            log_debug!(
                self.log,
                "cannot start the embedded NATS server watchdog; the server outlives a killed process"
            );
        }
        *lock(&self.running) = Some(Running { child, watchdog, output, conf_path, host, port });
        self.wait_ready()?;
        self.watch();
        // Upstream waits for JetStream in the background (`go WaitJetStreamReady`),
        // only logging the outcome.
        if self.config.jetstream {
            let max = Duration::from_secs(u64::try_from(self.config.jetstream_ready_timeout).unwrap_or(0));
            let (leader, log) = (self.is_jetstream_leader(), self.log.clone());
            let _ = std::thread::Builder::new()
                .name("cabby-enats-jetstream".into())
                .spawn(move || wait_jetstream(url, max, leader, log));
        }
        Ok(())
    }

    /// Supervises the started server: if it exits before [`Service::shutdown`],
    /// the exit is logged and reported to the [`Service::on_exit`] handler.
    fn watch(&self) {
        let (running, on_exit) = (Arc::clone(&self.running), Arc::clone(&self.on_exit));
        let log = self.log.clone();
        let spawned = std::thread::Builder::new().name("cabby-enats-watch".into()).spawn(move || {
            loop {
                std::thread::sleep(WATCH_INTERVAL);
                let mut guard = lock(&running);
                let Some(r) = guard.as_mut() else { return };
                let Ok(Some(status)) = r.child.try_wait() else { continue };
                let Some(mut r) = guard.take() else { return };
                drop(guard);
                // First, so that it never signals the server's pid once reused.
                stop_watchdog(&mut r.watchdog);
                let _ = std::fs::remove_file(&r.conf_path);
                let output = lock(&r.output).text(EXIT_OUTPUT_LIMIT);
                let err = EnatsError(format!("embedded NATS server exited unexpectedly: {status}"));
                log_error!(log, "embedded NATS server exited unexpectedly", "status" => status.to_string(), "output" => output);
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
                let addr = (connect_host(&r.host), r.port).to_socket_addrs().ok().and_then(|mut a| a.next());
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
            client_url(&r.host, r.port)
        };
        let max = Duration::from_secs(u64::try_from(max_seconds).unwrap_or(0));
        wait_jetstream(url, max, self.is_jetstream_leader(), self.log.clone())
    }

    /// Upstream's `JetStreamIsLeader()` is true for a server outside a cluster;
    /// a clustered child does not say whether it leads the meta group (false).
    fn is_jetstream_leader(&self) -> bool {
        self.config.cluster_addr.is_empty() || self.config.cluster_name.is_empty()
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
        // Before the server: once the server is reaped, its pid may be reused.
        stop_watchdog(&mut r.watchdog);
        stop_child(&mut r.child);
        let _ = std::fs::remove_file(&r.conf_path);
        Ok(())
    }
}

/// A `nats-server` output line (`[pid] date time [LVL] message`) as upstream's
/// `LogEntry` logs it: `Noticef` and `Infof` at info, `Tracef` and `Debugf` at
/// debug, `Warnf` at warn, `Errorf` (and `Fatalf`) at error. Lines in another
/// format are logged whole at info.
fn server_log_record(line: &str) -> (Level, &str) {
    let tagged = (|| {
        let rest = line.strip_prefix('[')?.split_once("] ")?.1;
        let rest = rest.split_once(' ')?.1.split_once(' ')?.1;
        let (tag, msg) = rest.strip_prefix('[')?.split_once("] ")?;
        let level = match tag {
            "INF" => Level::Info,
            "DBG" | "TRC" => Level::Debug,
            "WRN" => Level::Warn,
            "ERR" | "FTL" => Level::Error,
            _ => return None,
        };
        Some((level, msg))
    })();
    tagged.unwrap_or((Level::Info, line))
}

/// Upstream `WaitJetStreamReady`'s loop: waits up to `max` for JetStream to
/// answer API requests at `url`.
fn wait_jetstream(url: String, max: Duration, leader: bool, log: Logger) -> Result<(), EnatsError> {
    cabby_core::nats::client::block_on(async move {
        let start = Instant::now();
        loop {
            if start.elapsed() > max {
                return Err(EnatsError(format!("JetStream is not ready after {} seconds", max.as_secs())));
            }
            match async_connect(&url).await {
                Err(e) => {
                    log_debug!(log, "NATS server not accepting connections", "error" => e.to_string());
                    // Upstream retries at once (in-process connections fail fast).
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Ok(client) => {
                    let js = async_nats::jetstream::new(client);
                    if js.query_account().await.is_ok() {
                        log_debug!(log, "JetStream cluster is ready", "leader" => leader);
                        return Ok(());
                    }
                }
            }
            log_debug!(log, "JetStream cluster is not ready yet, waiting for 1 second...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

async fn async_connect(url: &str) -> Result<async_nats::Client, async_nats::ConnectError> {
    async_nats::ConnectOptions::new().connection_timeout(Duration::from_secs(1)).connect(url).await
}

/// Starts a process that stops the server (SIGINT, as [`Service::shutdown`]
/// does) when this process ends without stopping it, for example when it is
/// killed with SIGKILL, and removes its configuration file. Upstream's server
/// runs in-process and ends with it.
///
/// `sh` waits for the end of its standard input: a pipe whose other end only
/// this process holds (the standard library opens pipes close-on-exec, so other
/// children do not inherit it), and which the system closes when this process
/// exits. This process never writes to it. `None` when `sh` cannot be started
/// (an image without a shell): the server then keeps running after a SIGKILL.
fn spawn_watchdog(server: u32, conf_path: &Path) -> Option<Child> {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(r#"read -r _; kill -INT "$1" 2>/dev/null; rm -f "$2""#)
        .arg("cabby-enats-watchdog")
        .arg(server.to_string())
        .arg(conf_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    own_process_group(&mut command);
    command.spawn().ok()
}

/// Starts the child in a process group of its own, so that signals sent to
/// this process's group (Ctrl-C in a terminal) reach this process only, as
/// they reach an in-process server (upstream sets `NoSigs`): the server is
/// stopped by [`Service::shutdown`], after the components that use it.
fn own_process_group(command: &mut Command) {
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(command, 0);
    #[cfg(not(unix))]
    let _ = command;
}

/// Ends the watchdog without letting it signal the server.
fn stop_watchdog(watchdog: &mut Option<Child>) {
    if let Some(mut w) = watchdog.take() {
        let _ = w.kill();
        let _ = w.wait();
    }
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
    fn nats_server_bin_is_preferred_over_the_older_name() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| std::ffi::OsString::from(v))
        };
        let both = env(&[("CABBY_NATS_SERVER_BIN", "/a/nats-server"), ("CABBY_NATS_SERVER", "/b/nats-server")]);
        assert_eq!(configured_server_binary(both), Some(PathBuf::from("/a/nats-server")));
        let old = env(&[("CABBY_NATS_SERVER_BIN", ""), ("CABBY_NATS_SERVER", "/b/nats-server")]);
        assert_eq!(configured_server_binary(old), Some(PathBuf::from("/b/nats-server")));
        assert_eq!(configured_server_binary(env(&[])), None);
    }

    #[test]
    fn server_output_maps_to_log_levels() {
        assert_eq!(
            (Level::Info, "Starting nats-server"),
            server_log_record("[123] 2026/11/20 16:55:06.373456 [INF] Starting nats-server")
        );
        assert_eq!((Level::Debug, "x [y] z"), server_log_record("[1] 2026/11/20 16:55:06.373 [TRC] x [y] z"));
        assert_eq!((Level::Warn, "w"), server_log_record("[1] 2026/11/20 16:55:06.373 [WRN] w"));
        assert_eq!((Level::Error, "e"), server_log_record("[1] 2026/11/20 16:55:06.373 [FTL] e"));
        assert_eq!((Level::Info, "plain text"), server_log_record("plain text"));
    }

    #[test]
    fn parses_addresses() {
        assert_eq!(("127.0.0.1".to_owned(), Some(4222)), parse_host_port("nats://127.0.0.1:4222").unwrap());
        assert_eq!(("localhost".to_owned(), Some(6222)), parse_host_port("localhost:6222").unwrap());
        assert_eq!(("::1".to_owned(), Some(4222)), parse_host_port("nats://[::1]:4222").unwrap());
        assert_eq!(("h".to_owned(), None), parse_host_port("nats://user:pw@h").unwrap());
        assert!(parse_host_port("nats://h:port").is_err());
    }

    /// The URL this process connects to (JetStream readiness): an IPv6 host
    /// in brackets, an empty host (every interface) through the loopback
    /// address. Before, `nats://[::1]:4222` was probed as `nats://::1:4222`,
    /// which the client refuses, and an empty host could not be probed at all.
    #[test]
    fn client_urls_join_host_and_port() {
        assert_eq!("nats://127.0.0.1:4222", client_url("127.0.0.1", 4222));
        assert_eq!("nats://[::1]:4222", client_url("::1", 4222));
        assert_eq!("nats://[fe80::1%25en0]:4222", client_url("fe80::1%25en0", 4222));
        assert_eq!("nats://localhost:4222", client_url("localhost", 4222));
        assert_eq!("nats://127.0.0.1:4222", client_url("", 4222));
        for url in ["nats://[::1]:4222", "nats://127.0.0.1:4222"] {
            assert!(url.parse::<async_nats::ServerAddr>().is_ok(), "{url}");
        }
    }

    /// IPv6 addresses in the configuration file: hosts without brackets,
    /// routes and gateway URLs as given.
    #[test]
    fn server_config_with_ipv6_addresses() {
        let config = Config {
            name: "n1".into(),
            service_addr: "nats://[::1]:4242".into(),
            cluster_addr: "nats://[::1]:4342".into(),
            cluster_name: "c1".into(),
            gateway_addr: "nats://[::1]:4442".into(),
            gateways: vec!["c2:nats://[::1]:4443,nats://[::1]:4444".into()],
            routes: vec!["nats://[::1]:4343".into()],
            ..Config::new()
        };
        let conf = Service::new(&config).server_config().unwrap();
        for line in [
            "host: \"::1\"\nport: 4242",
            "  name: \"c1\"\n  host: \"::1\"\n  port: 4342",
            "  routes: [\"nats://[::1]:4343\"]",
            "gateway: {\n  name: \"c1\"\n  host: \"::1\"\n  port: 4442",
            "{ name: \"c2\", urls: [\"nats://[::1]:4443\", \"nats://[::1]:4444\"] }",
        ] {
            assert!(conf.contains(line), "{line:?} not in:\n{conf}");
        }
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

    /// The `nats-server` for tests that run the real server: the configured one
    /// (`CABBY_NATS_SERVER_BIN`, then `CABBY_NATS_SERVER`), else the test services
    /// binary (`target/test-services/nats-server`, built by the test kit), if it exists.
    fn test_server_binary() -> Option<PathBuf> {
        let built = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-services/nats-server");
        configured_server_binary(|k| std::env::var_os(k)).or(Some(built)).filter(|p| p.is_file())
    }

    /// Runs the real server when [`test_server_binary`] finds one.
    #[test]
    fn starts_and_stops_nats_server() {
        let Some(bin) = test_server_binary() else { return };
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

    /// A server on an IPv6 address, and one on every interface (an empty
    /// host), start and report JetStream ready. Before, the second failed to
    /// start ("failed to start NATS server within 5 seconds": the readiness
    /// probe could not resolve the empty host), and JetStream readiness was
    /// never reported for the first.
    #[test]
    fn starts_on_ipv6_and_on_every_interface() {
        let Some(bin) = test_server_binary() else { return };
        let v6 = std::net::TcpListener::bind("[::1]:0").ok().map(|l| ("[::1]", l.local_addr().unwrap().port()));
        let any = Some(("", std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()));
        for (host, port) in [v6, any].into_iter().flatten() {
            let store = std::env::temp_dir().join(format!("cabby-enats-test-{port}"));
            let config = Config {
                service_addr: format!("nats://{host}:{port}"),
                jetstream: true,
                store_dir: store.display().to_string(),
                ..Config::new()
            };
            let service = Service::new(&config);
            service.start_with(&bin).unwrap_or_else(|e| panic!("start on {host:?}: {e}"));
            service.wait_jetstream_ready(10).unwrap_or_else(|e| panic!("JetStream ready on {host:?}: {e}"));
            service.shutdown().expect("shutdown");
            let _ = std::fs::remove_dir_all(store);
        }
    }

    /// When this process ends without stopping the server (its end of the
    /// watchdog's pipe closes, as the system does for a killed process), the
    /// watchdog stops the server, which then frees its port. The test closes
    /// the pipe itself; the server's exit is reported as any other (D-044).
    #[test]
    fn the_server_stops_when_the_watchdog_pipe_closes() {
        let Some(bin) = test_server_binary() else { return };
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let config = Config { service_addr: format!("nats://127.0.0.1:{port}"), ..Config::new() };
        let service = Service::new(&config);
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = Mutex::new(tx);
        service.on_exit(move |e| {
            let _ = lock(&tx).send(e);
        });
        service.start_with(&bin).expect("start");

        let pipe = lock(&service.running).as_mut().and_then(|r| r.watchdog.as_mut()?.stdin.take());
        drop(pipe.expect("the watchdog's pipe"));

        let err = rx.recv_timeout(Duration::from_secs(5)).expect("the server stopped");
        assert!(err.0.starts_with("embedded NATS server exited unexpectedly: "), "{err}");
        assert!(TcpStream::connect(("127.0.0.1", port)).is_err(), "the port is free");
    }

    /// A shutdown stops the watchdog without a signal from it.
    #[test]
    fn a_shutdown_stops_the_watchdog() {
        let Some(bin) = test_server_binary() else { return };
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let config = Config { service_addr: format!("nats://127.0.0.1:{port}"), ..Config::new() };
        let service = Service::new(&config);
        service.start_with(&bin).expect("start");
        let watchdog = lock(&service.running).as_ref().and_then(|r| r.watchdog.as_ref().map(Child::id));
        let watchdog = watchdog.expect("a watchdog");

        service.shutdown().expect("shutdown");

        let alive = Command::new("kill").arg("-0").arg(watchdog.to_string()).stderr(Stdio::null()).status();
        assert!(!alive.expect("kill -0").success(), "the watchdog has ended");
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
        let Some(bin) = test_server_binary() else { return };
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

    #[cfg(unix)]
    fn scratch(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cabby-enats-{tag}-{}-{}", std::process::id(), random_suffix(8)))
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).expect("metadata").permissions().mode() & 0o777
    }

    #[cfg(unix)]
    fn my_uid() -> u32 {
        let (path, owner) = write_private_config(1, "").expect("a config file");
        let _ = std::fs::remove_file(path);
        owner
    }

    #[cfg(unix)]
    #[test]
    fn config_file_and_store_dir_are_private() {
        use std::os::unix::fs::MetadataExt;

        let (path, owner) = write_private_config(4222, "port: 4222\n").expect("write private config");
        assert_eq!(0o600, mode(&path));
        assert_eq!(owner, std::fs::metadata(&path).unwrap().uid());
        assert_eq!("port: 4222\n", std::fs::read_to_string(&path).expect("read"));
        let _ = std::fs::remove_file(&path);

        let dir = scratch("dir").join("store");
        create_private_dir(&dir, owner).expect("create private dir");
        assert_eq!(0o700, mode(&dir));

        // An existing directory of this user is made private.
        std::fs::set_permissions(&dir, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        create_private_dir(&dir, owner).expect("an existing directory");
        assert_eq!(0o700, mode(&dir));
        let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_config_write_removes_the_file_and_names_it() {
        let path = scratch("failed-write");
        let err = create_private_file(&path, |_| Err(std::io::Error::other("disk full"))).expect_err("the fill fails");
        assert!(!path.exists(), "the file is removed");
        assert_eq!(format!("{}: disk full", path.display()), err.to_string());

        // An existing path is never reused.
        std::fs::write(&path, "x").unwrap();
        let err = create_private_file(&path, |_| Ok(())).expect_err("the path exists");
        assert!(err.to_string().starts_with(&format!("{}: ", path.display())), "{err}");
        assert_eq!("x", std::fs::read_to_string(&path).unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_store_dir_is_refused_and_its_target_left_alone() {
        use std::os::unix::fs::PermissionsExt;

        let base = scratch("symlink");
        let target = base.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        let link = base.join("store");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = create_private_dir(&link, my_uid()).expect_err("a symlink");
        assert!(err.to_string().contains("not a directory"), "{err}");
        assert_eq!(0o755, mode(&target), "the target keeps its mode");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn a_store_dir_owned_by_another_user_is_refused() {
        let dir = scratch("owner");
        std::fs::create_dir_all(&dir).unwrap();
        let other = my_uid().wrapping_add(1);

        let err = create_private_dir(&dir, other).expect_err("owned by someone else");

        let text = err.to_string();
        assert!(text.starts_with(&format!("{}: owned by uid ", dir.display())), "{text}");
        assert!(
            text.ends_with(&format!("not by this user (uid {other}); remove it or set --enats_store_dir")),
            "{text}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn store_dir_errors_name_the_directory() {
        let base = scratch("named");
        std::fs::create_dir_all(&base).unwrap();
        let file = base.join("a-file");
        std::fs::write(&file, "x").unwrap();
        let dir = file.join("store");

        let err = create_private_dir(&dir, my_uid()).expect_err("a file is in the way");

        assert!(err.to_string().starts_with(&format!("{}: ", dir.display())), "{err}");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn the_shared_parent_of_the_store_dir_is_checked() {
        use std::os::unix::fs::PermissionsExt;

        let me = my_uid();
        let parent = scratch("parent");
        std::fs::create_dir_all(&parent).unwrap();
        let set_mode = |mode: u32| std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(mode)).unwrap();

        set_mode(0o700);
        check_shared_parent(&parent, me).expect("private and ours");
        set_mode(0o755);
        check_shared_parent(&parent, me).expect("ours, writable only by us");
        set_mode(0o1777);
        check_shared_parent(&parent, me).expect("sticky, like /tmp");

        set_mode(0o775);
        let err = check_shared_parent(&parent, me).expect_err("group-writable");
        assert_eq!(
            format!(
                "{}: writable by other users and not sticky (mode 775); remove it or set --enats_store_dir",
                parent.display()
            ),
            err.to_string()
        );
        set_mode(0o777);
        assert!(check_shared_parent(&parent, me).is_err(), "world-writable");

        set_mode(0o700);
        let other = me.wrapping_add(1);
        let err = check_shared_parent(&parent, other).expect_err("owned by someone else");
        assert!(err.to_string().starts_with(&format!("{}: owned by uid {me}, ", parent.display())), "{err}");

        let link = scratch("parent-link");
        std::os::unix::fs::symlink(&parent, &link).unwrap();
        let err = check_shared_parent(&link, me).expect_err("a symlink");
        assert!(err.to_string().contains("not a directory"), "{err}");
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_dir_all(&parent);
    }
}
