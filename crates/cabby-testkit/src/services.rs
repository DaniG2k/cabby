//! Required external services (Redis, NATS) for translated integration tests.
//!
//! `docs/translating-tests.md` ("Hard rules"): real Redis/NATS cases stay real and there is no silent skip.
//! Upstream Go tests skip when Redis is absent; here a missing or unreachable
//! service panics with a message naming the variable to set:
//!
//! * `CABBY_TEST_REDIS_URL`, e.g. `redis://127.0.0.1:6379/`;
//! * `CABBY_TEST_NATS_URL`, e.g. `nats://127.0.0.1:4222`, and
//!   `CABBY_TEST_NATS_MONITOR_URL` (the server's `-m` HTTP monitoring endpoint, e.g.
//!   `http://127.0.0.1:8222`), which is how subscription readiness is observed.
//!
//! Every test works in its own scope: unique key/channel/subject prefixes, and
//! for Redis a dedicated ACL user, so that connection-level operations (killing
//! pub/sub connections, counting stream readers) only touch that test's clients.
//! Tests that restart NATS get a private `nats-server` process ([`NatsServer`]).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;

use crate::ports::free_port;
use crate::process::Supervised;

const IO_DEADLINE: Duration = Duration::from_secs(5);
/// Interval between readiness probes of an external service. Readiness is
/// observed (Redis/NATS server state), and every wait has a deadline.
const PROBE_INTERVAL: Duration = Duration::from_millis(20);

/// A name unique to this test run and call (`<tag>_<pid>_<n>_<time>`), usable in
/// Redis keys, channels, ACL user names and NATS subjects.
pub fn unique(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() % 1_000_000_000).unwrap_or(0);
    format!("{tag}_{}_{}_{nanos}", std::process::id(), N.fetch_add(1, Ordering::Relaxed))
}

fn required_env(name: &str, what: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_owned(),
        _ => panic!(
            "{name} is not set: this test needs a real {what} (no silent skips). \
             Start one and export {name}, see crates/cabby-testkit/src/services.rs"
        ),
    }
}

/// Polls `probe` until it returns `Some`, or fails after `timeout`.
pub fn wait_until<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return Ok(v);
        }
        if Instant::now() >= deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        std::thread::sleep(PROBE_INTERVAL);
    }
}

// ---------------------------------------------------------------- Redis ---

/// A RESP2 reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Resp::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<String> {
        match self {
            Resp::Simple(s) => Some(s.clone()),
            Resp::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        }
    }

    pub fn items(&self) -> &[Resp] {
        match self {
            Resp::Array(Some(v)) => v,
            _ => &[],
        }
    }
}

/// Minimal synchronous Redis connection for test arrangement and observation
/// (publishing, stream entries, server-side client/subscription state).
pub struct RespConn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RespConn {
    pub fn connect(addr: SocketAddr, auth: Option<(&str, &str)>, db: i64) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, IO_DEADLINE).with_context(|| format!("connecting to {addr}"))?;
        stream.set_read_timeout(Some(IO_DEADLINE))?;
        stream.set_write_timeout(Some(IO_DEADLINE))?;
        let mut conn = Self { reader: BufReader::new(stream.try_clone()?), writer: stream };
        if let Some((user, pass)) = auth {
            let args: Vec<&str> = if user.is_empty() { vec!["AUTH", pass] } else { vec!["AUTH", user, pass] };
            conn.ok(&args)?;
        }
        if db != 0 {
            conn.ok(&["SELECT", &db.to_string()])?;
        }
        Ok(conn)
    }

    pub fn command(&mut self, args: &[&str]) -> Result<Resp> {
        let mut req = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            req.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            req.extend_from_slice(a.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&req)?;
        self.read_reply()
    }

    /// Runs a command and fails on a Redis error reply.
    pub fn ok(&mut self, args: &[&str]) -> Result<Resp> {
        match self.command(args)? {
            Resp::Error(e) => bail!("redis {args:?}: {e}"),
            r => Ok(r),
        }
    }

    fn read_line(&mut self) -> Result<String> {
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            bail!("redis closed the connection");
        }
        Ok(line.trim_end_matches("\r\n").to_owned())
    }

    fn read_reply(&mut self) -> Result<Resp> {
        let line = self.read_line()?;
        let (kind, rest) = line.split_at(1);
        Ok(match kind {
            "+" => Resp::Simple(rest.to_owned()),
            "-" => Resp::Error(rest.to_owned()),
            ":" => Resp::Int(rest.parse()?),
            "$" => {
                let n: i64 = rest.parse()?;
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    let mut buf = vec![0; n as usize + 2];
                    self.reader.read_exact(&mut buf)?;
                    buf.truncate(n as usize);
                    Resp::Bulk(Some(buf))
                }
            }
            "*" => {
                let n: i64 = rest.parse()?;
                if n < 0 {
                    Resp::Array(None)
                } else {
                    Resp::Array(Some((0..n).map(|_| self.read_reply()).collect::<Result<_>>()?))
                }
            }
            other => bail!("unexpected RESP type {other:?} in {line:?}"),
        })
    }
}

/// The Redis server named by `CABBY_TEST_REDIS_URL`.
pub struct RedisService {
    addr: SocketAddr,
    host_port: String,
    auth: Option<(String, String)>,
    db: i64,
}

/// The shared Redis service. Panics if `CABBY_TEST_REDIS_URL` is unset, malformed, or
/// the server does not answer `PING`.
pub fn redis() -> &'static RedisService {
    static R: OnceLock<RedisService> = OnceLock::new();
    R.get_or_init(|| {
        let url = required_env("CABBY_TEST_REDIS_URL", "Redis server");
        let svc = RedisService::parse(&url).unwrap_or_else(|e| panic!("CABBY_TEST_REDIS_URL={url:?}: {e:#}"));
        let pong = svc
            .admin()
            .and_then(|mut c| c.ok(&["PING"]))
            .unwrap_or_else(|e| panic!("Redis at CABBY_TEST_REDIS_URL={url:?} is unreachable: {e:#}"));
        assert_eq!(pong, Resp::Simple("PONG".into()), "unexpected PING reply from {url}");
        svc
    })
}

impl RedisService {
    fn parse(url: &str) -> Result<Self> {
        let rest = url.strip_prefix("redis://").ok_or_else(|| anyhow!("only redis:// URLs are supported"))?;
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let (auth, host_port) = match authority.rsplit_once('@') {
            Some((cred, hp)) => {
                let (u, p) = cred.split_once(':').unwrap_or(("", cred));
                (Some((u.to_owned(), p.to_owned())), hp)
            }
            None => (None, authority),
        };
        let host_port = if host_port.contains(':') { host_port.to_owned() } else { format!("{host_port}:6379") };
        let addr = host_port.to_socket_addrs()?.next().ok_or_else(|| anyhow!("cannot resolve {host_port}"))?;
        let db = if path.is_empty() { 0 } else { path.parse().context("database number")? };
        Ok(Self { addr, host_port, auth, db })
    }

    fn admin(&self) -> Result<RespConn> {
        RespConn::connect(self.addr, self.auth.as_ref().map(|(u, p)| (u.as_str(), p.as_str())), self.db)
    }

    /// A fresh per-test scope with its own ACL user and key prefix.
    /// The Redis server's address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn scope(&'static self, tag: &str) -> RedisScope {
        let admin = self.admin().unwrap_or_else(|e| panic!("connecting to Redis: {e:#}"));
        let mut scope = RedisScope {
            prefix: unique(tag),
            users: Vec::new(),
            admin: Mutex::new(Some(admin)),
            svc: self,
            await_delivery: None,
        };
        let user = scope.create_user("main");
        scope.users.push(user);
        scope
    }
}

/// One test's slice of the shared Redis: unique names and dedicated ACL users.
/// Dropping it kills the users' connections, deletes the users and the keys
/// under [`RedisScope::prefix`].
pub struct RedisScope {
    pub prefix: String,
    users: Vec<String>,
    admin: Mutex<Option<RespConn>>,
    svc: &'static RedisService,
    /// See [`RedisScope::await_stream_delivery`].
    await_delivery: Option<Duration>,
}

impl RedisScope {
    /// `prefix` + `name`: a key, channel or stream name private to this test.
    pub fn name(&self, name: &str) -> String {
        format!("{}:{name}", self.prefix)
    }

    fn create_user(&self, suffix: &str) -> String {
        let user = format!("{}_{suffix}", self.prefix);
        let pass = format!("pw_{user}");
        let pw = format!(">{pass}");
        self.command(&["ACL", "SETUSER", &user, "reset", "on", &pw, "~*", "&*", "+@all"])
            .unwrap_or_else(|e| panic!("creating Redis ACL user {user}: {e:#}"));
        user
    }

    /// Main test user name (clients connecting with [`RedisScope::url`]).
    pub fn user(&self) -> &str {
        &self.users[0]
    }

    /// Redis URL that authenticates as the main test user.
    pub fn url(&self) -> String {
        self.url_for(&self.users[0].clone())
    }

    /// Creates another ACL user (e.g. one per component whose connections are
    /// killed independently) and returns `(user, url)`.
    pub fn extra_user(&mut self, suffix: &str) -> (String, String) {
        let user = self.create_user(suffix);
        self.users.push(user.clone());
        let url = self.url_for(&user);
        (user, url)
    }

    /// Like [`RedisScope::url`], but connecting through `via` (e.g. a fault proxy
    /// that forwards to [`RedisService::addr`]).
    pub fn url_via(&self, via: SocketAddr) -> String {
        let user = &self.users[0];
        let db = if self.svc.db == 0 { String::new() } else { self.svc.db.to_string() };
        format!("redis://{user}:pw_{user}@{via}/{db}")
    }

    fn url_for(&self, user: &str) -> String {
        let db = if self.svc.db == 0 { String::new() } else { self.svc.db.to_string() };
        format!("redis://{user}:pw_{user}@{}/{db}", self.svc.host_port)
    }

    pub fn command(&self, args: &[&str]) -> Result<Resp> {
        let mut slot = self.admin.lock().unwrap_or_else(|e| e.into_inner());
        let conn = slot.as_mut().ok_or_else(|| anyhow!("redis scope closed"))?;
        conn.ok(args)
    }

    /// `PUBSUB NUMSUB channel`.
    pub fn numsub(&self, channel: &str) -> i64 {
        let r = self.command(&["PUBSUB", "NUMSUB", channel]).expect("PUBSUB NUMSUB");
        r.items().get(1).and_then(Resp::as_int).unwrap_or(0)
    }

    /// Waits until exactly `count` connections are subscribed to `channel`.
    pub fn wait_numsub(&self, channel: &str, count: i64, timeout: Duration) -> Result<()> {
        wait_until(&format!("{count} Redis subscriber(s) on {channel}"), timeout, || {
            (self.numsub(channel) == count).then_some(())
        })
    }

    /// Whether `PUBSUB CHANNELS` lists `channel`.
    pub fn channel_active(&self, channel: &str) -> bool {
        let r = self.command(&["PUBSUB", "CHANNELS", channel]).expect("PUBSUB CHANNELS");
        r.items().iter().any(|c| c.as_text().as_deref() == Some(channel))
    }

    pub fn publish(&self, channel: &str, payload: &str) -> Result<i64> {
        Ok(self.command(&["PUBLISH", channel, payload])?.as_int().unwrap_or(0))
    }

    /// `XADD stream * field value`; see also [`RedisScope::await_stream_delivery`].
    pub fn xadd(&self, stream: &str, field: &str, value: &str) -> Result<String> {
        let id = self.command(&["XADD", stream, "*", field, value])?.as_text().ok_or_else(|| anyhow!("XADD reply"))?;
        if let Some(timeout) = self.await_delivery {
            // Not an error when it times out: the test's own assertions report
            // an entry that no consumer read.
            let _ = wait_until(&format!("delivery of {id} on {stream}"), timeout, || {
                self.delivered(stream, &id).then_some(())
            });
        }
        Ok(id)
    }

    /// Makes [`RedisScope::xadd`] return only once every consumer group of
    /// the stream has delivered the entry to one of its consumers (its
    /// `last-delivered-id` reached the entry), or after `timeout`. A test that
    /// adds entries and then starts another consumer of the group knows that
    /// the consumers running before took them.
    pub fn await_stream_delivery(&mut self, timeout: Duration) {
        self.await_delivery = Some(timeout);
    }

    /// Whether every consumer group of `stream` has delivered entry `id`
    /// (true when the stream has no group).
    fn delivered(&self, stream: &str, id: &str) -> bool {
        let Ok(groups) = self.command(&["XINFO", "GROUPS", stream]) else { return false };
        groups.items().iter().all(|group| {
            let fields = group.items();
            let last = fields
                .chunks(2)
                .find(|kv| kv[0].as_text().as_deref() == Some("last-delivered-id"))
                .and_then(|kv| kv.get(1)?.as_text());
            last.is_some_and(|last| stream_id(&last) >= stream_id(id))
        })
    }

    /// Kills `user`'s pub/sub connections (upstream: `CLIENT KILL TYPE pubsub`,
    /// which here would also hit other tests sharing the server).
    pub fn kill_pubsub_connections(&self, user: &str) -> Result<i64> {
        self.command(&["CLIENT", "KILL", "USER", user, "TYPE", "pubsub"])?.as_int().ok_or_else(|| anyhow!("kill reply"))
    }

    /// Number of `user`'s connections whose last command was `cmd` (upstream
    /// counts `cmd=xreadgroup` in `CLIENT LIST` across all clients).
    pub fn clients_with_cmd(&self, user: &str, cmd: &str) -> usize {
        let list = self.command(&["CLIENT", "LIST"]).expect("CLIENT LIST").as_text().unwrap_or_default();
        list.lines()
            .filter(|l| {
                let field = |k: &str| l.split(' ').find_map(|kv| kv.strip_prefix(k)).unwrap_or("");
                field("user=") == user && field("cmd=") == cmd
            })
            .count()
    }

    pub fn wait_clients_with_cmd(&self, user: &str, cmd: &str, count: usize, timeout: Duration) -> Result<()> {
        wait_until(&format!("{count} client(s) of {user} running {cmd}"), timeout, || {
            (self.clients_with_cmd(user, cmd) >= count).then_some(())
        })
    }
}

/// A stream entry ID (`<ms>-<seq>`) as a comparable pair.
fn stream_id(id: &str) -> (u64, u64) {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    (ms.parse().unwrap_or(0), seq.parse().unwrap_or(0))
}

impl Drop for RedisScope {
    fn drop(&mut self) {
        for user in self.users.clone() {
            let _ = self.command(&["CLIENT", "KILL", "USER", &user]);
            let _ = self.command(&["ACL", "DELUSER", &user]);
        }
        let pattern = format!("{}:*", self.prefix);
        if let Ok(r) = self.command(&["KEYS", &pattern]) {
            for k in r.items().iter().filter_map(Resp::as_text) {
                let _ = self.command(&["DEL", &k]);
            }
        }
    }
}

// ----------------------------------------------------------------- NATS ---

/// Reads a NATS server's monitoring endpoint (`/subsz`).
#[derive(Debug, Clone)]
pub struct NatsMonitor {
    addr: SocketAddr,
}

impl NatsMonitor {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    fn get(&self, path: &str) -> Result<Value> {
        let mut s = TcpStream::connect_timeout(&self.addr, IO_DEADLINE)?;
        s.set_read_timeout(Some(IO_DEADLINE))?;
        write!(s, "GET {path} HTTP/1.0\r\nHost: {}\r\n\r\n", self.addr)?;
        let mut resp = String::new();
        s.read_to_string(&mut resp)?;
        let body = resp.split_once("\r\n\r\n").map(|(_, b)| b).ok_or_else(|| anyhow!("bad monitor reply"))?;
        Ok(serde_json::from_str(body)?)
    }

    /// Number of client subscriptions on exactly `subject` (system account excluded).
    pub fn subscriptions(&self, subject: &str) -> Result<usize> {
        let v = self.get("/subsz?subs=1&limit=1000000")?;
        Ok(v["subscriptions_list"]
            .as_array()
            .map(|l| l.iter().filter(|s| s["subject"] == subject && s["account"] != "$SYS").count())
            .unwrap_or(0))
    }

    /// Waits until exactly `count` subscriptions exist on `subject`. Unreachable
    /// monitors (e.g. during a restart) count as not ready.
    pub fn wait_subscriptions(&self, subject: &str, count: usize, timeout: Duration) -> Result<()> {
        wait_until(&format!("{count} NATS subscription(s) on {subject}"), timeout, || {
            (self.subscriptions(subject).ok()? == count).then_some(())
        })
    }
}

fn parse_http_addr(url: &str) -> Result<SocketAddr> {
    let rest = url.strip_prefix("http://").ok_or_else(|| anyhow!("expected http:// URL"))?;
    let hp = rest.trim_end_matches('/');
    hp.to_socket_addrs()?.next().ok_or_else(|| anyhow!("cannot resolve {hp}"))
}

fn parse_nats_addr(url: &str) -> Result<SocketAddr> {
    let rest = url.strip_prefix("nats://").ok_or_else(|| anyhow!("expected nats:// URL"))?;
    let hp = rest.rsplit_once('@').map_or(rest, |(_, hp)| hp).trim_end_matches('/');
    let hp = if hp.contains(':') { hp.to_owned() } else { format!("{hp}:4222") };
    hp.to_socket_addrs()?.next().ok_or_else(|| anyhow!("cannot resolve {hp}"))
}

/// The NATS server named by `CABBY_TEST_NATS_URL` / `CABBY_TEST_NATS_MONITOR_URL`.
pub struct NatsService {
    pub url: String,
    pub monitor: NatsMonitor,
}

/// The shared NATS service. Panics if a variable is unset or the server is
/// unreachable.
pub fn nats() -> &'static NatsService {
    static N: OnceLock<NatsService> = OnceLock::new();
    N.get_or_init(|| {
        let url = required_env("CABBY_TEST_NATS_URL", "NATS server");
        let murl = required_env("CABBY_TEST_NATS_MONITOR_URL", "NATS server monitoring endpoint (nats-server -m)");
        let addr = parse_nats_addr(&url).unwrap_or_else(|e| panic!("CABBY_TEST_NATS_URL={url:?}: {e:#}"));
        TcpStream::connect_timeout(&addr, IO_DEADLINE)
            .unwrap_or_else(|e| panic!("NATS at CABBY_TEST_NATS_URL={url:?} is unreachable: {e}"));
        let monitor = NatsMonitor::new(
            parse_http_addr(&murl).unwrap_or_else(|e| panic!("CABBY_TEST_NATS_MONITOR_URL={murl:?}: {e:#}")),
        );
        monitor.get("/varz").unwrap_or_else(|e| {
            panic!("NATS monitoring at CABBY_TEST_NATS_MONITOR_URL={murl:?} is unreachable: {e:#}")
        });
        NatsService { url, monitor }
    })
}

impl NatsService {
    /// A subject prefix private to one test.
    pub fn prefix(&self, tag: &str) -> String {
        unique(tag)
    }
}

/// Path to a `nats-server` binary: `CABBY_NATS_SERVER_BIN` or `CABBY_NATS_SERVER`, one
/// already built under `target/`, or else built once from the Go module cache (the version pinned by the reference `go.sum`, the same server
/// upstream embeds through `enats`).
fn nats_server_binary() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        for var in ["CABBY_NATS_SERVER_BIN", "CABBY_NATS_SERVER"] {
            if let Some(p) = std::env::var_os(var).map(PathBuf::from).filter(|p| p.exists()) {
                return p;
            }
        }
        let root = crate::workspace_root();
        // Binaries built earlier (by the xtask runner or by this function) need no Go.
        for built in ["target/test-services/nats-server", "target/reference-adapters/nats-server"] {
            if root.join(built).exists() {
                return root.join(built);
            }
        }
        let how = crate::go_build::Build {
            what: "nats-server",
            hint: "set CABBY_NATS_SERVER_BIN to a nats-server binary",
            reuse: crate::go_build::Reuse::Existing,
        };
        crate::go_build::build(&root.join("target/reference-adapters/nats-server"), &how, |tmp| {
            let mut cmd = Command::new("go");
            cmd.args(["build", "-o"])
                .arg(tmp)
                .arg("github.com/nats-io/nats-server/v2")
                .current_dir(root.join("compat/reference-adapters/bridge"));
            cmd
        })
        .unwrap_or_else(|e| panic!("{e:#}"))
    })
    .clone()
}

/// A private `nats-server` process for tests that stop and restart the server
/// (upstream restarts an embedded `enats.Service`). Embedded NATS itself is a
/// separate module, `cabby_server::enats`.
pub struct NatsServer {
    pub port: u16,
    pub monitor_port: u16,
    process: Option<Supervised>,
}

impl NatsServer {
    pub fn start() -> Self {
        let mut s = Self { port: free_port(), monitor_port: free_port(), process: None };
        s.restart_process().unwrap_or_else(|e| panic!("{e:#}"));
        s
    }

    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    pub fn monitor(&self) -> NatsMonitor {
        NatsMonitor::new(SocketAddr::from(([127, 0, 0, 1], self.monitor_port)))
    }

    fn restart_process(&mut self) -> Result<()> {
        let mut cmd = Command::new(nats_server_binary());
        cmd.args(["-a", "127.0.0.1", "-p", &self.port.to_string(), "-m", &self.monitor_port.to_string()]);
        let mut p = Supervised::spawn("nats-server", cmd)?;
        p.wait_tcp(SocketAddr::from(([127, 0, 0, 1], self.port)), Duration::from_secs(20))?;
        p.wait_tcp(SocketAddr::from(([127, 0, 0, 1], self.monitor_port)), Duration::from_secs(20))?;
        self.process = Some(p);
        Ok(())
    }

    /// Stops the server (upstream `server.Shutdown`).
    pub fn shutdown(&mut self) {
        if let Some(mut p) = self.process.take() {
            p.stop(Duration::from_secs(10)).expect("stopping nats-server");
        }
    }

    /// Starts the server again on the same ports (upstream `server.Start`).
    pub fn start_again(&mut self) {
        self.restart_process().unwrap_or_else(|e| panic!("{e:#}"));
    }
}
