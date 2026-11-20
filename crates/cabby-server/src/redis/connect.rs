//! Connecting to Redis: what a [`super::RedisConfig`] resolves to, the runtime
//! Redis background work runs on, and connection builders (single instance,
//! sentinel-managed master, cluster) on top of the `redis` crate.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::sentinel::{SentinelClientBuilder, SentinelServerType};
use redis::{
    AsyncConnectionConfig, Client, ClientTlsConfig, Cmd, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, Pipeline,
    ProtocolVersion, PushInfo, RedisConnectionInfo, RedisFuture, RedisResult, TlsCertificates, TlsMode, Value,
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::UnboundedSender;

/// The runtime Redis background tasks run on. It is separate from the caller's
/// runtime so that the synchronous adapter interfaces (`start`, `shutdown`,
/// `subscribe`, ...) can be called from anywhere, including from inside another
/// Tokio runtime, and may wait for background work without deadlocking it.
pub(crate) fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("cabby-redis")
            .enable_all()
            .build()
            .expect("building the Redis runtime")
    })
}

/// Runs `fut` on the Redis runtime and waits (synchronously) up to `timeout`
/// for its result; `None` if it did not finish in time. Safe to call from any
/// thread except the Redis runtime's own workers.
pub(crate) fn wait_on<F>(timeout: Duration, fut: F) -> Option<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv_timeout(timeout).ok()
}

/// A one-shot flag that threads can wait for.
#[derive(Default)]
pub(crate) struct Signal {
    set: std::sync::Mutex<bool>,
    cond: std::sync::Condvar,
}

impl Signal {
    pub(crate) fn set(&self) {
        *self.set.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.cond.notify_all();
    }

    /// Waits until set; false if `timeout` elapsed first.
    pub(crate) fn wait(&self, timeout: Duration) -> bool {
        let g = self.set.lock().unwrap_or_else(|e| e.into_inner());
        let (g, _) = self.cond.wait_timeout_while(g, timeout, |set| !*set).unwrap_or_else(|e| e.into_inner());
        *g
    }
}

/// Upstream `utils.NextRetry(step)`: 250 ms, then a random delay in
/// `[2^step, 2^(step+1))` seconds (whole seconds).
pub(crate) fn next_retry(step: u32) -> Duration {
    if step == 0 {
        return Duration::from_millis(250);
    }
    let left = 2f64.powi(step.min(30) as i32);
    let secs = left + left * random_unit();
    Duration::from_secs(secs as u64)
}

fn random_unit() -> f64 {
    let mut b = [0u8; 8];
    if getrandom::fill(&mut b).is_err() {
        return 0.5;
    }
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64
}

/// A 6-character consumer name (upstream `nanoid.Nanoid(6)`).
pub(crate) fn nanoid6() -> String {
    const ALPHABET: &[u8] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut b = [0u8; 6];
    let _ = getrandom::fill(&mut b);
    b.iter().map(|x| ALPHABET[(*x & 63) as usize] as char).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Addr {
    Tcp(String, u16),
    Unix(PathBuf),
}

impl Addr {
    /// Parses a resolved `host:port` (rueidis init address) or a socket path.
    pub(crate) fn parse(addr: &str, unix: bool) -> Result<Self, String> {
        if unix {
            return Ok(Addr::Unix(PathBuf::from(addr)));
        }
        let (host, port) = match addr.strip_prefix('[') {
            Some(rest) => {
                let (h, p) = rest.split_once("]:").ok_or_else(|| format!("invalid Redis address {addr:?}"))?;
                (h, p)
            }
            None => addr.rsplit_once(':').ok_or_else(|| format!("invalid Redis address {addr:?}"))?,
        };
        let port = port.parse().map_err(|_| format!("invalid port in Redis address {addr:?}"))?;
        Ok(Addr::Tcp(host.to_owned(), port))
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Tcp(h, p) => f.write_str(&super::url::join_host_port(h, &p.to_string())),
            Addr::Unix(p) => write!(f, "{}", p.display()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TlsSpec {
    /// Skip server certificate verification (upstream `InsecureSkipVerify`).
    pub insecure: bool,
    /// Custom root CA bundle (PEM).
    pub root_pem: Option<Vec<u8>>,
    /// Client certificate and key (PEM) for mutual TLS.
    pub client: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Topology {
    Single(Addr),
    /// Several init addresses (upstream treats them as a cluster).
    Cluster(Vec<Addr>),
    Sentinel {
        sentinels: Vec<Addr>,
        master: String,
        username: String,
        password: String,
    },
}

/// Everything needed to open connections for a Redis configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectSpec {
    pub topology: Topology,
    /// How often an idle commands connection is checked with `PING` (rueidis
    /// `Dialer.KeepAlive`); `None` disables the check.
    pub keepalive: Option<Duration>,
    /// How long replies on a commands connection may take (rueidis
    /// `ConnWriteTimeout`).
    pub write_timeout: Duration,
    pub username: String,
    pub password: String,
    pub db: i64,
    pub tls: Option<TlsSpec>,
    pub dial_timeout: Option<Duration>,
    pub client_name: String,
}

/// Commands connection: one node (single instance or sentinel master) or a cluster.
#[derive(Clone)]
pub(crate) enum Commands {
    Single(MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Commands {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Commands::Single(c) => c.req_packed_command(cmd),
            Commands::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Commands::Single(c) => c.req_packed_commands(cmd, offset, count),
            Commands::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Commands::Single(c) => c.get_db(),
            Commands::Cluster(c) => c.get_db(),
        }
    }
}

fn install_crypto_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // The `redis` crate builds its rustls configurations from the process
        // default provider; Cabby uses ring everywhere.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl ConnectSpec {
    fn tls_mode(&self) -> Option<TlsMode> {
        self.tls.as_ref().map(|t| if t.insecure { TlsMode::Insecure } else { TlsMode::Secure })
    }

    fn certificates(&self) -> Option<TlsCertificates> {
        let tls = self.tls.as_ref()?;
        if tls.root_pem.is_none() && tls.client.is_none() {
            return None;
        }
        Some(TlsCertificates {
            client_tls: tls
                .client
                .as_ref()
                .map(|(cert, key)| ClientTlsConfig { client_cert: cert.clone(), client_key: key.clone() }),
            root_cert: tls.root_pem.clone(),
        })
    }

    fn redis_info(&self, protocol: ProtocolVersion) -> RedisConnectionInfo {
        let mut info = RedisConnectionInfo::default().set_db(self.db).set_protocol(protocol);
        if !self.username.is_empty() {
            info = info.set_username(&self.username);
        }
        if !self.password.is_empty() {
            info = info.set_password(&self.password);
        }
        info
    }

    fn connection_addr(&self, addr: &Addr) -> ConnectionAddr {
        match addr {
            Addr::Unix(p) => ConnectionAddr::Unix(p.clone()),
            Addr::Tcp(host, port) => match &self.tls {
                None => ConnectionAddr::Tcp(host.clone(), *port),
                Some(t) => {
                    ConnectionAddr::TcpTls { host: host.clone(), port: *port, insecure: t.insecure, tls_params: None }
                }
            },
        }
    }

    /// A client for one node.
    fn client_for(&self, addr: &Addr, protocol: ProtocolVersion) -> RedisResult<Client> {
        install_crypto_provider();
        let info = self.connection_addr(addr).into_connection_info()?.set_redis_settings(self.redis_info(protocol));
        self.client_from_info(info)
    }

    fn client_from_info(&self, info: ConnectionInfo) -> RedisResult<Client> {
        match (self.certificates(), info.addr()) {
            (Some(certs), ConnectionAddr::TcpTls { .. }) => Client::build_with_tls(info, certs),
            _ => Client::open(info),
        }
    }

    /// Clients for the nodes to try, in order (the resolved master for sentinels).
    async fn node_clients(&self, protocol: ProtocolVersion) -> RedisResult<Vec<Client>> {
        match &self.topology {
            Topology::Single(addr) => Ok(vec![self.client_for(addr, protocol)?]),
            Topology::Cluster(addrs) => {
                let mut clients = Vec::with_capacity(addrs.len());
                for addr in addrs {
                    clients.push(self.client_for(addr, protocol)?);
                }
                // Spread nodes' subscriber connections over the cluster.
                if clients.len() > 1 {
                    let mut b = [0u8; 1];
                    let _ = getrandom::fill(&mut b);
                    let n = clients.len();
                    clients.rotate_left(b[0] as usize % n);
                }
                Ok(clients)
            }
            Topology::Sentinel { sentinels, master, username, password } => {
                install_crypto_provider();
                let addrs: Vec<ConnectionAddr> = sentinels.iter().map(|a| self.connection_addr(a)).collect();
                let mut builder = SentinelClientBuilder::new(addrs, master, SentinelServerType::Master)?
                    .set_client_to_redis_db(self.db)
                    .set_client_to_redis_protocol(protocol);
                if !self.username.is_empty() {
                    builder = builder.set_client_to_redis_username(&self.username);
                }
                if !self.password.is_empty() {
                    builder = builder.set_client_to_redis_password(&self.password);
                }
                if !username.is_empty() {
                    builder = builder.set_client_to_sentinel_username(username);
                }
                if !password.is_empty() {
                    builder = builder.set_client_to_sentinel_password(password);
                }
                if let Some(mode) = self.tls_mode() {
                    builder = builder.set_client_to_redis_tls_mode(mode).set_client_to_sentinel_tls_mode(mode);
                }
                if let Some(certs) = self.certificates() {
                    builder = builder
                        .set_client_to_redis_certificates(certs.clone())
                        .set_client_to_sentinel_certificates(certs);
                }
                let mut sentinel = builder.build()?;
                Ok(vec![sentinel.async_get_client().await?])
            }
        }
    }

    fn connection_config(&self, response_timeout: Option<Duration>) -> AsyncConnectionConfig {
        AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.dial_timeout.unwrap_or(Duration::from_secs(5))))
            .set_response_timeout(response_timeout)
    }

    async fn first_connection(
        &self,
        protocol: ProtocolVersion,
        config: &AsyncConnectionConfig,
    ) -> RedisResult<MultiplexedConnection> {
        let clients = self.node_clients(protocol).await?;
        let mut last = None;
        for client in clients {
            match client.get_multiplexed_async_connection_with_config(config).await {
                Ok(mut conn) => {
                    if !self.client_name.is_empty() {
                        let _: RedisResult<Value> =
                            redis::cmd("CLIENT").arg("SETNAME").arg(&self.client_name).query_async(&mut conn).await;
                    }
                    return Ok(conn);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| redis::RedisError::from((redis::ErrorKind::Io, "no Redis address to connect to"))))
    }

    /// A dedicated RESP3 connection whose pushes (messages, subscription
    /// changes, disconnection) go to `pushes`.
    pub(crate) async fn subscriber(
        &self,
        pushes: UnboundedSender<PushInfo>,
        response_timeout: Duration,
    ) -> RedisResult<MultiplexedConnection> {
        let config = self.connection_config(Some(response_timeout)).set_push_sender(pushes);
        self.first_connection(ProtocolVersion::RESP3, &config).await
    }

    /// A connection for regular commands. `response_timeout: None` waits for
    /// replies indefinitely.
    ///
    /// Several addresses are a cluster. A single address is probed with
    /// `CLUSTER SLOTS` first, as rueidis `NewClient` does: a node of a Redis
    /// Cluster gets a cluster connection (seeded with that node), anything
    /// else a plain one.
    pub(crate) async fn commands(&self, response_timeout: Option<Duration>) -> RedisResult<Commands> {
        if let Topology::Cluster(addrs) = &self.topology {
            return self.cluster_commands(addrs, response_timeout).await;
        }
        let config = self.connection_config(response_timeout);
        let mut conn = self.first_connection(ProtocolVersion::RESP2, &config).await?;
        if let Topology::Single(addr @ Addr::Tcp(..)) = &self.topology
            && is_cluster_node(&mut conn).await?
        {
            return self.cluster_commands(std::slice::from_ref(addr), response_timeout).await;
        }
        Ok(Commands::Single(conn))
    }

    async fn cluster_commands(&self, addrs: &[Addr], response_timeout: Option<Duration>) -> RedisResult<Commands> {
        install_crypto_provider();
        let nodes: Vec<ConnectionInfo> =
            addrs.iter().map(|a| self.connection_addr(a).into_connection_info()).collect::<RedisResult<_>>()?;
        let mut builder = ClusterClientBuilder::new(nodes)
            .connection_timeout(self.dial_timeout.unwrap_or(Duration::from_secs(5)))
            .response_timeout(response_timeout.unwrap_or(Duration::from_secs(365 * 24 * 3600)));
        if !self.username.is_empty() {
            builder = builder.username(&self.username);
        }
        if !self.password.is_empty() {
            builder = builder.password(&self.password);
        }
        if let Some(mode) = self.tls_mode() {
            builder = builder.tls(mode);
        }
        if let Some(certs) = self.certificates() {
            builder = builder.certs(certs);
        }
        let client = builder.build()?;
        Ok(Commands::Cluster(client.get_async_connection().await?))
    }
}

/// rueidis' cluster probe: a node answering `CLUSTER SLOTS` with slots belongs
/// to a Redis Cluster. Servers without cluster support answer with an error
/// (`cluster support disabled`, an unknown or forbidden command), which means
/// a standalone server; only a lost connection is an error.
async fn is_cluster_node(conn: &mut MultiplexedConnection) -> RedisResult<bool> {
    match redis::cmd("CLUSTER").arg("SLOTS").query_async::<Value>(conn).await {
        Ok(Value::Array(slots)) => Ok(!slots.is_empty()),
        Ok(_) => Ok(false),
        Err(e) if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() => Err(e),
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_addresses() {
        assert_eq!(Addr::Tcp("localhost".into(), 6379), Addr::parse("localhost:6379", false).unwrap());
        assert_eq!(Addr::Tcp("::1".into(), 7000), Addr::parse("[::1]:7000", false).unwrap());
        assert_eq!("[::1]:7000", Addr::parse("[::1]:7000", false).unwrap().to_string());
        assert_eq!(Addr::Unix("/tmp/r.sock".into()), Addr::parse("/tmp/r.sock", true).unwrap());
        assert!(Addr::parse("nohost", false).is_err());
    }

    #[test]
    fn retry_delays_follow_upstream() {
        assert_eq!(Duration::from_millis(250), next_retry(0));
        for _ in 0..20 {
            let d = next_retry(2);
            assert!(d >= Duration::from_secs(4) && d < Duration::from_secs(8), "{d:?}");
        }
    }

    #[test]
    fn consumer_names() {
        let n = nanoid6();
        assert_eq!(6, n.len());
    }
}
