//! Connecting to PostgreSQL: the runtime the adapters run on, what a
//! [`PostgresConfig`] resolves to (TLS through the workspace `rustls`), a small
//! pool of command connections and the dedicated `LISTEN` connection.
//!
//! Topology (D-070): `LISTEN` runs on its own long-lived connection, which
//! needs a direct or session-mode connection (not PgBouncer transaction
//! pooling). It is checked with `SELECT 1` every `keepalive_interval`, so a
//! half-open TCP connection is detected; on any failure it reconnects with
//! backoff and issues `LISTEN` again for every channel. `NOTIFY`, `INSERT`,
//! `SELECT` and cleanup queries use separate command connections, so they
//! never wait behind notifications.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use cabby_core::logger::Logger;
use cabby_core::{log_error, log_info, log_warn};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use tokio::sync::{Notify, mpsc};
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, Client, Row, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;

use super::{Metrics, PgError, PostgresConfig, error_text};

/// How long opening a connection may take.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long one query (or a pipelined batch) may take before its connection
/// is considered broken and dropped.
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
/// Notifications read from a `LISTEN` connection and not yet handed on.
const NOTICES_CAPACITY: usize = 1024;
/// Received messages waiting for the node (per adapter); beyond this they
/// are dropped and counted.
pub(crate) const INBOX_CAPACITY: usize = 8192;

/// A bounded queue of received messages: pushing never blocks the `LISTEN`
/// connection; a message that finds it full is dropped, counted
/// (`postgres_dropped_total`) and logged (at the 1st, 2nd, 4th, 8th, ... drop).
pub(crate) struct Inbox<T> {
    tx: mpsc::Sender<T>,
    dropped: AtomicUsize,
    metrics: Metrics,
    log: Logger,
}

impl<T> Inbox<T> {
    pub fn new(capacity: usize, metrics: Metrics, log: Logger) -> (Self, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx, dropped: AtomicUsize::new(0), metrics, log }, rx)
    }

    pub fn push(&self, item: T) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(item) {
            self.metrics.dropped();
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                log_warn!(self.log, "the node is not keeping up with Postgres notifications; dropped messages", "dropped" => n);
            }
        }
    }
}

/// How long a `LISTEN` connection must stay up before a loss reconnects
/// without the accumulated backoff.
pub(crate) const STABLE_CONNECTION: Duration = Duration::from_secs(30);
/// The longest wait between reconnect attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// The runtime Postgres connections and background tasks run on, separate
/// from the caller's runtime so that the synchronous adapter interfaces can
/// wait for it from anywhere (as the Redis and NATS adapters do).
pub(crate) fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("cabby-postgres")
            .enable_all()
            .build()
            .expect("building the Postgres runtime")
    })
}

/// Runs a blocking closure, telling a multi-threaded Tokio runtime first when
/// the caller is one of its workers.
pub(crate) fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Runs `fut` on the Postgres runtime and waits for it.
pub(crate) fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    blocking(move || rx.recv()).expect("Postgres runtime task panicked")
}

/// The delay before reconnect attempt `attempt` (0-based): 250 ms, then
/// `2^attempt` seconds with up to 50% jitter, at most [`MAX_BACKOFF`].
pub(crate) fn backoff(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::from_millis(250);
    }
    let base = Duration::from_secs(1u64 << attempt.min(5)).min(MAX_BACKOFF);
    let mut b = [0u8; 2];
    let jitter = if getrandom::fill(&mut b).is_ok() { u64::from(u16::from_le_bytes(b)) } else { 0 };
    (base + base.mul_f64(jitter as f64 / f64::from(u16::MAX) / 2.0)).min(MAX_BACKOFF)
}

/// `sslmode` as libpq names it; tokio-postgres knows the first three.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// How the server certificate is checked when TLS is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verify {
    /// Chain and host name.
    Full,
    /// Chain only (`sslmode=verify-ca`).
    Chain,
    /// Nothing (`--postgres_tls_verify=false` with `prefer` or `require`).
    None,
}

/// The TLS-related parameters taken out of a connection string, and the
/// string tokio-postgres parses (with `sslmode` mapped to what it knows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedUrl {
    pub conninfo: String,
    pub sslmode: SslMode,
    pub sslrootcert: Option<String>,
}

fn parse_sslmode(v: &str) -> Result<SslMode, String> {
    Ok(match v {
        "disable" => SslMode::Disable,
        "prefer" | "allow" => SslMode::Prefer,
        "require" => SslMode::Require,
        "verify-ca" => SslMode::VerifyCa,
        "verify-full" => SslMode::VerifyFull,
        other => return Err(format!("invalid sslmode {other:?}")),
    })
}

fn tokio_sslmode(m: SslMode) -> &'static str {
    match m {
        SslMode::Disable => "disable",
        SslMode::Prefer => "prefer",
        _ => "require",
    }
}

/// Takes `sslmode` and `sslrootcert` out of a URI (`postgres://...?...`) or
/// a key/value string, as tokio-postgres doesn't know `verify-ca`,
/// `verify-full` or `sslrootcert`; everything else is left to tokio-postgres's
/// own parser. In a URI, the user name and password are percent-encoded
/// again, so a literal `?`, `/` or `@` in a password parses.
pub(crate) fn parse_url(url: &str) -> Result<ParsedUrl, String> {
    let mut sslmode = SslMode::Prefer;
    let mut sslrootcert = None;
    let conninfo = if let Some(scheme_end) =
        ["postgres://", "postgresql://"].iter().find_map(|p| url.starts_with(p).then_some(p.len()))
    {
        let (scheme, rest) = url.split_at(scheme_end);
        let (userinfo, hostpart) = split_userinfo(rest);
        let (base, query) = match hostpart.split_once('?') {
            Some((b, q)) => (b, Some(q)),
            None => (hostpart, None),
        };
        let mut kept = Vec::new();
        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match k {
                "sslmode" => sslmode = parse_sslmode(&percent_decode(v))?,
                "sslrootcert" => sslrootcert = Some(percent_decode(v)),
                _ => kept.push(pair.to_owned()),
            }
        }
        kept.push(format!("sslmode={}", tokio_sslmode(sslmode)));
        let userinfo = userinfo.map(|u| {
            let (user, password) = match u.split_once(':') {
                Some((user, password)) => (user, Some(password)),
                None => (u, None),
            };
            match password {
                Some(p) => format!("{}:{}@", encode_userinfo(user), encode_userinfo(p)),
                None => format!("{}@", encode_userinfo(user)),
            }
        });
        format!("{scheme}{}{base}?{}", userinfo.unwrap_or_default(), kept.join("&"))
    } else {
        let mut kept = Vec::new();
        for (key, value, raw) in key_values(url)? {
            match key.as_str() {
                "sslmode" => sslmode = parse_sslmode(&value)?,
                "sslrootcert" => sslrootcert = Some(value),
                _ => kept.push(raw),
            }
        }
        kept.push(format!("sslmode={}", tokio_sslmode(sslmode)));
        kept.join(" ")
    };
    Ok(ParsedUrl { conninfo, sslmode, sslrootcert })
}

/// Splits `[userinfo@]host...`: the user info ends at the last `@` before
/// the first `/` or `?` that follows the first `@`, so `@`, `/` and `?` in a
/// password are kept in it.
fn split_userinfo(rest: &str) -> (Option<&str>, &str) {
    let Some(first_at) = rest.find('@') else { return (None, rest) };
    let end = rest[first_at..].find(['/', '?']).map_or(rest.len(), |i| first_at + i);
    let at = rest[..end].rfind('@').unwrap_or(first_at);
    (Some(&rest[..at]), &rest[at + 1..])
}

/// Percent-encodes what a URI's user info can't hold literally; `%XX`
/// sequences already encoded are kept.
fn encode_userinfo(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    for (i, &b) in bytes.iter().enumerate() {
        let encoded = b == b'%' && bytes.get(i + 1..i + 3).is_some_and(|h| h.iter().all(u8::is_ascii_hexdigit));
        if b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=".contains(&b) || encoded {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The `key=value` pairs of a libpq key/value string: (key, unquoted value,
/// the pair as written). Values may be single-quoted, with `\'` and `\\`
/// escapes; spaces around `=` are allowed.
fn key_values(s: &str) -> Result<Vec<(String, String, String)>, String> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let skip_ws = |i: &mut usize| {
        while *i < chars.len() && chars[*i].is_whitespace() {
            *i += 1;
        }
    };
    loop {
        skip_ws(&mut i);
        if i >= chars.len() {
            return Ok(out);
        }
        let start = i;
        while i < chars.len() && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let key: String = chars[start..i].iter().collect();
        skip_ws(&mut i);
        if i >= chars.len() || chars[i] != '=' {
            return Err(format!("missing \"=\" after {key:?}"));
        }
        i += 1;
        skip_ws(&mut i);
        let mut value = String::new();
        if i < chars.len() && chars[i] == '\'' {
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err(format!("unterminated quoted value of {key:?}")),
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some('\\') if i + 1 < chars.len() => {
                        value.push(chars[i + 1]);
                        i += 2;
                    }
                    Some(c) => {
                        value.push(*c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < chars.len() && !chars[i].is_whitespace() {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                value.push(chars[i]);
                i += 1;
            }
        }
        let raw: String = chars[start..i].iter().collect();
        out.push((key, value, raw));
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(h) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(h);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// How certificates are verified for `mode`: `verify-full` and `verify-ca`
/// always verify; `prefer` and `require` verify chain and host name unless
/// `--postgres_tls_verify=false`.
pub(crate) fn verification(mode: SslMode, tls_verify: bool) -> Verify {
    match mode {
        SslMode::VerifyFull => Verify::Full,
        SslMode::VerifyCa => Verify::Chain,
        _ if tls_verify => Verify::Full,
        _ => Verify::None,
    }
}

/// Builds the rustls client configuration (ring, TLS 1.2+).
pub(crate) fn tls_config(verify: Verify, ca_path: &str) -> Result<ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| e.to_string())?;
    if verify == Verify::None {
        return Ok(builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification))
            .with_no_client_auth());
    }
    let native = if ca_path.is_empty() { rustls_native_certs::load_native_certs().certs } else { Vec::new() };
    let roots = roots(native, ca_path)?;
    let webpki = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
        .build()
        .map_err(|e| format!("failed to build the TLS verifier: {e}"))?;
    let verifier: Arc<dyn ServerCertVerifier> = match verify {
        Verify::Chain => Arc::new(ChainOnly(webpki)),
        _ => webpki,
    };
    Ok(builder.dangerous().with_custom_certificate_verifier(verifier).with_no_client_auth())
}

/// The CA certificates to verify the server with: the file at `ca_path`, or
/// else the system roots (`native`). None at all is an error naming the
/// settings that fix it.
fn roots(native: Vec<CertificateDer<'static>>, ca_path: &str) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    if ca_path.is_empty() {
        for cert in native {
            let _ = roots.add(cert);
        }
        if roots.is_empty() {
            return Err("no system CA certificates found to verify the Postgres server: set \
                        --postgres_tls_ca_cert_path, or --postgres_tls_verify=false, or use sslmode=disable"
                .to_owned());
        }
        return Ok(roots);
    }
    let pem =
        std::fs::read(ca_path).map_err(|e| format!("failed to read --postgres_tls_ca_cert_path {ca_path:?}: {e}"))?;
    let added = CertificateDer::pem_slice_iter(&pem).flatten().filter(|c| roots.add(c.clone()).is_ok()).count();
    if added == 0 {
        return Err(format!("no CA certificate found in --postgres_tls_ca_cert_path {ca_path:?}"));
    }
    Ok(roots)
}

/// `sslmode=verify-ca`: the chain must verify; the host name is not checked.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        match self.0.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// `--postgres_tls_verify=false`: any certificate is accepted.
#[derive(Debug)]
struct NoVerification;

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

/// Everything needed to open connections for a [`PostgresConfig`].
#[derive(Clone)]
pub(crate) struct ConnectSpec {
    config: tokio_postgres::Config,
    tls: MakeRustlsConnect,
    /// `host:port/database`, without credentials (for logs and errors).
    pub description: String,
    pub keepalive: Option<Duration>,
}

impl ConnectSpec {
    /// Resolves the configuration; `flag` names the adapter in errors
    /// (`--pubsub=postgres`).
    pub fn from_config(c: &PostgresConfig, flag: &str) -> Result<Self, String> {
        if c.url.trim().is_empty() {
            return Err(format!(
                "{flag} needs a connection URL: set --postgres_url, CABBY_POSTGRES_URL or [postgres] url"
            ));
        }
        let parsed = parse_url(c.url.trim()).map_err(|e| format!("invalid --postgres_url: {e}"))?;
        let config: tokio_postgres::Config = parsed
            .conninfo
            .parse()
            .map_err(|e: tokio_postgres::Error| format!("invalid --postgres_url: {}", error_text(&e)))?;
        let ca_path = if c.tls_ca_cert_path.is_empty() {
            parsed.sslrootcert.unwrap_or_default()
        } else {
            c.tls_ca_cert_path.clone()
        };
        let tls = if parsed.sslmode == SslMode::Disable {
            // Never used: the connection is plain.
            tls_config(Verify::None, "")?
        } else {
            tls_config(verification(parsed.sslmode, c.tls_verify), &ca_path)?
        };
        Ok(Self { description: describe(&config), config, tls: MakeRustlsConnect::new(tls), keepalive: c.keepalive() })
    }

    /// Opens a connection named `app` (`application_name`); its I/O runs on
    /// the Postgres runtime. Messages other than query results
    /// (notifications) go to `notices` when given.
    pub async fn connect(&self, app: &str, notices: Option<mpsc::Sender<(String, String)>>) -> Result<Client, PgError> {
        let mut config = self.config.clone();
        config.application_name(truncate(app, super::MAX_IDENTIFIER));
        config.connect_timeout(CONNECT_TIMEOUT);
        config.keepalives(true);
        config.keepalives_idle(Duration::from_secs(30));
        config.keepalives_interval(Duration::from_secs(10));
        config.keepalives_retries(3);
        let (client, mut conn) = tokio::time::timeout(CONNECT_TIMEOUT, config.connect(self.tls.clone()))
            .await
            .map_err(|_| PgError(format!("timed out connecting to {}", self.description)))?
            .map_err(|e| PgError(format!("failed to connect to {}: {}", self.description, error_text(&e))))?;
        runtime().spawn(async move {
            let mut conn = std::pin::pin!(&mut conn);
            while let Some(message) = std::future::poll_fn(|cx| conn.as_mut().poll_message(cx)).await {
                match message {
                    Ok(AsyncMessage::Notification(n)) => {
                        // A full queue stops reading the socket; PostgreSQL keeps
                        // the notifications queued on its side meanwhile.
                        if let Some(tx) = &notices
                            && tx.send((n.channel().to_owned(), n.payload().to_owned())).await.is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        Ok(client)
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// `host:port[,host:port]/database`: what a connection string points at,
/// without user name or password.
pub(crate) fn describe(config: &tokio_postgres::Config) -> String {
    let ports = config.get_ports();
    let hosts: Vec<String> = config
        .get_hosts()
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let port = ports.get(i).or(ports.first()).copied().unwrap_or(5432);
            match h {
                tokio_postgres::config::Host::Tcp(host) => {
                    if host.contains(':') {
                        format!("[{host}]:{port}")
                    } else {
                        format!("{host}:{port}")
                    }
                }
                #[cfg(unix)]
                tokio_postgres::config::Host::Unix(path) => format!("{}:{port}", path.display()),
            }
        })
        .collect();
    let hosts = if hosts.is_empty() { "localhost:5432".to_owned() } else { hosts.join(",") };
    format!("{hosts}/{}", config.get_dbname().or(config.get_user()).unwrap_or_default())
}

/// A command connection with its prepared statements.
pub(crate) struct Conn {
    pub client: Client,
    statements: Mutex<HashMap<String, Statement>>,
}

impl Conn {
    pub fn new(client: Client) -> Self {
        Self { client, statements: Mutex::new(HashMap::new()) }
    }

    /// The prepared statement for `sql` (prepared once per connection).
    pub async fn prepare(&self, sql: &str) -> Result<Statement, tokio_postgres::Error> {
        if let Some(s) = self.statements.lock().unwrap_or_else(PoisonError::into_inner).get(sql) {
            return Ok(s.clone());
        }
        let statement = self.client.prepare(sql).await?;
        self.statements.lock().unwrap_or_else(PoisonError::into_inner).insert(sql.to_owned(), statement.clone());
        Ok(statement)
    }

    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, tokio_postgres::Error> {
        let statement = self.prepare(sql).await?;
        self.client.query(&statement, params).await
    }
}

/// Owned query parameters (queries run on the Postgres runtime).
pub(crate) type Params = Vec<Box<dyn ToSql + Sync + Send>>;

/// Borrows owned parameters as tokio-postgres takes them.
pub(crate) fn refs(params: &Params) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect()
}

/// A few command connections, opened on first use and reopened after a
/// failure. Queries are spread over them round-robin; each connection
/// pipelines the queries sent to it.
///
/// Every call has one deadline ([`QUERY_TIMEOUT`]) covering the connect and
/// the query, and uses an open connection when there is one: the chosen slot,
/// else any slot (the chosen one then reconnects in the background). Only a
/// caller with no open connection connects itself, without holding a lock;
/// others wait for that connect within their deadline. After such a connect
/// fails, calls fail at once for [`FAIL_FAST`] while no connection is open,
/// so an unreachable server costs one deadline, not one per queued caller.
pub(crate) struct Pool {
    spec: Arc<ConnectSpec>,
    app: String,
    slots: Vec<Mutex<Slot>>,
    next: AtomicUsize,
    /// Signalled whenever a connect finishes.
    connected: Notify,
    /// Until when calls with no open connection fail at once, and why.
    failed: Mutex<Option<(Instant, String)>>,
    metrics: Metrics,
}

#[derive(Default)]
struct Slot {
    conn: Option<Arc<Conn>>,
    connecting: bool,
    /// No background reconnect before then (after one failed).
    retry_after: Option<Instant>,
}

/// How long calls fail at once after a failed connect (while no connection
/// is open), and how long a slot waits before reconnecting in the background
/// again.
pub(crate) const FAIL_FAST: Duration = Duration::from_secs(1);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A slot being connected; dropping it unfinished (a cancelled connect)
/// frees the slot and wakes the waiters.
struct Connecting<'a> {
    pool: &'a Pool,
    slot: usize,
    done: bool,
}

impl Connecting<'_> {
    /// Stores the outcome; `background` failures don't make callers fail fast.
    fn finish(mut self, result: Result<Client, PgError>, background: bool) -> Result<Arc<Conn>, PgError> {
        self.done = true;
        let out = {
            let mut slot = lock(&self.pool.slots[self.slot]);
            slot.connecting = false;
            match result {
                Ok(client) => {
                    let conn = Arc::new(Conn::new(client));
                    slot.conn = Some(conn.clone());
                    slot.retry_after = None;
                    *lock(&self.pool.failed) = None;
                    Ok(conn)
                }
                Err(e) => {
                    slot.retry_after = Some(Instant::now() + FAIL_FAST);
                    if !background {
                        *lock(&self.pool.failed) = Some((Instant::now() + FAIL_FAST, e.0.clone()));
                    }
                    Err(e)
                }
            }
        };
        self.pool.connected.notify_waiters();
        out
    }
}

impl Drop for Connecting<'_> {
    fn drop(&mut self) {
        if !self.done {
            lock(&self.pool.slots[self.slot]).connecting = false;
            self.pool.connected.notify_waiters();
        }
    }
}

impl Pool {
    pub fn new(spec: Arc<ConnectSpec>, app: &str, size: usize, metrics: Metrics) -> Arc<Self> {
        Arc::new(Self {
            spec,
            app: app.to_owned(),
            slots: (0..size.max(1)).map(|_| Mutex::new(Slot::default())).collect(),
            next: AtomicUsize::new(0),
            connected: Notify::new(),
            failed: Mutex::new(None),
            metrics,
        })
    }

    fn open(slot: &Slot) -> Option<Arc<Conn>> {
        slot.conn.as_ref().filter(|c| !c.client.is_closed()).cloned()
    }

    /// Marks slot `i` as connecting if it is free (and, when `respect_retry`,
    /// not waiting after a failed background connect).
    fn claim(&self, i: usize, respect_retry: bool) -> Option<Connecting<'_>> {
        let mut slot = lock(&self.slots[i]);
        let waiting = respect_retry && slot.retry_after.is_some_and(|t| Instant::now() < t);
        if slot.connecting || Self::open(&slot).is_some() || waiting {
            return None;
        }
        slot.connecting = true;
        slot.conn = None;
        Some(Connecting { pool: self, slot: i, done: false })
    }

    /// Reconnects slot `i` on the runtime, unless it is already connecting.
    fn reconnect_in_background(self: &Arc<Self>, i: usize) {
        if self.claim(i, true).map(|mut c| c.done = true).is_none() {
            return;
        }
        let pool = self.clone();
        runtime().spawn(async move {
            // Claimed above; the guard takes over here.
            let guard = Connecting { pool: &pool, slot: i, done: false };
            let result = pool.spec.connect(&pool.app, None).await;
            if result.is_err() {
                pool.metrics.error();
            }
            let _ = guard.finish(result, true);
        });
    }

    /// A connection by `deadline`: an open one if any, else a new one.
    pub async fn get(self: &Arc<Self>, deadline: tokio::time::Instant) -> Result<Arc<Conn>, PgError> {
        let first = self.next.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        loop {
            let notified = self.connected.notified();
            if let Some(c) = Self::open(&lock(&self.slots[first])) {
                return Ok(c);
            }
            if let Some(c) = self.slots.iter().find_map(|s| Self::open(&lock(s))) {
                self.reconnect_in_background(first);
                return Ok(c);
            }
            if let Some((until, why)) = lock(&self.failed).as_ref()
                && Instant::now() < *until
            {
                return Err(PgError(why.clone()));
            }
            let order = (0..self.slots.len()).map(|k| (first + k) % self.slots.len());
            let Some(guard) = order.into_iter().find_map(|i| self.claim(i, false)) else {
                // Every slot is connecting: wait for one.
                if tokio::time::timeout_at(deadline, notified).await.is_err() {
                    return Err(PgError(format!("timed out connecting to {}", self.spec.description)));
                }
                continue;
            };
            let result = match tokio::time::timeout_at(deadline, self.spec.connect(&self.app, None)).await {
                Ok(r) => r,
                Err(_) => Err(PgError(format!("timed out connecting to {}", self.spec.description))),
            };
            return guard.finish(result, false);
        }
    }

    /// Drops `conn` from the pool (after a timeout or a broken connection).
    pub fn discard(&self, conn: &Arc<Conn>) {
        for slot in &self.slots {
            let mut slot = lock(slot);
            if slot.conn.as_ref().is_some_and(|c| Arc::ptr_eq(c, conn)) {
                slot.conn = None;
            }
        }
    }

    /// Runs one query within one deadline (connect included); failures are
    /// counted, and a connection that timed out or closed is dropped.
    pub async fn query(self: &Arc<Self>, sql: &str, params: &Params) -> Result<Vec<Row>, PgError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
        let conn = match self.get(deadline).await {
            Ok(c) => c,
            Err(e) => {
                self.metrics.error();
                return Err(e);
            }
        };
        let result = tokio::time::timeout_at(deadline, conn.query(sql, &refs(params))).await;
        self.metrics.took(started.elapsed());
        match result {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(e)) => {
                self.metrics.error();
                if conn.client.is_closed() {
                    self.discard(&conn);
                }
                Err(e.into())
            }
            Err(_) => {
                self.metrics.error();
                self.discard(&conn);
                Err(PgError(format!("query timed out after {}s", QUERY_TIMEOUT.as_secs())))
            }
        }
    }
}

/// Receives what the `LISTEN` connection gets.
pub(crate) trait ListenSink: Send + Sync + 'static {
    fn notification(&self, channel: &str, payload: &str);
    /// The connection is up and every channel is listened to (again);
    /// `reconnected` is false for the first connection.
    fn connected(&self, _reconnected: bool) {}
}

#[derive(Default)]
struct ListenState {
    /// Channels to listen to.
    desired: BTreeSet<String>,
    /// Channels listened to on the current connection.
    confirmed: BTreeSet<String>,
    connected: bool,
}

struct ListenShared {
    state: Mutex<ListenState>,
    changed: Condvar,
    wake: Notify,
}

impl ListenShared {
    fn lock(&self) -> std::sync::MutexGuard<'_, ListenState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The dedicated `LISTEN` connection.
pub(crate) struct Listener {
    shared: Arc<ListenShared>,
    stop: CancellationToken,
}

impl Listener {
    /// Starts listening to `channels` in the background.
    pub fn start(
        spec: Arc<ConnectSpec>,
        app: &str,
        channels: impl IntoIterator<Item = String>,
        sink: Arc<dyn ListenSink>,
        metrics: Metrics,
        log: Logger,
    ) -> Self {
        let shared = Arc::new(ListenShared {
            state: Mutex::new(ListenState { desired: channels.into_iter().collect(), ..Default::default() }),
            changed: Condvar::new(),
            wake: Notify::new(),
        });
        let stop = CancellationToken::new();
        runtime().spawn(listen_loop(spec, app.to_owned(), shared.clone(), sink, metrics, log, stop.clone()));
        Self { shared, stop }
    }

    pub fn listen(&self, channel: &str) {
        if self.shared.lock().desired.insert(channel.to_owned()) {
            self.shared.wake.notify_one();
        }
    }

    pub fn unlisten(&self, channel: &str) {
        if self.shared.lock().desired.remove(channel) {
            self.shared.wake.notify_one();
        }
    }

    /// A handle to wait on the connection's state without holding any lock.
    pub fn waiter(&self) -> ListenWaiter {
        ListenWaiter(self.shared.clone())
    }

    pub fn shutdown(&self) {
        self.stop.cancel();
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Waits for a [`Listener`]'s state (cheap to clone; holds no lock).
#[derive(Clone)]
pub(crate) struct ListenWaiter(Arc<ListenShared>);

impl ListenWaiter {
    /// Waits up to `timeout` until `channel` is listened to (across
    /// reconnects); false at once when it is not wanted.
    pub fn wait_listening(&self, channel: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        blocking(|| {
            let mut st = self.0.lock();
            loop {
                if st.confirmed.contains(channel) {
                    return true;
                }
                if !st.desired.contains(channel) {
                    return false;
                }
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                st = self.0.changed.wait_timeout(st, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
            }
        })
    }

    /// Waits up to `timeout` until connected with every channel listened to.
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        blocking(|| {
            let mut st = self.0.lock();
            loop {
                if st.connected && st.confirmed == st.desired {
                    return true;
                }
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                st = self.0.changed.wait_timeout(st, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
            }
        })
    }
}

/// `LISTEN "a"; UNLISTEN "b"` bringing `confirmed` to `desired`.
fn sync_sql(desired: &BTreeSet<String>, confirmed: &BTreeSet<String>) -> String {
    let mut sql = String::new();
    for c in desired.difference(confirmed) {
        sql.push_str(&format!("LISTEN {};", super::quote(c)));
    }
    for c in confirmed.difference(desired) {
        sql.push_str(&format!("UNLISTEN {};", super::quote(c)));
    }
    sql
}

/// The statements bringing the connection to the desired channels, and the
/// desired set they reach. Channels about to be unlistened leave `confirmed`
/// first, so nothing waits on a channel while its `UNLISTEN` is in flight.
fn begin_sync(st: &mut ListenState) -> (String, BTreeSet<String>) {
    let sql = sync_sql(&st.desired, &st.confirmed);
    let desired = st.desired.clone();
    st.confirmed.retain(|c| desired.contains(c));
    (sql, desired)
}

async fn listen_loop(
    spec: Arc<ConnectSpec>,
    app: String,
    shared: Arc<ListenShared>,
    sink: Arc<dyn ListenSink>,
    metrics: Metrics,
    log: Logger,
    stop: CancellationToken,
) {
    let mut attempt: u32 = 0;
    let mut ever_connected = false;
    loop {
        let (tx, mut rx) = mpsc::channel(NOTICES_CAPACITY);
        let client = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            c = spec.connect(&app, Some(tx)) => c,
        };
        let client = match client {
            Ok(c) => c,
            Err(e) => {
                metrics.error();
                let delay = backoff(attempt);
                attempt = attempt.saturating_add(1);
                log_warn!(log, "failed to connect the LISTEN connection, retrying", "error" => e.0, "retry_in_ms" => i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
                tokio::select! {
                    biased;
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(delay) => continue,
                }
            }
        };
        let connected_at = Instant::now();
        let reconnected = ever_connected;
        if reconnected {
            metrics.reconnect();
            log_info!(log, "LISTEN connection restored");
        }
        ever_connected = true;

        let lost = serve(&client, &mut rx, &shared, sink.as_ref(), spec.keepalive, &stop, reconnected).await;
        {
            let mut st = shared.lock();
            st.connected = false;
            st.confirmed.clear();
        }
        shared.changed.notify_all();
        let Some(reason) = lost else { return };
        metrics.error();
        // Only a connection that stayed up resets the backoff, so a server
        // that closes sessions right after they connect isn't hammered.
        if connected_at.elapsed() >= STABLE_CONNECTION {
            attempt = 0;
        }
        let delay = backoff(attempt);
        attempt = attempt.saturating_add(1);
        log_error!(log, "LISTEN connection lost, reconnecting", "error" => reason, "retry_in_ms" => i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

/// Runs one connection: keeps `LISTEN` in sync with the desired channels,
/// forwards notifications and sends heartbeats. Returns why it ended (`None`:
/// stopped).
async fn serve(
    client: &Client,
    rx: &mut mpsc::Receiver<(String, String)>,
    shared: &ListenShared,
    sink: &dyn ListenSink,
    keepalive: Option<Duration>,
    stop: &CancellationToken,
    reconnected: bool,
) -> Option<String> {
    let mut announced = false;
    let mut next_ping = keepalive.map(|k| tokio::time::Instant::now() + k);
    loop {
        let (sql, desired) = begin_sync(&mut shared.lock());
        if !sql.is_empty() {
            match tokio::time::timeout(QUERY_TIMEOUT, client.batch_execute(&sql)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Some(error_text(&e)),
                Err(_) => return Some("LISTEN timed out".to_owned()),
            }
        }
        {
            let mut st = shared.lock();
            st.confirmed = desired;
            st.connected = true;
        }
        shared.changed.notify_all();
        if !announced {
            announced = true;
            sink.connected(reconnected);
        }
        let ping = async {
            match next_ping {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => return None,
            n = rx.recv() => match n {
                Some((channel, payload)) => sink.notification(&channel, &payload),
                None => return Some("connection closed".to_owned()),
            },
            () = shared.wake.notified() => {}
            () = ping => {
                let period = keepalive.unwrap_or(QUERY_TIMEOUT);
                match tokio::time::timeout(period.min(QUERY_TIMEOUT), client.simple_query("SELECT 1")).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Some(error_text(&e)),
                    Err(_) => return Some("heartbeat timed out".to_owned()),
                }
                next_ping = keepalive.map(|k| tokio::time::Instant::now() + k);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sslmode_is_taken_out_of_uris() {
        let p = parse_url("postgres://u:p@db:5432/app?sslmode=verify-full&application_name=x").unwrap();
        assert_eq!(SslMode::VerifyFull, p.sslmode);
        assert_eq!("postgres://u:p@db:5432/app?application_name=x&sslmode=require", p.conninfo);

        let p = parse_url("postgresql://db/app?sslrootcert=%2Fetc%2Fca.pem&sslmode=verify-ca").unwrap();
        assert_eq!((SslMode::VerifyCa, Some("/etc/ca.pem".to_owned())), (p.sslmode, p.sslrootcert));

        let p = parse_url("postgres://db/app").unwrap();
        assert_eq!((SslMode::Prefer, "postgres://db/app?sslmode=prefer"), (p.sslmode, p.conninfo.as_str()));

        let p = parse_url("postgres://db/app?sslmode=disable").unwrap();
        assert_eq!(SslMode::Disable, p.sslmode);

        assert!(parse_url("postgres://db/app?sslmode=bogus").is_err());
    }

    #[test]
    fn sslmode_is_taken_out_of_key_value_strings() {
        let p = parse_url("host=db user=u password=p dbname=app sslmode=verify-full sslrootcert=/ca.pem").unwrap();
        assert_eq!(SslMode::VerifyFull, p.sslmode);
        assert_eq!(Some("/ca.pem".to_owned()), p.sslrootcert);
        assert_eq!("host=db user=u password=p dbname=app sslmode=require", p.conninfo);
    }

    fn parsed(url: &str) -> (SslMode, tokio_postgres::Config) {
        let p = parse_url(url).unwrap_or_else(|e| panic!("{url}: {e}"));
        let config = p.conninfo.parse().unwrap_or_else(|e| panic!("{url}: {e}"));
        (p.sslmode, config)
    }

    #[test]
    fn passwords_with_reserved_characters_parse() {
        let (mode, c) = parsed("postgres://u:pa?ss/w@rd@db:5432/app?sslmode=verify-full");
        assert_eq!(SslMode::VerifyFull, mode);
        assert_eq!(Some("u"), c.get_user());
        assert_eq!(Some(&b"pa?ss/w@rd"[..]), c.get_password());
        assert_eq!("db:5432/app", describe(&c));

        let (mode, c) = parsed("postgres://u:p%40ss@db/app");
        assert_eq!(SslMode::Prefer, mode);
        assert_eq!(Some(&b"p@ss"[..]), c.get_password());
    }

    #[test]
    fn quoted_key_value_strings_parse() {
        let (mode, c) = parsed(
            r"host=db password='with space \'quote\'' dbname = app sslmode='verify-ca' sslrootcert='/etc/my ca.pem'",
        );
        assert_eq!(SslMode::VerifyCa, mode);
        assert_eq!(Some(&b"with space 'quote'"[..]), c.get_password());
        assert_eq!(Some("app"), c.get_dbname());
        let p = parse_url(r"host=db sslrootcert='/etc/my ca.pem'").unwrap();
        assert_eq!(Some("/etc/my ca.pem".to_owned()), p.sslrootcert);
    }

    #[test]
    fn verification_is_secure_by_default() {
        assert_eq!(Verify::Full, verification(SslMode::Require, true));
        assert_eq!(Verify::Full, verification(SslMode::Prefer, true));
        assert_eq!(Verify::None, verification(SslMode::Require, false));
        assert_eq!(Verify::Full, verification(SslMode::VerifyFull, false), "explicit verify-full wins");
        assert_eq!(Verify::Chain, verification(SslMode::VerifyCa, false));
    }

    #[test]
    fn descriptions_never_contain_credentials() {
        let mut c = PostgresConfig::new();
        for url in [
            "postgres://cabby:s3cret@db.internal:6432/app?sslmode=disable",
            "postgres://cabby@db.internal:6432/app?password=s3cret&sslmode=disable",
            "host=db.internal port=6432 user=cabby password=s3cret dbname=app sslmode=disable",
        ] {
            c.url = url.into();
            let spec = ConnectSpec::from_config(&c, "--pubsub=postgres").unwrap();
            assert_eq!("db.internal:6432/app", spec.description, "{url}");
            assert!(!spec.description.contains("s3cret") && !spec.description.contains("cabby"));
        }
    }

    #[test]
    fn errors_never_contain_credentials() {
        let mut c = PostgresConfig::new();
        c.url = "postgres://cabby:s3cret@db/app?bogus_option=1".into();
        let err = ConnectSpec::from_config(&c, "--pubsub=postgres").err().unwrap();
        assert!(err.starts_with("invalid --postgres_url"), "{err}");
        assert!(!err.contains("s3cret"), "{err}");

        c.url = "postgres://cabby:s3cret@db/app?sslmode=nope".into();
        let err = ConnectSpec::from_config(&c, "--pubsub=postgres").err().unwrap();
        assert!(!err.contains("s3cret"), "{err}");
    }

    #[test]
    fn an_empty_url_is_reported_with_the_flag() {
        let err = ConnectSpec::from_config(&PostgresConfig::new(), "--broker=postgres").err().unwrap();
        assert_eq!(
            "--broker=postgres needs a connection URL: set --postgres_url, CABBY_POSTGRES_URL or [postgres] url",
            err
        );
    }

    #[test]
    fn a_missing_ca_file_is_reported() {
        let mut c = PostgresConfig::new();
        c.url = "postgres://db/app?sslmode=verify-full".into();
        c.tls_ca_cert_path = "/nonexistent/ca.pem".into();
        let err = ConnectSpec::from_config(&c, "--pubsub=postgres").err().unwrap();
        assert!(err.contains("--postgres_tls_ca_cert_path"), "{err}");
    }

    #[test]
    fn missing_system_roots_name_the_settings() {
        let err = roots(Vec::new(), "").err().unwrap();
        assert!(err.contains("--postgres_tls_ca_cert_path") && err.contains("--postgres_tls_verify=false"), "{err}");
    }

    #[test]
    fn sslmode_disable_needs_no_roots() {
        let mut c = PostgresConfig::new();
        c.url = "postgres://db/app?sslmode=disable".into();
        c.tls_ca_cert_path = "/nonexistent/ca.pem".into();
        assert!(ConnectSpec::from_config(&c, "--pubsub=postgres").is_ok(), "no TLS: no CA is read");
    }

    #[test]
    fn listen_sync_quotes_channels() {
        let desired: BTreeSet<String> = ["a_1".to_owned(), "a_cmd".to_owned()].into();
        let confirmed: BTreeSet<String> = ["a_2".to_owned(), "a_cmd".to_owned()].into();
        assert_eq!("LISTEN \"a_1\";UNLISTEN \"a_2\";", sync_sql(&desired, &confirmed));
    }

    #[test]
    fn unlistened_channels_leave_confirmed_before_the_unlisten_is_sent() {
        let mut st = ListenState {
            desired: ["a".to_owned()].into(),
            confirmed: ["a".to_owned(), "b".to_owned()].into(),
            connected: true,
        };
        let (sql, desired) = begin_sync(&mut st);
        assert_eq!("UNLISTEN \"b\";", sql);
        let only_a: BTreeSet<String> = ["a".to_owned()].into();
        assert_eq!(only_a, st.confirmed, "b is no longer reported as listened to");
        assert_eq!(only_a, desired);
    }

    #[test]
    fn a_full_inbox_drops_and_counts() {
        let registry = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        let (inbox, mut rx) = Inbox::new(2, Metrics::register(registry.clone()), Logger::default());
        for i in 0..5 {
            inbox.push(i);
        }
        assert_eq!(3, registry.counter(super::super::DROPPED).unwrap().value());
        assert_eq!((Ok(0), Ok(1)), (rx.try_recv(), rx.try_recv()), "the first ones are kept");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_cancelled_connect_frees_its_slot() {
        // A server that accepts connections and never answers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for s in listener.incoming().flatten() {
                held.push(s);
            }
        });
        let mut c = PostgresConfig::new();
        c.url = format!("postgres://u@127.0.0.1:{port}/db?sslmode=disable");
        let pool = Pool::new(Arc::new(ConnectSpec::from_config(&c, "test").unwrap()), "test", 1, Metrics::none());
        let p = pool.clone();
        let connecting = block_on(async move {
            let far = tokio::time::Instant::now() + Duration::from_secs(30);
            let cancelled = tokio::time::timeout(Duration::from_millis(200), p.get(far)).await;
            assert!(cancelled.is_err(), "the connect was still running");
            lock(&p.slots[0]).connecting
        });
        assert!(!connecting, "a dropped connect must free its slot");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(Duration::from_millis(250), backoff(0));
        assert!(backoff(1) >= Duration::from_secs(2) && backoff(1) <= Duration::from_secs(3));
        assert!(backoff(30) <= MAX_BACKOFF);
    }
}
