//! Redis connection configuration and the Redis Streams consumer (upstream
//! package `redis`).
//!
//! Connections use the `redis` crate (see [`connect`]); the pub/sub
//! subscription engine with liveness checks (deviation D-003) is in
//! [`subscription`].

use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub(crate) mod connect;
pub(crate) mod publisher;
mod streamer;
pub(crate) mod subscription;
pub(crate) mod url;

pub use streamer::{StreamHandler, Streamer, StreamerOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisError(pub String);

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RedisError {}

/// rueidis `DefaultTCPKeepAlive`.
const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(1);

/// Redis adapter configuration (upstream `redis.RedisConfig`).
///
/// `Default` is Go's zero value (`RedisConfig{}`); upstream defaults come from
/// [`RedisConfig::new`]. The resolved host list is derived state, filled in by
/// [`RedisConfig::to_client_options`] as upstream does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    /// Instance URL, sentinel master name, or comma-separated cluster URLs.
    pub url: String,
    /// Comma-separated sentinel addresses.
    pub sentinels: String,
    /// Seconds.
    pub sentinel_discovery_interval: i64,
    /// Seconds.
    pub keepalive_ping_interval: i64,
    pub tls_verify: bool,
    pub tls_ca_cert_path: String,
    pub tls_client_cert_path: String,
    pub tls_client_key_path: String,
    pub max_reconnect_attempts: i64,
    pub disable_cache: bool,
    hosts: Vec<String>,
    /// For each of `hosts`: whether it comes from a URL that may hold part of a
    /// password (D-093).
    hidden_hosts: Vec<bool>,
    sentinel_master: String,
}

/// Client options derived from a [`RedisConfig`] (the fields upstream tests read
/// from `rueidis.ClientOption`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientOptions {
    pub init_address: Vec<String>,
    pub select_db: i64,
    pub username: String,
    pub password: String,
    pub keep_alive: Duration,
    pub dial_timeout: Duration,
    pub shuffle_init: bool,
    /// `None`: plain TCP (upstream `TLSConfig == nil`).
    pub tls: Option<TlsOptions>,
    pub sentinel: SentinelOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsOptions {
    pub insecure_skip_verify: bool,
    /// Number of loaded client certificates (mutual TLS).
    pub certificates: usize,
    /// Whether a custom root CA pool is configured.
    pub root_cas: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SentinelOptions {
    pub master_set: String,
    pub username: String,
    pub password: String,
}

cabby_core::toml_section!(RedisConfig, "redis.RedisConfig" {
    "url" => url,
    "sentinels" => sentinels,
    "sentinel_discovery_interval" => sentinel_discovery_interval,
    "keepalive_ping_interval" => keepalive_ping_interval,
    "tls_verify" => tls_verify,
    "tls_ca_cert_path" => tls_ca_cert_path,
    "tls_client_cert_path" => tls_client_cert_path,
    "tls_client_key_path" => tls_client_key_path,
    "max_reconnect_attempts" => max_reconnect_attempts,
    "disable_cache" => disable_cache,
});

impl RedisConfig {
    /// Upstream `redis.NewRedisConfig()` (defaults).
    pub fn new() -> Self {
        Self {
            url: "redis://localhost:6379".to_owned(),
            keepalive_ping_interval: 30,
            sentinel_discovery_interval: 30,
            tls_verify: false,
            max_reconnect_attempts: 5,
            disable_cache: false,
            ..Default::default()
        }
    }

    pub fn is_cluster(&self) -> bool {
        self.hosts.len() > 1 && !self.is_sentinel()
    }

    pub fn is_sentinel(&self) -> bool {
        !self.sentinels.is_empty() || !self.sentinel_master.is_empty()
    }

    /// Whether connections use TLS without verifying the server's certificate.
    pub fn tls_verification_skipped(&self) -> bool {
        !self.tls_verify && self.tls_used().unwrap_or_else(|| self.tls_requested())
    }

    /// Whether some URL asks for TLS but connections to Redis do not use it: the
    /// first URL decides for all, and with sentinels, the first sentinel's.
    pub fn tls_request_unused(&self) -> bool {
        self.tls_requested() && self.tls_used() == Some(false)
    }

    /// Whether connections to Redis use TLS; `None` when the settings are invalid.
    fn tls_used(&self) -> Option<bool> {
        self.connect_spec().ok().map(|spec| spec.tls.is_some())
    }

    /// Whether the scheme of some URL (of Redis or a sentinel) asks for TLS.
    fn tls_requested(&self) -> bool {
        [&self.url, &self.sentinels].iter().flat_map(|urls| urls.split(',')).any(|raw| {
            url::parse(&ensure_redis_scheme(raw)).is_ok_and(|u| matches!(u.scheme.as_str(), "rediss" | "valkeys"))
        })
    }

    pub fn hostnames(&self) -> Vec<String> {
        self.hosts.clone()
    }

    /// Whether the `i`-th host comes from a URL that may hold part of a password (D-093).
    fn host_hidden(&self, i: usize) -> bool {
        self.hidden_hosts.get(i).copied().unwrap_or(false)
    }

    /// [`Self::hostnames`] for logs: a host from a URL that may hold part of a
    /// password is shown as `***` (D-093).
    pub(crate) fn logged_hostnames(&self) -> Vec<String> {
        let hide = |(i, h): (usize, &String)| if self.host_hidden(i) { "***".to_owned() } else { h.clone() };
        self.hosts.iter().enumerate().map(hide).collect()
    }

    /// [`Self::hostname`] for logs, as [`Self::logged_hostnames`] (D-093).
    pub(crate) fn logged_hostname(&self) -> String {
        if self.is_sentinel() || !self.host_hidden(0) { self.hostname() } else { "***".to_owned() }
    }

    /// The sentinel master, or the first host (upstream panics without hosts;
    /// this returns `""`).
    pub fn hostname(&self) -> String {
        if self.is_sentinel() { self.sentinel_master.clone() } else { self.hosts.first().cloned().unwrap_or_default() }
    }

    pub fn tls_client_cert_available(&self) -> bool {
        !self.tls_client_cert_path.is_empty() && !self.tls_client_key_path.is_empty()
    }

    /// Upstream `ToRueidisOptions`; also records the resolved hosts.
    pub fn to_client_options(&mut self) -> Result<ClientOptions, RedisError> {
        self.resolve().map(|(options, _)| options)
    }

    /// What connections for this configuration are opened with (resolves a
    /// copy, so the configuration itself is not changed).
    pub(crate) fn connect_spec(&self) -> Result<connect::ConnectSpec, RedisError> {
        self.clone().resolve().map(|(_, spec)| spec)
    }

    fn resolve(&mut self) -> Result<(ClientOptions, connect::ConnectSpec), RedisError> {
        // Not `is_sentinel()`: an implicit sentinel URL (`?master_set=`) sets
        // `sentinel_master`, and must parse the same way every time (D-082).
        let (mut options, parsed) =
            if self.sentinels.is_empty() { parse_redis_url(&self.url)? } else { self.parse_sentinels()? };

        self.hosts = options.init_address.clone();
        self.hidden_hosts = parsed.hidden.clone();
        self.sentinel_master = options.sentinel.master_set.clone();

        options.keep_alive = Duration::from_secs(self.keepalive_ping_interval.max(0) as u64);
        options.shuffle_init = self.is_cluster();

        let mut tls_spec = None;
        if let Some(tls) = options.tls.as_mut() {
            tls.insecure_skip_verify = !self.tls_verify;
            let mut spec = connect::TlsSpec { insecure: !self.tls_verify, ..Default::default() };

            // Load CA certificate for server verification if configured
            if !self.tls_ca_cert_path.is_empty() {
                let pem = std::fs::read(&self.tls_ca_cert_path).map_err(|e| {
                    RedisError(format!(
                        "failed to read Redis CA certificate: {}",
                        go_path_error("open", &self.tls_ca_cert_path, &e)
                    ))
                })?;
                if !CertificateDer::pem_slice_iter(&pem).any(|c| c.is_ok()) {
                    return Err(RedisError("failed to parse Redis CA certificate".to_owned()));
                }
                tls.root_cas = true;
                spec.root_pem = Some(pem);
            }

            // Load client certificate for mutual TLS if configured
            if self.tls_client_cert_available() {
                let pair = load_key_pair(&self.tls_client_cert_path, &self.tls_client_key_path)
                    .map_err(|e| RedisError(format!("failed to load Redis client certificate: {e}")))?;
                tls.certificates = 1;
                spec.client = Some(pair);
            }
            tls_spec = Some(spec);
        }

        let addrs = options
            .init_address
            .iter()
            .enumerate()
            .map(|(i, a)| connect::Addr::parse_shown(a, parsed.unix, !self.host_hidden(i)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(RedisError)?;
        let topology = if self.is_sentinel() {
            connect::Topology::Sentinel {
                sentinels: addrs,
                master: options.sentinel.master_set.clone(),
                username: options.sentinel.username.clone(),
                password: options.sentinel.password.clone(),
            }
        } else if addrs.len() > 1 {
            connect::Topology::Cluster(addrs)
        } else {
            let addr = addrs.into_iter().next().ok_or_else(|| RedisError("no Redis address".to_owned()))?;
            connect::Topology::Single(addr)
        };
        // rueidis: `Dialer.KeepAlive` (1 s when unset) paces the PINGs on idle
        // connections, and `ConnWriteTimeout` (the URL's `write_timeout`, by
        // default ten keep-alive periods) bounds how long replies may take.
        let keepalive = match self.keepalive_ping_interval {
            0 => Some(DEFAULT_TCP_KEEPALIVE),
            n if n < 0 => None,
            n => Some(Duration::from_secs(n as u64)),
        };
        let write_timeout = if parsed.write_timeout.is_zero() {
            keepalive.unwrap_or(DEFAULT_TCP_KEEPALIVE).max(DEFAULT_TCP_KEEPALIVE) * 10
        } else {
            parsed.write_timeout
        };
        let spec = connect::ConnectSpec {
            topology,
            keepalive,
            write_timeout,
            username: options.username.clone(),
            password: options.password.clone(),
            db: options.select_db,
            tls: tls_spec,
            dial_timeout: (!options.dial_timeout.is_zero()).then_some(options.dial_timeout),
            client_name: parsed.client_name,
        };
        Ok((options, spec))
    }

    /// Upstream `parseSentinels`.
    fn parse_sentinels(&self) -> Result<(ClientOptions, url::ParsedRedisUrl), RedisError> {
        let sentinel_master = url::parse(&self.url).map_err(RedisError)?;

        let (mut options, parsed) = parse_redis_url(&self.sentinels)?;

        options.sentinel.master_set = sentinel_master.host;

        // Move sentinel creds to sentinel-specific fields
        options.sentinel.username = std::mem::take(&mut options.username);
        options.sentinel.password = std::mem::take(&mut options.password);

        // Restore data-node creds from main URL
        if let Ok((master_opts, _)) = parse_redis_url(&self.url) {
            options.username = master_opts.username;
            options.password = master_opts.password;
            if redis_url_has_db(&self.url) {
                options.select_db = master_opts.select_db;
            }
        }

        Ok((options, parsed))
    }

    pub fn to_toml(&self) -> String {
        fn optional(out: &mut String, key: &str, value: &str, placeholder: &str) {
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"{placeholder}\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }

        let mut out = String::new();
        out.push_str("# Redis instance URL or master name in case of sentinels usage\n");
        out.push_str("# or list of URLs if cluster usage\n");
        out.push_str(&format!("url = \"{}\"\n", self.url));
        out.push_str("# Sentinel addresses (comma-separated list)\n");
        out.push_str(&format!("sentinels = \"{}\"\n", self.sentinels));
        out.push_str("# Sentinel discovery interval (seconds)\n");
        out.push_str(&format!("sentinel_discovery_interval = {}\n", self.sentinel_discovery_interval));
        out.push_str("# Keepalive ping interval (seconds)\n");
        out.push_str(&format!("keepalive_ping_interval = {}\n", self.keepalive_ping_interval));
        out.push_str("# Enable TLS Verify\n");
        out.push_str(if self.tls_verify { "tls_verify = true\n" } else { "# tls_verify = true\n" });
        out.push_str("# Path to CA certificate file to verify Redis server\n");
        optional(&mut out, "tls_ca_cert_path", &self.tls_ca_cert_path, "/path/to/ca.pem");
        out.push_str("# Path to client TLS certificate file for mutual TLS\n");
        optional(&mut out, "tls_client_cert_path", &self.tls_client_cert_path, "/path/to/cert.pem");
        out.push_str("# Path to client TLS private key file for mutual TLS\n");
        optional(&mut out, "tls_client_key_path", &self.tls_client_key_path, "/path/to/key.pem");
        out.push_str("# Max number of reconnect attempts\n");
        out.push_str(&format!("max_reconnect_attempts = {}\n", self.max_reconnect_attempts));
        out.push_str("# Disable client-side caching\n");
        out.push_str(if self.disable_cache { "disable_cache = true\n" } else { "# disable_cache = true\n" });
        out.push('\n');
        out
    }

    /// Decodes TOML into `self`, keeping fields the document omits (upstream
    /// `toml.Decode(str, &config)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), RedisError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| RedisError(e.0))?;
        Ok(())
    }
}

/// Upstream `parseRedisURL`: comma-separated URLs, init addresses merged into
/// the first URL's options.
fn parse_redis_url(raw: &str) -> Result<(ClientOptions, url::ParsedRedisUrl), RedisError> {
    let mut first: Option<url::ParsedRedisUrl> = None;
    for (addr, shown) in cabby_core::utils::url_list(raw) {
        let current =
            url::parse_rueidis_listed(&ensure_redis_scheme(chomp_trailing_slash_hostname(addr)), shown.as_deref())
                .map_err(RedisError)?;
        match first.as_mut() {
            None => first = Some(current),
            Some(f) => {
                f.init_address.extend(current.init_address);
                f.hidden.extend(current.hidden);
            }
        }
    }
    let parsed = first.unwrap_or_default();
    let options = ClientOptions {
        init_address: parsed.init_address.clone(),
        select_db: parsed.select_db,
        username: parsed.username.clone(),
        password: parsed.password.clone(),
        dial_timeout: parsed.dial_timeout,
        tls: parsed.tls.then(|| TlsOptions { insecure_skip_verify: parsed.skip_verify, ..Default::default() }),
        sentinel: SentinelOptions { master_set: parsed.master_set.clone(), ..Default::default() },
        ..Default::default()
    };
    Ok((options, parsed))
}

/// Upstream `redisURLHasDB`.
fn redis_url_has_db(raw: &str) -> bool {
    let first = raw.split(',').next().unwrap_or("");
    match url::parse(&ensure_redis_scheme(chomp_trailing_slash_hostname(first))) {
        Ok(u) => !u.path.is_empty() && u.path != "/",
        Err(_) => false,
    }
}

/// Upstream `chompTrailingSlashHostname`.
fn chomp_trailing_slash_hostname(url: &str) -> &str {
    url.strip_suffix('/').unwrap_or(url)
}

/// Upstream `ensureRedisScheme`.
fn ensure_redis_scheme(url: &str) -> String {
    if url.contains("://") { url.to_owned() } else { format!("redis://{url}") }
}

/// Go's `*fs.PathError` text for common failures (`open <path>: no such file or directory`).
fn go_path_error(op: &str, path: &str, err: &std::io::Error) -> String {
    let reason = match err.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        std::io::ErrorKind::IsADirectory => "is a directory".to_owned(),
        _ => err.to_string(),
    };
    format!("{op} {path}: {reason}")
}

/// Upstream `tls.LoadX509KeyPair` (file reading and PEM checks); returns the
/// PEM certificate and key.
fn load_key_pair(cert_path: &str, key_path: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let cert = std::fs::read(cert_path).map_err(|e| go_path_error("open", cert_path, &e))?;
    let key = std::fs::read(key_path).map_err(|e| go_path_error("open", key_path, &e))?;
    if !CertificateDer::pem_slice_iter(&cert).any(|c| c.is_ok()) {
        return Err("tls: failed to find any PEM data in certificate input".to_owned());
    }
    if PrivateKeyDer::from_pem_slice(&key).is_err() {
        return Err("tls: failed to find any PEM data in key input".to_owned());
    }
    Ok((cert, key))
}

/// Upstream pub/sub start-up log line: `<prefix> (sentinels): [hosts]`,
/// `<prefix> (cluster): [hosts]` or `<prefix>: host`.
/// Hosts from a URL that may hold part of a password are shown as `***` (D-093).
pub(crate) fn pubsub_start_message(prefix: &str, config: &RedisConfig) -> String {
    let hosts = format!("[{}]", config.logged_hostnames().join(" "));
    if config.is_sentinel() {
        format!("{prefix} (sentinels): {hosts}")
    } else if config.is_cluster() {
        format!("{prefix} (cluster): {hosts}")
    } else {
        format!("{prefix}: {}", config.logged_hostname())
    }
}

/// An adapter's Redis settings, or upstream defaults when none are set
/// (upstream dereferences a nil pointer there).
pub(crate) fn or_defaults(config: Option<&RedisConfig>) -> RedisConfig {
    match config {
        Some(c) => c.clone(),
        None => RedisConfig::new(),
    }
}

/// Writes a log record with `context` (and further attributes).
pub(crate) fn log(level: cabby_core::logger::Level, context: &str, msg: &str, attrs: &[(&str, String)]) {
    use cabby_core::logger::{Attr, LogValue};
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("context", LogValue::String(context.to_owned())));
    all.extend(attrs.iter().map(|(k, v)| Attr::new(k, LogValue::String(v.clone()))));
    crate::metrics::log(level, msg, &all);
}

#[cfg(test)]
mod tls_warning_tests {
    use super::RedisConfig;

    fn config(url: &str, sentinels: &str) -> RedisConfig {
        RedisConfig { url: url.to_owned(), sentinels: sentinels.to_owned(), ..RedisConfig::new() }
    }

    /// Pins D-081: the warnings follow the TLS setting connections have.
    #[test]
    fn deviation_d081_tls_warnings_follow_the_parsed_urls() {
        for (url, sentinels, skipped, unused) in [
            ("redis://h", "", false, false),
            ("h:6379", "", false, false),
            ("rediss://h", "", true, false),
            ("valkeys://h", "", true, false),
            ("REDISS://h", "", true, false),
            ("valkey://h", "", false, false),
            ("rediss://h,redis://i", "", true, false),
            ("redis://h,rediss://i", "", false, true),
            ("redis://master", "rediss://s", true, false),
            ("redis://master", "VALKEYS://s", true, false),
            ("rediss://master", "rediss://s", true, false),
            ("rediss://master", "s:26379", false, true),
            ("Valkeys://master", "redis://s", false, true),
            ("redis://master", "s:26379,rediss://t", false, true),
            ("redis://u:p@h/1?x=rediss://", "", false, false),
        ] {
            let mut c = config(url, sentinels);
            assert_eq!(skipped, c.tls_verification_skipped(), "{url} {sentinels}");
            assert_eq!(unused, c.tls_request_unused(), "{url} {sentinels}");
            c.tls_verify = true;
            assert!(!c.tls_verification_skipped(), "{url} {sentinels}");
            assert_eq!(unused, c.tls_request_unused(), "{url} {sentinels}");
        }

        // Settings that cannot be used: the URLs decide.
        let mut c = config("rediss://h", "");
        c.tls_ca_cert_path = "/nonexistent/ca.pem".to_owned();
        assert!(c.tls_verification_skipped() && !c.tls_request_unused());
    }
}
