//! HTTP server helpers (upstream package `server`).

use std::collections::HashMap;

use cabby_core::pending;
use cabby_core::toml_config::go_quote;

pub use cabby_core::logger::TomlError;

/// Whether an `Origin` header value matches one of the allowed host patterns
/// (exact host, `*` for any, `*.example.com` or `*example.com` wildcards).
///
/// Both `*.example.com` and `*example.com` match subdomains (the latter also
/// matches the apex), but never a different domain sharing the suffix. The
/// origin's host keeps its port, as Go's `url.URL.Host` does.
pub fn origin_allowed(origin: &str, hosts: &[&str]) -> bool {
    let origin = origin.to_lowercase();
    let Some(host) = url_host(&origin) else {
        return false;
    };

    for pattern in hosts {
        let pattern = pattern.trim().to_lowercase();
        if pattern.is_empty() {
            continue;
        }
        if host == pattern {
            return true;
        }
        let Some(suffix) = pattern.strip_prefix('*') else {
            continue;
        };
        if suffix.is_empty() {
            return true;
        }
        if suffix.starts_with('.') {
            if host.ends_with(suffix) {
                return true;
            }
        } else if host == suffix || host.ends_with(&format!(".{suffix}")) {
            return true;
        }
    }
    false
}

/// The non-empty host (with port) of an absolute URL; `None` where Go's
/// `url.Parse` fails or yields no host.
fn url_host(raw: &str) -> Option<&str> {
    let raw = raw.split('#').next().unwrap_or_default();
    // Scheme: a letter, then letters, digits, `+`, `-` or `.`, then `:`.
    let colon = raw.find(':')?;
    let mut scheme = raw[..colon].chars();
    if !scheme.next().is_some_and(|c| c.is_ascii_alphabetic())
        || !scheme.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        return None;
    }
    let rest = raw[colon + 1..].split('?').next().unwrap_or_default();
    let authority = rest.strip_prefix("//")?.split('/').next().unwrap_or_default();
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    (!host.is_empty() && valid_host(host)).then_some(host)
}

/// Go `parseHost` plus host-mode unescaping: an optional numeric port, and
/// only characters a URL host may carry unescaped.
fn valid_host(host: &str) -> bool {
    let (name, port) = if host.starts_with('[') {
        match host.find(']') {
            Some(end) => host.split_at(end + 1),
            None => return false,
        }
    } else {
        match host.rfind(':') {
            Some(i) => host.split_at(i),
            None => (host, ""),
        }
    };
    let port_ok = port.is_empty() || port.strip_prefix(':').is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit()));
    port_ok && name.bytes().all(|b| b >= 0x80 || b.is_ascii_alphanumeric() || b"-_.~!$&'()*+,;=:[]<>\"".contains(&b))
}

// --- Configuration ------------------------------------------------------------

/// TLS certificate settings (upstream `server.SSLConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SslConfig {
    pub cert_path: String,
    pub key_path: String,
}

cabby_core::toml_section!(SslConfig, "server.SSLConfig" {
    "cert_path" => cert_path,
    "key_path" => key_path,
});

impl SslConfig {
    /// Upstream `NewSSLConfig`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether both the certificate and the key are configured.
    pub fn available(&self) -> bool {
        !self.cert_path.is_empty() && !self.key_path.is_empty()
    }
}

/// HTTP server settings (upstream `server.Config`, TOML table `[server]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub host: String,
    pub port: i64,
    pub allowed_origins: String,
    pub max_conn: i64,
    pub health_path: String,
    pub ssl: SslConfig,
}

cabby_core::toml_section!(Config, "server.Config" {
    "host" => host,
    "port" => port,
    "allowed_origins" => allowed_origins,
    "max_conn" => max_conn,
    "health_path" => health_path,
    "ssl" => ssl,
});

impl Config {
    /// Defaults (upstream `server.NewConfig`).
    pub fn new() -> Self {
        Self {
            host: "localhost".to_owned(),
            port: 8080,
            health_path: "/health".to_owned(),
            ssl: SslConfig::new(),
            ..Default::default()
        }
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Host address to bind to\n");
        out.push_str(&format!("host = {}\n", go_quote(&self.host)));
        out.push_str("# Port to listen on\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# Allowed origins (a comma-separated list)\n");
        out.push_str(&format!("allowed_origins = \"{}\"\n", self.allowed_origins));
        out.push_str("# Maximum number of allowed concurrent connections\n");
        if self.max_conn == 0 {
            out.push_str("# max_conn = 1000\n");
        } else {
            out.push_str(&format!("max_conn = {}\n", self.max_conn));
        }
        out.push_str("# Health check endpoint path\n");
        out.push_str(&format!("health_path = {}\n", go_quote(&self.health_path)));
        out.push_str("# SSL configuration\n");
        if self.ssl.cert_path.is_empty() {
            out.push_str("# ssl.cert_path =\n");
        } else {
            out.push_str(&format!("ssl.cert_path = {}\n", go_quote(&self.ssl.cert_path)));
        }
        if self.ssl.key_path.is_empty() {
            out.push_str("# ssl.key_path =\n");
        } else {
            out.push_str(&format!("ssl.key_path = {}\n", go_quote(&self.ssl.key_path)));
        }
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| TomlError(e.0))?;
        Ok(())
    }
}

// --- Requests -----------------------------------------------------------------

/// The parts of an incoming HTTP request the server inspects.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpRequest {
    pub method: String,
    /// The request target as received: a path (`/cable?x=1`) or an absolute URL.
    pub target: String,
    /// The `Host` the request was addressed to.
    pub host: String,
    /// Header fields in arrival order; names are case-insensitive.
    pub headers: Vec<(String, String)>,
    /// Peer address, `ip:port`.
    pub remote_addr: String,
    /// Whether the request arrived over TLS.
    pub tls: bool,
}

/// A response produced by a plain HTTP handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Upstream `HealthHandler`.
pub fn health_handler(_req: &HttpRequest) -> HttpResponse {
    pending!("server::health_handler")
}

/// Extracts the headers forwarded to the RPC application (upstream `HeadersExtractor`;
/// `extract_headers` is upstream `FromRequest`).
pub trait HeadersExtractor {
    fn extract_headers(&self, req: &HttpRequest) -> HashMap<String, String>;
}

/// Upstream `DefaultHeadersExtractor`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DefaultHeadersExtractor {
    pub auth_header: String,
    pub headers: Vec<String>,
    pub cookies: Vec<String>,
}

impl HeadersExtractor for DefaultHeadersExtractor {
    fn extract_headers(&self, _req: &HttpRequest) -> HashMap<String, String> {
        pending!("server::headers_extractor_from_request")
    }
}

/// A request-handling failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RequestError(pub String);

/// Connection request data (upstream `server.RequestInfo`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestInfo {
    pub uid: String,
    pub url: String,
    pub headers: Option<HashMap<String, String>>,
    _anycable_headers: Option<HashMap<String, String>>,
    _params: Option<HashMap<String, String>>,
}

impl RequestInfo {
    /// Upstream `NewRequestInfo` (`extractor: None` collects no headers).
    pub fn new(_req: &HttpRequest, _extractor: Option<&dyn HeadersExtractor>) -> Result<RequestInfo, RequestError> {
        pending!("server::new_request_info")
    }

    /// The last value of a URL query parameter (`""` when missing).
    pub fn param(&self, _key: &str) -> String {
        pending!("server::request_info_param")
    }

    /// The value of an `X-AnyCable-*` header, case-insensitively (`""` when missing).
    pub fn anycable_header(&self, _key: &str) -> String {
        pending!("server::request_info_anycable_header")
    }
}

/// The connection UID: a valid `X-Request-ID`, or a generated one (upstream `FetchUID`).
pub fn fetch_uid(_req: &HttpRequest) -> Result<String, RequestError> {
    pending!("server::fetch_uid")
}
