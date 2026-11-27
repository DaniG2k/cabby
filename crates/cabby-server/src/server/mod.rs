//! HTTP server (upstream package `server`): the hyper-based server shared by the
//! WebSocket, health, broadcast, API and metrics endpoints ([`HttpServer`],
//! [`for_port`]), request inspection ([`RequestInfo`], [`fetch_uid`], the headers
//! extractor), origin matching and CORS headers.

use cabby_core::toml_config::toml_quote;

mod http_server;
mod mounts;
mod request;
mod response_writer;
#[cfg(test)]
pub(crate) mod tests;

pub use mounts::{api_handler, metrics_handler};

pub use cabby_core::logger::TomlError;
pub use http_server::{
    BODY_READ_TIMEOUT, Body, BodyError, BoxError, ConnInfo, Handler, HttpServer, Request, Response, ResponseFuture,
    ServerDefaults, ServerError, client_closed, conn_info, current_defaults, empty, error_response, for_port, full,
    not_found, read_body, read_body_within, runtime_handle, set_defaults, status_response,
};
pub use request::{
    DefaultHeadersExtractor, HEALTH_MESSAGE, HeadersExtractor, HttpRequest, HttpResponse, NotUtf8, REMOTE_ADDR_HEADER,
    RequestError, RequestInfo, fetch_uid, health_handler, query_param, query_param_utf8, write_cors_headers,
};
pub use response_writer::{
    ChannelResponseWriter, MAX_BUFFERED, ResponseWriter, SharedResponseWriter, WRITE_TIMEOUT, channel_response,
    channel_response_for, lock_writer, shared_writer,
};

/// The health endpoint as a server handler (upstream `http.HandlerFunc(HealthHandler)`).
/// The handler sets no content type; Go's server sniffs the body, which gives
/// `text/plain; charset=utf-8`.
pub fn health_endpoint() -> impl Handler {
    |_req: Request| async {
        let mut resp = ::http::Response::new(full(HEALTH_MESSAGE));
        resp.headers_mut().insert(
            ::http::header::CONTENT_TYPE,
            ::http::HeaderValue::from_static(response_writer::sniff_content_type(HEALTH_MESSAGE)),
        );
        resp
    }
}

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
        out.push_str(&format!("host = {}\n", toml_quote(&self.host)));
        out.push_str("# Port to listen on\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# Allowed origins (a comma-separated list)\n");
        out.push_str(&format!("allowed_origins = {}\n", toml_quote(&self.allowed_origins)));
        out.push_str("# Maximum number of allowed concurrent connections\n");
        if self.max_conn == 0 {
            out.push_str("# max_conn = 1000\n");
        } else {
            out.push_str(&format!("max_conn = {}\n", self.max_conn));
        }
        out.push_str("# Health check endpoint path\n");
        out.push_str(&format!("health_path = {}\n", toml_quote(&self.health_path)));
        out.push_str("# SSL configuration\n");
        if self.ssl.cert_path.is_empty() {
            out.push_str("# ssl.cert_path =\n");
        } else {
            out.push_str(&format!("ssl.cert_path = {}\n", toml_quote(&self.ssl.cert_path)));
        }
        if self.ssl.key_path.is_empty() {
            out.push_str("# ssl.key_path =\n");
        } else {
            out.push_str(&format!("ssl.key_path = {}\n", toml_quote(&self.ssl.key_path)));
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
