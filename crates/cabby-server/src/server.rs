//! HTTP server helpers (upstream package `server`).

use std::collections::HashMap;

use cabby_core::pending;

pub use cabby_core::logger::TomlError;

/// Whether an `Origin` header value matches one of the allowed host patterns
/// (exact host, `*` for any, `*.example.com` or `*example.com` wildcards).
pub fn origin_allowed(_origin: &str, _hosts: &[&str]) -> bool {
    pending!("server::origin_allowed")
}

// --- Configuration ------------------------------------------------------------

/// TLS certificate settings (upstream `server.SSLConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SslConfig {
    pub cert_path: String,
    pub key_path: String,
}

impl SslConfig {
    /// Upstream `NewSSLConfig`.
    pub fn new() -> Self {
        pending!("server::new_ssl_config")
    }

    /// Whether both the certificate and the key are configured.
    pub fn available(&self) -> bool {
        pending!("server::ssl_available")
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

impl Config {
    /// Defaults (upstream `server.NewConfig`).
    pub fn new() -> Self {
        pending!("server::new_config")
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("server::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), TomlError> {
        pending!("server::config_decode_toml")
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
