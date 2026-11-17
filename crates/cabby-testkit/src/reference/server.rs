//! Facade for upstream package `server`.
//!
//! Requests are described by [`HttpRequest`]; the Go arm rebuilds them with
//! `httptest.NewRequest(method, target)` plus `Header.Set` for each header (in
//! order) and the TLS flag, so the request host and peer address are httptest's
//! (`example.com` or the absolute URL's host, `192.0.2.1:1234`). A request with
//! neither host nor peer address is rebuilt with `http.NewRequest` instead.

use std::collections::HashMap;

use cabby_server::server::{
    self as rs, Config, DefaultHeadersExtractor, HeadersExtractor, HttpRequest, HttpResponse, RequestError,
    RequestInfo as RsRequestInfo, SslConfig, TomlError,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::reference::common::GoParsed;
use crate::{Backend, backend, bridge};

/// A request without host and peer address is what `http.NewRequest` builds (the
/// bridge then uses it instead of `httptest.NewRequest`).
fn go_request(req: &HttpRequest) -> Value {
    let plain = req.host.is_empty() && req.remote_addr.is_empty();
    json!({"Method": req.method, "Target": req.target, "Headers": req.headers, "TLS": req.tls, "Plain": plain})
}

// --- Config -------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoSslConfig {
    cert_path: String,
    key_path: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoServerConfig {
    host: String,
    port: i64,
    allowed_origins: String,
    max_conn: i64,
    health_path: String,
    #[serde(rename = "SSL")]
    ssl: GoSslConfig,
}

impl From<&SslConfig> for GoSslConfig {
    fn from(c: &SslConfig) -> Self {
        GoSslConfig { cert_path: c.cert_path.clone(), key_path: c.key_path.clone() }
    }
}

impl From<GoSslConfig> for SslConfig {
    fn from(g: GoSslConfig) -> Self {
        SslConfig { cert_path: g.cert_path, key_path: g.key_path }
    }
}

impl From<&Config> for GoServerConfig {
    fn from(c: &Config) -> Self {
        GoServerConfig {
            host: c.host.clone(),
            port: c.port,
            allowed_origins: c.allowed_origins.clone(),
            max_conn: c.max_conn,
            health_path: c.health_path.clone(),
            ssl: (&c.ssl).into(),
        }
    }
}

impl From<GoServerConfig> for Config {
    fn from(g: GoServerConfig) -> Self {
        Config {
            host: g.host,
            port: g.port,
            allowed_origins: g.allowed_origins,
            max_conn: g.max_conn,
            health_path: g.health_path,
            ssl: g.ssl.into(),
        }
    }
}

/// `server.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoServerConfig>("server.NewConfig", json!({})).into(),
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("server.Config.ToToml", json!({"Config": GoServerConfig::from(c)})),
    }
}

/// `toml.Decode(s, &conf)`.
pub fn config_decode_toml(c: &mut Config, toml: &str) -> Result<(), TomlError> {
    match backend() {
        Backend::Rust => c.decode_toml(toml),
        Backend::Go => {
            let p: GoParsed<GoServerConfig> =
                bridge::call("server.Config.DecodeToml", json!({"Config": GoServerConfig::from(&*c), "Toml": toml}));
            *c = p.result.expect("decoded config").into();
            p.error.map_or(Ok(()), |e| Err(TomlError(e)))
        }
    }
}

/// `server.NewSSLConfig()`.
pub fn new_ssl_config() -> SslConfig {
    match backend() {
        Backend::Rust => SslConfig::new(),
        Backend::Go => bridge::call::<_, GoSslConfig>("server.NewSSLConfig", json!({})).into(),
    }
}

/// `config.Available()`.
pub fn ssl_available(c: &SslConfig) -> bool {
    match backend() {
        Backend::Rust => c.available(),
        Backend::Go => bridge::call("server.SSLConfig.Available", json!({"Config": GoSslConfig::from(c)})),
    }
}

// --- Handlers and requests ------------------------------------------------------------

/// `http.HandlerFunc(HealthHandler).ServeHTTP(recorder, req)`.
pub fn health_handler(req: &HttpRequest) -> HttpResponse {
    match backend() {
        Backend::Rust => rs::health_handler(req),
        Backend::Go => {
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            struct Out {
                code: u16,
                body: String,
            }
            let out: Out = bridge::call("server.HealthHandler", json!({"Request": go_request(req)}));
            HttpResponse { status: out.code, body: out.body.into_bytes() }
        }
    }
}

/// `extractor.FromRequest(req)`.
pub fn headers_from_request(extractor: &DefaultHeadersExtractor, req: &HttpRequest) -> HashMap<String, String> {
    match backend() {
        Backend::Rust => extractor.extract_headers(req),
        Backend::Go => bridge::call(
            "server.DefaultHeadersExtractor.FromRequest",
            json!({
                "Extractor": {
                    "AuthHeader": extractor.auth_header,
                    "Headers": extractor.headers,
                    "Cookies": extractor.cookies,
                },
                "Request": go_request(req),
            }),
        ),
    }
}

/// `FetchUID(req)`.
pub fn fetch_uid(req: &HttpRequest) -> Result<String, RequestError> {
    match backend() {
        Backend::Rust => rs::fetch_uid(req),
        Backend::Go => {
            let p: GoParsed<String> = bridge::call("server.FetchUID", json!({"Request": go_request(req)}));
            p.into_result(RequestError).map(Option::unwrap_or_default)
        }
    }
}

/// A `RequestInfo` built by `NewRequestInfo(req, nil)` (or blank, `RequestInfo{}`).
pub enum RequestInfo {
    Rust(RsRequestInfo),
    /// Go keeps lookups behind unexported maps: each lookup rebuilds the info from
    /// the same request in the bridge (`None`: blank).
    Go {
        request: Option<HttpRequest>,
        url: String,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoRequestInfo {
    #[serde(rename = "URL")]
    url: String,
    params: HashMap<String, String>,
    #[serde(rename = "AnyCableHeaders")]
    anycable_headers: HashMap<String, String>,
}

fn go_request_info(
    req: Option<&HttpRequest>,
    params: &[&str],
    headers: &[&str],
) -> Result<GoRequestInfo, RequestError> {
    let p: GoParsed<GoRequestInfo> = bridge::call(
        "server.RequestInfo",
        json!({"Request": req.map(go_request), "Params": params, "AnyCableHeaders": headers}),
    );
    p.into_result(RequestError).map(|r| r.expect("request info"))
}

impl RequestInfo {
    /// `NewRequestInfo(req, nil)`.
    pub fn new(req: &HttpRequest) -> Result<RequestInfo, RequestError> {
        match backend() {
            Backend::Rust => RsRequestInfo::new(req, None).map(RequestInfo::Rust),
            Backend::Go => {
                let info = go_request_info(Some(req), &[], &[])?;
                Ok(RequestInfo::Go { request: Some(req.clone()), url: info.url })
            }
        }
    }

    /// `RequestInfo{}`.
    pub fn blank() -> RequestInfo {
        match backend() {
            Backend::Rust => RequestInfo::Rust(RsRequestInfo::default()),
            Backend::Go => RequestInfo::Go { request: None, url: String::new() },
        }
    }

    pub fn url(&self) -> String {
        match self {
            RequestInfo::Rust(i) => i.url.clone(),
            RequestInfo::Go { url, .. } => url.clone(),
        }
    }

    pub fn param(&self, key: &str) -> String {
        match self {
            RequestInfo::Rust(i) => i.param(key),
            RequestInfo::Go { request, .. } => {
                let mut info = go_request_info(request.as_ref(), &[key], &[]).expect("request info");
                info.params.remove(key).unwrap_or_default()
            }
        }
    }

    pub fn anycable_header(&self, key: &str) -> String {
        match self {
            RequestInfo::Rust(i) => i.anycable_header(key),
            RequestInfo::Go { request, .. } => {
                let mut info = go_request_info(request.as_ref(), &[], &[key]).expect("request info");
                info.anycable_headers.remove(key).unwrap_or_default()
            }
        }
    }
}
