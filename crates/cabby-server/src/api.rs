//! HTTP management API: publish and presence endpoints (upstream package `api`).
//!
//! Requests are modeled as plain values so handlers can be exercised without a
//! socket, as upstream does with `httptest.NewRecorder` and `ServeHTTP`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cabby_core::broker::{Broker, PresenceInfoOptions};
use cabby_core::logger::Logger;
use cabby_core::{log_debug, log_error};

/// Upstream `defaultAPIPath`.
pub const DEFAULT_API_PATH: &str = "/api";
/// Upstream `defaultMaxBodySize` (1 MB).
pub const DEFAULT_MAX_BODY_SIZE: i64 = 1_048_576;

/// Invalid TOML, a type mismatch while decoding, or a failed secret derivation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    pub message: String,
}

/// API server settings (upstream `api.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    /// Host to bind (only used when the port differs from the main server).
    pub host: String,
    /// 0: use the main server port.
    pub port: u16,
    /// Base path for the endpoints.
    pub path: String,
    /// Bearer token required by requests.
    pub secret: String,
    /// Secret used to derive `secret` when it is not set (not a TOML key upstream).
    pub secret_base: String,
    pub add_cors_headers: bool,
    /// Comma-separated CORS hosts.
    pub cors_hosts: String,
    /// Maximum request body size in bytes (0: unlimited).
    pub max_body_size: i64,
}

/// Upstream `apiKeyPhrase`: the message signed with the application secret to
/// derive the API token.
const API_KEY_PHRASE: &str = "api-cable";

cabby_core::toml_section!(Config, "api.Config" {
    "host" => host,
    "port" => port,
    "path" => path,
    "secret" => secret,
    "SecretBase" => secret_base,
    "cors_headers" => add_cors_headers,
    "cors_hosts" => cors_hosts,
    "max_body_size" => max_body_size,
});

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self { path: DEFAULT_API_PATH.to_owned(), max_body_size: DEFAULT_MAX_BODY_SIZE, ..Default::default() }
    }

    pub fn is_secured(&self) -> bool {
        !self.secret.is_empty() || !self.secret_base.is_empty()
    }

    /// Derives `secret` from `secret_base` unless a secret is set: the hex
    /// HMAC-SHA256 of [`API_KEY_PHRASE`] (upstream `MessageVerifier.Sign`).
    pub fn derive_secret(&mut self) -> Result<(), ApiError> {
        use hmac::{Hmac, KeyInit, Mac};

        if !self.secret.is_empty() || self.secret_base.is_empty() {
            return Ok(());
        }

        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(self.secret_base.as_bytes())
            .map_err(|e| ApiError { message: format!("failed to auto-generate authentication key for API: {e}") })?;
        mac.update(API_KEY_PHRASE.as_bytes());
        self.secret = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect();
        Ok(())
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# API server host (only used when port differs from main server)\n");
        if self.host.is_empty() {
            out.push_str("# host = \"localhost\"\n");
        } else {
            out.push_str(&format!("host = \"{}\"\n", self.host));
        }
        out.push_str("# API server port (0 = use main server port)\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# Base path for API endpoints\n");
        out.push_str(&format!("path = \"{}\"\n", self.path));
        out.push_str("# Secret token to authenticate API requests\n");
        if self.secret.is_empty() {
            out.push_str("# secret = \"\"\n");
        } else {
            out.push_str(&format!("secret = \"{}\"\n", self.secret));
        }
        out.push_str("# Enable CORS headers\n");
        out.push_str(if self.add_cors_headers { "cors_headers = true\n" } else { "# cors_headers = false\n" });
        out.push_str("# Maximum allowed size of the request body (in bytes)\n");
        out.push_str(&format!("max_body_size = {}\n", self.max_body_size));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), ApiError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| ApiError { message: e.0 })?;
        Ok(())
    }
}

/// An HTTP request as seen by a handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpRequest {
    pub method: String,
    /// Request target path (no query string in the translated tests).
    pub path: String,
    /// Header name/value pairs as sent; names match case-insensitively.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// An HTTP response produced by a handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Bearer-token authentication for API requests (upstream `api.Authenticator`).
#[derive(Debug, Clone)]
pub struct Authenticator {
    secret: String,
}

impl Authenticator {
    /// Upstream `NewAuthenticator(secret)`.
    pub fn new(secret: &str) -> Result<Self, ApiError> {
        Ok(Self { secret: secret.to_owned() })
    }

    pub fn secret(&self) -> String {
        self.secret.clone()
    }

    pub fn is_enabled(&self) -> bool {
        !self.secret.is_empty()
    }

    /// Accepts `Authorization: Bearer <secret>` (constant-time comparison), or
    /// anything when no secret is configured.
    pub fn authenticate(&self, req: &HttpRequest) -> bool {
        if self.secret.is_empty() {
            return true;
        }
        let Some(token) = req.header("Authorization").and_then(|h| h.strip_prefix("Bearer ")) else {
            return false;
        };
        constant_time_eq(token.as_bytes(), self.secret.as_bytes())
    }
}

/// Go `subtle.ConstantTimeCompare(a, b) == 1`: the time depends only on the
/// lengths, not on the contents.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl HttpRequest {
    /// First value of a header (Go `r.Header.Get`), matching names case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

impl HttpResponse {
    fn with_status(status: u16) -> Self {
        Self { status, ..Default::default() }
    }

    /// Go `w.Header().Set(name, value)`.
    fn set_header(&mut self, name: &str, value: &str) {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_owned(), value.to_owned()));
    }

    /// Go `w.Header().Add(name, value)`.
    fn add_header(&mut self, name: &str, value: &str) {
        self.headers.push((name.to_owned(), value.to_owned()));
    }
}

/// Accepts broadcast payloads (upstream `broadcast.Handler`).
pub trait BroadcastHandler: Send + Sync {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), ApiError>;
    fn handle_pubsub(&self, payload: &[u8]);
}

/// The API server (upstream `api.Server`).
///
/// Request handling is transport-independent: [`Server::serve`] routes one
/// request to the publish or presence handler. Binding a listener (upstream
/// `server.ForPort` / `server.NewServer` and `Run`) belongs to the HTTP
/// server, which mounts [`Server::serve`] under the configured path.
pub struct Server {
    conf: Config,
    broker: Arc<dyn Broker>,
    handler: Arc<dyn BroadcastHandler>,
    authenticator: Authenticator,
    /// `Some(hosts)` when CORS headers are enabled; no hosts allow any origin.
    cors_hosts: Option<Vec<String>>,
    started: AtomicBool,
    closed: AtomicBool,
    /// Upstream `l.With("context", "api")`.
    log: Logger,
}

impl Server {
    /// Upstream `NewServer(conf, broker, handler)`; derives the secret first.
    pub fn new(
        conf: &mut Config,
        broker: Arc<dyn Broker>,
        handler: Arc<dyn BroadcastHandler>,
    ) -> Result<Self, ApiError> {
        conf.derive_secret()?;
        let authenticator = Authenticator::new(&conf.secret)
            .map_err(|e| ApiError { message: format!("failed to create authenticator: {e}") })?;
        let cors_hosts = conf.add_cors_headers.then(|| {
            if conf.cors_hosts.is_empty() {
                Vec::new()
            } else {
                conf.cors_hosts.split(',').map(str::to_owned).collect()
            }
        });
        Ok(Self {
            conf: conf.clone(),
            broker,
            handler,
            authenticator,
            cors_hosts,
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            log: api_logger(),
        })
    }

    /// Registers the publish and presence handlers.
    pub fn start(&self) -> Result<(), ApiError> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Stops serving; later calls are no-ops.
    pub fn shutdown(&self) -> Result<(), ApiError> {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.started.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    pub fn authenticator(&self) -> Authenticator {
        self.authenticator.clone()
    }

    /// Routes one request through the server's handlers (upstream
    /// `server.ServeHTTP`): `<path>/publish` exactly and anything under
    /// `<path>/presence/`; everything else (or before `start`) is 404.
    pub fn serve(&self, req: &HttpRequest) -> HttpResponse {
        if !self.started.load(Ordering::SeqCst) {
            return HttpResponse::with_status(404);
        }
        let path = req.path.split(['?', '#']).next().unwrap_or_default();
        match path.strip_prefix(self.conf.path.as_str()) {
            Some("/publish") => self.publish(req),
            Some(rest) if rest.starts_with("/presence/") => self.presence(req, path),
            _ => HttpResponse::with_status(404),
        }
    }

    /// Upstream `PublishHandler`.
    fn publish(&self, req: &HttpRequest) -> HttpResponse {
        let mut resp = HttpResponse::default();
        if let Some(hosts) = &self.cors_hosts {
            write_cors_headers(&mut resp, req, hosts);
            if req.method == "OPTIONS" {
                resp.status = 200;
                return resp;
            }
        }

        resp.status = if req.method != "POST" {
            log_debug!(self.log, "invalid request method", "method" => &req.method);
            422
        } else if !self.authenticator.authenticate(req) {
            401
        } else if self.conf.max_body_size > 0 && req.body.len() as u64 > self.conf.max_body_size as u64 {
            log_debug!(self.log, "request body is too large", "limit" => self.conf.max_body_size);
            413
        } else {
            match self.handler.handle_broadcast(&req.body) {
                Ok(()) => 201,
                Err(e) => {
                    log_error!(self.log, "failed to handle broadcast", "error" => e.message);
                    501
                }
            }
        };
        resp
    }

    /// Upstream `PresenceHandler`: `GET <path>/presence/<stream>/users`.
    fn presence(&self, req: &HttpRequest, path: &str) -> HttpResponse {
        if req.method != "GET" {
            log_debug!(self.log, "invalid request method", "method" => &req.method);
            return HttpResponse::with_status(422);
        }
        if !self.authenticator.authenticate(req) {
            return HttpResponse::with_status(401);
        }
        let decoded = percent_decode(path);
        let Some(stream) = presence_users_stream(&decoded) else {
            log_debug!(self.log, "invalid path for get users", "path" => decoded);
            return HttpResponse::with_status(404);
        };
        if !self.broker.supports_presence() {
            return HttpResponse::with_status(501);
        }
        match self.broker.presence_info(&stream, Some(&PresenceInfoOptions::new())) {
            Err(e) => {
                // Upstream's key is misspelled.
                log_debug!(self.log, "failed to get presence info", "strean" => stream, "err" => e.message);
                HttpResponse::with_status(400)
            }
            Ok(info) => {
                let mut resp = HttpResponse::with_status(200);
                resp.set_header("Content-Type", "application/json");
                resp.body = cabby_core::gojson::to_vec(&info);
                resp
            }
        }
    }
}

/// The API server's logger (upstream `l.With("context", "api")`).
pub fn api_logger() -> Logger {
    Logger::default().with("context", "api")
}

/// Upstream `server.WriteCORSHeaders`.
fn write_cors_headers(resp: &mut HttpResponse, req: &HttpRequest, hosts: &[String]) {
    if hosts.is_empty() {
        resp.set_header("Access-Control-Allow-Origin", "*");
    } else {
        resp.add_header("Vary", "Origin");
        let origin = req.header("Origin").unwrap_or_default();
        let hosts: Vec<&str> = hosts.iter().map(String::as_str).collect();
        if crate::server::origin_allowed(origin, &hosts) {
            resp.set_header("Access-Control-Allow-Origin", &origin.to_lowercase());
        }
    }
    resp.set_header("Access-Control-Allow-Methods", "POST, OPTIONS");
    resp.set_header("Access-Control-Allow-Credentials", "true");
    resp.set_header(
        "Access-Control-Allow-Headers",
        "Origin, X-Requested-With, X-Request-ID, Content-Type, Accept, X-CSRF-Token, Authorization",
    );
}

/// The stream of a `.../presence/<stream>/users` path (upstream regexp
/// `/presence/([^/]+)/users$`, leftmost match).
fn presence_users_stream(path: &str) -> Option<String> {
    const PREFIX: &str = "/presence/";
    path.match_indices(PREFIX).find_map(|(i, _)| {
        let stream = path[i + PREFIX.len()..].strip_suffix("/users")?;
        (!stream.is_empty() && !stream.contains('/')).then(|| stream.to_owned())
    })
}

/// Decodes `%XX` escapes (Go matches on the decoded `URL.Path`). Invalid
/// escapes are kept as written.
fn percent_decode(path: &str) -> String {
    if !path.contains('%') {
        return path.to_owned();
    }
    let hex = |b: Option<&u8>| b.and_then(|&b| (b as char).to_digit(16));
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let (Some(h), Some(l)) = (hex(bytes.get(i + 1)), hex(bytes.get(i + 2)))
        {
            out.push((h * 16 + l) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_path_matches_the_upstream_pattern() {
        assert_eq!(Some("a".into()), presence_users_stream("/api/presence/a/users"));
        assert_eq!(Some("b".into()), presence_users_stream("/api/presence/x/presence/b/users"));
        assert_eq!(None, presence_users_stream("/api/presence//users"));
        assert_eq!(None, presence_users_stream("/api/presence/a/b/users"));
        assert_eq!(None, presence_users_stream("/api/presence/a/users/"));
        // Go matches the decoded path: an escaped slash splits the stream name.
        assert_eq!(None, presence_users_stream(&percent_decode("/api/presence/a%2Fb/users")));
        assert_eq!(Some("a b".into()), presence_users_stream(&percent_decode("/api/presence/a%20b/users")));
    }
}
