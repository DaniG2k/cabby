//! HTTP management API: publish and presence endpoints (upstream package `api`).
//!
//! Requests are modeled as plain values so handlers can be exercised without a
//! socket, as upstream does with `httptest.NewRecorder` and `ServeHTTP`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cabby_core::broker::{Broker, PresenceInfoOptions};
use cabby_core::logger::Logger;
use cabby_core::toml_config::toml_quote;
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
            out.push_str(&format!("host = {}\n", toml_quote(&self.host)));
        }
        out.push_str("# API server port (0 = use main server port)\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# Base path for API endpoints\n");
        out.push_str(&format!("path = {}\n", toml_quote(&self.path)));
        out.push_str("# Secret token to authenticate API requests\n");
        if self.secret.is_empty() {
            out.push_str("# secret = \"\"\n");
        } else {
            out.push_str(&format!("secret = {}\n", toml_quote(&self.secret)));
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
        cabby_core::utils::constant_time_eq(token.as_bytes(), self.secret.as_bytes())
    }
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
    /// `<path>/presence/`; everything else (or before `start`) is the router's
    /// `404 page not found`.
    pub fn serve(&self, req: &HttpRequest) -> HttpResponse {
        match self.check(req) {
            Check::Done(resp) => resp,
            Check::ReadBody(resp) => {
                let body = match self.max_body_size() {
                    Some(max) if req.body.len() as u64 > max => PublishBody::TooLarge,
                    _ => PublishBody::Read(&req.body),
                };
                self.finish_publish(resp, body)
            }
        }
    }

    /// The first step of [`Server::serve`] for a request whose body has not
    /// been read: everything up to the publish handler's body read (routing,
    /// CORS, method and authorization), as upstream, so an unauthorized client
    /// cannot make the server read its body.
    pub fn check(&self, req: &HttpRequest) -> Check {
        if !self.started.load(Ordering::SeqCst) {
            return Check::Done(not_found());
        }
        let path = req.path.split(['?', '#']).next().unwrap_or_default();
        match path.strip_prefix(self.conf.path.as_str()) {
            Some("/publish") => self.publish_check(req),
            Some(rest) if rest.starts_with("/presence/") => Check::Done(self.presence(req, path)),
            _ => Check::Done(not_found()),
        }
    }

    /// The body limit of publish requests (`max_body_size`; `None`: unlimited).
    pub fn max_body_size(&self) -> Option<u64> {
        u64::try_from(self.conf.max_body_size).ok().filter(|m| *m > 0)
    }

    /// Upstream `PublishHandler` up to reading the body.
    fn publish_check(&self, req: &HttpRequest) -> Check {
        let mut resp = HttpResponse::default();
        if let Some(hosts) = &self.cors_hosts {
            write_cors_headers(&mut resp, req, hosts);
            if req.method == "OPTIONS" {
                resp.status = 200;
                return Check::Done(resp);
            }
        }
        if req.method != "POST" {
            log_debug!(self.log, "invalid request method", "method" => &req.method);
            resp.status = 422;
            return Check::Done(resp);
        }
        if !self.authenticator.authenticate(req) {
            resp.status = 401;
            return Check::Done(resp);
        }
        Check::ReadBody(resp)
    }

    /// Upstream `PublishHandler` after reading the body: `resp` is the one
    /// [`Server::check`] returned in [`Check::ReadBody`]. A failed read is logged
    /// by the caller, which has the error.
    pub fn finish_publish(&self, mut resp: HttpResponse, body: PublishBody<'_>) -> HttpResponse {
        resp.status = match body {
            PublishBody::TooLarge => {
                log_debug!(self.log, "request body is too large", "limit" => self.conf.max_body_size);
                413
            }
            PublishBody::Failed => 422,
            PublishBody::Read(body) => match self.handler.handle_broadcast(body) {
                Ok(()) => 201,
                Err(e) => {
                    log_error!(self.log, "failed to handle broadcast", "error" => e.message);
                    501
                }
            },
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
            // Go logs the decoded bytes; invalid UTF-8 is converted lossily (D-121).
            log_debug!(self.log, "invalid path for get users", "path" => String::from_utf8_lossy(&decoded).into_owned());
            return HttpResponse::with_status(404);
        };
        if !self.broker.supports_presence() {
            return HttpResponse::with_status(501);
        }
        // Stream names are UTF-8: a name that is not names no stream (D-121).
        let Ok(stream) = std::str::from_utf8(stream) else {
            log_debug!(self.log, "failed to get presence info", "strean" => String::from_utf8_lossy(stream).into_owned(), "err" => "stream name is not valid UTF-8");
            return HttpResponse::with_status(400);
        };
        match self.broker.presence_info(stream, Some(&PresenceInfoOptions::new())) {
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

/// What [`Server::check`] decided before the request body is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// The final response; the body is not needed.
    Done(HttpResponse),
    /// A publish request that passed the checks: read the body (at most
    /// [`Server::max_body_size`]) and pass this response to
    /// [`Server::finish_publish`].
    ReadBody(HttpResponse),
}

/// The outcome of reading a publish request's body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishBody<'a> {
    Read(&'a [u8]),
    /// Over the limit (Go `http.MaxBytesError`): 413.
    TooLarge,
    /// The body could not be read: 422.
    Failed,
}

/// Go `http.NotFound`, as chi answers paths without a handler.
fn not_found() -> HttpResponse {
    HttpResponse {
        status: 404,
        headers: vec![
            ("Content-Type".to_owned(), "text/plain; charset=utf-8".to_owned()),
            ("X-Content-Type-Options".to_owned(), "nosniff".to_owned()),
        ],
        body: b"404 page not found\n".to_vec(),
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
/// `/presence/([^/]+)/users$`, leftmost match), as bytes: Go matches the
/// decoded path, whose bytes need not be UTF-8.
fn presence_users_stream(path: &[u8]) -> Option<&[u8]> {
    const PREFIX: &[u8] = b"/presence/";
    (0..path.len()).filter(|&i| path[i..].starts_with(PREFIX)).find_map(|i| {
        let stream = path[i + PREFIX.len()..].strip_suffix(b"/users")?;
        (!stream.is_empty() && !stream.contains(&b'/')).then_some(stream)
    })
}

/// Decodes `%XX` escapes into bytes (Go matches on the decoded `URL.Path`).
/// Invalid escapes are kept as written.
fn percent_decode(path: &str) -> Vec<u8> {
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
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_path_matches_the_upstream_pattern() {
        let stream = |path: &str| presence_users_stream(&percent_decode(path)).map(<[u8]>::to_vec);
        assert_eq!(Some(b"a".to_vec()), stream("/api/presence/a/users"));
        assert_eq!(Some(b"b".to_vec()), stream("/api/presence/x/presence/b/users"));
        assert_eq!(None, stream("/api/presence//users"));
        assert_eq!(None, stream("/api/presence/a/b/users"));
        assert_eq!(None, stream("/api/presence/a/users/"));
        // Go matches the decoded path: an escaped slash splits the stream name.
        assert_eq!(None, stream("/api/presence/a%2Fb/users"));
        assert_eq!(Some(b"a b".to_vec()), stream("/api/presence/a%20b/users"));
        // The decoded bytes, also when they are not UTF-8 (D-121).
        assert_eq!(Some(vec![0xff]), stream("/api/presence/%ff/users"));
    }

    /// Pins D-121: a presence stream that is not valid UTF-8 once decoded is
    /// answered 400 (Go reads the presence of the stream the bytes name), and
    /// after the presence support check (a broker without presence still gives
    /// 501); the encoded U+FFFD reads its stream.
    #[test]
    fn deviation_d121_a_presence_stream_that_is_not_utf8_is_answered_400() {
        use cabby_core::broker::{self as core_broker, Legacy};

        struct Noop;
        impl BroadcastHandler for Noop {
            fn handle_broadcast(&self, _: &[u8]) -> Result<(), ApiError> {
                Ok(())
            }
            fn handle_pubsub(&self, _: &[u8]) {}
        }
        struct NoSubscriber;
        impl core_broker::Broadcaster for NoSubscriber {
            fn broadcast(&self, _: &cabby_core::common::StreamMessage) {}
            fn broadcast_command(&self, _: &cabby_core::common::RemoteCommandMessage) {}
            fn subscribe(&self, _: &str) {}
            fn unsubscribe(&self, _: &str) {}
        }
        let get = |api: &Server, path: &str| {
            api.serve(&HttpRequest { method: "GET".into(), path: path.into(), ..Default::default() }).status
        };

        let memory = Arc::new(core_broker::Memory::new(
            None,
            None,
            &core_broker::Config::new(),
            Arc::new(core_broker::SystemClock),
        ));
        memory.presence_add("\u{FFFD}", "s1", "p1", serde_json::json!("x")).unwrap();
        let api = Server::new(&mut Config::new(), memory, Arc::new(Noop)).unwrap();
        api.start().unwrap();
        assert_eq!(400, get(&api, "/api/presence/%ff/users"));
        assert_eq!(400, get(&api, "/api/presence/a%C3/users"));
        assert_eq!(200, get(&api, "/api/presence/%EF%BF%BD/users"));
        assert_eq!(200, get(&api, "/api/presence/caf%C3%A9/users"));

        let legacy = Server::new(&mut Config::new(), Arc::new(Legacy::new(Arc::new(NoSubscriber))), Arc::new(Noop));
        let legacy = legacy.unwrap();
        legacy.start().unwrap();
        assert_eq!(501, get(&legacy, "/api/presence/%ff/users"));
    }
}
