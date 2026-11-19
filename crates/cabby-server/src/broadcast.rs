//! Broadcasters: HTTP endpoint, Redis Streams, legacy Redis pub/sub and legacy
//! NATS (upstream package `broadcast`; interfaces in `cabby_core::broadcast`).
//!
//! Status: the HTTP broadcaster is implemented; Redis Streams and the legacy
//! Redis/NATS broadcasters are `pending!` (M5).

use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};

use cabby_core::broadcast::{BroadcastError, Broadcaster, Handler};
use cabby_core::logger::{Attr, Level, LogValue};
use cabby_core::pending;
use cabby_core::toml_config::{self, TomlSection};
use cabby_core::utils::MessageVerifier;

use crate::server::{self as http_server, HttpServer};

use crate::nats::NatsConfig;
use crate::redis::RedisConfig as RedisClientConfig;

/// Upstream `broadcast.HTTPConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpConfig {
    pub port: i64,
    pub path: String,
    /// Bearer token required on requests (empty: none).
    pub secret: String,
    /// Application secret the token is derived from when `secret` is empty
    /// (not a TOML field).
    pub secret_base: String,
    pub add_cors_headers: bool,
    /// Comma-separated CORS hosts.
    pub cors_hosts: String,
    /// Bytes; 0 disables the limit.
    pub max_body_size: i64,
}

/// Upstream `defaultHTTPPath`.
pub const DEFAULT_HTTP_PATH: &str = "/_broadcast";
/// Upstream `defaultMaxBodySize` (1 MB).
pub const DEFAULT_MAX_BODY_SIZE: i64 = 1_048_576;

fn decode(target: &mut impl TomlSection, toml: &str) -> Result<(), BroadcastError> {
    toml_config::decode(target, toml).map_err(|e| BroadcastError(e.0))?;
    Ok(())
}

cabby_core::toml_section!(HttpConfig, "broadcast.HTTPConfig" {
    "port" => port,
    "path" => path,
    "secret" => secret,
    "SecretBase" => secret_base,
    "cors_headers" => add_cors_headers,
    "cors_hosts" => cors_hosts,
    "max_body_size" => max_body_size,
});

impl HttpConfig {
    /// Upstream `broadcast.NewHTTPConfig()`.
    pub fn new() -> Self {
        Self { path: DEFAULT_HTTP_PATH.to_owned(), max_body_size: DEFAULT_MAX_BODY_SIZE, ..Default::default() }
    }

    pub fn is_secured(&self) -> bool {
        !self.secret.is_empty() || !self.secret_base.is_empty()
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# HTTP server port (can be the same as the main server port)\n");
        out.push_str(&format!("port = {}\n", self.port));
        out.push_str("# HTTP endpoint path for broadcasts\n");
        out.push_str(&format!("path = \"{}\"\n", self.path));
        out.push_str("# Secret token to authenticate broadcasting requests\n");
        if self.secret.is_empty() {
            out.push_str("# secret = \"\"\n");
        } else {
            out.push_str(&format!("secret = \"{}\"\n", self.secret));
        }
        out.push_str("# Enable CORS headers (allowed origins are used as allowed hosts)\n");
        out.push_str(if self.add_cors_headers { "cors_headers = true\n" } else { "# cors_headers = false\n" });
        out.push_str("# Maximum allowed size of the request body (in bytes)\n");
        out.push_str(&format!("max_body_size = {}\n", self.max_body_size));
        out.push('\n');
        out
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// An HTTP request as seen by the broadcast handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpRequest {
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Upstream `broadcastKeyPhrase`: the secret is derived by signing it.
const BROADCAST_KEY_PHRASE: &str = "broadcast-cable";

impl HttpRequest {
    /// First value of a header (Go `r.Header.Get`), matching names case-insensitively.
    pub fn header(&self, name: &str) -> &str {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map_or("", |(_, v)| v.as_str())
    }
}

/// A broadcast endpoint response: status and headers (CORS).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: http::HeaderMap,
}

impl HttpResponse {
    fn status(status: u16, headers: http::HeaderMap) -> Self {
        Self { status, headers }
    }
}

/// What `Prepare` derives from the configuration.
#[derive(Debug, Default)]
struct Prepared {
    /// `Bearer <secret>`, or empty when requests are not authenticated.
    auth_header: String,
    /// `Some(hosts)` when CORS is enabled (no hosts: any origin).
    cors_hosts: Option<Vec<String>>,
}

fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 2);
    all.push(Attr::new("context", LogValue::String("broadcast".into())));
    all.push(Attr::new("provider", LogValue::String("http".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

struct HttpInner {
    node: Arc<dyn Handler>,
    conf: Mutex<HttpConfig>,
    prepared: RwLock<Prepared>,
}

/// HTTP broadcast endpoint (upstream `broadcast.HTTPBroadcaster`): accepts
/// `POST <path>` on its port (the main server's when the ports match), optionally
/// authenticated with `Authorization: Bearer <secret>`.
pub struct HttpBroadcaster {
    inner: Arc<HttpInner>,
    server: Mutex<Option<Arc<HttpServer>>>,
}

impl HttpBroadcaster {
    pub fn new(node: Arc<dyn Handler>, config: &HttpConfig) -> Self {
        Self {
            inner: Arc::new(HttpInner {
                node,
                conf: Mutex::new(config.clone()),
                prepared: RwLock::new(Prepared::default()),
            }),
            server: Mutex::new(None),
        }
    }

    /// Derives the authorization token and CORS settings (upstream `Prepare`).
    pub fn prepare(&self) -> Result<(), BroadcastError> {
        let mut conf = self.inner.conf.lock().unwrap_or_else(|e| e.into_inner());
        if conf.secret.is_empty() && !conf.secret_base.is_empty() {
            conf.secret = MessageVerifier::new(&conf.secret_base).sign(BROADCAST_KEY_PHRASE.as_bytes());
            log(Level::Info, "auto-generated authorization secret from the application secret", &[]);
        }
        let auth_header = if conf.secret.is_empty() { String::new() } else { format!("Bearer {}", conf.secret) };
        let cors_hosts = conf.add_cors_headers.then(|| {
            if conf.cors_hosts.is_empty() {
                Vec::new()
            } else {
                conf.cors_hosts.split(',').map(str::to_owned).collect()
            }
        });
        *self.inner.prepared.write().unwrap_or_else(|e| e.into_inner()) = Prepared { auth_header, cors_hosts };
        Ok(())
    }

    /// Serves one request and returns the response status (upstream `Handler`).
    pub fn handle(&self, request: &HttpRequest) -> u16 {
        self.handle_full(request).status
    }

    /// [`HttpBroadcaster::handle`] with the response headers.
    pub fn handle_full(&self, request: &HttpRequest) -> HttpResponse {
        let inner = &self.inner;
        match inner.check(&request.method, |name| request.header(name)) {
            Err(resp) => resp,
            Ok(headers) => {
                let max = inner.max_body_size();
                if max.is_some_and(|max| request.body.len() as u64 > max) {
                    log(Level::Debug, "request body is too large", &[]);
                    return HttpResponse::status(413, headers);
                }
                HttpResponse::status(inner.deliver(&request.body), headers)
            }
        }
    }

    /// The endpoint as a server handler (reads the body only after the method
    /// and authorization checks, at most `max_body_size` bytes, as upstream).
    pub fn http_handler(&self) -> impl http_server::Handler {
        let inner = Arc::clone(&self.inner);
        move |req: http_server::Request| {
            let inner = Arc::clone(&inner);
            async move {
                let (parts, body) = req.into_parts();
                let header = |name: &str| parts.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("");
                let resp = match inner.check(parts.method.as_str(), header) {
                    Err(resp) => resp,
                    Ok(headers) => match http_server::read_body(body, inner.max_body_size()).await {
                        Ok(body) => HttpResponse::status(inner.deliver(&body), headers),
                        Err(http_server::BodyError::TooLarge(limit)) => {
                            log(
                                Level::Debug,
                                "request body is too large",
                                &[Attr::new("limit", LogValue::Uint(limit))],
                            );
                            HttpResponse::status(413, headers)
                        }
                        Err(http_server::BodyError::Read(_)) => {
                            log(Level::Error, "failed to read request body", &[]);
                            HttpResponse::status(422, headers)
                        }
                    },
                };
                let mut out = http_server::status_response(resp.status);
                *out.headers_mut() = resp.headers;
                out
            }
        }
    }
}

impl HttpInner {
    fn max_body_size(&self) -> Option<u64> {
        let max = self.conf.lock().unwrap_or_else(|e| e.into_inner()).max_body_size;
        u64::try_from(max).ok().filter(|m| *m > 0)
    }

    /// CORS, preflight, method and authorization checks: `Ok(headers)` to go on
    /// with the body, or the final response.
    fn check<'a>(&self, method: &str, header: impl Fn(&str) -> &'a str) -> Result<http::HeaderMap, HttpResponse> {
        let prepared = self.prepared.read().unwrap_or_else(|e| e.into_inner());
        let mut headers = http::HeaderMap::new();
        if let Some(hosts) = &prepared.cors_hosts {
            http_server::write_cors_headers(&mut headers, header("Origin"), hosts);
            if method == "OPTIONS" {
                return Err(HttpResponse::status(200, headers));
            }
        }
        if method != "POST" {
            log(Level::Debug, "invalid request method", &[Attr::new("method", LogValue::String(method.to_owned()))]);
            return Err(HttpResponse::status(422, headers));
        }
        if !prepared.auth_header.is_empty()
            && !constant_time_eq(header("Authorization").as_bytes(), prepared.auth_header.as_bytes())
        {
            return Err(HttpResponse::status(401, headers));
        }
        Ok(headers)
    }

    /// Hands the payload to the node: 201, or 501 when it fails.
    fn deliver(&self, body: &[u8]) -> u16 {
        if self.node.handle_broadcast(body).is_ok() { 201 } else { 501 }
    }
}

/// Go `subtle.ConstantTimeCompare(a, b) == 1`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Broadcaster for HttpBroadcaster {
    /// Mounts the endpoint on the server for the configured port and starts it
    /// if needed (upstream `Start`). Bind errors are returned; `done` is unused
    /// since a listening server does not stop on its own.
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        let (port, path) = {
            let conf = self.inner.conf.lock().unwrap_or_else(|e| e.into_inner());
            (conf.port, conf.path.clone())
        };
        let server = http_server::for_port(&port.to_string()).map_err(|e| BroadcastError(e.0))?;
        self.prepare()?;
        server.setup_handler(&path, self.http_handler());

        let prepared = self.inner.prepared.read().unwrap_or_else(|e| e.into_inner());
        let mut verified_via =
            if prepared.auth_header.is_empty() { "no authorization" } else { "authorization required" }.to_owned();
        if prepared.cors_hosts.is_some() {
            verified_via.push_str(", CORS enabled");
        }
        drop(prepared);
        log(Level::Info, &format!("Accept broadcast requests at {}{path} ({verified_via})", server.address()), &[]);

        server
            .start_and_announce("broadcasting HTTP server")
            .map_err(|e| BroadcastError(format!("broadcasting HTTP server at {} stopped: {e}", server.address())))?;
        *self.server.lock().unwrap_or_else(|e| e.into_inner()) = Some(server);
        Ok(())
    }

    /// Shuts the server down (upstream `Shutdown`).
    fn shutdown(&self) -> Result<(), BroadcastError> {
        if let Some(server) = self.server.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            server.shutdown();
        }
        Ok(())
    }

    fn is_fanout(&self) -> bool {
        false
    }
}

/// Upstream `broadcast.RedisConfig` (Redis Streams). `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    pub stream: String,
    pub group: String,
    pub stream_read_block_milliseconds: i64,
    pub redis: Option<RedisClientConfig>,
}

cabby_core::toml_section!(RedisConfig, "broadcast.RedisConfig" {
    "stream" => stream,
    "group" => group,
    "stream_read_block_milliseconds" => stream_read_block_milliseconds,
    "redis" => redis,
});

impl RedisConfig {
    /// Upstream `broadcast.NewRedisConfig()`.
    pub fn new() -> Self {
        Self {
            stream: "__anycable__".to_owned(),
            group: "bx".to_owned(),
            stream_read_block_milliseconds: 2000,
            redis: None,
        }
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Redis stream name for broadcasts\n");
        out.push_str(&format!("stream = \"{}\"\n", self.stream));
        out.push_str("# Stream consumer group name\n");
        out.push_str(&format!("group = \"{}\"\n", self.group));
        out.push_str("# Streams read wait time in milliseconds\n");
        out.push_str(&format!("stream_read_block_milliseconds = {}\n", self.stream_read_block_milliseconds));
        out.push('\n');
        out
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Redis Streams broadcaster (upstream `broadcast.RedisBroadcaster`).
pub struct RedisBroadcaster {
    _private: (),
}

impl RedisBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &RedisConfig) -> Self {
        pending!("broadcast::new_redis_broadcaster")
    }
}

impl Broadcaster for RedisBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::redis_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::redis_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::redis_is_fanout")
    }
}

/// Upstream `broadcast.LegacyRedisConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LegacyRedisConfig {
    pub channel: String,
    pub redis: Option<RedisClientConfig>,
}

cabby_core::toml_section!(LegacyRedisConfig, "broadcast.LegacyRedisConfig" {
    "channel" => channel,
    "redis" => redis,
});

impl LegacyRedisConfig {
    /// Upstream `broadcast.NewLegacyRedisConfig()`.
    pub fn new() -> Self {
        Self { channel: "__anycable__".to_owned(), redis: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Legacy Redis pub/sub broadcaster (upstream `broadcast.LegacyRedisBroadcaster`).
pub struct LegacyRedisBroadcaster {
    _private: (),
}

impl LegacyRedisBroadcaster {
    pub fn new(_node: Arc<dyn Handler>, _config: &LegacyRedisConfig) -> Self {
        pending!("broadcast::new_legacy_redis_broadcaster")
    }
}

impl Broadcaster for LegacyRedisBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_redis_start")
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        pending!("broadcast::legacy_redis_shutdown")
    }

    fn is_fanout(&self) -> bool {
        pending!("broadcast::legacy_redis_is_fanout")
    }
}

/// Upstream `broadcast.LegacyNATSConfig`. `Default` is Go's zero value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LegacyNatsConfig {
    pub channel: String,
    pub nats: Option<NatsConfig>,
}

cabby_core::toml_section!(LegacyNatsConfig, "broadcast.LegacyNATSConfig" {
    "channel" => channel,
    "nats" => nats,
});

impl LegacyNatsConfig {
    /// Upstream `broadcast.NewLegacyNATSConfig()`.
    pub fn new() -> Self {
        Self { channel: "__anycable__".to_owned(), nats: None }
    }

    pub fn to_toml(&self) -> String {
        format!("channel = \"{}\"\n\n", self.channel)
    }

    pub fn decode_toml(&mut self, toml: &str) -> Result<(), BroadcastError> {
        decode(self, toml)
    }
}

/// Legacy NATS broadcaster (upstream `broadcast.LegacyNATSBroadcaster`):
/// every message on the channel subject is a fan-out broadcast.
pub struct LegacyNatsBroadcaster {
    node: Arc<dyn Handler>,
    config: LegacyNatsConfig,
    conn: std::sync::Mutex<Option<cabby_core::nats::client::Connection>>,
}

impl LegacyNatsBroadcaster {
    /// Upstream `NewLegacyNATSBroadcaster(node, config)`.
    pub fn new(node: Arc<dyn Handler>, config: &LegacyNatsConfig) -> Self {
        Self { node, config: config.clone(), conn: std::sync::Mutex::new(None) }
    }
}

impl Broadcaster for LegacyNatsBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        let nats = self.config.nats.clone().ok_or_else(|| BroadcastError("NATS configuration is missing".into()))?;
        let node = self.node.clone();
        let conn = cabby_core::nats::client::Connection::open(&nats, "cabby-nats-broadcast", move |msg| {
            node.handle_pubsub(&msg.payload)
        })
        .map_err(|e| BroadcastError(e.0))?;
        if let Err(e) = conn.subscribe_sync(&self.config.channel) {
            conn.close();
            return Err(BroadcastError(e.0));
        }
        *self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(conn);
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        let conn = self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(c) = conn {
            c.close();
        }
        Ok(())
    }

    fn is_fanout(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::support::{request, start};

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Vec<u8>>>);

    impl Handler for Recorder {
        fn handle_broadcast(&self, json: &[u8]) -> Result<(), BroadcastError> {
            self.0.lock().unwrap().push(json.to_vec());
            Ok(())
        }

        fn handle_pubsub(&self, _json: &[u8]) {}
    }

    #[test]
    fn serves_broadcasts_over_http() {
        let recorder = Arc::new(Recorder::default());
        let mut config = HttpConfig::new();
        config.secret = "s3cr3t".into();
        config.add_cors_headers = true;
        config.max_body_size = 32;
        let broadcaster = HttpBroadcaster::new(Arc::clone(&recorder) as Arc<dyn Handler>, &config);
        broadcaster.prepare().unwrap();

        let server = HttpServer::new("127.0.0.1", "0", None, 0).unwrap();
        server.setup_handler(DEFAULT_HTTP_PATH, broadcaster.http_handler());
        let addr = start(&server);
        let post = |headers: &str, body: &[u8]| {
            request(addr, &format!("POST /_broadcast HTTP/1.1\r\nHost: {addr}{headers}"), body)
        };

        let ok = post("\r\nAuthorization: Bearer s3cr3t", b"{\"stream\":\"a\"}");
        assert_eq!(201, ok.status);
        assert_eq!(Some("*"), ok.header("access-control-allow-origin"));
        assert_eq!(vec![b"{\"stream\":\"a\"}".to_vec()], *recorder.0.lock().unwrap());

        assert_eq!(401, post("\r\nAuthorization: Bearer wrong", b"{}").status);
        assert_eq!(413, post("\r\nAuthorization: Bearer s3cr3t", &[b'x'; 64]).status);
        // The method is checked before the body is read.
        let get = request(addr, &format!("GET /_broadcast HTTP/1.1\r\nHost: {addr}"), &[b'x'; 64]);
        assert_eq!(422, get.status);
        let preflight = request(addr, &format!("OPTIONS /_broadcast HTTP/1.1\r\nHost: {addr}"), b"");
        assert_eq!(200, preflight.status);
        assert_eq!(Some("POST, OPTIONS"), preflight.header("access-control-allow-methods"));
        assert_eq!(1, recorder.0.lock().unwrap().len());
        server.shutdown();
    }

    #[test]
    fn derives_the_secret_from_the_application_secret() {
        let mut config = HttpConfig::new();
        config.secret_base = "qwerty".into();
        let broadcaster = HttpBroadcaster::new(Arc::new(Recorder::default()), &config);
        broadcaster.prepare().unwrap();
        let auth = "Bearer 42923a28b760e667fc92f7c6123bb07a282822b329dd2ef48e7aee7830d98485";
        let req = |headers: Vec<(String, String)>| HttpRequest { method: "POST".into(), headers, body: b"{}".to_vec() };
        assert_eq!(201, broadcaster.handle(&req(vec![("authorization".into(), auth.into())])));
        assert_eq!(401, broadcaster.handle(&req(vec![])));
        assert!(!broadcaster.is_fanout());
    }
}
