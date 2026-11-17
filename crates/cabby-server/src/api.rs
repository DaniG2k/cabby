//! HTTP management API: publish and presence endpoints (upstream package `api`).
//!
//! Requests are modeled as plain values so handlers can be exercised without a
//! socket, as upstream does with `httptest.NewRecorder` and `ServeHTTP`.

use std::sync::Arc;

use cabby_core::broker::Broker;
use cabby_core::pending;

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
    _secret: String,
}

impl Authenticator {
    /// Upstream `NewAuthenticator(secret)`.
    pub fn new(_secret: &str) -> Result<Self, ApiError> {
        pending!("api::new_authenticator")
    }

    pub fn secret(&self) -> String {
        pending!("api::authenticator_secret")
    }

    pub fn is_enabled(&self) -> bool {
        pending!("api::authenticator_is_enabled")
    }

    pub fn authenticate(&self, _req: &HttpRequest) -> bool {
        pending!("api::authenticator_authenticate")
    }
}

/// Accepts broadcast payloads (upstream `broadcast.Handler`).
pub trait BroadcastHandler: Send + Sync {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), ApiError>;
    fn handle_pubsub(&self, payload: &[u8]);
}

/// The API server (upstream `api.Server`).
pub struct Server {
    _private: (),
}

impl Server {
    /// Upstream `NewServer(conf, broker, handler)`; derives the secret first.
    pub fn new(
        _conf: &mut Config,
        _broker: Arc<dyn Broker>,
        _handler: Arc<dyn BroadcastHandler>,
    ) -> Result<Self, ApiError> {
        pending!("api::new_server")
    }

    /// Registers the publish and presence handlers.
    pub fn start(&self) -> Result<(), ApiError> {
        pending!("api::server_start")
    }

    pub fn shutdown(&self) -> Result<(), ApiError> {
        pending!("api::server_shutdown")
    }

    pub fn authenticator(&self) -> Authenticator {
        pending!("api::server_authenticator")
    }

    /// Routes one request through the server's handlers (upstream
    /// `server.ServeHTTP`).
    pub fn serve(&self, _req: &HttpRequest) -> HttpResponse {
        pending!("api::server_serve_http")
    }
}
