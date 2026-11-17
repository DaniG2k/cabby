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

impl Config {
    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        pending!("api::new_config")
    }

    pub fn is_secured(&self) -> bool {
        pending!("api::config_is_secured")
    }

    /// Derives `secret` from `secret_base` unless a secret is set.
    pub fn derive_secret(&mut self) -> Result<(), ApiError> {
        pending!("api::config_derive_secret")
    }

    pub fn to_toml(&self) -> String {
        pending!("api::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, _toml: &str) -> Result<(), ApiError> {
        pending!("api::config_merge_toml")
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
