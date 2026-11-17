//! RPC configuration (upstream `rpc/config.go`).

use std::fmt;
use std::sync::Arc;

use cabby_core::pending;

use super::{ClientHelper, RpcClient, RpcError};

pub const DEFAULT_RPC_HOST: &str = "localhost:50051";
/// Slightly less than the default Ruby gRPC server concurrency.
pub const DEFAULT_RPC_CONCURRENCY: i64 = 28;

/// A dialed client and its helper.
pub type Dialed = (Arc<dyn RpcClient>, Arc<dyn ClientHelper>);

/// Builds a client and its helper (upstream `Dialer`).
pub type Dialer = Arc<dyn Fn(&Config) -> Result<Dialed, RpcError> + Send + Sync>;

/// An alternative dialer (upstream `Config.DialFun`). Compares by identity.
#[derive(Clone)]
pub struct DialFun(pub Dialer);

impl fmt::Debug for DialFun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DialFun(..)")
    }
}

impl PartialEq for DialFun {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// RPC controller configuration. Durations are milliseconds, as upstream; signed
/// integers keep upstream sentinels (`wait_timeout = -1`: use the command timeout).
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// RPC instance host (`host:port`, `grpc://`, `grpc-list://a,b`, `dns:///`, `http(s)://`).
    pub host: String,
    /// HTTP headers to add to the RPC request env.
    pub proxy_headers: Vec<String>,
    /// Cookies to keep in the proxied Cookie header (empty: all).
    pub proxy_cookies: Vec<String>,
    /// Max number of simultaneous requests.
    pub concurrency: i64,
    pub enable_tls: bool,
    pub tls_verify: bool,
    /// CA root certificate path or PEM contents.
    pub tls_root_ca: String,
    pub max_recv_size: i64,
    pub max_send_size: i64,
    /// `grpc`, `http`, `none` or empty (derived from the host scheme).
    pub implementation: String,
    pub dial_fun: Option<DialFun>,
    /// Bearer secret for HTTP RPC.
    pub secret: String,
    pub request_timeout: i64,
    pub command_timeout: i64,
    pub wait_timeout: i64,
    pub http_request_timeout: i64,
    /// Application secret used to derive [`Config::secret`] for HTTP RPC.
    pub secret_base: String,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    /// Upstream `NewConfig` defaults.
    pub fn new() -> Self {
        pending!("rpc::new_config")
    }

    /// The implementation, explicit or derived from the host scheme.
    pub fn implementation(&self) -> String {
        pending!("rpc::config_impl")
    }

    pub fn tls_enabled(&self) -> bool {
        pending!("rpc::config_tls_enabled")
    }

    /// HTTP request timeout with the legacy fallbacks (ms).
    pub fn effective_http_request_timeout(&self) -> i64 {
        pending!("rpc::config_http_request_timeout")
    }

    /// The commented TOML section written by `anycable-go --print-config`.
    pub fn to_toml(&self) -> String {
        pending!("rpc::config_to_toml")
    }

    /// Decodes a TOML document on top of `self` (config files override defaults).
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), String> {
        pending!("rpc::config_decode_toml")
    }
}
