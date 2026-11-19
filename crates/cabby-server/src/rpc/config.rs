//! RPC configuration (upstream `rpc/config.go`).

use std::fmt;
use std::sync::Arc;

use cabby_core::toml_config::{self, string_list};

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

// Upstream's untagged `DialFun` (a function) is not decodable: a `DialFun` key is
// reported as unknown here, where BurntSushi/toml fails with "unsupported type".
cabby_core::toml_section!(Config, "rpc.Config" {
    "host" => host,
    "proxy_headers" => proxy_headers,
    "proxy_cookies" => proxy_cookies,
    "concurrency" => concurrency,
    "enable_tls" => enable_tls,
    "tls_verify" => tls_verify,
    "tls_root_ca_path" => tls_root_ca,
    "max_recv_size" => max_recv_size,
    "max_send_size" => max_send_size,
    "implementation" => implementation,
    "secret" => secret,
    "request_timeout" => request_timeout,
    "command_timeout" => command_timeout,
    "wait_timeout" => wait_timeout,
    "http_request_timeout" => http_request_timeout,
    "SecretBase" => secret_base,
});

impl Config {
    /// Upstream `NewConfig` defaults.
    pub fn new() -> Self {
        Self {
            host: DEFAULT_RPC_HOST.to_owned(),
            proxy_headers: vec!["cookie".to_owned()],
            proxy_cookies: Vec::new(),
            concurrency: DEFAULT_RPC_CONCURRENCY,
            enable_tls: false,
            tls_verify: true,
            tls_root_ca: String::new(),
            max_recv_size: 0,
            max_send_size: 0,
            implementation: String::new(),
            dial_fun: None,
            secret: String::new(),
            request_timeout: 0,
            command_timeout: 3000,
            // Defaults to the command timeout.
            wait_timeout: -1,
            http_request_timeout: 0,
            secret_base: String::new(),
        }
    }

    /// The implementation, explicit or derived from the host scheme (upstream `Impl`).
    pub fn implementation(&self) -> String {
        if !self.implementation.is_empty() {
            return self.implementation.clone();
        }

        // Upstream matches `^([^\/]+)://.*` against the host with a default
        // `grpc://` scheme.
        let url = if self.host.contains("://") { self.host.clone() } else { format!("grpc://{}", self.host) };
        match url.find("://") {
            Some(i) if i > 0 && !url[..i].contains('/') => match &url[..i] {
                "http" | "https" => "http".to_owned(),
                _ => "grpc".to_owned(),
            },
            _ => format!("<invalid RPC host: {}>", self.host),
        }
    }

    pub fn tls_enabled(&self) -> bool {
        self.enable_tls || !self.tls_root_ca.is_empty()
    }

    /// The TLS client configuration when TLS is enabled (upstream `TLSConfig`).
    pub(crate) fn tls_config(&self, alpn: &[&[u8]]) -> Result<Option<Arc<rustls::ClientConfig>>, RpcError> {
        if !self.tls_enabled() {
            return Ok(None);
        }
        let options = crate::client_tls::TlsOptions {
            root_ca: self.tls_root_ca.clone(),
            verify: self.tls_verify,
            alpn: alpn.iter().map(|p| p.to_vec()).collect(),
        };
        crate::client_tls::client_config(&options).map(Some).map_err(RpcError::Other)
    }

    /// HTTP request timeout with the legacy fallbacks (ms).
    pub fn effective_http_request_timeout(&self) -> i64 {
        if self.http_request_timeout > 0 {
            self.http_request_timeout
        } else if self.request_timeout > 0 {
            self.request_timeout
        } else {
            // Legacy default
            3000
        }
    }

    /// The commented TOML section written by `anycable-go --print-config`.
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# RPC implementation (grpc, http, or none)\n");
        out.push_str(&format!("implementation = \"{}\"\n", self.implementation));
        out.push_str("# RPC service hostname (including port, e.g., 'anycable-rpc:50051')\n");
        out.push_str(&format!("host = \"{}\"\n", self.host));
        out.push_str("# Specify HTTP headers that must be proxied to the RPC service\n");
        if self.proxy_headers.is_empty() {
            out.push_str("# proxy_headers = [\"cookie\"]\n");
        } else {
            out.push_str(&format!("proxy_headers = {}\n", string_list(&self.proxy_headers)));
        }
        out.push_str("# Specify which cookies must be kept in the proxied Cookie header\n");
        if self.proxy_cookies.is_empty() {
            out.push_str("# proxy_cookies = [\"_session_id\"]\n");
        } else {
            out.push_str(&format!("proxy_cookies = {}\n", string_list(&self.proxy_cookies)));
        }
        out.push_str("# RPC concurrency (max number of concurrent RPC requests)\n");
        out.push_str(&format!("concurrency = {}\n", self.concurrency));
        out.push_str("# Enable client-side TLS on RPC connections\n");
        out.push_str(if self.enable_tls { "enable_tls = true\n" } else { "# enable_tls = true\n" });
        out.push_str("# Enable TLS Verify for RPC connections\n");
        out.push_str(if self.tls_verify { "tls_verify = true\n" } else { "# tls_verify = true\n" });
        out.push_str("# CA root TLS certificate path\n");
        // Upstream writes the (empty) path when unset and a placeholder when set;
        // kept as is.
        if self.tls_root_ca.is_empty() {
            out.push_str(&format!("tls_root_ca_path = \"{}\"\n", self.tls_root_ca));
        } else {
            out.push_str("# tls_root_ca_path =\n");
        }
        out.push_str("# HTTP RPC specific settings\n");
        out.push_str("# Secret for HTTP RPC authentication\n");
        if self.secret.is_empty() {
            out.push_str("# secret =\n");
        } else {
            out.push_str(&format!("secret = \"{}\"\n", self.secret));
        }
        out.push_str("# Timeout for RPC requests (in ms)\n");
        if self.request_timeout > 0 {
            out.push_str(&format!("request_timeout = {}\n", self.request_timeout));
        } else {
            out.push_str("# request_timeout = 3000\n");
        }
        out.push_str("# Total timeout for RPC commands including retries (in ms)\n");
        if self.command_timeout > 0 {
            out.push_str(&format!("command_timeout = {}\n", self.command_timeout));
        } else {
            out.push_str("# command_timeout = 3000\n");
        }
        out.push_str(
            "# Max time to wait for a free RPC slot (in ms); defaults to command_timeout, zero means no limit\n",
        );
        if self.wait_timeout >= 0 {
            out.push_str(&format!("wait_timeout = {}\n", self.wait_timeout));
        } else {
            out.push_str("# wait_timeout = 3000\n");
        }
        out.push_str("# GRPC fine-tuning\n");
        out.push_str("# Max allowed incoming message size (bytes)\n");
        out.push_str(&format!("max_recv_size = {}\n", self.max_recv_size));
        out.push_str("# Max allowed outgoing message size (bytes)\n");
        out.push_str(&format!("max_send_size = {}\n", self.max_send_size));
        out.push('\n');
        out
    }

    /// Decodes a TOML document on top of `self` (config files override defaults).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), String> {
        toml_config::decode(self, toml).map_err(|e| e.0)?;
        Ok(())
    }
}
