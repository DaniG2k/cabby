//! RPC configuration (upstream `rpc/config.go`).

use std::fmt;
use std::sync::Arc;

use cabby_core::toml_config::{self, string_list, toml_quote};

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

    /// The commented TOML section written by `anycable-go --print-config`. Decoded on
    /// top of the defaults, it gives back these settings (D-092).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# RPC implementation (grpc, http, or none)\n");
        out.push_str(&format!("implementation = \"{}\"\n", self.implementation));
        out.push_str("# RPC service hostname (including port, e.g., 'anycable-rpc:50051')\n");
        out.push_str(&format!("host = \"{}\"\n", self.host));
        out.push_str("# Specify HTTP headers that must be proxied to the RPC service\n");
        if self.proxy_headers.is_empty() {
            // The default is `["cookie"]`: an empty list is written as such (D-092).
            out.push_str("proxy_headers = []\n");
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
        // The default is `true`: `false` is written as such (D-092).
        out.push_str(if self.tls_verify { "tls_verify = true\n" } else { "tls_verify = false\n" });
        out.push_str("# CA root TLS certificate path\n");
        // The path or PEM contents, quoted so that it decodes back (D-092); `""` when unset.
        out.push_str(&format!("tls_root_ca_path = {}\n", toml_quote(&self.tls_root_ca)));
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
        // The default is 3000 and zero means no limit: the value is always written (D-092).
        out.push_str(&format!("command_timeout = {}\n", self.command_timeout));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn printed_and_loaded(config: &Config) -> (String, Config) {
        let printed = config.to_toml();
        let mut loaded = Config::new();
        loaded.decode_toml(&printed).unwrap_or_else(|e| panic!("{e}\n{printed}"));
        (printed, loaded)
    }

    #[test]
    fn deviation_d092_a_root_ca_loads_back() {
        let mut config = Config::new();
        config.enable_tls = false;
        config.tls_root_ca = "/etc/cabby/rpc ca \"private\".pem".to_owned();
        assert!(config.tls_enabled());

        let (printed, loaded) = printed_and_loaded(&config);

        assert!(printed.contains("tls_root_ca_path = \"/etc/cabby/rpc ca \\\"private\\\".pem\"\n"), "{printed}");
        assert!(!printed.contains("# tls_root_ca_path"), "{printed}");
        assert_eq!(loaded.tls_root_ca, config.tls_root_ca);
        assert!(loaded.tls_enabled());
        assert_eq!(loaded, config);
    }

    #[test]
    fn deviation_d092_root_ca_contents_load_back() {
        let mut config = Config::new();
        config.tls_root_ca =
            "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIU\n-----END CERTIFICATE-----\n".to_owned();

        let (printed, loaded) = printed_and_loaded(&config);

        assert!(
            printed.contains("tls_root_ca_path = \"-----BEGIN CERTIFICATE-----\\nMIIBszCCAVmgAwIBAgIU\\n"),
            "{printed}"
        );
        assert_eq!(loaded.tls_root_ca, config.tls_root_ca);
        assert!(loaded.tls_enabled());
    }

    #[test]
    fn an_unset_root_ca_prints_as_before() {
        let (printed, loaded) = printed_and_loaded(&Config::new());

        assert!(printed.contains("# CA root TLS certificate path\ntls_root_ca_path = \"\"\n"), "{printed}");
        assert!(!loaded.tls_enabled());
        assert_eq!(loaded, Config::new());
    }

    #[test]
    fn deviation_d092_values_other_than_the_defaults_load_back() {
        let mut config = Config::new();
        config.tls_verify = false;
        config.command_timeout = 0;
        config.proxy_headers = Vec::new();

        let (printed, loaded) = printed_and_loaded(&config);

        assert!(printed.contains("\ntls_verify = false\n"), "{printed}");
        assert!(printed.contains("\ncommand_timeout = 0\n"), "{printed}");
        assert!(printed.contains("\nproxy_headers = []\n"), "{printed}");
        assert_eq!(loaded, config);

        config.command_timeout = -5;
        assert_eq!(printed_and_loaded(&config).1.command_timeout, -5);
    }

    #[test]
    fn defaults_print_as_before() {
        let printed = Config::new().to_toml();

        for line in [
            "proxy_headers = [\"cookie\"]\n",
            "# proxy_cookies = [\"_session_id\"]\n",
            "# enable_tls = true\n",
            "tls_verify = true\n",
            "tls_root_ca_path = \"\"\n",
            "# secret =\n",
            "# request_timeout = 3000\n",
            "command_timeout = 3000\n",
            "# wait_timeout = 3000\n",
        ] {
            assert!(printed.contains(line), "{line:?} in\n{printed}");
        }
    }
}
