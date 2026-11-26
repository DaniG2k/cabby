//! NATS client configuration (upstream package `nats`).
//!
//! Lives in `cabby-core` because the NATS broker uses it; `cabby-server`
//! re-exports it as `cabby_server::nats` for the NATS pub/sub, broadcaster and
//! embedded server.

use crate::logger::{LogValue, LogValuer};

pub mod client;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsError(pub String);

impl std::fmt::Display for NatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NatsError {}

/// Upstream `nats.NATSConfig`. `Default` is Go's zero value; upstream defaults
/// come from [`NatsConfig::new`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NatsConfig {
    /// Comma-separated server URLs.
    pub servers: String,
    pub dont_randomize_servers: bool,
    pub max_reconnect_attempts: i64,
}

/// A server URL for logs: credentials and query values masked (upstream
/// `FilteredURL`). Callers pass a URL that may hold part of a password as
/// `crate::utils::url_list` shows it (D-093).
pub(crate) fn logged_server_url(server: &str) -> String {
    // A scheme alone (how such a URL is shown) is not a URL the filter keeps as is.
    if server.ends_with("://") {
        return server.to_owned();
    }
    match crate::logger::filtered_url(server).log_value() {
        LogValue::String(s) => s,
        other => other.to_string(),
    }
}

/// The NATS client's default server URL (nats.go `DefaultURL`).
pub const DEFAULT_URL: &str = "nats://127.0.0.1:4222";

crate::toml_section!(NatsConfig, "nats.NATSConfig" {
    "servers" => servers,
    "dont_randomize_servers" => dont_randomize_servers,
    "max_reconnect_attempts" => max_reconnect_attempts,
});

impl NatsConfig {
    /// Upstream `nats.NewNATSConfig()`.
    pub fn new() -> Self {
        Self { servers: DEFAULT_URL.to_owned(), dont_randomize_servers: false, max_reconnect_attempts: 5 }
    }

    /// Server list with credentials masked, for logs. A URL that may hold part
    /// of a password is shown as in error texts first (D-093).
    pub fn filtered_servers(&self) -> String {
        crate::utils::url_list(&self.servers)
            .into_iter()
            .map(|(server, shown)| match shown {
                Some(shown) => logged_server_url(shown.trim()),
                // Without a scheme, the URL filter reads the user name as one and
                // shows the rest as is (D-093).
                None if !server.contains("://") && server.contains('@') => {
                    logged_server_url(&crate::utils::redact_refused_url(server.trim()))
                }
                None => logged_server_url(server.trim()),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# NATS server URLs (comma-separated)\n");
        out.push_str(&format!("servers = {}\n", crate::toml_config::toml_quote(&self.servers)));
        out.push_str("# Don't randomize servers during connection\n");
        out.push_str(if self.dont_randomize_servers {
            "dont_randomize_servers = true\n"
        } else {
            "# dont_randomize_servers = true\n"
        });
        out.push_str("# Max number of reconnect attempts\n");
        out.push_str(&format!("max_reconnect_attempts = {}\n", self.max_reconnect_attempts));
        out.push('\n');
        out
    }

    /// Decodes TOML into `self`, keeping fields the document omits.
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), NatsError> {
        crate::toml_config::decode(self, toml).map_err(|e| NatsError(e.0))?;
        Ok(())
    }
}
