//! NATS client configuration (upstream package `nats`).
//!
//! Lives in `cabby-core` because the NATS broker uses it; `cabby-server`
//! re-exports it as `cabby_server::nats` for the NATS pub/sub, broadcaster and
//! embedded server.

use crate::logger::{LogValue, LogValuer};

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

    /// Server list with credentials masked, for logs.
    pub fn filtered_servers(&self) -> String {
        self.servers
            .split(',')
            .map(|server| match crate::logger::filtered_url(server.trim()).log_value() {
                LogValue::String(s) => s,
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# NATS server URLs (comma-separated)\n");
        out.push_str(&format!("servers = \"{}\"\n", self.servers));
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
