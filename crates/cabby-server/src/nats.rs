//! NATS client configuration (upstream package `nats`).
//!
//! Status: interfaces only; every behavior is `pending!`.

use cabby_core::pending;

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

impl NatsConfig {
    /// Upstream `nats.NewNATSConfig()`.
    pub fn new() -> Self {
        pending!("nats::new_nats_config")
    }

    /// Server list with credentials masked, for logs.
    pub fn filtered_servers(&self) -> String {
        pending!("nats::filtered_servers")
    }

    pub fn to_toml(&self) -> String {
        pending!("nats::to_toml")
    }

    /// Decodes TOML into `self`, keeping fields the document omits.
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), NatsError> {
        pending!("nats::decode_toml")
    }
}
