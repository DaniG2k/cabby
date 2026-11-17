//! Embedded NATS server (upstream package `enats`).
//!
//! Status: interfaces only; every behavior is `pending!`. The embedded-NATS
//! strategy is a separate milestone (SPEC.md §3); translated tests that need a
//! NATS server use an external `nats-server` for now.

use cabby_core::pending;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnatsError(pub String);

impl std::fmt::Display for EnatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EnatsError {}

/// Upstream `enats.Config`. `Default` is Go's zero value (`var c Config`), which
/// upstream tests use directly; defaults come from [`Config::new`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub enabled: bool,
    pub debug: bool,
    pub trace: bool,
    pub name: String,
    pub service_addr: String,
    pub cluster_addr: String,
    pub cluster_name: String,
    pub gateway_addr: String,
    pub gateway_advertise: String,
    pub gateways: Vec<String>,
    pub routes: Vec<String>,
    pub jetstream: bool,
    pub store_dir: String,
    /// Seconds.
    pub jetstream_ready_timeout: i64,
    /// Bytes; 0 means the server default.
    pub max_payload: i64,
}

impl Config {
    /// Upstream `enats.NewConfig()`.
    pub fn new() -> Self {
        pending!("enats::new_config")
    }

    pub fn to_toml(&self) -> String {
        pending!("enats::to_toml")
    }

    /// Decodes TOML into `self`, keeping fields the document omits.
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), EnatsError> {
        pending!("enats::decode_toml")
    }
}

/// Embedded NATS service (upstream `enats.Service`).
pub struct Service {
    _private: (),
}

impl Service {
    pub fn new(_config: &Config) -> Self {
        pending!("enats::new_service")
    }

    pub fn start(&self) -> Result<(), EnatsError> {
        pending!("enats::start")
    }

    pub fn wait_ready(&self) -> Result<(), EnatsError> {
        pending!("enats::wait_ready")
    }

    pub fn shutdown(&self) -> Result<(), EnatsError> {
        pending!("enats::shutdown")
    }
}
