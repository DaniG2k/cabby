//! Facade for upstream package `nats` (client configuration).

use cabby_server::nats::NatsConfig;
use serde_json::json;

use super::msg_shapes::{GoNatsConfig, go_config_decode_toml, go_config_new, go_config_to_toml};
use crate::{Backend, backend, bridge};

const PREFIX: &str = "nats.NATSConfig";

/// Upstream `nats.NewNATSConfig()`.
pub fn new_nats_config() -> NatsConfig {
    match backend() {
        Backend::Rust => NatsConfig::new(),
        Backend::Go => go_config_new::<GoNatsConfig>(PREFIX).into(),
    }
}

pub fn to_toml(c: &NatsConfig) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => go_config_to_toml(PREFIX, &GoNatsConfig::from(c)),
    }
}

/// `toml.Decode(toml, &c)`, returning the decoded config.
pub fn decode_toml(mut c: NatsConfig, toml: &str) -> Result<NatsConfig, String> {
    match backend() {
        Backend::Rust => c.decode_toml(toml).map(|()| c).map_err(|e| e.0),
        Backend::Go => go_config_decode_toml(PREFIX, &GoNatsConfig::from(&c), toml).map(Into::into),
    }
}

pub fn filtered_servers(c: &NatsConfig) -> String {
    match backend() {
        Backend::Rust => c.filtered_servers(),
        Backend::Go => bridge::call("nats.NATSConfigFilteredServers", json!({"Config": GoNatsConfig::from(c)})),
    }
}
