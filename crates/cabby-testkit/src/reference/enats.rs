//! Facade for upstream package `enats` (embedded NATS configuration).

use cabby_server::enats::Config;
use serde::{Deserialize, Serialize};

use super::msg_shapes::{de_go_strings, go_config_decode_toml, go_config_to_toml, ser_go_strings};
use crate::{Backend, backend};

const PREFIX: &str = "enats.Config";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    enabled: bool,
    debug: bool,
    trace: bool,
    name: String,
    service_addr: String,
    cluster_addr: String,
    cluster_name: String,
    gateway_addr: String,
    gateway_advertise: String,
    #[serde(serialize_with = "ser_go_strings", deserialize_with = "de_go_strings")]
    gateways: Vec<String>,
    #[serde(serialize_with = "ser_go_strings", deserialize_with = "de_go_strings")]
    routes: Vec<String>,
    #[serde(rename = "JetStream")]
    jetstream: bool,
    store_dir: String,
    #[serde(rename = "JetStreamReadyTimeout")]
    jetstream_ready_timeout: i64,
    max_payload: i64,
}

impl From<&Config> for GoConfig {
    fn from(c: &Config) -> Self {
        GoConfig {
            enabled: c.enabled,
            debug: c.debug,
            trace: c.trace,
            name: c.name.clone(),
            service_addr: c.service_addr.clone(),
            cluster_addr: c.cluster_addr.clone(),
            cluster_name: c.cluster_name.clone(),
            gateway_addr: c.gateway_addr.clone(),
            gateway_advertise: c.gateway_advertise.clone(),
            gateways: c.gateways.clone(),
            routes: c.routes.clone(),
            jetstream: c.jetstream,
            store_dir: c.store_dir.clone(),
            jetstream_ready_timeout: c.jetstream_ready_timeout,
            max_payload: c.max_payload,
        }
    }
}

impl From<GoConfig> for Config {
    fn from(g: GoConfig) -> Self {
        Config {
            enabled: g.enabled,
            debug: g.debug,
            trace: g.trace,
            name: g.name,
            service_addr: g.service_addr,
            cluster_addr: g.cluster_addr,
            cluster_name: g.cluster_name,
            gateway_addr: g.gateway_addr,
            gateway_advertise: g.gateway_advertise,
            gateways: g.gateways,
            routes: g.routes,
            jetstream: g.jetstream,
            store_dir: g.store_dir,
            jetstream_ready_timeout: g.jetstream_ready_timeout,
            max_payload: g.max_payload,
        }
    }
}

pub fn to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => go_config_to_toml(PREFIX, &GoConfig::from(c)),
    }
}

/// `toml.Decode(toml, &c)`, returning the decoded config.
pub fn decode_toml(mut c: Config, toml: &str) -> Result<Config, String> {
    match backend() {
        Backend::Rust => c.decode_toml(toml).map(|()| c).map_err(|e| e.0),
        Backend::Go => go_config_decode_toml(PREFIX, &GoConfig::from(&c), toml).map(Into::into),
    }
}
