//! Facade for upstream package `ws`.

use serde_json::json;

use crate::{Backend, backend, bridge};

/// `ws.CheckOrigin(origins)(request)` where the request carries `origin` as its
/// `Origin` header (`None`: header absent).
pub fn check_origin(origins: &str, origin: Option<&str>) -> bool {
    match backend() {
        Backend::Rust => cabby_server::ws::check_origin(origins).allows(origin),
        Backend::Go => bridge::call("ws.CheckOrigin", json!({"Origins": origins, "Origin": origin})),
    }
}

// --- Config (ws/config_test.go) ---------------------------------------------------

use cabby_server::ws::{Config, TomlError};
use serde::{Deserialize, Serialize};

use crate::reference::common::GoParsed;

/// `ws.Config` as encoding/json renders it (exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoWsConfig {
    paths: Option<Vec<String>>,
    read_buffer_size: i64,
    write_buffer_size: i64,
    max_message_size: i64,
    write_timeout: i64,
    max_pending_size: u64,
    enable_compression: bool,
    allowed_origins: String,
}

impl From<&Config> for GoWsConfig {
    fn from(c: &Config) -> Self {
        GoWsConfig {
            paths: Some(c.paths.clone()),
            read_buffer_size: c.read_buffer_size,
            write_buffer_size: c.write_buffer_size,
            max_message_size: c.max_message_size,
            write_timeout: c.write_timeout,
            max_pending_size: c.max_pending_size,
            enable_compression: c.enable_compression,
            allowed_origins: c.allowed_origins.clone(),
        }
    }
}

impl From<GoWsConfig> for Config {
    fn from(g: GoWsConfig) -> Self {
        Config {
            paths: g.paths.unwrap_or_default(),
            read_buffer_size: g.read_buffer_size,
            write_buffer_size: g.write_buffer_size,
            max_message_size: g.max_message_size,
            write_timeout: g.write_timeout,
            max_pending_size: g.max_pending_size,
            enable_compression: g.enable_compression,
            allowed_origins: g.allowed_origins,
        }
    }
}

/// `ws.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoWsConfig>("ws.NewConfig", json!({})).into(),
    }
}

/// `conf.ToToml()`.
pub fn config_to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("ws.Config.ToToml", json!({"Config": GoWsConfig::from(c)})),
    }
}

/// `toml.Decode(s, &conf)`.
pub fn config_decode_toml(c: &mut Config, toml: &str) -> Result<(), TomlError> {
    match backend() {
        Backend::Rust => c.decode_toml(toml),
        Backend::Go => {
            let p: GoParsed<GoWsConfig> =
                bridge::call("ws.Config.DecodeToml", json!({"Config": GoWsConfig::from(&*c), "Toml": toml}));
            *c = p.result.expect("decoded config").into();
            p.error.map_or(Ok(()), |e| Err(TomlError(e)))
        }
    }
}
