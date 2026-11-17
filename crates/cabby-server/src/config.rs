//! Main application configuration (upstream package `config`).
//!
//! Only the settings exercised by translated tests are modelled so far. Sections
//! owned by other upstream packages (redis, metrics, sse, ds, node, broadcast,
//! enats, broker, rpc) are minimal views here.
// TODO(merge): replace the section views with those packages' config types.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use cabby_core::pending;

/// Process environment passed explicitly (presets and CLI read `FLY_*`, `HEROKU_*`,
/// `PORT` and `ANYCABLE_*` variables), so tests never mutate the global environment.
pub type EnvMap = BTreeMap<String, String>;

/// `[redis]` (upstream `redis.RedisConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisConfig {
    pub url: String,
}

/// `[metrics]` (upstream `metrics.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetricsConfig {
    /// `http_path`
    pub http: String,
    pub tags: HashMap<String, String>,
}

/// `[sse]` (upstream `sse.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseConfig {
    pub enabled: bool,
    pub comment_pings: bool,
    pub ping_interval: i64,
}

/// `[ds]` (upstream `ds.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DsConfig {
    pub sse_ping_interval: i64,
}

/// `[app]` (upstream `node.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppConfig {
    pub ping_interval: i64,
}

/// `[http_broadcast]` (upstream `broadcast.HTTPConfig`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpBroadcastConfig {
    pub port: i64,
}

/// `[embedded_nats]` (upstream `enats.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EmbeddedNatsConfig {
    pub enabled: bool,
    pub name: String,
    pub service_addr: String,
    pub cluster_addr: String,
    pub cluster_name: String,
    pub routes: Vec<String>,
}

/// `[broker]` (upstream `broker.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BrokerConfig {
    pub adapter: String,
}

/// `[rpc]` (upstream `rpc.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RpcConfig {
    pub host: String,
    pub implementation: String,
}

/// Upstream `config.Config`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub secret: String,
    pub broadcast_adapters: Vec<String>,
    pub pubsub_adapter: String,
    /// `presets`: `None` means "detect from the environment" (upstream nil slice),
    /// `Some(vec![])` disables presets explicitly.
    pub user_presets: Option<Vec<String>>,
    pub disconnector_disabled: bool,
    pub log: cabby_core::logger::Config,
    pub server: crate::server::Config,
    pub app: AppConfig,
    pub ws: crate::ws::Config,
    pub rpc: RpcConfig,
    pub broker: BrokerConfig,
    pub redis: RedisConfig,
    pub http_broadcast: HttpBroadcastConfig,
    pub metrics: MetricsConfig,
    pub embedded_nats: EmbeddedNatsConfig,
    pub sse: SseConfig,
    pub ds: DsConfig,
    pub config_file_path: Option<PathBuf>,
    /// Configuration file keys that match no setting (dotted paths).
    pub unknown_config_keys: Vec<String>,
}

/// A configuration failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

impl Config {
    /// Defaults (upstream `config.NewConfig`).
    pub fn new() -> Self {
        pending!("config::new_config")
    }

    /// Decodes `config_file_path` over the current values and records unknown keys
    /// (upstream `Config.LoadFromFile`).
    pub fn load_from_file(&mut self) -> Result<(), ConfigError> {
        pending!("config::load_from_file")
    }

    /// Renders the full annotated configuration file (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("config::to_toml")
    }

    /// The presets to apply: the user's, or those detected from `env` (upstream `Presets`).
    pub fn presets(&self, _env: &EnvMap) -> Vec<String> {
        pending!("config::presets")
    }

    /// Applies the presets (upstream `LoadPresets`).
    pub fn load_presets(&mut self, _env: &EnvMap) -> Result<(), ConfigError> {
        pending!("config::load_presets")
    }
}
